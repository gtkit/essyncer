package essyncer

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	json "github.com/gtkit/json"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type DocumentInspectResult struct {
	Model      string `json:"model"`
	Table      string `json:"table"`
	Alias      string `json:"alias"`
	DocumentID string `json:"document_id"`

	DBFound      bool   `json:"db_found"`
	ESFound      bool   `json:"es_found"`
	OutboxPending int64 `json:"outbox_pending"`
	OutboxDead    int64 `json:"outbox_dead"`
	LastDeadError string `json:"last_dead_error"`
}

func (s *Syncer) EnqueueDocumentCreate(ctx context.Context, model Syncable, primaryKey string, unscoped bool) error {
	return s.enqueueDocumentFromDB(ctx, model, primaryKey, actionIndex, unscoped)
}

func (s *Syncer) EnqueueDocumentUpdate(ctx context.Context, model Syncable, primaryKey string, unscoped bool) error {
	return s.enqueueDocumentFromDB(ctx, model, primaryKey, actionUpdate, unscoped)
}

func (s *Syncer) EnqueueDocumentDelete(ctx context.Context, model Syncable, primaryKey, documentID string, unscoped bool) error {
	entry, modelProto, err := s.requireModelEntry(model)
	if err != nil {
		return err
	}

	docID := strings.TrimSpace(documentID)
	if docID == "" {
		if strings.TrimSpace(primaryKey) == "" {
			return fmt.Errorf("enqueue document delete: primary key or document id is required")
		}
		loaded, err := s.loadSyncableByPrimaryKey(ctx, modelProto, strings.TrimSpace(primaryKey), unscoped)
		if err != nil {
			return err
		}
		docID = loaded.GetID()
	}

	return s.persistSyncEvents(ctx, nil, []syncEvent{{
		source:    failureSourceOps,
		tableName: entry.tableName,
		action:    actionDelete,
		indexName: entry.indexName,
		docID:     docID,
	}})
}

func (s *Syncer) InspectDocument(ctx context.Context, model Syncable, primaryKey, documentID string, unscoped bool) (*DocumentInspectResult, error) {
	entry, modelProto, err := s.requireModelEntry(model)
	if err != nil {
		return nil, err
	}

	result := &DocumentInspectResult{
		Model: entry.modelType.Name(),
		Table: entry.tableName,
		Alias: entry.indexName,
	}

	docID := strings.TrimSpace(documentID)
	if strings.TrimSpace(primaryKey) != "" {
		loaded, err := s.loadSyncableByPrimaryKey(ctx, modelProto, strings.TrimSpace(primaryKey), unscoped)
		if err == nil {
			result.DBFound = true
			docID = loaded.GetID()
		} else if err != gorm.ErrRecordNotFound {
			return nil, err
		}
	}
	if docID == "" {
		return nil, fmt.Errorf("inspect document: primary key or document id is required")
	}
	result.DocumentID = docID

	resp, err := s.es.Get(entry.indexName, docID, s.es.Get.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("inspect document es get: %w", err)
	}
	func() {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode == 404 {
			return
		}
		if resp.IsError() {
			err = fmt.Errorf("inspect document es get: %s", resp.Status())
			return
		}
		var payload struct {
			Found bool `json:"found"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&payload); decodeErr != nil {
			err = fmt.Errorf("inspect document decode es response: %w", decodeErr)
			return
		}
		result.ESFound = payload.Found
	}()
	if err != nil {
		return nil, err
	}

	if findErr := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("index_alias = ? AND document_id = ? AND status = ?", entry.indexName, docID, OutboxStatusPending).
		Count(&result.OutboxPending).Error; findErr != nil {
		return nil, fmt.Errorf("inspect document pending outbox: %w", findErr)
	}
	if findErr := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("index_alias = ? AND document_id = ? AND status = ?", entry.indexName, docID, OutboxStatusDead).
		Count(&result.OutboxDead).Error; findErr != nil {
		return nil, fmt.Errorf("inspect document dead outbox: %w", findErr)
	}
	var dead OutboxEvent
	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("index_alias = ? AND document_id = ? AND status = ?", entry.indexName, docID, OutboxStatusDead).
		Order("id DESC").
		Limit(1).
		Take(&dead).Error; err == nil {
		result.LastDeadError = dead.LastError
	} else if err != nil && err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("inspect document dead outbox detail: %w", err)
	}

	return result, nil
}

func (s *Syncer) enqueueDocumentFromDB(ctx context.Context, model Syncable, primaryKey string, action actionType, unscoped bool) error {
	entry, modelProto, err := s.requireModelEntry(model)
	if err != nil {
		return err
	}
	loaded, err := s.loadSyncableByPrimaryKey(ctx, modelProto, strings.TrimSpace(primaryKey), unscoped)
	if err != nil {
		return err
	}

	return s.persistSyncEvents(ctx, nil, []syncEvent{{
		source:    failureSourceOps,
		tableName: entry.tableName,
		action:    action,
		indexName: entry.indexName,
		docID:     loaded.GetID(),
		doc:       loaded,
	}})
}

func (s *Syncer) requireModelEntry(model Syncable) (*modelEntry, Syncable, error) {
	if model == nil {
		return nil, nil, fmt.Errorf("model is required")
	}
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return nil, nil, fmt.Errorf("essyncer: model %s not registered", getTableName(s.db, model))
	}
	return entry, model, nil
}

func (s *Syncer) loadSyncableByPrimaryKey(ctx context.Context, model Syncable, raw string, unscoped bool) (Syncable, error) {
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return nil, fmt.Errorf("essyncer: model %s not registered", getTableName(s.db, model))
	}
	if entry.schema == nil || len(entry.schema.PrimaryFields) != 1 {
		return nil, fmt.Errorf("essyncer: model %s requires exactly one primary key for document ops", entry.tableName)
	}
	field := entry.schema.PrimaryFields[0]
	value, err := parsePrimaryKeyValue(field, raw)
	if err != nil {
		return nil, fmt.Errorf("essyncer: parse primary key for %s: %w", entry.tableName, err)
	}

	modelPtr := reflect.New(entry.modelType).Interface()
	query := s.db.WithContext(ctx)
	if unscoped {
		query = query.Unscoped()
	}
	if err := query.Where(clause.Eq{Column: clause.Column{Name: field.DBName}, Value: value}).Take(modelPtr).Error; err != nil {
		return nil, err
	}
	syncable, ok := modelPtr.(Syncable)
	if !ok {
		return nil, fmt.Errorf("essyncer: model %s does not implement Syncable", entry.tableName)
	}
	return syncable, nil
}

func parsePrimaryKeyValue(field *schema.Field, raw string) (any, error) {
	if field == nil {
		return nil, fmt.Errorf("missing primary key field")
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, fmt.Errorf("empty primary key")
	}
	switch field.IndirectFieldType.Kind() {
	case reflect.String:
		return value, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	default:
		return nil, fmt.Errorf("unsupported primary key type %s", field.IndirectFieldType.String())
	}
}
