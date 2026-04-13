package essyncer

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm"
)

const (
	cbAfterCreate       = "essyncer:after_create"
	cbAfterUpdate       = "essyncer:after_update"
	cbAfterDelete       = "essyncer:after_delete"
	cbAfterCreateCommit = "essyncer:after_create_commit"
	cbAfterUpdateCommit = "essyncer:after_update_commit"
	cbAfterDeleteCommit = "essyncer:after_delete_commit"
)

func (s *Syncer) EnableAutoSync(db *gorm.DB, models ...Syncable) error {
	if len(models) == 0 {
		s.registry.setAutoSyncAll(true)
	} else {
		s.registry.setAutoSyncAll(false)
		if err := s.registry.setAutoSyncForModels(db, true, models...); err != nil {
			return err
		}
	}

	s.removeAutoSyncCallbacks(db)

	if err := db.Callback().Create().Before("gorm:after_create").Register(cbAfterCreate, s.afterCreate); err != nil {
		return fmt.Errorf("register create callback: %w", err)
	}
	if err := db.Callback().Create().After("gorm:commit_or_rollback_transaction").Register(cbAfterCreateCommit, s.afterCommit); err != nil {
		return fmt.Errorf("register create commit callback: %w", err)
	}
	if err := db.Callback().Update().Before("gorm:after_update").Register(cbAfterUpdate, s.afterUpdate); err != nil {
		return fmt.Errorf("register update callback: %w", err)
	}
	if err := db.Callback().Update().After("gorm:commit_or_rollback_transaction").Register(cbAfterUpdateCommit, s.afterCommit); err != nil {
		return fmt.Errorf("register update commit callback: %w", err)
	}
	if err := db.Callback().Delete().Before("gorm:after_delete").Register(cbAfterDelete, s.afterDelete); err != nil {
		return fmt.Errorf("register delete callback: %w", err)
	}
	if err := db.Callback().Delete().After("gorm:commit_or_rollback_transaction").Register(cbAfterDeleteCommit, s.afterCommit); err != nil {
		return fmt.Errorf("register delete commit callback: %w", err)
	}
	return nil
}

func (s *Syncer) DisableAutoSync(db *gorm.DB, models ...Syncable) error {
	if len(models) == 0 {
		s.registry.setAutoSyncAll(false)
		s.removeAutoSyncCallbacks(db)
		return nil
	}

	return s.registry.setAutoSyncForModels(db, false, models...)
}

func (s *Syncer) removeAutoSyncCallbacks(db *gorm.DB) {
	_ = db.Callback().Create().Remove(cbAfterCreate)
	_ = db.Callback().Create().Remove(cbAfterCreateCommit)
	_ = db.Callback().Update().Remove(cbAfterUpdate)
	_ = db.Callback().Update().Remove(cbAfterUpdateCommit)
	_ = db.Callback().Delete().Remove(cbAfterDelete)
	_ = db.Callback().Delete().Remove(cbAfterDeleteCommit)
}

func (s *Syncer) afterCreate(db *gorm.DB) {
	if db.Error != nil || db.Statement == nil {
		return
	}
	models, entry := s.resolveModels(db)
	if entry == nil || !entry.autoSync {
		return
	}
	events := make([]syncEvent, 0, len(models))
	for _, m := range models {
		if syncable, ok := m.(Syncable); ok {
			events = append(events, syncEvent{
				source:    failureSourceCallback,
				tableName: entry.tableName,
				action:    actionIndex,
				indexName: entry.indexName,
				docID:     syncable.GetID(),
				doc:       m,
			})
		}
	}
	s.enqueueEvents(db, events...)
}

func (s *Syncer) afterUpdate(db *gorm.DB) {
	if db.Error != nil || db.Statement == nil {
		return
	}
	models, entry := s.resolveModels(db)
	if entry == nil || !entry.autoSync {
		return
	}

	changedFields := map[string]any(nil)
	if !entry.fullDocUpdate {
		changedFields = extractChangedFields(db)
	}

	events := make([]syncEvent, 0, len(models))
	for _, m := range models {
		syncable, ok := m.(Syncable)
		if !ok {
			continue
		}
		if entry.fullDocUpdate {
			events = append(events, syncEvent{
				source:    failureSourceCallback,
				tableName: entry.tableName,
				action:    actionIndex,
				indexName: entry.indexName,
				docID:     syncable.GetID(),
				doc:       m,
			})
		} else {
			if len(changedFields) == 0 {
				events = append(events, syncEvent{
					source:    failureSourceCallback,
					tableName: entry.tableName,
					action:    actionIndex,
					indexName: entry.indexName,
					docID:     syncable.GetID(),
					doc:       m,
				})
				continue
			}
			events = append(events, syncEvent{
				source:    failureSourceCallback,
				tableName: entry.tableName,
				action:    actionUpdate,
				indexName: entry.indexName,
				docID:     syncable.GetID(),
				doc:       changedFields,
			})
		}
	}
	s.enqueueEvents(db, events...)
}

func (s *Syncer) afterDelete(db *gorm.DB) {
	if db.Error != nil || db.Statement == nil {
		return
	}
	models, entry := s.resolveModels(db)
	if entry == nil || !entry.autoSync {
		return
	}
	isUnscoped := db.Statement.Unscoped
	deletedAt := time.Now()

	events := make([]syncEvent, 0, len(models))
	for _, m := range models {
		syncable, ok := m.(Syncable)
		if !ok {
			continue
		}
		if entry.hasSoftDelete && !isUnscoped {
			switch entry.softDeleteMode {
			case SoftDeleteModeUpdate:
				events = append(events, syncEvent{
					source:    failureSourceCallback,
					tableName: entry.tableName,
					action:    actionUpdate,
					indexName: entry.indexName,
					docID:     syncable.GetID(),
					doc:       map[string]any{"deleted_at": deletedAt},
				})
			case SoftDeleteModeDelete:
				events = append(events, syncEvent{
					source:    failureSourceCallback,
					tableName: entry.tableName,
					action:    actionDelete,
					indexName: entry.indexName,
					docID:     syncable.GetID(),
				})
			}
		} else {
			events = append(events, syncEvent{
				source:    failureSourceCallback,
				tableName: entry.tableName,
				action:    actionDelete,
				indexName: entry.indexName,
				docID:     syncable.GetID(),
			})
		}
	}
	s.enqueueEvents(db, events...)
}

func (s *Syncer) resolveModels(db *gorm.DB) ([]any, *modelEntry) {
	if db.Statement == nil {
		return nil, nil
	}

	entry := s.resolveModelEntry(db)
	if entry == nil {
		return nil, nil
	}

	return resolveModelCandidates(db.Statement.Context, entry,
		db.Statement.ReflectValue,
		reflect.ValueOf(db.Statement.Model),
		reflect.ValueOf(db.Statement.Dest),
	), entry
}

func (s *Syncer) resolveModelEntry(db *gorm.DB) *modelEntry {
	if db.Statement == nil {
		return nil
	}
	if db.Statement.Schema != nil {
		if entry, ok := s.registry.get(db.Statement.Schema.Table); ok {
			return entry
		}
	}
	if db.Statement.Table != "" {
		if entry, ok := s.registry.get(db.Statement.Table); ok {
			return entry
		}
	}
	if db.Statement.Model == nil {
		return nil
	}

	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(db.Statement.Model); err != nil || stmt.Schema == nil {
		return nil
	}
	entry, _ := s.registry.get(stmt.Schema.Table)
	return entry
}

func resolveModelCandidates(ctx context.Context, entry *modelEntry, candidates ...reflect.Value) []any {
	var fallback []any

	for _, candidate := range candidates {
		models := extractModels(candidate)
		if len(models) == 0 {
			continue
		}
		if fallback == nil {
			fallback = models
		}
		if entry != nil && modelsHavePrimaryIdentity(ctx, entry, models) {
			return models
		}
	}

	return fallback
}

func modelsHavePrimaryIdentity(ctx context.Context, entry *modelEntry, models []any) bool {
	if entry == nil || entry.schema == nil || len(entry.schema.PrimaryFields) == 0 {
		return false
	}

	for _, model := range models {
		value := reflect.ValueOf(model)
		for value.Kind() == reflect.Interface || value.Kind() == reflect.Ptr {
			if value.IsNil() {
				return false
			}
			value = value.Elem()
		}
		for _, field := range entry.schema.PrimaryFields {
			fieldValue, zero := field.ValueOf(ctx, value)
			if zero || fieldValue == nil {
				return false
			}
		}
	}

	return true
}

func extractModels(v reflect.Value) []any {
	if !v.IsValid() {
		return nil
	}
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		models := make([]any, 0, v.Len())
		for i := range v.Len() {
			elem := v.Index(i)
			for elem.Kind() == reflect.Interface {
				elem = elem.Elem()
			}
			if elem.Kind() == reflect.Ptr {
				if !elem.IsNil() {
					models = append(models, elem.Interface())
				}
				continue
			}
			if elem.CanAddr() {
				models = append(models, elem.Addr().Interface())
			}
		}
		return models
	case reflect.Struct:
		if v.CanAddr() {
			return []any{v.Addr().Interface()}
		}
	}
	return nil
}

func extractChangedFields(db *gorm.DB) map[string]any {
	if db.Statement == nil {
		return nil
	}
	if dest, ok := db.Statement.Dest.(map[string]any); ok {
		changed := make(map[string]any, len(dest))
		for k, v := range dest {
			changed[k] = v
		}
		return changed
	}
	if db.Statement.Schema == nil {
		return nil
	}
	changed := make(map[string]any)
	destVal := reflect.ValueOf(db.Statement.Dest)
	if destVal.Kind() == reflect.Ptr {
		destVal = destVal.Elem()
	}
	for _, field := range db.Statement.Schema.Fields {
		if db.Statement.Changed(field.Name) {
			val, _ := field.ValueOf(db.Statement.Context, destVal)
			changed[field.DBName] = val
		}
	}
	return changed
}
