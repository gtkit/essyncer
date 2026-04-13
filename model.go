package essyncer

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Syncable 是需要同步到 ES 的 GORM 模型必须实现的接口。
type Syncable interface {
	GetID() string
}

type modelEntry struct {
	modelType      reflect.Type
	schema         *schema.Schema
	tableName      string
	indexName      string
	mapping        json.RawMessage
	optionErr      error
	batchSize      int
	autoSync       bool
	softDeleteMode SoftDeleteMode
	fullDocUpdate  bool
	hasSoftDelete  bool
	fullSyncScan   FullSyncScanStrategy
}

type modelRegistry struct {
	entries sync.Map
}

func newModelRegistry() *modelRegistry { return &modelRegistry{} }

type fullSyncKey struct {
	columnName string
	field      *schema.Field
	unsigned   bool
}

func (r *modelRegistry) register(db *gorm.DB, model Syncable, defaultBatchSize int, opts ...RegisterOption) error {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return fmt.Errorf("essyncer: parse model: %w", err)
	}
	tableName := stmt.Schema.Table
	entry := &modelEntry{
		modelType:      reflect.TypeOf(model).Elem(),
		schema:         stmt.Schema,
		tableName:      tableName,
		indexName:      tableName,
		batchSize:      defaultBatchSize,
		autoSync:       true,
		softDeleteMode: SoftDeleteModeUpdate,
		hasSoftDelete:  hasSoftDeleteField(model),
		fullSyncScan:   newDefaultFullSyncScanStrategy(stmt.Schema),
	}
	for _, opt := range opts {
		opt(entry)
	}
	if entry.optionErr != nil {
		return fmt.Errorf("essyncer: register %s: %w", tableName, entry.optionErr)
	}
	r.entries.Store(tableName, entry)
	return nil
}

func (r *modelRegistry) unregister(db *gorm.DB, model Syncable) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return
	}
	r.entries.Delete(stmt.Schema.Table)
}

func (r *modelRegistry) get(tableName string) (*modelEntry, bool) {
	v, ok := r.entries.Load(tableName)
	if !ok {
		return nil, false
	}
	entry, ok := v.(*modelEntry)
	return entry, ok
}

func (r *modelRegistry) getByModel(db *gorm.DB, model Syncable) (*modelEntry, bool) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return nil, false
	}
	return r.get(stmt.Schema.Table)
}

func (r *modelRegistry) allEntries() []*modelEntry {
	var entries []*modelEntry
	r.entries.Range(func(_, value any) bool {
		if entry, ok := value.(*modelEntry); ok {
			entries = append(entries, entry)
		}
		return true
	})
	return entries
}

func (r *modelRegistry) forEach(fn func(string, *modelEntry) bool) {
	r.entries.Range(func(key, value any) bool {
		tableName, ok := key.(string)
		if !ok {
			return true
		}
		entry, ok := value.(*modelEntry)
		if !ok {
			return true
		}
		return fn(tableName, entry)
	})
}

func (r *modelRegistry) setAutoSyncAll(enabled bool) {
	r.entries.Range(func(_, value any) bool {
		if entry, ok := value.(*modelEntry); ok {
			entry.autoSync = enabled
		}
		return true
	})
}

func (r *modelRegistry) setAutoSyncForModels(db *gorm.DB, enabled bool, models ...Syncable) error {
	for _, model := range models {
		entry, ok := r.getByModel(db, model)
		if !ok {
			return fmt.Errorf("essyncer: model %s not registered", getTableName(db, model))
		}
		entry.autoSync = enabled
	}
	return nil
}

func (r *modelRegistry) entriesForModels(db *gorm.DB, models ...Syncable) ([]*modelEntry, error) {
	if len(models) == 0 {
		return r.allEntries(), nil
	}

	entries := make([]*modelEntry, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		entry, ok := r.getByModel(db, model)
		if !ok {
			return nil, fmt.Errorf("essyncer: model %s not registered", getTableName(db, model))
		}
		if _, ok := seen[entry.tableName]; ok {
			continue
		}
		seen[entry.tableName] = struct{}{}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (e *modelEntry) fullSyncInfo() FullSyncModelInfo {
	return FullSyncModelInfo{
		TableName:     e.tableName,
		IndexAlias:    e.indexName,
		HasSoftDelete: e.hasSoftDelete,
	}
}

func newFullSyncKey(s *schema.Schema) (*fullSyncKey, error) {
	if len(s.PrimaryFields) != 1 {
		return nil, fmt.Errorf("requires exactly one integer primary key, got %d primary key fields", len(s.PrimaryFields))
	}

	field := s.PrimaryFields[0]
	switch field.IndirectFieldType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &fullSyncKey{
			columnName: cmp.Or(field.DBName, field.Name),
			field:      field,
		}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &fullSyncKey{
			columnName: cmp.Or(field.DBName, field.Name),
			field:      field,
			unsigned:   true,
		}, nil
	default:
		return nil, fmt.Errorf(
			"requires exactly one integer primary key, got %s (%s)",
			field.Name,
			field.IndirectFieldType.String(),
		)
	}
}

func newDefaultFullSyncScanStrategy(s *schema.Schema) FullSyncScanStrategy {
	key, err := newFullSyncKey(s)
	if err != nil {
		return unsupportedFullSyncScanStrategy{err: err}
	}
	return integerPrimaryKeyScanStrategy{key: key}
}

func (e *modelEntry) fullSyncCursor(startID int64) (FullSyncScanCursor, error) {
	if e.fullSyncScan == nil {
		return nil, fmt.Errorf("essyncer: full sync table %s has no scan strategy", e.tableName)
	}
	cursor, err := e.fullSyncScan.Prepare(e.fullSyncInfo(), startID)
	if err != nil {
		return nil, fmt.Errorf("essyncer: prepare full sync cursor for %s: %w", e.tableName, err)
	}
	return cursor, nil
}

func (k *fullSyncKey) valueOf(ctx context.Context, v reflect.Value) (any, error) {
	value, _ := k.field.ValueOf(ctx, v)
	if value == nil {
		return nil, fmt.Errorf("primary key %s is nil", k.columnName)
	}

	fieldValue := reflect.ValueOf(value)
	switch fieldValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fieldValue.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fieldValue.Uint(), nil
	default:
		return nil, fmt.Errorf("primary key %s is not an integer (%T)", k.columnName, value)
	}
}

// --- 反射工具 ---

func hasSoftDeleteField(model any) bool {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	return containsType(t, reflect.TypeOf(gorm.DeletedAt{}))
}

func containsType(t, target reflect.Type) bool {
	for i := range t.NumField() {
		field := t.Field(i)
		if field.Type == target {
			return true
		}
		if field.Anonymous {
			ft := field.Type
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct && containsType(ft, target) {
				return true
			}
		}
	}
	return false
}

func newModelSlice(entry *modelEntry) any {
	return reflect.New(reflect.SliceOf(entry.modelType)).Interface()
}

func getTableName(db *gorm.DB, model any) string {
	if tabler, ok := model.(schema.Tabler); ok {
		return tabler.TableName()
	}
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return ""
	}
	return stmt.Schema.Table
}

func (e *modelEntry) newManagedIndexName(kind string, now time.Time) string {
	return fmt.Sprintf("%s__%s__%d", e.indexName, kind, now.UnixNano())
}

// deepCopyModel 使用 copier 做高性能深拷贝，map 类型直接返回。
func deepCopyModel(src any) (any, error) {
	if _, ok := src.(map[string]any); ok {
		return src, nil
	}
	srcVal := reflect.ValueOf(src)
	if srcVal.Kind() == reflect.Ptr {
		dst := reflect.New(srcVal.Elem().Type()).Interface()
		if err := copier.CopyWithOption(dst, src, copier.Option{DeepCopy: true}); err != nil {
			return nil, fmt.Errorf("essyncer: deep copy: %w", err)
		}
		return dst, nil
	}
	// 非指针非 map，JSON 降级
	data, err := json.Marshal(src)
	if err != nil {
		return nil, fmt.Errorf("essyncer: marshal fallback copy: %w", err)
	}
	var copied map[string]any
	if err := json.Unmarshal(data, &copied); err != nil {
		return nil, fmt.Errorf("essyncer: unmarshal fallback copy: %w", err)
	}
	return copied, nil
}
