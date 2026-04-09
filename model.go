package essyncer

import (
	"encoding/json"
	"fmt"
	"github.com/jinzhu/copier"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"reflect"
	"sync"
)

// Syncable 是需要同步到 ES 的 GORM 模型必须实现的接口。
type Syncable interface {
	GetID() string
}

type modelEntry struct {
	modelType      reflect.Type
	tableName      string
	indexName      string
	mapping        json.RawMessage
	optionErr      error
	batchSize      int
	autoSync       bool
	softDeleteMode SoftDeleteMode
	fullDocUpdate  bool
	hasSoftDelete  bool
}

type modelRegistry struct {
	entries sync.Map
}

func newModelRegistry() *modelRegistry { return &modelRegistry{} }

func (r *modelRegistry) register(db *gorm.DB, model Syncable, defaultBatchSize int, opts ...RegisterOption) error {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return fmt.Errorf("essyncer: parse model: %w", err)
	}
	tableName := stmt.Schema.Table
	entry := &modelEntry{
		modelType:      reflect.TypeOf(model).Elem(),
		tableName:      tableName,
		indexName:      tableName,
		batchSize:      defaultBatchSize,
		autoSync:       true,
		softDeleteMode: SoftDeleteModeUpdate,
		hasSoftDelete:  hasSoftDeleteField(model),
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
	return v.(*modelEntry), true
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
		entries = append(entries, value.(*modelEntry))
		return true
	})
	return entries
}

func (r *modelRegistry) forEach(fn func(string, *modelEntry) bool) {
	r.entries.Range(func(key, value any) bool {
		return fn(key.(string), value.(*modelEntry))
	})
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

func extractID(v reflect.Value) int64 {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for _, name := range []string{"ID", "Id"} {
		field := v.FieldByName(name)
		if field.IsValid() && field.CanInt() {
			return field.Int()
		}
		if field.IsValid() && field.CanUint() {
			return int64(field.Uint())
		}
	}
	return 0
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
		return nil, err
	}
	var copied map[string]any
	if err := json.Unmarshal(data, &copied); err != nil {
		return nil, err
	}
	return copied, nil
}
