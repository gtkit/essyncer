package essyncer

import (
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
		return err
	}
	if err := db.Callback().Create().After("gorm:commit_or_rollback_transaction").Register(cbAfterCreateCommit, s.afterCommit); err != nil {
		return err
	}
	if err := db.Callback().Update().Before("gorm:after_update").Register(cbAfterUpdate, s.afterUpdate); err != nil {
		return err
	}
	if err := db.Callback().Update().After("gorm:commit_or_rollback_transaction").Register(cbAfterUpdateCommit, s.afterCommit); err != nil {
		return err
	}
	if err := db.Callback().Delete().Before("gorm:after_delete").Register(cbAfterDelete, s.afterDelete); err != nil {
		return err
	}
	return db.Callback().Delete().After("gorm:commit_or_rollback_transaction").Register(cbAfterDeleteCommit, s.afterCommit)
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
			changed := extractChangedFields(db)
			if len(changed) == 0 {
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
				doc:       changed,
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
					doc:       map[string]any{"deleted_at": time.Now()},
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
	var (
		entry *modelEntry
		ok    bool
	)
	if db.Statement.Schema != nil {
		entry, ok = s.registry.get(db.Statement.Schema.Table)
	} else if db.Statement.Table != "" {
		entry, ok = s.registry.get(db.Statement.Table)
	}
	if !ok && db.Statement.Model != nil {
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(db.Statement.Model); err == nil && stmt.Schema != nil {
			entry, ok = s.registry.get(stmt.Schema.Table)
		}
	}
	if !ok {
		return nil, nil
	}
	dest := db.Statement.Dest
	if dest == nil {
		dest = db.Statement.Model
	}
	if models := extractModels(reflect.ValueOf(dest)); len(models) > 0 {
		return models, entry
	}
	if db.Statement.ReflectValue.IsValid() {
		if models := extractModels(db.Statement.ReflectValue); len(models) > 0 {
			return models, entry
		}
	}
	if db.Statement.Model != nil {
		if models := extractModels(reflect.ValueOf(db.Statement.Model)); len(models) > 0 {
			return models, entry
		}
	}
	return nil, entry
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
