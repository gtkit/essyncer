package essyncer

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/gorm"
)

const instanceOutboxPersistedCountKey = "essyncer:outbox_persisted_count"

type syncEvent struct {
	source    string
	tableName string
	action    actionType
	indexName string
	docID     string
	doc       any
}

func flushContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func (s *Syncer) persistSyncEvents(ctx context.Context, tx *gorm.DB, events []syncEvent) error {
	if len(events) == 0 {
		return nil
	}
	if s.outbox == nil {
		if s.db == nil {
			return fmt.Errorf("persist outbox events: nil db")
		}
		s.outbox = newOutboxStore(s.db)
	}

	rows := make([]outboxWriteEvent, 0, len(events))
	for _, event := range events {
		rows = append(rows, outboxWriteEvent{
			TableName:  event.tableName,
			IndexAlias: event.indexName,
			DocumentID: event.docID,
			Action:     event.action,
			Doc:        event.doc,
		})
	}

	persistDB := tx
	if persistDB == nil {
		persistDB = s.db
	}
	persistDB = persistDB.Session(&gorm.Session{NewDB: true}).Model(&OutboxEvent{})
	if err := s.outbox.insertFromEvents(ctx, persistDB, rows); err != nil {
		s.metrics.DroppedTotal.Add(int64(len(events)))
		for _, event := range events {
			s.recordFailure(FailureEvent{
				Source:     event.source,
				Index:      event.indexName,
				Action:     string(event.action),
				DocumentID: event.docID,
				Error:      err.Error(),
			})
		}
		return fmt.Errorf("persist outbox events: %w", err)
	}
	if tx != nil {
		current, _ := tx.InstanceGet(instanceOutboxPersistedCountKey)
		count, _ := current.(int64)
		tx.InstanceSet(instanceOutboxPersistedCountKey, count+int64(len(events)))
	}
	return nil
}

func (s *Syncer) enqueueEvents(db *gorm.DB, events ...syncEvent) {
	if len(events) == 0 || db == nil || db.Statement == nil {
		return
	}

	if err := s.persistSyncEvents(flushContext(db.Statement.Context), db, events); err != nil {
		_ = db.AddError(err)
	}
}

func (s *Syncer) afterCommit(db *gorm.DB) {
	if db == nil || db.Error != nil {
		return
	}
	value, ok := db.InstanceGet(instanceOutboxPersistedCountKey)
	if !ok {
		return
	}
	count, ok := value.(int64)
	if !ok || count <= 0 {
		return
	}
	s.metrics.OutboxEventsPersisted.Add(count)
}

func (s *Syncer) Transaction(ctx context.Context, fn func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	if fn == nil {
		return nil
	}

	return s.db.WithContext(ctx).Transaction(fn, opts...)
}
