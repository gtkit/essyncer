package essyncer

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

const instanceSyncEventsKey = "essyncer:sync_events"

type syncEvent struct {
	source    string
	action    actionType
	indexName string
	docID     string
	doc       any
}

type syncEventBuffer struct {
	mu     sync.Mutex
	events []syncEvent
}

type syncBufferContextKey struct{}

func newSyncEventBuffer() *syncEventBuffer {
	return &syncEventBuffer{}
}

func (b *syncEventBuffer) append(events ...syncEvent) {
	if len(events) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, events...)
}

func (b *syncEventBuffer) snapshot() []syncEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return nil
	}
	events := make([]syncEvent, len(b.events))
	copy(events, b.events)
	return events
}

func withSyncBuffer(ctx context.Context) (context.Context, *syncEventBuffer) {
	if ctx == nil {
		ctx = context.Background()
	}
	buffer := newSyncEventBuffer()
	return context.WithValue(ctx, syncBufferContextKey{}, buffer), buffer
}

func syncBufferFromContext(ctx context.Context) *syncEventBuffer {
	if ctx == nil {
		return nil
	}
	buffer, _ := ctx.Value(syncBufferContextKey{}).(*syncEventBuffer)
	return buffer
}

func flushContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func getOrInitInstanceSyncBuffer(db *gorm.DB) *syncEventBuffer {
	if value, ok := db.InstanceGet(instanceSyncEventsKey); ok {
		if buffer, ok := value.(*syncEventBuffer); ok {
			return buffer
		}
	}
	buffer := newSyncEventBuffer()
	db.InstanceSet(instanceSyncEventsKey, buffer)
	return buffer
}

func startedTransaction(db *gorm.DB) bool {
	if db == nil {
		return false
	}
	_, ok := db.InstanceGet("gorm:started_transaction")
	return ok
}

func isUserManagedTransaction(db *gorm.DB) bool {
	if db == nil || db.Statement == nil || startedTransaction(db) {
		return false
	}
	_, ok := db.Statement.ConnPool.(gorm.TxCommitter)
	return ok
}

func (s *Syncer) flushSyncEvents(ctx context.Context, events []syncEvent) {
	start := time.Now()
	for _, event := range events {
		s.enqueue(ctx, event.source, event.action, event.indexName, event.docID, event.doc)
	}
	s.metrics.SyncEventsFlushed.Add(int64(len(events)))
	s.recordFlush(start, len(events))
}

func (s *Syncer) enqueueEvents(db *gorm.DB, events ...syncEvent) {
	if len(events) == 0 || db == nil || db.Statement == nil {
		return
	}

	if buffer := syncBufferFromContext(db.Statement.Context); buffer != nil {
		buffer.append(events...)
		s.metrics.SyncEventsBuffered.Add(int64(len(events)))
		return
	}

	if startedTransaction(db) {
		getOrInitInstanceSyncBuffer(db).append(events...)
		s.metrics.SyncEventsBuffered.Add(int64(len(events)))
		return
	}

	if isUserManagedTransaction(db) {
		s.metrics.DroppedTotal.Add(int64(len(events)))
		s.metrics.SyncEventsSkippedTx.Add(int64(len(events)))
		for _, event := range events {
			s.recordFailure(FailureEvent{
				Source:     failureSourceTxSkip,
				Index:      event.indexName,
				Action:     actionSkip,
				DocumentID: event.docID,
				Error:      "auto sync skipped in user-managed transaction; use Syncer.Transaction",
			})
		}
		s.logger.Warn("essyncer: auto sync skipped in user-managed transaction; use Syncer.Transaction",
			zap.Int("events", len(events)),
		)
		return
	}

	s.flushSyncEvents(flushContext(db.Statement.Context), events)
}

func (s *Syncer) afterCommit(db *gorm.DB) {
	if db == nil {
		return
	}
	value, ok := db.InstanceGet(instanceSyncEventsKey)
	if !ok {
		return
	}

	buffer, ok := value.(*syncEventBuffer)
	if !ok {
		return
	}
	events := buffer.snapshot()
	if len(events) == 0 {
		return
	}

	if db.Error != nil {
		s.metrics.DroppedTotal.Add(int64(len(events)))
		return
	}

	s.flushSyncEvents(flushContext(db.Statement.Context), events)
}

func (s *Syncer) Transaction(ctx context.Context, fn func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	if fn == nil {
		return nil
	}

	txCtx, buffer := withSyncBuffer(ctx)
	err := s.db.WithContext(txCtx).Transaction(func(tx *gorm.DB) error {
		return fn(tx.WithContext(txCtx))
	}, opts...)
	if err != nil {
		return err
	}

	s.flushSyncEvents(flushContext(ctx), buffer.snapshot())
	return nil
}
