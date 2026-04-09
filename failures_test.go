package essyncer

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

func TestFailureRecorder_RecentFailuresNewestFirstAndBounded(t *testing.T) {
	rec := newFailureRecorder(2)

	rec.record(FailureEvent{DocumentID: "1", Error: "first"})
	rec.record(FailureEvent{DocumentID: "2", Error: "second"})
	rec.record(FailureEvent{DocumentID: "3", Error: "third"})

	failures := rec.recent(0)
	if len(failures) != 2 {
		t.Fatalf("expected 2 retained failures, got %d", len(failures))
	}
	if failures[0].DocumentID != "3" || failures[1].DocumentID != "2" {
		t.Fatalf("expected newest-first ordering, got %#v", failures)
	}
}

func TestFailureRecorder_RecentFailuresReturnsCopy(t *testing.T) {
	rec := newFailureRecorder(2)
	rec.record(FailureEvent{DocumentID: "1", Error: "original"})

	failures := rec.recent(1)
	failures[0].Error = "mutated"

	again := rec.recent(1)
	if again[0].Error != "original" {
		t.Fatalf("expected internal recorder state to remain unchanged, got %#v", again[0])
	}
}

func TestFailureHook_ReceivesFailureEvent(t *testing.T) {
	var received []FailureEvent

	s := &Syncer{
		failureBufferSize: 4,
		failureHook: func(event FailureEvent) {
			received = append(received, event)
		},
	}

	s.recordFailure(FailureEvent{
		Source:     "callback",
		Index:      "articles",
		Action:     "update",
		DocumentID: "42",
		Error:      "boom",
	})

	if len(received) != 1 {
		t.Fatalf("expected 1 hook event, got %d", len(received))
	}
	if received[0].DocumentID != "42" || received[0].Error != "boom" {
		t.Fatalf("unexpected hook event: %#v", received[0])
	}
}

func TestCallbackFailure_RecordsFailureEvent(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{
		itemFailAt:     1,
		itemFailStatus: 429,
		itemFailReason: "too many requests",
	}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}
	if err := db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "after"}).Error; err != nil {
		t.Fatalf("update article: %v", err)
	}

	failures := s.RecentFailures(1)
	if len(failures) != 1 {
		t.Fatalf("expected 1 failure event, got %d", len(failures))
	}
	if failures[0].Source != "callback" || failures[0].Status != 429 || !failures[0].Retryable {
		t.Fatalf("unexpected callback failure event: %#v", failures[0])
	}
}

func TestUserManagedTransactionSkip_RecordsFailureEvent(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "tx skip"}).Error
	}); err != nil {
		t.Fatalf("raw transaction update: %v", err)
	}

	failures := s.RecentFailures(1)
	if len(failures) != 1 {
		t.Fatalf("expected 1 skip event, got %d", len(failures))
	}
	if failures[0].Source != "tx_skip" || failures[0].Action != "skip" || failures[0].DocumentID != "1" {
		t.Fatalf("unexpected tx skip failure event: %#v", failures[0])
	}
}

func TestFullSyncAddFailure_RecordsFailureEvent(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{addErrAt: 1}
	s := newBlockerTestSyncer(t, db, indexer)
	entry, ok := s.registry.getByModel(db, &blockerArticle{})
	if !ok {
		t.Fatal("expected registered model entry")
	}

	_, err := s.fullSyncTable(context.Background(), entry)
	if err == nil {
		t.Fatal("expected full sync error")
	}

	failures := s.RecentFailures(1)
	if len(failures) != 1 {
		t.Fatalf("expected 1 fullsync failure event, got %d", len(failures))
	}
	if failures[0].Source != "fullsync" || failures[0].Action != "index" || failures[0].DocumentID != "1" {
		t.Fatalf("unexpected fullsync failure event: %#v", failures[0])
	}
}

func TestGetMetrics_IncludesFailureAndFlushSummary(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}
	if err := s.Transaction(context.Background(), func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "committed"}).Error
	}); err != nil {
		t.Fatalf("transaction commit: %v", err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "tx skip"}).Error
	}); err != nil {
		t.Fatalf("raw transaction update: %v", err)
	}

	metrics := s.GetMetrics()
	if metrics.LastFlushItems != 1 || metrics.SyncEventsBuffered == 0 || metrics.SyncEventsFlushed == 0 {
		t.Fatalf("expected flush metrics to be populated, got %#v", metrics)
	}
	if metrics.LastErrorSource != "tx_skip" || metrics.SyncEventsSkippedTx == 0 || metrics.FailureSamplesRetained == 0 {
		t.Fatalf("expected failure summary to be populated, got %#v", metrics)
	}
}
