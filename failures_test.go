package essyncer

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

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

func TestRelayFailure_RecordsFailureEvent(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	s.cfg.Outbox.PollInterval = 10 * time.Millisecond
	s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusTooManyRequests, `{"error":{"reason":"too many requests"}}`), nil
	}))

	row := blockerOutboxEvent{
		Table:      "blocker_articles",
		IndexAlias: "blocker_articles",
		DocumentID: "1",
		Action:     string(actionUpdate),
		Payload:    []byte(`{"doc":{"title":"after"}}`),
		Status:     OutboxStatusPending,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}

	relayCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := s.StartOutboxRelay(relayCtx); err != nil {
		t.Fatalf("start outbox relay: %v", err)
	}

	if err := waitFor(t.Context(), 2*time.Second, func() (bool, error) {
		return len(s.RecentFailures(10)) > 0, nil
	}); err != nil {
		t.Fatalf("wait relay failure event: %v", err)
	}

	failures := s.RecentFailures(10)
	if failures[0].Source != failureSourceRelay || failures[0].Status != 429 || !failures[0].Retryable {
		t.Fatalf("unexpected relay failure event: %#v", failures[0])
	}
}

func TestUserManagedTransactionSkip_PersistsOutboxWithoutSkipFailure(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
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

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row after raw transaction commit, got %d", len(rows))
	}
	if rows[0].Action != string(actionUpdate) {
		t.Fatalf("outbox action = %s, want %s", rows[0].Action, string(actionUpdate))
	}
	if rows[0].DocumentID != "1" {
		t.Fatalf("outbox document_id = %s, want 1", rows[0].DocumentID)
	}
	var payload map[string]map[string]any
	if err := json.Unmarshal(rows[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal outbox payload: %v", err)
	}
	if payload["doc"]["title"] != "tx skip" {
		t.Fatalf("outbox payload doc.title = %#v, want tx skip", payload["doc"]["title"])
	}

	for _, failure := range s.RecentFailures(10) {
		if failure.Source == failureSourceTxSkip {
			t.Fatalf("unexpected tx_skip failure event: %#v", failure)
		}
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

	_, err := s.fullSyncTable(t.Context(), entry)
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
	setupOutboxTable(t, db)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}
	if err := s.Transaction(t.Context(), func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "committed"}).Error
	}); err != nil {
		t.Fatalf("transaction commit: %v", err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "raw tx"}).Error
	}); err != nil {
		t.Fatalf("raw transaction update: %v", err)
	}
	if rows := listOutboxEvents(t, db); len(rows) == 0 {
		t.Fatal("expected outbox rows after transactions")
	}

	metrics := s.GetMetrics()
	if metrics.OutboxEventsPersisted == 0 {
		t.Fatalf("expected outbox persistence metrics to be populated, got %#v", metrics)
	}
	if metrics.SyncEventsBuffered != 0 || metrics.SyncEventsFlushed != 0 || metrics.LastFlushItems != 0 {
		t.Fatalf("expected no callback-time buffer/flush metrics in outbox mode, got %#v", metrics)
	}
	if metrics.LastErrorSource == failureSourceTxSkip || metrics.SyncEventsSkippedTx != 0 {
		t.Fatalf("expected no tx_skip metrics in outbox path, got %#v", metrics)
	}
}
