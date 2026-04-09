package essyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type blockerArticle struct {
	ID        int64          `gorm:"primaryKey" json:"id"`
	Title     string         `json:"title"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at,omitzero"`
}

func (blockerArticle) TableName() string { return "blocker_articles" }
func (a *blockerArticle) GetID() string  { return strconv.FormatInt(a.ID, 10) }

type recordedBulkItem struct {
	Index      string
	Action     string
	DocumentID string
	Body       []byte
}

type fakeBulkIndexer struct {
	mu             sync.Mutex
	items          []recordedBulkItem
	addErrAt       int
	itemFailAt     int
	itemFailStatus int
	itemFailReason string
	stats          esutil.BulkIndexerStats
}

func (f *fakeBulkIndexer) Add(_ context.Context, item esutil.BulkIndexerItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.addErrAt > 0 && len(f.items)+1 == f.addErrAt {
		return errors.New("bulk add failed")
	}

	var body []byte
	if item.Body != nil {
		var err error
		body, err = io.ReadAll(item.Body)
		if err != nil {
			return err
		}
		item.Body = bytes.NewReader(body)
	}

	f.items = append(f.items, recordedBulkItem{
		Index:      item.Index,
		Action:     item.Action,
		DocumentID: item.DocumentID,
		Body:       body,
	})
	f.stats.NumAdded++
	if f.itemFailAt > 0 && len(f.items) == f.itemFailAt {
		f.stats.NumFailed++
		if item.OnFailure != nil {
			resp := esutil.BulkIndexerResponseItem{
				Status:     f.itemFailStatus,
				Index:      item.Index,
				DocumentID: item.DocumentID,
			}
			resp.Error.Reason = f.itemFailReason
			item.OnFailure(context.Background(), item, resp, nil)
		}
	}
	return nil
}

func (f *fakeBulkIndexer) Close(context.Context) error { return nil }
func (f *fakeBulkIndexer) Stats() esutil.BulkIndexerStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

func openBlockerTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&blockerArticle{}); err != nil {
		t.Fatalf("migrate sqlite: %v", err)
	}
	if err := db.Create(&blockerArticle{ID: 1, Title: "before"}).Error; err != nil {
		t.Fatalf("seed article: %v", err)
	}
	return db
}

func newBlockerTestSyncer(t *testing.T, db *gorm.DB, idx esutil.BulkIndexer) *Syncer {
	t.Helper()

	s := &Syncer{
		cfg:      DefaultConfig(),
		db:       db,
		indexer:  idx,
		logger:   defaultLogger(),
		registry: newModelRegistry(),
	}
	s.newBulkIndexer = func(esutil.BulkIndexerConfig) (esutil.BulkIndexer, error) {
		return idx, nil
	}
	if err := s.Register(&blockerArticle{}, WithIndexName("blocker_articles")); err != nil {
		t.Fatalf("register blocker article: %v", err)
	}
	return s
}

func TestDefaultConfig_DisablesInsecureTLS(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Elasticsearch.AllowInsecureTLS {
		t.Fatal("default config must not allow insecure TLS")
	}
}

func TestAfterUpdate_MapUpdatesAreBufferedUntilCommit(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	var itemsSeenBeforeCommit int
	if err := db.Callback().Update().
		Before("gorm:commit_or_rollback_transaction").
		Register("test:before_commit_no_flush", func(tx *gorm.DB) {
			itemsSeenBeforeCommit = len(indexer.items)
		}); err != nil {
		t.Fatalf("register before-commit assertion: %v", err)
	}

	if err := db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "after"}).Error; err != nil {
		t.Fatalf("update article: %v", err)
	}

	if itemsSeenBeforeCommit != 0 {
		t.Fatalf("expected no ES writes before commit, saw %d", itemsSeenBeforeCommit)
	}
	if len(indexer.items) != 1 {
		t.Fatalf("expected 1 ES write after commit, got %d", len(indexer.items))
	}
	if indexer.items[0].Action != string(actionUpdate) {
		t.Fatalf("expected update action, got %s", indexer.items[0].Action)
	}
	if indexer.items[0].DocumentID != "1" {
		t.Fatalf("expected document id 1, got %s", indexer.items[0].DocumentID)
	}

	var payload map[string]map[string]any
	if err := json.Unmarshal(indexer.items[0].Body, &payload); err != nil {
		t.Fatalf("unmarshal update payload: %v", err)
	}
	if payload["doc"]["title"] != "after" {
		t.Fatalf("expected partial update payload, got %#v", payload)
	}
}

func TestAfterUpdate_RollbackDropsBufferedEvents(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	if err := db.Callback().Update().
		Before("gorm:commit_or_rollback_transaction").
		Register("test:force_rollback", func(tx *gorm.DB) {
			tx.AddError(errors.New("force rollback"))
		}); err != nil {
		t.Fatalf("register rollback callback: %v", err)
	}

	err := db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "rolled back"}).Error
	if err == nil {
		t.Fatal("expected update error to force rollback")
	}
	if len(indexer.items) != 0 {
		t.Fatalf("expected 0 ES writes after rollback, got %d", len(indexer.items))
	}
}

func TestSyncerTransaction_CommitsBufferedEvents(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	if err := s.Transaction(context.Background(), func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "tx commit"}).Error
	}); err != nil {
		t.Fatalf("transaction commit: %v", err)
	}

	if len(indexer.items) != 1 {
		t.Fatalf("expected 1 ES write after committed transaction, got %d", len(indexer.items))
	}
}

func TestSyncerTransaction_RollbackDropsBufferedEvents(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	err := s.Transaction(context.Background(), func(tx *gorm.DB) error {
		if err := tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "tx rollback"}).Error; err != nil {
			return err
		}
		return errors.New("rollback outer transaction")
	})
	if err == nil {
		t.Fatal("expected transaction error to trigger rollback")
	}
	if len(indexer.items) != 0 {
		t.Fatalf("expected 0 ES writes after rolled back transaction, got %d", len(indexer.items))
	}
}

func TestFullSyncTable_CountsOnlySuccessfullyQueuedDocs(t *testing.T) {
	db := openBlockerTestDB(t)
	if err := db.Create(&blockerArticle{ID: 2, Title: "second"}).Error; err != nil {
		t.Fatalf("seed second article: %v", err)
	}

	indexer := &fakeBulkIndexer{addErrAt: 2}
	s := newBlockerTestSyncer(t, db, indexer)
	entry, ok := s.registry.getByModel(db, &blockerArticle{})
	if !ok {
		t.Fatal("expected registered model entry")
	}

	total, err := s.fullSyncTable(context.Background(), entry)
	if err == nil {
		t.Fatal("expected full sync error when bulk add fails")
	}
	if total != 1 {
		t.Fatalf("expected total synced to count only successful queue operations, got %d", total)
	}
	if len(indexer.items) != 1 {
		t.Fatalf("expected exactly 1 queued document before failure, got %d", len(indexer.items))
	}
}
