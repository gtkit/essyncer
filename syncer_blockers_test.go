package essyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type blockerArticle struct {
	ID        int64          `json:"id"                  gorm:"primaryKey"`
	Title     string         `json:"title"`
	DeletedAt gorm.DeletedAt `json:"deleted_at,omitzero" gorm:"index"`
}

func (blockerArticle) TableName() string { return "blocker_articles" }
func (a blockerArticle) GetID() string   { return strconv.FormatInt(a.ID, 10) }

type blockerUUIDArticle struct {
	DocID string `json:"doc_id" gorm:"column:doc_id;primaryKey"`
	Title string `json:"title"`
}

func (blockerUUIDArticle) TableName() string { return "blocker_uuid_articles" }
func (a blockerUUIDArticle) GetID() string   { return a.DocID }

type blockerOrder struct {
	OrderID int64  `json:"order_id" gorm:"column:order_id;primaryKey"`
	Title   string `json:"title"`
}

func (blockerOrder) TableName() string { return "blocker_orders" }
func (o blockerOrder) GetID() string   { return strconv.FormatInt(o.OrderID, 10) }

type blockerCompositeDoc struct {
	TenantID int64  `json:"tenant_id" gorm:"primaryKey"`
	DocID    int64  `json:"doc_id"    gorm:"primaryKey"`
	Title    string `json:"title"`
}

func (blockerCompositeDoc) TableName() string { return "blocker_composite_docs" }
func (d blockerCompositeDoc) GetID() string {
	return strconv.FormatInt(d.TenantID, 10) + ":" + strconv.FormatInt(d.DocID, 10)
}

type blockerOutboxEvent struct {
	ID          int64      `gorm:"primaryKey"`
	Table       string     `gorm:"column:table_name"`
	IndexAlias  string     `gorm:"column:index_alias"`
	DocumentID  string     `gorm:"column:document_id"`
	Action      string     `gorm:"column:action"`
	Payload     []byte     `gorm:"column:payload"`
	Status      string     `gorm:"column:status"`
	Attempts    int        `gorm:"column:attempts"`
	NextRetryAt *time.Time `gorm:"column:next_retry_at"`
	LastError   string     `gorm:"column:last_error"`
	LeasedUntil *time.Time `gorm:"column:leased_until"`
	LeaseToken  string     `gorm:"column:lease_token"`
	CreatedAt   time.Time  `gorm:"column:created_at"`
	SentAt      *time.Time `gorm:"column:sent_at"`
}

func (blockerOutboxEvent) TableName() string { return "outbox_events" }

type recordedBulkItem struct {
	Index      string
	Action     string
	DocumentID string
	Body       []byte
}

type recordedESRequest struct {
	Method string
	Path   string
	Body   string
}

type fakeBulkIndexer struct {
	mu             sync.Mutex
	defaultIndex   string
	items          []recordedBulkItem
	addErrAt       int
	itemFailAt     int
	itemFailStatus int
	itemFailReason string
	stats          esutil.BulkIndexerStats
}

func (f *fakeBulkIndexer) ensureDefaultIndex(index string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.defaultIndex = firstNonEmpty(f.defaultIndex, index)
}

func (f *fakeBulkIndexer) Add(ctx context.Context, item esutil.BulkIndexerItem) error {
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
		Index:      firstNonEmpty(item.Index, f.defaultIndex),
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
			item.OnFailure(ctx, item, resp, nil)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func newTestElasticsearchClient(t *testing.T, transport http.RoundTripper) *elasticsearch.Client {
	t.Helper()

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://example.test"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("create fake elasticsearch client: %v", err)
	}
	return client
}

func jsonResponse(req *http.Request, statusCode int, body string) *http.Response {
	header := make(http.Header)
	header.Set("X-Elastic-Product", "Elasticsearch")
	return &http.Response{
		StatusCode: statusCode,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func itemDocumentIDs(items []recordedBulkItem) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.DocumentID)
	}
	return ids
}

func captureRequest(req *http.Request) (recordedESRequest, error) {
	record := recordedESRequest{
		Method: req.Method,
		Path:   req.URL.Path,
	}
	if req.Body == nil {
		return record, nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return recordedESRequest{}, err
	}
	record.Body = string(body)
	return record, nil
}

type aliasUpdateRequest struct {
	Actions []struct {
		Add struct {
			Alias string `json:"alias"`
			Index string `json:"index"`
		} `json:"add,omitempty"`
		Remove struct {
			Alias string `json:"alias"`
			Index string `json:"index"`
		} `json:"remove,omitempty"`
		RemoveIndex struct {
			Index string `json:"index"`
		} `json:"remove_index,omitempty"`
	} `json:"actions"`
}

type blockerReverseTitleStrategy struct{}

func (blockerReverseTitleStrategy) Prepare(_ FullSyncModelInfo, _ int64) (FullSyncScanCursor, error) {
	return blockerReverseTitleCursor{}, nil
}

type blockerReverseTitleCursor struct{}

func (blockerReverseTitleCursor) Scope(query *gorm.DB, batchSize int) *gorm.DB {
	return query.Order("title DESC").Limit(batchSize)
}

func (blockerReverseTitleCursor) Advance(context.Context, any) error { return nil }

func (blockerReverseTitleCursor) Checkpoint() any { return nil }

func openBlockerTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared&_busy_timeout=5000"), &gorm.Config{})
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

func setupOutboxTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.AutoMigrate(&blockerOutboxEvent{}); err != nil {
		t.Fatalf("migrate outbox events: %v", err)
	}
}

func listOutboxEvents(t *testing.T, db *gorm.DB) []blockerOutboxEvent {
	t.Helper()
	var rows []blockerOutboxEvent
	if err := db.Order("id ASC").Find(&rows).Error; err != nil {
		t.Fatalf("list outbox events: %v", err)
	}
	return rows
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
	s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, `{}`), nil
	}))
	s.newBulkIndexer = func(cfg esutil.BulkIndexerConfig) (esutil.BulkIndexer, error) {
		if fake, ok := idx.(*fakeBulkIndexer); ok {
			fake.ensureDefaultIndex(cfg.Index)
		}
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

func TestNew_UsesConfiguredRequestTimeout(t *testing.T) {
	tests := []struct {
		name            string
		responseDelay   time.Duration
		requestTimeout  time.Duration
		wantErrContains string
		wantWithin      time.Duration
	}{
		{
			name:            "info request times out before a stalled elasticsearch node responds",
			responseDelay:   300 * time.Millisecond,
			requestTimeout:  50 * time.Millisecond,
			wantErrContains: "timeout",
			wantWithin:      200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				time.Sleep(tt.responseDelay)
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				_, _ = io.WriteString(w, `{}`)
			}))
			defer srv.Close()

			cfg := DefaultConfig()
			cfg.Elasticsearch.Addresses = []string{srv.URL}
			cfg.Elasticsearch.RequestTimeout = tt.requestTimeout

			startedAt := time.Now()
			_, err := New(openBlockerTestDB(t), cfg)
			elapsed := time.Since(startedAt)

			if err == nil {
				t.Fatal("expected syncer construction to fail on elasticsearch timeout")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tt.wantErrContains) {
				t.Fatalf("constructor error = %q, want containing %q", err.Error(), tt.wantErrContains)
			}
			if elapsed > tt.wantWithin {
				t.Fatalf("constructor elapsed = %s, want <= %s", elapsed, tt.wantWithin)
			}
		})
	}
}

func TestAfterUpdate_MapUpdatesPersistOutboxAndAvoidImmediateESWrites(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	if err := db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "after"}).Error; err != nil {
		t.Fatalf("update article: %v", err)
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row after update, got %d", len(rows))
	}
	if len(indexer.items) != 0 {
		t.Fatalf("expected 0 immediate ES writes, got %d", len(indexer.items))
	}
	if rows[0].Action != string(actionUpdate) {
		t.Fatalf("expected update action, got %s", rows[0].Action)
	}
	if rows[0].DocumentID != "1" {
		t.Fatalf("expected document id 1, got %s", rows[0].DocumentID)
	}

	var payload map[string]map[string]any
	if err := json.Unmarshal(rows[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal update payload: %v", err)
	}
	if payload["doc"]["title"] != "after" {
		t.Fatalf("expected partial update payload, got %#v", payload)
	}
}

func TestAfterUpdate_StructUpdatesKeepModelPrimaryKey(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	if err := db.Model(&blockerArticle{ID: 1}).Select("Title").Updates(&blockerArticle{Title: "struct-update"}).Error; err != nil {
		t.Fatalf("struct update article: %v", err)
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row after struct update, got %d", len(rows))
	}
	if rows[0].DocumentID != "1" {
		t.Fatalf("expected document id 1 for struct update, got %s", rows[0].DocumentID)
	}
	if !strings.Contains(string(rows[0].Payload), "struct-update") {
		t.Fatalf("expected payload to include updated title, got %s", string(rows[0].Payload))
	}
}

func TestAfterUpdate_RollbackDropsBufferedEvents(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	if err := db.Callback().Update().
		Before("gorm:commit_or_rollback_transaction").
		Register("test:force_rollback", func(tx *gorm.DB) {
			_ = tx.AddError(errors.New("force rollback"))
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
	if rows := listOutboxEvents(t, db); len(rows) != 0 {
		t.Fatalf("expected 0 outbox rows after rollback, got %d", len(rows))
	}
}

func TestSyncerTransaction_CommitsBufferedEvents(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	if err := s.Transaction(t.Context(), func(tx *gorm.DB) error {
		return tx.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "tx commit"}).Error
	}); err != nil {
		t.Fatalf("transaction commit: %v", err)
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row after committed transaction, got %d", len(rows))
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
	if payload["doc"]["title"] != "tx commit" {
		t.Fatalf("outbox payload doc.title = %#v, want tx commit", payload["doc"]["title"])
	}
}

func TestSyncerTransaction_RollbackDropsBufferedEvents(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	err := s.Transaction(t.Context(), func(tx *gorm.DB) error {
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
	if rows := listOutboxEvents(t, db); len(rows) != 0 {
		t.Fatalf("expected 0 outbox rows after rolled back transaction, got %d", len(rows))
	}
}

func TestOutbox_CommittedCreateUpdateDeletePersistRows(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(t *testing.T, db *gorm.DB) error
		wantAction string
		wantDocID  string
	}{
		{
			name: "create",
			mutate: func(t *testing.T, db *gorm.DB) error {
				t.Helper()
				return db.Create(&blockerArticle{ID: 2, Title: "created"}).Error
			},
			wantAction: string(actionIndex),
			wantDocID:  "2",
		},
		{
			name: "update",
			mutate: func(t *testing.T, db *gorm.DB) error {
				t.Helper()
				return db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "updated"}).Error
			},
			wantAction: string(actionUpdate),
			wantDocID:  "1",
		},
		{
			name: "delete",
			mutate: func(t *testing.T, db *gorm.DB) error {
				t.Helper()
				return db.Unscoped().Delete(&blockerArticle{ID: 1}).Error
			},
			wantAction: string(actionDelete),
			wantDocID:  "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			indexer := &fakeBulkIndexer{}
			s := newBlockerTestSyncer(t, db, indexer)

			if err := s.EnableAutoSync(db); err != nil {
				t.Fatalf("enable auto sync: %v", err)
			}
			if err := tt.mutate(t, db); err != nil {
				t.Fatalf("apply %s mutation: %v", tt.name, err)
			}

			rows := listOutboxEvents(t, db)
			if len(rows) != 1 {
				t.Fatalf("expected 1 outbox row for %s mutation, got %d", tt.name, len(rows))
			}
			if rows[0].Action != tt.wantAction {
				t.Fatalf("outbox action = %s, want %s", rows[0].Action, tt.wantAction)
			}
			if rows[0].DocumentID != tt.wantDocID {
				t.Fatalf("outbox document_id = %s, want %s", rows[0].DocumentID, tt.wantDocID)
			}
			if len(indexer.items) != 0 {
				t.Fatalf("expected 0 immediate ES writes for %s mutation, got %d", tt.name, len(indexer.items))
			}
		})
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

	total, err := s.fullSyncTable(t.Context(), entry)
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

func TestFullSyncTable_SoftDeleteSemantics(t *testing.T) {
	tests := []struct {
		name    string
		mode    SoftDeleteMode
		wantIDs []string
	}{
		{
			name:    "update mode includes soft deleted rows",
			mode:    SoftDeleteModeUpdate,
			wantIDs: []string{"1", "2"},
		},
		{
			name:    "delete mode excludes soft deleted rows",
			mode:    SoftDeleteModeDelete,
			wantIDs: []string{"1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			if err := db.Create(&blockerArticle{ID: 2, Title: "deleted"}).Error; err != nil {
				t.Fatalf("seed deleted article: %v", err)
			}
			if err := db.Delete(&blockerArticle{ID: 2}).Error; err != nil {
				t.Fatalf("soft delete article: %v", err)
			}

			indexer := &fakeBulkIndexer{}
			s := newBlockerTestSyncer(t, db, indexer)
			entry, ok := s.registry.getByModel(db, &blockerArticle{})
			if !ok {
				t.Fatal("expected registered model entry")
			}
			entry.softDeleteMode = tt.mode

			total, err := s.fullSyncTable(t.Context(), entry)
			if err != nil {
				t.Fatalf("full sync table: %v", err)
			}
			if total != int64(len(tt.wantIDs)) {
				t.Fatalf("expected %d synced docs, got %d", len(tt.wantIDs), total)
			}

			gotIDs := itemDocumentIDs(indexer.items)
			if !slices.Equal(gotIDs, tt.wantIDs) {
				t.Fatalf("synced document ids = %#v, want %#v", gotIDs, tt.wantIDs)
			}
		})
	}
}

func TestEnsureIndices_FailsFast(t *testing.T) {
	tests := []struct {
		name              string
		wantErrContains   string
		wantSkippedMethod string
		wantSkippedPath   string
	}{
		{
			name:              "stop after first bootstrap create error",
			wantErrContains:   "create index blocker_articles__bootstrap__",
			wantSkippedMethod: http.MethodGet,
			wantSkippedPath:   "/_alias/blocker_users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openScopedTestDB(t)
			indexer := &fakeBulkIndexer{}
			s := newBlockerTestSyncer(t, db, indexer)
			if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
				t.Fatalf("register blocker user: %v", err)
			}

			var requests []string
			var mu sync.Mutex
			s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				requests = append(requests, req.Method+" "+req.URL.Path)
				mu.Unlock()

				switch req.Method + " " + req.URL.Path {
				case http.MethodGet + " /_alias/blocker_articles":
					return jsonResponse(req, http.StatusNotFound, `{}`), nil
				case http.MethodHead + " /blocker_articles":
					return jsonResponse(req, http.StatusNotFound, `{}`), nil
				case http.MethodPut + " " + req.URL.Path:
					if strings.HasPrefix(req.URL.Path, "/blocker_articles__bootstrap__") {
						return jsonResponse(req, http.StatusInternalServerError, `{"error":"create failed"}`), nil
					}
					return jsonResponse(req, http.StatusOK, `{}`), nil
				case http.MethodGet + " /_alias/blocker_users":
					return jsonResponse(req, http.StatusNotFound, `{}`), nil
				case http.MethodHead + " /blocker_users":
					return jsonResponse(req, http.StatusNotFound, `{}`), nil
				default:
					return jsonResponse(req, http.StatusInternalServerError, `{"error":"create failed"}`), nil
				}
			}))

			articleEntry, ok := s.registry.getByModel(db, &blockerArticle{})
			if !ok {
				t.Fatal("expected blocker article entry")
			}
			userEntry, ok := s.registry.getByModel(db, &blockerUser{})
			if !ok {
				t.Fatal("expected blocker user entry")
			}

			failedEntry, err := s.ensureIndices(t.Context(), []*modelEntry{articleEntry, userEntry})
			if err == nil {
				t.Fatal("expected ensure indices to fail")
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantErrContains)
			}
			if failedEntry != articleEntry {
				t.Fatalf("failed entry = %#v, want blocker article entry", failedEntry)
			}

			skipped := tt.wantSkippedMethod + " " + tt.wantSkippedPath
			if slices.Contains(requests, skipped) {
				t.Fatalf("expected fail-fast before %s, requests = %#v", skipped, requests)
			}
		})
	}
}

func TestFullSyncTable_PrimaryKeySupport(t *testing.T) {
	tests := []struct {
		name            string
		model           Syncable
		setup           func(t *testing.T, db *gorm.DB)
		wantSynced      int64
		wantIDs         []string
		wantErrContains string
	}{
		{
			name:  "single integer primary key with custom column syncs",
			model: &blockerOrder{},
			setup: func(t *testing.T, db *gorm.DB) {
				t.Helper()
				if err := db.AutoMigrate(&blockerOrder{}); err != nil {
					t.Fatalf("migrate blocker order: %v", err)
				}
				if err := db.Create(&blockerOrder{OrderID: 7, Title: "order"}).Error; err != nil {
					t.Fatalf("seed blocker order: %v", err)
				}
			},
			wantSynced: 1,
			wantIDs:    []string{"7"},
		},
		{
			name:  "string primary key returns explicit error",
			model: &blockerUUIDArticle{},
			setup: func(t *testing.T, db *gorm.DB) {
				t.Helper()
				if err := db.AutoMigrate(&blockerUUIDArticle{}); err != nil {
					t.Fatalf("migrate blocker uuid article: %v", err)
				}
				if err := db.Create(&blockerUUIDArticle{DocID: "doc-1", Title: "uuid"}).Error; err != nil {
					t.Fatalf("seed blocker uuid article: %v", err)
				}
			},
			wantErrContains: "requires exactly one integer primary key",
		},
		{
			name:  "composite primary key returns explicit error",
			model: &blockerCompositeDoc{},
			setup: func(t *testing.T, db *gorm.DB) {
				t.Helper()
				if err := db.AutoMigrate(&blockerCompositeDoc{}); err != nil {
					t.Fatalf("migrate blocker composite doc: %v", err)
				}
				if err := db.Create(&blockerCompositeDoc{TenantID: 1, DocID: 2, Title: "composite"}).Error; err != nil {
					t.Fatalf("seed blocker composite doc: %v", err)
				}
			},
			wantErrContains: "requires exactly one integer primary key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			tt.setup(t, db)

			indexer := &fakeBulkIndexer{}
			s := newBlockerTestSyncer(t, db, indexer)
			if err := s.Register(tt.model, WithIndexName(getTableName(db, tt.model))); err != nil {
				t.Fatalf("register test model: %v", err)
			}

			total, err := s.FullSyncTable(t.Context(), tt.model)
			if tt.wantErrContains != "" {
				if err == nil {
					t.Fatal("expected full sync table error")
				}
				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantErrContains)
				}
				if len(indexer.items) != 0 {
					t.Fatalf("expected no indexed docs on error, got %#v", indexer.items)
				}
				return
			}

			if err != nil {
				t.Fatalf("full sync table: %v", err)
			}
			if total != tt.wantSynced {
				t.Fatalf("synced docs = %d, want %d", total, tt.wantSynced)
			}

			gotIDs := itemDocumentIDs(indexer.items)
			if !slices.Equal(gotIDs, tt.wantIDs) {
				t.Fatalf("synced document ids = %#v, want %#v", gotIDs, tt.wantIDs)
			}
		})
	}
}

func TestFullSync_AliasSwap(t *testing.T) {
	tests := []struct {
		name              string
		getAliasStatus    int
		getAliasBody      string
		legacyIndexExists bool
		wantDeletePath    string
		wantRemoveIndex   string
	}{
		{
			name:           "existing alias target is replaced and deleted",
			getAliasStatus: http.StatusOK,
			getAliasBody: `{
				"blocker_articles__old": {
					"aliases": {
						"blocker_articles": {}
					}
				}
			}`,
			wantDeletePath:  "/blocker_articles__old",
			wantRemoveIndex: "",
		},
		{
			name:              "legacy concrete index is migrated into alias",
			getAliasStatus:    http.StatusNotFound,
			legacyIndexExists: true,
			wantDeletePath:    "",
			wantRemoveIndex:   "blocker_articles",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			indexer := &fakeBulkIndexer{}
			s := newBlockerTestSyncer(t, db, indexer)

			var (
				mu       sync.Mutex
				requests []recordedESRequest
			)
			s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				record, err := captureRequest(req)
				if err != nil {
					return nil, err
				}
				mu.Lock()
				requests = append(requests, record)
				mu.Unlock()

				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/_alias/blocker_articles":
					return jsonResponse(req, tt.getAliasStatus, tt.getAliasBody), nil
				case req.Method == http.MethodHead && req.URL.Path == "/blocker_articles":
					if tt.legacyIndexExists {
						return jsonResponse(req, http.StatusOK, `{}`), nil
					}
					return jsonResponse(req, http.StatusNotFound, `{}`), nil
				case req.Method == http.MethodPut && strings.HasPrefix(req.URL.Path, "/blocker_articles__fullsync__"):
					return jsonResponse(req, http.StatusOK, `{"acknowledged":true}`), nil
				case req.Method == http.MethodPost && req.URL.Path == "/_aliases":
					return jsonResponse(req, http.StatusOK, `{"acknowledged":true}`), nil
				case req.Method == http.MethodDelete && req.URL.Path == tt.wantDeletePath:
					return jsonResponse(req, http.StatusOK, `{"acknowledged":true}`), nil
				default:
					return jsonResponse(req, http.StatusOK, `{}`), nil
				}
			}))

			total, err := s.FullSyncTable(t.Context(), &blockerArticle{})
			if err != nil {
				t.Fatalf("full sync table: %v", err)
			}
			if total != 1 {
				t.Fatalf("synced docs = %d, want 1", total)
			}
			if len(indexer.items) != 1 {
				t.Fatalf("expected exactly one full-sync item, got %#v", indexer.items)
			}
			if !strings.HasPrefix(indexer.items[0].Index, "blocker_articles__fullsync__") {
				t.Fatalf("expected full sync to write to a physical rebuild index, got %q", indexer.items[0].Index)
			}

			var aliasReq aliasUpdateRequest
			foundAliasUpdate := false
			for _, req := range requests {
				if req.Method == http.MethodPost && req.Path == "/_aliases" {
					foundAliasUpdate = true
					if err := json.Unmarshal([]byte(req.Body), &aliasReq); err != nil {
						t.Fatalf("unmarshal alias update body: %v", err)
					}
				}
			}
			if !foundAliasUpdate {
				t.Fatal("expected alias update request")
			}

			var (
				addIndex      string
				removeIndex   string
				removeAliasTo string
			)
			for _, action := range aliasReq.Actions {
				if action.Add.Index != "" {
					addIndex = action.Add.Index
				}
				if action.Remove.Index != "" {
					removeAliasTo = action.Remove.Index
				}
				if action.RemoveIndex.Index != "" {
					removeIndex = action.RemoveIndex.Index
				}
			}
			if !strings.HasPrefix(addIndex, "blocker_articles__fullsync__") {
				t.Fatalf("expected alias add action to target rebuild index, got %q", addIndex)
			}
			if tt.wantRemoveIndex != "" {
				if removeIndex != tt.wantRemoveIndex {
					t.Fatalf("remove_index target = %q, want %q", removeIndex, tt.wantRemoveIndex)
				}
			} else if removeAliasTo != "blocker_articles__old" {
				t.Fatalf("remove alias target = %q, want blocker_articles__old", removeAliasTo)
			}

			if tt.wantDeletePath != "" {
				if !slices.ContainsFunc(requests, func(req recordedESRequest) bool {
					return req.Method == http.MethodDelete && req.Path == tt.wantDeletePath
				}) {
					t.Fatalf("expected delete request for old backing index, requests = %#v", requests)
				}
			}
		})
	}
}

func TestFullSync_AliasSwapFailureKeepsPreviousAlias(t *testing.T) {
	db := openBlockerTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	var (
		mu       sync.Mutex
		requests []recordedESRequest
	)
	s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		record, err := captureRequest(req)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		requests = append(requests, record)
		mu.Unlock()

		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/_alias/blocker_articles":
			return jsonResponse(req, http.StatusOK, `{
				"blocker_articles__old": {
					"aliases": {
						"blocker_articles": {}
					}
				}
			}`), nil
		case req.Method == http.MethodPut && strings.HasPrefix(req.URL.Path, "/blocker_articles__fullsync__"):
			return jsonResponse(req, http.StatusOK, `{"acknowledged":true}`), nil
		case req.Method == http.MethodPost && req.URL.Path == "/_aliases":
			return jsonResponse(req, http.StatusInternalServerError, `{"error":"alias switch failed"}`), nil
		default:
			return jsonResponse(req, http.StatusOK, `{}`), nil
		}
	}))

	_, err := s.FullSyncTable(t.Context(), &blockerArticle{})
	if err == nil {
		t.Fatal("expected alias swap failure")
	}
	if !strings.Contains(err.Error(), "alias") {
		t.Fatalf("error = %q, want alias-related failure", err.Error())
	}
	if slices.ContainsFunc(requests, func(req recordedESRequest) bool {
		return req.Method == http.MethodDelete && req.Path == "/blocker_articles__old"
	}) {
		t.Fatalf("expected previous backing index to remain untouched on alias failure, requests = %#v", requests)
	}
}

func TestFullSync_CustomScanStrategy(t *testing.T) {
	db := openBlockerTestDB(t)
	if err := db.Create(&blockerArticle{ID: 2, Title: "zulu"}).Error; err != nil {
		t.Fatalf("seed second article: %v", err)
	}
	if err := db.Model(&blockerArticle{ID: 1}).Update("title", "alpha").Error; err != nil {
		t.Fatalf("update first article title: %v", err)
	}

	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)

	s.Unregister(&blockerArticle{})
	if err := s.Register(
		&blockerArticle{},
		WithIndexName("blocker_articles"),
		WithBatchSize(10),
		WithFullSyncScanStrategy(blockerReverseTitleStrategy{}),
	); err != nil {
		t.Fatalf("register blocker article with custom strategy: %v", err)
	}

	total, err := s.FullSyncTable(t.Context(), &blockerArticle{})
	if err != nil {
		t.Fatalf("full sync table: %v", err)
	}
	if total != 2 {
		t.Fatalf("synced docs = %d, want 2", total)
	}

	if got := itemDocumentIDs(indexer.items); !slices.Equal(got, []string{"2", "1"}) {
		t.Fatalf("document order = %#v, want [2 1]", got)
	}
}
