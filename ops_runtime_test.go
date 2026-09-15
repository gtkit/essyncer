package essyncer

import (
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9/esutil"
	"gorm.io/gorm"
)

func TestOutboxMaintenanceOperations(t *testing.T) {
	tests := []struct {
		name  string
		check func(*testing.T, *Syncer, *gorm.DB)
	}{
		{
			name: "cleanup outbox honors status and limit filters",
			check: func(t *testing.T, syncer *Syncer, db *gorm.DB) {
				t.Helper()
				rows := []OutboxEvent{
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "1", Action: string(actionUpdate), Status: OutboxStatusDead},
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "2", Action: string(actionUpdate), Status: OutboxStatusDead},
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "3", Action: string(actionUpdate), Status: OutboxStatusPending},
				}
				if err := db.Create(&rows).Error; err != nil {
					t.Fatalf("seed outbox rows: %v", err)
				}

				deleted, err := syncer.CleanupOutbox(t.Context(), OutboxCleanupOptions{
					Statuses: []string{OutboxStatusDead},
					Limit:    1,
				})
				if err != nil {
					t.Fatalf("cleanup outbox: %v", err)
				}
				if deleted != 1 {
					t.Fatalf("deleted rows = %d, want 1", deleted)
				}
			},
		},
		{
			name: "replay outbox resets dead rows back to pending",
			check: func(t *testing.T, syncer *Syncer, db *gorm.DB) {
				t.Helper()
				rows := []OutboxEvent{
					{
						TableName:  "blocker_articles",
						IndexAlias: "blocker_articles",
						DocumentID: "1",
						Action:     string(actionUpdate),
						Status:     OutboxStatusDead,
						Attempts:   3,
						LastError:  "boom",
					},
				}
				if err := db.Create(&rows).Error; err != nil {
					t.Fatalf("seed replay row: %v", err)
				}

				replayed, err := syncer.ReplayOutbox(t.Context(), OutboxReplayOptions{AllDead: true})
				if err != nil {
					t.Fatalf("replay outbox: %v", err)
				}
				if replayed != 1 {
					t.Fatalf("replayed rows = %d, want 1", replayed)
				}

				var got OutboxEvent
				if err := db.First(&got, rows[0].ID).Error; err != nil {
					t.Fatalf("load replayed row: %v", err)
				}
				if got.Status != OutboxStatusPending || got.Attempts != 0 || got.LastError != "" {
					t.Fatalf("unexpected replayed row: %#v", got)
				}
			},
		},
		{
			name: "cleanup outbox applies default dead status filter without limit",
			check: func(t *testing.T, syncer *Syncer, db *gorm.DB) {
				t.Helper()
				rows := []OutboxEvent{
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "1", Action: string(actionDelete), Status: OutboxStatusDead},
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "2", Action: string(actionDelete), Status: OutboxStatusPending},
				}
				if err := db.Create(&rows).Error; err != nil {
					t.Fatalf("seed cleanup rows: %v", err)
				}

				deleted, err := syncer.CleanupOutbox(t.Context(), OutboxCleanupOptions{})
				if err != nil {
					t.Fatalf("cleanup outbox defaults: %v", err)
				}
				if deleted != 1 {
					t.Fatalf("deleted rows = %d, want 1", deleted)
				}
			},
		},
		{
			name: "replay outbox rejects empty selection",
			check: func(t *testing.T, syncer *Syncer, _ *gorm.DB) {
				t.Helper()
				if _, err := syncer.ReplayOutbox(t.Context(), OutboxReplayOptions{}); err == nil {
					t.Fatal("expected replay outbox to reject empty selection")
				}
			},
		},
		{
			name: "replay outbox limit resets only the selected subset",
			check: func(t *testing.T, syncer *Syncer, db *gorm.DB) {
				t.Helper()
				rows := []OutboxEvent{
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "1", Action: string(actionUpdate), Status: OutboxStatusDead},
					{TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "2", Action: string(actionUpdate), Status: OutboxStatusDead},
				}
				if err := db.Create(&rows).Error; err != nil {
					t.Fatalf("seed limited replay rows: %v", err)
				}

				replayed, err := syncer.ReplayOutbox(t.Context(), OutboxReplayOptions{AllDead: true, Limit: 1})
				if err != nil {
					t.Fatalf("replay outbox with limit: %v", err)
				}
				if replayed != 1 {
					t.Fatalf("replayed rows = %d, want 1", replayed)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			tt.check(t, syncer, db)
		})
	}
}

func TestReconcileCountsAndSyncerUtilities(t *testing.T) {
	tests := []struct {
		name  string
		check func(*testing.T)
	}{
		{
			name: "reconcile counts, transport helpers, and metrics snapshots expose runtime state",
			check: func(t *testing.T) {
				t.Helper()

				db := openScopedTestDB(t)
				syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
				if err := syncer.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
					t.Fatalf("register blocker user: %v", err)
				}

				syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
					switch req.URL.Path {
					case "/blocker_articles/_count":
						return jsonResponse(req, http.StatusOK, `{"count":1}`), nil
					case "/blocker_users/_count":
						return jsonResponse(req, http.StatusOK, `{"count":0}`), nil
					default:
						return jsonResponse(req, http.StatusOK, `{}`), nil
					}
				}))

				results, err := syncer.ReconcileCounts(t.Context(), &blockerArticle{}, &blockerUser{})
				if err != nil {
					t.Fatalf("reconcile counts: %v", err)
				}
				if len(results) != 2 || results[0].DBCount == 0 {
					t.Fatalf("unexpected reconcile results: %#v", results)
				}

				cfg := normalizeOutboxConfig(Config{})
				if cfg.Outbox.BatchSize != 100 || cfg.Outbox.Lease != 30*time.Second {
					t.Fatalf("unexpected normalized outbox config: %#v", cfg.Outbox)
				}

				transport, err := newElasticsearchTransport(ESConfig{
					Addresses:        []string{"https://example.test"},
					RequestTimeout:   time.Second,
					AllowInsecureTLS: true,
				})
				if err != nil {
					t.Fatalf("new elasticsearch transport: %v", err)
				}
				if transport.ResponseHeaderTimeout != time.Second || transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
					t.Fatalf("unexpected transport configuration: %#v", transport)
				}

				startupCtx, cancel := newStartupContext(10 * time.Millisecond)
				defer cancel()
				deadline, ok := startupCtx.Deadline()
				if !ok || time.Until(deadline) <= 0 {
					t.Fatalf("expected startup context deadline, got %v %v", deadline, ok)
				}

				syncer.failures = newFailureRecorder(2)
				syncer.failureInit = sync.Once{}
				syncer.recordFailure(FailureEvent{Source: failureSourceRelay, Error: "latest failure"})
				syncer.indexer = &fakeBulkIndexer{
					stats: esutil.BulkIndexerStats{
						NumAdded:   math.MaxUint64,
						NumFlushed: 2,
					},
				}

				metrics := syncer.GetMetrics()
				if metrics.BulkNumAdded != math.MaxInt64 || metrics.LastErrorText != "latest failure" {
					t.Fatalf("unexpected metrics snapshot: %#v", metrics)
				}
				stoppedSyncer := &Syncer{}
				stoppedSyncer.stopped.Store(true)
				if healthErr := stoppedSyncer.Health(t.Context()); healthErr == nil {
					t.Fatal("expected stopped syncer health check to fail")
				}

				customIndexer := &fakeBulkIndexer{}
				customSyncer := &Syncer{
					newBulkIndexer: func(esutil.BulkIndexerConfig) (esutil.BulkIndexer, error) {
						return customIndexer, nil
					},
				}
				indexer, err := customSyncer.makeBulkIndexer(esutil.BulkIndexerConfig{})
				if err != nil {
					t.Fatalf("make bulk indexer: %v", err)
				}
				if indexer != customIndexer {
					t.Fatalf("expected injected bulk indexer, got %#v", indexer)
				}

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("X-Elastic-Product", "Elasticsearch")
					switch r.Method {
					case http.MethodHead:
						w.WriteHeader(http.StatusOK)
					default:
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write([]byte(`{}`))
					}
				}))
				defer server.Close()

				esCfg := DefaultConfig()
				esCfg.Elasticsearch.Addresses = []string{server.URL}
				syncerFromNew, err := New(db, esCfg)
				if err != nil {
					t.Fatalf("new syncer for default bulk indexer: %v", err)
				}
				defaultIndexer, err := (&Syncer{}).makeBulkIndexer(esutil.BulkIndexerConfig{
					Client: syncerFromNew.ESClient(),
					Index:  "articles",
				})
				if err != nil {
					t.Fatalf("default makeBulkIndexer: %v", err)
				}
				if err := defaultIndexer.Close(t.Context()); err != nil {
					t.Fatalf("close default bulk indexer: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t)
		})
	}
}
