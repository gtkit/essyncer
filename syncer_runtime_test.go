package essyncer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestLoadConfig_ParsesRequestTimeoutAndDefaults(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantTimeout time.Duration
	}{
		{
			name: "yaml request timeout is parsed and defaults are preserved",
			content: `
elasticsearch:
  addresses:
    - http://localhost:9200
  request_timeout: 250ms
sync:
  tables:
    - model: Article
      index_name: articles_alias
`,
			wantTimeout: 250 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir() + "/essyncer.yaml"
			if err := os.WriteFile(path, []byte(strings.TrimSpace(tt.content)), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			if cfg.Elasticsearch.RequestTimeout != tt.wantTimeout {
				t.Fatalf("request timeout = %s, want %s", cfg.Elasticsearch.RequestTimeout, tt.wantTimeout)
			}
			if cfg.Outbox.BatchSize != DefaultConfig().Outbox.BatchSize {
				t.Fatalf("outbox batch size = %d, want default %d", cfg.Outbox.BatchSize, DefaultConfig().Outbox.BatchSize)
			}
			if len(cfg.Sync.Tables) != 1 || cfg.Sync.Tables[0].IndexName != "articles_alias" {
				t.Fatalf("unexpected sync tables: %#v", cfg.Sync.Tables)
			}
		})
	}
}

func TestOptionHelpers_ApplyConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		check func(*testing.T, *Syncer, *modelEntry)
	}{
		{
			name: "syncer and register options mutate configuration",
			check: func(t *testing.T, syncer *Syncer, entry *modelEntry) {
				t.Helper()
				if syncer.logger == nil {
					t.Fatal("expected logger to be set")
				}
				if syncer.failureHook == nil || syncer.failureBufferSize != 64 {
					t.Fatalf("unexpected failure options: hook=%v size=%d", syncer.failureHook != nil, syncer.failureBufferSize)
				}
				if syncer.cfg.Sync.Workers != 8 || syncer.cfg.Sync.FlushBytes != 2048 || syncer.cfg.Sync.FlushInterval != 5*time.Second {
					t.Fatalf("unexpected sync options: %#v", syncer.cfg.Sync)
				}
				if syncer.cfg.Sync.DefaultBatchSize != 222 {
					t.Fatalf("unexpected default batch size: %d", syncer.cfg.Sync.DefaultBatchSize)
				}
				if syncer.cfg.Outbox.PollInterval != time.Second || syncer.cfg.Outbox.BatchSize != 33 || syncer.cfg.Outbox.MaxAttempts != 9 || syncer.cfg.Outbox.Lease != 45*time.Second {
					t.Fatalf("unexpected outbox options: %#v", syncer.cfg.Outbox)
				}
				if entry.indexName != "articles_alias" || entry.batchSize != 44 || entry.autoSync || entry.softDeleteMode != SoftDeleteModeDelete || !entry.fullDocUpdate {
					t.Fatalf("unexpected register options: %#v", entry)
				}
				if entry.fullSyncScan == nil {
					t.Fatal("expected full sync strategy")
				}
				if string(entry.mapping) == "" {
					t.Fatal("expected mapping payload")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer := &Syncer{cfg: DefaultConfig()}
			WithLogger(defaultLogger())(syncer)
			WithFailureHook(func(FailureEvent) {})(syncer)
			WithFailureBufferSize(64)(syncer)
			WithWorkers(8)(syncer)
			WithFlushBytes(2048)(syncer)
			WithFlushInterval(5 * time.Second)(syncer)
			WithDefaultBatchSize(222)(syncer)
			WithOutboxPollInterval(time.Second)(syncer)
			WithOutboxBatchSize(33)(syncer)
			WithOutboxMaxAttempts(9)(syncer)
			WithOutboxLease(45 * time.Second)(syncer)

			entry := &modelEntry{batchSize: 100, autoSync: true, softDeleteMode: SoftDeleteModeUpdate}
			WithIndexName("articles_alias")(entry)
			WithMapping(json.RawMessage(`{"mappings":{"properties":{"title":{"type":"text"}}}}`))(entry)
			WithMappingMap(map[string]any{"settings": map[string]any{"number_of_shards": 1}})(entry)
			WithBatchSize(44)(entry)
			WithAutoSync(false)(entry)
			WithSoftDeleteMode(SoftDeleteModeDelete)(entry)
			WithFullDocUpdate(true)(entry)
			WithFullSyncScanStrategy(blockerReverseTitleStrategy{})(entry)

			tt.check(t, syncer, entry)
		})
	}
}

func TestSyncer_PublicAPIsAndRegistration(t *testing.T) {
	tests := []struct {
		name  string
		check func(*testing.T, *Syncer)
	}{
		{
			name: "register from config, lookup table config, ensure indices, and toggle auto sync",
			check: func(t *testing.T, syncer *Syncer) {
				t.Helper()
				if err := syncer.RegisterFromConfig(map[string]Syncable{
					"Article": &blockerArticle{},
					"User":    &blockerUser{},
				}); err != nil {
					t.Fatalf("register from config: %v", err)
				}

				if cfg, ok := syncer.GetTableConfig("Article"); !ok || cfg.IndexName != "articles_alias" {
					t.Fatalf("unexpected article table config: %#v %v", cfg, ok)
				}
				if _, ok := syncer.GetTableConfig("missing"); ok {
					t.Fatal("expected missing table config lookup to fail")
				}

				articleEntry, ok := syncer.registry.getByModel(syncer.db, &blockerArticle{})
				if !ok {
					t.Fatal("expected registered article entry")
				}
				if articleEntry.indexName != "articles_alias" || articleEntry.batchSize != 55 || articleEntry.autoSync || articleEntry.softDeleteMode != SoftDeleteModeDelete || !articleEntry.fullDocUpdate {
					t.Fatalf("unexpected article entry: %#v", articleEntry)
				}

				if err := syncer.EnableAutoSync(syncer.db, &blockerArticle{}, &blockerUser{}); err != nil {
					t.Fatalf("enable auto sync: %v", err)
				}
				if err := syncer.DisableAutoSync(syncer.db); err != nil {
					t.Fatalf("disable auto sync: %v", err)
				}
				if articleEntry.autoSync {
					t.Fatal("expected auto sync to be disabled")
				}

				var createCalls int
				syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
					switch {
					case req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/_alias/"):
						return jsonResponse(req, http.StatusNotFound, `{}`), nil
					case req.Method == http.MethodHead:
						return jsonResponse(req, http.StatusNotFound, `{}`), nil
					case req.Method == http.MethodPut:
						createCalls++
						return jsonResponse(req, http.StatusOK, `{}`), nil
					default:
						return jsonResponse(req, http.StatusOK, `{}`), nil
					}
				}))

				if err := syncer.EnsureAllIndices(t.Context()); err != nil {
					t.Fatalf("ensure all indices: %v", err)
				}
				if createCalls != 2 {
					t.Fatalf("expected 2 bootstrap index creations, got %d", createCalls)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openScopedTestDB(t)
			syncer := &Syncer{
				cfg:      DefaultConfig(),
				db:       db,
				logger:   defaultLogger(),
				registry: newModelRegistry(),
			}
			autoSyncDisabled := false
			syncer.cfg.Sync.Tables = []TableConfig{
				{
					Model:          "Article",
					IndexName:      "articles_alias",
					BatchSize:      55,
					AutoSync:       &autoSyncDisabled,
					SoftDeleteMode: SoftDeleteModeDelete,
					FullDocUpdate:  true,
				},
				{
					Model:     "User",
					IndexName: "users_alias",
				},
			}

			tt.check(t, syncer)
		})
	}
}

func TestSyncer_NewAndHealth(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "new success exposes clients and health check works"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				switch req.Method {
				case http.MethodHead:
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer server.Close()

			cfg := DefaultConfig()
			cfg.Elasticsearch.Addresses = []string{server.URL}
			cfg.Elasticsearch.RequestTimeout = time.Second

			syncer, err := New(openBlockerTestDB(t), cfg, WithLogger(defaultLogger()))
			if err != nil {
				t.Fatalf("new syncer: %v", err)
			}
			if syncer.ESClient() == nil || syncer.TypedClient() == nil {
				t.Fatal("expected elasticsearch clients to be initialized")
			}
			if err := syncer.Health(t.Context()); err != nil {
				t.Fatalf("health check: %v", err)
			}
			if err := syncer.Shutdown(t.Context()); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
		})
	}
}

func TestFullSyncWithCheckpoint_StartsAfterCheckpoint(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "only rows after the checkpoint are synced"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			if err := db.Create(&blockerArticle{ID: 2, Title: "after checkpoint"}).Error; err != nil {
				t.Fatalf("seed article: %v", err)
			}
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

			total, err := syncer.FullSyncWithCheckpoint(t.Context(), &blockerArticle{}, 1)
			if err != nil {
				t.Fatalf("full sync with checkpoint: %v", err)
			}
			if total != 1 {
				t.Fatalf("checkpoint sync total = %d, want 1", total)
			}
		})
	}
}

func TestHealth_ReturnsErrorOnElasticsearchFailure(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "non-200 ping is surfaced as unhealthy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer := &Syncer{
				es: newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return jsonResponse(req, http.StatusServiceUnavailable, `{}`), nil
				})),
			}

			if err := syncer.Health(t.Context()); err == nil {
				t.Fatal("expected health check to fail when elasticsearch returns an error status")
			}
		})
	}
}

func TestNew_ReturnsValidationErrors(t *testing.T) {
	tests := []struct {
		name            string
		cfg             func(string) Config
		wantErrContains string
	}{
		{
			name: "missing ca cert path fails before client initialization completes",
			cfg: func(serverURL string) Config {
				cfg := DefaultConfig()
				cfg.Elasticsearch.Addresses = []string{serverURL}
				cfg.Elasticsearch.CACert = "/definitely/missing/ca.pem"
				return cfg
			},
			wantErrContains: "read ca cert",
		},
		{
			name: "missing mapping file is rejected during startup validation",
			cfg: func(serverURL string) Config {
				cfg := DefaultConfig()
				cfg.Elasticsearch.Addresses = []string{serverURL}
				cfg.Sync.Tables = []TableConfig{{Model: "Article", MappingFile: "/definitely/missing/mapping.json"}}
				return cfg
			},
			wantErrContains: "validate mapping",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer server.Close()

			if _, err := New(openBlockerTestDB(t), tt.cfg(server.URL)); err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("New error = %v, want containing %q", err, tt.wantErrContains)
			}
		})
	}
}

func TestResolveModels_UsesStatementFallbacks(t *testing.T) {
	tests := []struct {
		name    string
		buildTx func(*testing.T, *gorm.DB) *gorm.DB
		wantLen int
	}{
		{
			name: "schema lookup resolves registered models",
			buildTx: func(t *testing.T, db *gorm.DB) *gorm.DB {
				tx := db.Session(&gorm.Session{})
				stmt := &gorm.Statement{DB: tx, Context: t.Context()}
				if err := stmt.Parse(&blockerArticle{}); err != nil {
					t.Fatalf("parse blocker article: %v", err)
				}
				stmt.Dest = &blockerArticle{ID: 1}
				tx.Statement = stmt
				return tx
			},
			wantLen: 1,
		},
		{
			name: "table lookup resolves registered models without schema",
			buildTx: func(_ *testing.T, db *gorm.DB) *gorm.DB {
				tx := db.Session(&gorm.Session{})
				tx.Statement = &gorm.Statement{
					DB:      tx,
					Context: t.Context(),
					Table:   "blocker_articles",
					Dest:    &blockerArticle{ID: 2},
				}
				return tx
			},
			wantLen: 1,
		},
		{
			name: "model fallback resolves registered models when dest is nil",
			buildTx: func(_ *testing.T, db *gorm.DB) *gorm.DB {
				tx := db.Session(&gorm.Session{})
				tx.Statement = &gorm.Statement{
					DB:      tx,
					Context: t.Context(),
					Model:   &blockerArticle{ID: 3},
				}
				return tx
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

			models, entry, identified := syncer.resolveModels(tt.buildTx(t, db))
			if entry == nil || len(models) != tt.wantLen {
				t.Fatalf("resolveModels entry=%#v models=%#v", entry, models)
			}
			if !identified {
				t.Fatal("expected resolved models to carry primary key identity")
			}
		})
	}
}

func TestOutboxRelayTimingHelpers(t *testing.T) {
	tests := []struct {
		name            string
		lease           time.Duration
		poll            time.Duration
		attempt         int
		wantSendTimeout time.Duration
		wantBackoff     time.Duration
	}{
		{
			name:            "defaults are used when config values are unset",
			lease:           0,
			poll:            0,
			attempt:         1,
			wantSendTimeout: 30 * time.Second,
			wantBackoff:     2 * time.Second,
		},
		{
			name:            "configured lease and backoff are respected",
			lease:           45 * time.Second,
			poll:            time.Second,
			attempt:         3,
			wantSendTimeout: 45 * time.Second,
			wantBackoff:     4 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer := &Syncer{cfg: DefaultConfig()}
			syncer.cfg.Outbox.Lease = tt.lease
			syncer.cfg.Outbox.PollInterval = tt.poll

			if got := syncer.outboxSendTimeout(); got != tt.wantSendTimeout {
				t.Fatalf("outboxSendTimeout = %s, want %s", got, tt.wantSendTimeout)
			}
			if got := syncer.relayBackoff(tt.attempt); got != tt.wantBackoff {
				t.Fatalf("relayBackoff = %s, want %s", got, tt.wantBackoff)
			}
		})
	}
}
