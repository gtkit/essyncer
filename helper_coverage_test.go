package essyncer

import (
	"crypto/tls"
	"reflect"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9/esutil"
	"gorm.io/gorm"
)

func TestExtractModelsAndChangedFields(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		wantLen int
	}{
		{name: "struct pointer yields one model", value: &blockerArticle{ID: 1}, wantLen: 1},
		{name: "slice of structs yields one entry per element", value: []blockerArticle{{ID: 1}, {ID: 2}}, wantLen: 2},
		{name: "slice of pointers skips nil entries", value: []*blockerArticle{{ID: 1}, nil}, wantLen: 1},
		{name: "nil pointer yields no models", value: (*blockerArticle)(nil), wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			models := extractModels(reflect.ValueOf(tt.value))
			if len(models) != tt.wantLen {
				t.Fatalf("extractModels len = %d, want %d", len(models), tt.wantLen)
			}
		})
	}

	db := openBlockerTestDB(t)
	tx := db.Session(&gorm.Session{})
	tx.Statement = &gorm.Statement{
		DB:      tx,
		Context: t.Context(),
		Dest:    map[string]any{"title": "after", "status": "published"},
	}
	changed := extractChangedFields(tx)
	if len(changed) != 2 || changed["title"] != "after" || changed["status"] != "published" {
		t.Fatalf("unexpected changed fields: %#v", changed)
	}

	if result := extractChangedFields(&gorm.DB{}); result != nil {
		t.Fatalf("expected nil changed fields for empty statement, got %#v", result)
	}
}

func TestModelAndUtilityHelpers(t *testing.T) {
	tests := []struct {
		name  string
		check func(*testing.T, *gorm.DB)
	}{
		{
			name: "table names deep copy and full sync key helpers work",
			check: func(t *testing.T, db *gorm.DB) {
				t.Helper()

				type inlineModel struct {
					ID int64 `gorm:"primaryKey"`
				}

				if got := getTableName(db, &inlineModel{}); got != "inline_models" {
					t.Fatalf("getTableName = %q, want inline_models", got)
				}

				copied, err := deepCopyModel(struct {
					Title string `json:"title"`
				}{Title: "hello"})
				if err != nil {
					t.Fatalf("deepCopyModel fallback: %v", err)
				}
				copyMap, ok := copied.(map[string]any)
				if !ok || copyMap["title"] != "hello" {
					t.Fatalf("unexpected copied value: %#v", copied)
				}

				stmt := &gorm.Statement{DB: db}
				if parseErr := stmt.Parse(&blockerArticle{}); parseErr != nil {
					t.Fatalf("parse blocker article: %v", parseErr)
				}
				key, err := newFullSyncKey(stmt.Schema)
				if err != nil {
					t.Fatalf("newFullSyncKey: %v", err)
				}
				value, err := key.valueOf(t.Context(), reflect.ValueOf(blockerArticle{ID: 9}))
				if err != nil {
					t.Fatalf("fullSyncKey.valueOf: %v", err)
				}
				if value != int64(9) {
					t.Fatalf("fullSyncKey.valueOf = %#v, want 9", value)
				}
			},
		},
		{
			name: "transport and metrics helpers use safe defaults",
			check: func(t *testing.T, _ *gorm.DB) {
				t.Helper()

				transport, err := newElasticsearchTransport(ESConfig{})
				if err != nil {
					t.Fatalf("newElasticsearchTransport: %v", err)
				}
				if transport.ResponseHeaderTimeout != 5*time.Second || transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
					t.Fatalf("unexpected transport defaults: %#v", transport)
				}

				syncer := &Syncer{
					indexer: &fakeBulkIndexer{
						stats: esutil.BulkIndexerStats{
							NumAdded:   5,
							NumDeleted: 1,
						},
					},
				}
				snapshot := syncer.GetMetrics()
				if snapshot.BulkNumAdded != 5 || snapshot.BulkNumDeleted != 1 {
					t.Fatalf("unexpected metrics snapshot: %#v", snapshot)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, openBlockerTestDB(t))
		})
	}
}

func TestResolveModelsAndChangedFields_FromDryRunUpdate(t *testing.T) {
	tests := []struct {
		name        string
		buildTx     func(*gorm.DB) *gorm.DB
		wantDocID   string
		wantChanged map[string]any
	}{
		{
			name: "struct update resolves model and changed fields from schema metadata",
			buildTx: func(db *gorm.DB) *gorm.DB {
				return db.Session(&gorm.Session{DryRun: true}).
					Model(&blockerArticle{ID: 1}).
					Select("Title").
					Updates(&blockerArticle{ID: 1, Title: "after"})
			},
			wantDocID:   "1",
			wantChanged: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

			tx := tt.buildTx(db)
			models, entry, identified := syncer.resolveModels(tx)
			if entry == nil || len(models) != 1 {
				t.Fatalf("unexpected resolved models: entry=%#v models=%#v", entry, models)
			}
			if !identified {
				t.Fatal("expected resolved models to carry primary key identity")
			}

			syncable, ok := models[0].(Syncable)
			if !ok || syncable.GetID() != tt.wantDocID {
				t.Fatalf("unexpected resolved syncable: %#v", models[0])
			}

			changed := extractChangedFields(tx)
			if !reflect.DeepEqual(changed, tt.wantChanged) {
				t.Fatalf("changed fields = %#v, want %#v", changed, tt.wantChanged)
			}
		})
	}
}
