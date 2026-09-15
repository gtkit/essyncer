package essyncer

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestDocumentOperations_InspectAndEnqueueDelete(t *testing.T) {
	tests := []struct {
		name  string
		check func(*testing.T, *Syncer, *gorm.DB)
	}{
		{
			name: "inspect document reports db es and outbox state",
			check: func(t *testing.T, syncer *Syncer, db *gorm.DB) {
				t.Helper()

				deadAt := time.Now().UTC()
				seedRows := []OutboxEvent{
					{
						TableName:  "blocker_articles",
						IndexAlias: "blocker_articles",
						DocumentID: "1",
						Action:     string(actionUpdate),
						Payload:    []byte(`{"doc":{"title":"pending"}}`),
						Status:     OutboxStatusPending,
					},
					{
						TableName:   "blocker_articles",
						IndexAlias:  "blocker_articles",
						DocumentID:  "1",
						Action:      string(actionUpdate),
						Payload:     []byte(`{"doc":{"title":"dead"}}`),
						Status:      OutboxStatusDead,
						LastError:   "boom",
						LeasedUntil: &deadAt,
					},
				}
				if err := db.Create(&seedRows).Error; err != nil {
					t.Fatalf("seed outbox rows: %v", err)
				}

				syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return jsonResponse(req, http.StatusOK, `{"found":true}`), nil
				}))

				result, err := syncer.InspectDocument(t.Context(), &blockerArticle{}, "1", "", false)
				if err != nil {
					t.Fatalf("inspect document: %v", err)
				}
				if !result.DBFound || !result.ESFound {
					t.Fatalf("expected db/es presence, got %#v", result)
				}
				if result.OutboxPending != 1 || result.OutboxDead != 1 || result.LastDeadError != "boom" {
					t.Fatalf("unexpected outbox counts: %#v", result)
				}
			},
		},
		{
			name: "enqueue delete accepts primary key lookup and direct document id",
			check: func(t *testing.T, syncer *Syncer, db *gorm.DB) {
				t.Helper()

				if err := syncer.EnqueueDocumentDelete(t.Context(), &blockerArticle{}, "1", "", false); err != nil {
					t.Fatalf("enqueue delete by primary key: %v", err)
				}
				if err := syncer.EnqueueDocumentDelete(t.Context(), &blockerArticle{}, "", "doc-99", false); err != nil {
					t.Fatalf("enqueue delete by document id: %v", err)
				}

				rows := listOutboxEvents(t, db)
				if len(rows) != 2 {
					t.Fatalf("expected 2 outbox rows, got %d", len(rows))
				}
				if rows[0].DocumentID != "1" || rows[1].DocumentID != "doc-99" {
					t.Fatalf("unexpected delete rows: %#v", rows)
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

func TestParsePrimaryKeyValueAndModelLookup(t *testing.T) {
	tests := []struct {
		name            string
		field           *schema.Field
		raw             string
		want            any
		wantErrContains string
	}{
		{
			name:  "string primary key parses directly",
			field: &schema.Field{IndirectFieldType: reflect.TypeFor[string]()},
			raw:   "doc-1",
			want:  "doc-1",
		},
		{
			name:  "int primary key parses as int64",
			field: &schema.Field{IndirectFieldType: reflect.TypeFor[int64]()},
			raw:   "42",
			want:  int64(42),
		},
		{
			name:  "uint primary key parses as uint64",
			field: &schema.Field{IndirectFieldType: reflect.TypeFor[uint64]()},
			raw:   "24",
			want:  uint64(24),
		},
		{
			name:            "invalid integer returns wrapped error",
			field:           &schema.Field{IndirectFieldType: reflect.TypeFor[int64]()},
			raw:             "nope",
			wantErrContains: "parse int primary key",
		},
		{
			name:            "nil field is rejected",
			field:           nil,
			raw:             "1",
			wantErrContains: "missing primary key field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := parsePrimaryKeyValue(tt.field, tt.raw)
			if tt.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("parsePrimaryKeyValue error = %v, want containing %q", err, tt.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePrimaryKeyValue: %v", err)
			}
			if value != tt.want {
				t.Fatalf("parsePrimaryKeyValue = %#v, want %#v", value, tt.want)
			}
		})
	}
}

func TestRequireModelEntryAndLoadSyncableByPrimaryKey(t *testing.T) {
	tests := []struct {
		name            string
		model           Syncable
		rawPrimaryKey   string
		wantErrContains string
	}{
		{
			name:          "registered model loads from db",
			model:         &blockerArticle{},
			rawPrimaryKey: "1",
		},
		{
			name:            "nil model is rejected",
			model:           nil,
			wantErrContains: "model is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

			entry, _, err := syncer.requireModelEntry(tt.model)
			if tt.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("requireModelEntry error = %v, want containing %q", err, tt.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("requireModelEntry: %v", err)
			}
			if entry.tableName != "blocker_articles" {
				t.Fatalf("unexpected entry: %#v", entry)
			}

			loaded, err := syncer.loadSyncableByPrimaryKey(t.Context(), tt.model, tt.rawPrimaryKey, false)
			if err != nil {
				t.Fatalf("loadSyncableByPrimaryKey: %v", err)
			}
			if loaded.GetID() != "1" {
				t.Fatalf("unexpected loaded syncable: %#v", loaded)
			}
		})
	}
}
