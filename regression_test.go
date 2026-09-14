package essyncer

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// TestAutoSync_BulkWriteWithoutPrimaryKeyIsSkippedAndObservable 锁定这样一条契约：
// 当 WHERE 条件不含主键时，callback 只能看到零值 model（GetID() 返回 "0"），
// 此时必须跳过而不是拿零值主键造一条 ES 文档，并且这个缺口必须可观测。
// 去掉 resolveModels 的 identified 判定，本测试会因为多出一条 document_id="0"
// 的 outbox 行而失败。
func TestAutoSync_BulkWriteWithoutPrimaryKeyIsSkippedAndObservable(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*gorm.DB) error
		wantAction actionType
		wantTitle  string
		wantRows   int64
	}{
		{
			name: "bulk update without primary key in where clause",
			mutate: func(db *gorm.DB) error {
				return db.Model(&blockerArticle{}).Where("title = ?", "before").Update("title", "bulk").Error
			},
			wantAction: actionUpdate,
			wantTitle:  "bulk",
			wantRows:   1,
		},
		{
			name: "bulk delete without primary key in where clause",
			mutate: func(db *gorm.DB) error {
				return db.Where("title = ?", "before").Delete(&blockerArticle{}).Error
			},
			wantAction: actionDelete,
			wantRows:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			if err := syncer.EnableAutoSync(db); err != nil {
				t.Fatalf("enable auto sync: %v", err)
			}

			// 业务写本身必须成功：同步侧的缺口不该把业务写变成失败。
			if err := tt.mutate(db); err != nil {
				t.Fatalf("bulk write: %v", err)
			}

			var remaining int64
			if err := db.Model(&blockerArticle{}).Count(&remaining).Error; err != nil {
				t.Fatalf("count articles: %v", err)
			}
			if remaining != tt.wantRows {
				t.Fatalf("rows after bulk write = %d, want %d", remaining, tt.wantRows)
			}
			if tt.wantTitle != "" {
				var got blockerArticle
				if err := db.First(&got, 1).Error; err != nil {
					t.Fatalf("reload article: %v", err)
				}
				if got.Title != tt.wantTitle {
					t.Fatalf("title = %q, want %q", got.Title, tt.wantTitle)
				}
			}

			// 关键断言：不能产生任何 outbox 行，尤其不能是 document_id="0" 的假行。
			if rows := listOutboxEvents(t, db); len(rows) != 0 {
				t.Fatalf("expected no outbox rows for an unidentified bulk write, got %+v", rows)
			}

			snapshot := syncer.GetMetrics()
			if snapshot.SyncEventsSkippedUnidentified != 1 {
				t.Fatalf("SyncEventsSkippedUnidentified = %d, want 1", snapshot.SyncEventsSkippedUnidentified)
			}

			var found *FailureEvent
			for _, failure := range syncer.RecentFailures(10) {
				if failure.Source == failureSourceUnidentified {
					found = &failure
					break
				}
			}
			if found == nil {
				t.Fatal("expected a failure sample recording the skipped write")
			}
			if found.Action != string(tt.wantAction) {
				t.Fatalf("failure action = %q, want %q", found.Action, tt.wantAction)
			}
			if !strings.Contains(found.Error, "FullSyncTable") {
				t.Fatalf("failure message must point at the recovery path, got %q", found.Error)
			}
		})
	}
}

// TestAfterUpdate_MapUpdateKeysNormalizedToColumnNames 锁定：Updates(map) 允许用
// Go 字段名，而 ES 文档必须收到数据库列名。去掉 normalizeColumnName，payload 会是
// {"doc":{"Title":...}}，本测试失败。
func TestAfterUpdate_MapUpdateKeysNormalizedToColumnNames(t *testing.T) {
	tests := []struct {
		name    string
		updates map[string]any
	}{
		{name: "go field name", updates: map[string]any{"Title": "after"}},
		{name: "column name", updates: map[string]any{"title": "after"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			if err := syncer.EnableAutoSync(db); err != nil {
				t.Fatalf("enable auto sync: %v", err)
			}

			article := blockerArticle{ID: 1}
			if err := db.Model(&article).Updates(tt.updates).Error; err != nil {
				t.Fatalf("update article: %v", err)
			}

			rows := listOutboxEvents(t, db)
			if len(rows) != 1 {
				t.Fatalf("expected 1 outbox row, got %d", len(rows))
			}
			if rows[0].Action != string(actionUpdate) {
				t.Fatalf("action = %q, want %q", rows[0].Action, actionUpdate)
			}
			if got := string(rows[0].Payload); got != `{"doc":{"title":"after"}}` {
				t.Fatalf("payload = %s, want {\"doc\":{\"title\":\"after\"}}", got)
			}
		})
	}
}

// TestProcessOutboxRow_TransportFailureKeepsRowRetryable 锁定：ES 整体不可达
// （拿不到 HTTP 响应，status == 0）不消耗 MaxAttempts 预算。把判定改回
// row.Attempts < MaxAttempts，本测试会因为行被判成 dead 而失败。
func TestProcessOutboxRow_TransportFailureKeepsRowRetryable(t *testing.T) {
	tests := []struct {
		name       string
		transport  roundTripFunc
		attempts   int
		action     string
		wantStatus string
	}{
		{
			name: "transport failure never exhausts the retry budget",
			transport: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial tcp: connect: connection refused")
			},
			attempts:   99,
			wantStatus: OutboxStatusPending,
		},
		{
			name: "unsupported action is a poison row and must reach dead",
			transport: func(req *http.Request) (*http.Response, error) {
				return jsonResponse(req, http.StatusOK, `{}`), nil
			},
			attempts:   99,
			action:     "bogus",
			wantStatus: OutboxStatusDead,
		},
		{
			name: "http level failure still transitions to dead once attempts are exhausted",
			transport: func(req *http.Request) (*http.Response, error) {
				return jsonResponse(req, http.StatusServiceUnavailable, `{"error":{"reason":"overloaded"}}`), nil
			},
			attempts:   99,
			wantStatus: OutboxStatusDead,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			syncer.es = newTestElasticsearchClient(t, tt.transport)
			syncer.cfg.Outbox.MaxAttempts = 8
			syncer.outbox = newOutboxStore(db)

			action := tt.action
			if action == "" {
				action = string(actionIndex)
			}
			seed := OutboxEvent{
				TableName:  "blocker_articles",
				IndexAlias: "blocker_articles",
				DocumentID: "1",
				Action:     action,
				Payload:    []byte(`{"id":1,"title":"before"}`),
				Status:     OutboxStatusPending,
				Attempts:   tt.attempts,
			}
			if err := db.WithContext(t.Context()).Create(&seed).Error; err != nil {
				t.Fatalf("seed outbox row: %v", err)
			}

			claimed, err := syncer.outbox.claimPending(t.Context(), 1, 30*time.Second)
			if err != nil {
				t.Fatalf("claim pending: %v", err)
			}
			if len(claimed) != 1 {
				t.Fatalf("expected 1 claimed row, got %d", len(claimed))
			}

			outcome, err := syncer.processOutboxRow(t.Context(), claimed[0])
			if err != nil {
				t.Fatalf("process outbox row: %v", err)
			}
			if outcome != tt.wantStatus {
				t.Fatalf("outcome = %q, want %q", outcome, tt.wantStatus)
			}

			var got OutboxEvent
			if err := db.WithContext(t.Context()).First(&got, claimed[0].ID).Error; err != nil {
				t.Fatalf("reload outbox row: %v", err)
			}
			if got.Status != tt.wantStatus {
				t.Fatalf("persisted status = %q, want %q", got.Status, tt.wantStatus)
			}
			if got.LeaseToken != "" {
				t.Fatalf("lease token must be cleared after the row leaves processing, got %q", got.LeaseToken)
			}
		})
	}
}
