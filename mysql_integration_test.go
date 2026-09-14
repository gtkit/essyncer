package essyncer

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// mysqlDSNEnv 指向一个可写的测试库。未设置时本文件的集成测试全部跳过。
const mysqlDSNEnv = "ESSYNCER_MYSQL_DSN"

// outboxDDL 按指定的时间列类型建 outbox_events 表。
// 列类型是参数，因为 lease fencing 的正确性必须与 DATETIME 的存储精度无关：
// MySQL 按列精度四舍五入存储时间，Go 侧的微秒时间戳与之相等比较必然失败。
const outboxDDL = `CREATE TABLE outbox_events (
	id BIGINT NOT NULL AUTO_INCREMENT,
	table_name VARCHAR(128) NOT NULL,
	index_alias VARCHAR(128) NOT NULL,
	document_id VARCHAR(191) NOT NULL,
	action VARCHAR(32) NOT NULL,
	payload JSON NOT NULL,
	status VARCHAR(32) NOT NULL,
	attempts INT NOT NULL DEFAULT 0,
	next_retry_at %[1]s NULL,
	last_error TEXT,
	leased_until %[1]s NULL,
	lease_token VARCHAR(32),
	created_at %[1]s NOT NULL,
	sent_at %[1]s NULL,
	PRIMARY KEY (id),
	KEY idx_status (status),
	KEY idx_next_retry (next_retry_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

func openMySQLOutboxDB(t *testing.T, timeColumnType string) *gorm.DB {
	t.Helper()

	dsn := os.Getenv(mysqlDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run MySQL integration tests", mysqlDSNEnv)
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	drop := func() {
		if err := db.Exec("DROP TABLE IF EXISTS outbox_events").Error; err != nil {
			t.Fatalf("drop outbox table: %v", err)
		}
	}
	drop()
	if err := db.Exec(fmt.Sprintf(outboxDDL, timeColumnType)).Error; err != nil {
		t.Fatalf("create outbox table (%s): %v", timeColumnType, err)
	}
	t.Cleanup(drop)
	return db
}

func seedPendingRow(t *testing.T, db *gorm.DB, docID string) OutboxEvent {
	t.Helper()

	row := OutboxEvent{
		TableName:  "articles",
		IndexAlias: "articles",
		DocumentID: docID,
		Action:     string(actionIndex),
		Payload:    []byte(`{"id":1,"title":"mysql"}`),
		Status:     OutboxStatusPending,
	}
	if err := db.WithContext(t.Context()).Create(&row).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
	return row
}

// TestMySQLOutboxLease_SurvivesDateTimeColumnPrecision 是 lease fencing 的反证测试：
// 只要 fencing 判定回到 leased_until 的相等比较，claim 之后的每一次
// markSent / markRetry / markDead 都会在 MySQL 上报 "lease mismatch"，
// 行永远停在 processing，relay 陷入「重复投递 → 标不上 → lease 过期 → 再投」的死循环。
// sqlite 把时间存成全精度文本，测不出这个问题，所以这条用例必须跑在 MySQL 上。
func TestMySQLOutboxLease_SurvivesDateTimeColumnPrecision(t *testing.T) {
	timeColumnTypes := []struct {
		name   string
		column string
	}{
		{name: "datetime without fractional seconds (hand-written DDL)", column: "DATETIME"},
		{name: "datetime(3) (gorm AutoMigrate default)", column: "DATETIME(3)"},
		{name: "datetime(6) (microsecond precision)", column: "DATETIME(6)"},
	}

	transitions := []struct {
		name       string
		apply      func(*testing.T, *outboxStore, OutboxEvent) error
		wantStatus string
	}{
		{
			name: "markSent",
			apply: func(t *testing.T, store *outboxStore, row OutboxEvent) error {
				t.Helper()
				return store.markSent(t.Context(), row)
			},
			wantStatus: OutboxStatusSent,
		},
		{
			name: "markRetry",
			apply: func(t *testing.T, store *outboxStore, row OutboxEvent) error {
				t.Helper()
				return store.markRetry(t.Context(), row, time.Now().UTC().Add(time.Minute), fmt.Errorf("es unavailable"))
			},
			wantStatus: OutboxStatusPending,
		},
		{
			name: "markDead",
			apply: func(t *testing.T, store *outboxStore, row OutboxEvent) error {
				t.Helper()
				return store.markDead(t.Context(), row, fmt.Errorf("mapping rejected"))
			},
			wantStatus: OutboxStatusDead,
		},
	}

	for _, tc := range timeColumnTypes {
		for _, tr := range transitions {
			t.Run(tc.name+"/"+tr.name, func(t *testing.T) {
				db := openMySQLOutboxDB(t, tc.column)
				store := newOutboxStore(db)
				seedPendingRow(t, db, "1")

				claimed, err := store.claimPending(t.Context(), 1, 30*time.Second)
				if err != nil {
					t.Fatalf("claim pending: %v", err)
				}
				if len(claimed) != 1 {
					t.Fatalf("expected 1 claimed row, got %d", len(claimed))
				}
				if claimed[0].LeaseToken == "" {
					t.Fatal("claim must hand back a lease token")
				}

				if err := tr.apply(t, store, claimed[0]); err != nil {
					t.Fatalf("%s on a %s column: %v", tr.name, tc.column, err)
				}

				var got OutboxEvent
				if err := db.WithContext(t.Context()).First(&got, claimed[0].ID).Error; err != nil {
					t.Fatalf("reload row: %v", err)
				}
				if got.Status != tr.wantStatus {
					t.Fatalf("status = %q, want %q", got.Status, tr.wantStatus)
				}
				if got.LeaseToken != "" {
					t.Fatalf("lease token must be cleared once the row leaves processing, got %q", got.LeaseToken)
				}
			})
		}
	}
}

// TestMySQLOutboxLease_StaleTokenCannotWriteBack 锁定 fencing 的另一半：
// 租约被另一个 relay 接管后，旧持有者的 token 必须再也改不动这一行。
func TestMySQLOutboxLease_StaleTokenCannotWriteBack(t *testing.T) {
	db := openMySQLOutboxDB(t, "DATETIME")
	store := newOutboxStore(db)
	seedPendingRow(t, db, "1")

	claimed, claimErr := store.claimPending(t.Context(), 1, 30*time.Second)
	if claimErr != nil {
		t.Fatalf("claim pending: %v", claimErr)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed row, got %d", len(claimed))
	}

	if err := db.WithContext(t.Context()).Model(&OutboxEvent{}).
		Where("id = ?", claimed[0].ID).
		Updates(map[string]any{
			"leased_until": time.Now().UTC().Add(time.Minute),
			"lease_token":  newLeaseToken(),
		}).Error; err != nil {
		t.Fatalf("simulate lease handoff: %v", err)
	}

	err := store.markSent(t.Context(), claimed[0])
	if err == nil {
		t.Fatal("expected the stale lease token to be rejected")
	}
	if !strings.Contains(err.Error(), "lease mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}

	var got OutboxEvent
	if err := db.WithContext(t.Context()).First(&got, claimed[0].ID).Error; err != nil {
		t.Fatalf("reload row: %v", err)
	}
	if got.Status != OutboxStatusProcessing {
		t.Fatalf("status = %q, want %q", got.Status, OutboxStatusProcessing)
	}
}

// TestMySQLOutboxClaim_ConcurrentRelaysClaimDisjointRows 验证多实例 relay 在真实
// MySQL 上不会把同一行 claim 两次。
func TestMySQLOutboxClaim_ConcurrentRelaysClaimDisjointRows(t *testing.T) {
	db := openMySQLOutboxDB(t, "DATETIME(3)")

	const rowCount = 20
	for i := range rowCount {
		seedPendingRow(t, db, fmt.Sprint(i+1))
	}

	const relayCount = 4
	var (
		mu      sync.Mutex
		claimed []OutboxEvent
		wg      sync.WaitGroup
	)
	for range relayCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store := newOutboxStore(db)
			rows, err := store.claimPending(t.Context(), rowCount, 30*time.Second)
			if err != nil {
				t.Errorf("claim pending: %v", err)
				return
			}
			mu.Lock()
			claimed = append(claimed, rows...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	seen := make(map[int64]string, len(claimed))
	for _, row := range claimed {
		if previous, duplicate := seen[row.ID]; duplicate {
			t.Fatalf("row %d claimed twice (tokens %s and %s)", row.ID, previous, row.LeaseToken)
		}
		seen[row.ID] = row.LeaseToken
	}
	if len(seen) != rowCount {
		t.Fatalf("claimed %d distinct rows, want %d", len(seen), rowCount)
	}
}

// TestMySQLEndToEnd_CallbackToRelayDelivery 在真实 MySQL 上跑完整链路：
// 业务写 → callback 在同事务写 outbox → relay 认领 → 投递 ES → 标记 sent。
// 这是 P0 卡死问题的端到端形态：fencing 一旦依赖 DATETIME 精度，
// 投递会成功但 markSent 失败，行停在 processing，Remaining 不归零。
func TestMySQLEndToEnd_CallbackToRelayDelivery(t *testing.T) {
	db := openMySQLOutboxDB(t, "DATETIME")
	if err := db.Exec("DROP TABLE IF EXISTS blocker_articles").Error; err != nil {
		t.Fatalf("drop article table: %v", err)
	}
	if err := db.AutoMigrate(&blockerArticle{}); err != nil {
		t.Fatalf("migrate article table: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Exec("DROP TABLE IF EXISTS blocker_articles").Error; err != nil {
			t.Fatalf("drop article table: %v", err)
		}
	})

	var (
		mu       sync.Mutex
		requests []string
	)
	syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		requests = append(requests, req.Method+" "+req.URL.Path)
		mu.Unlock()
		return jsonResponse(req, http.StatusOK, `{"result":"updated"}`), nil
	}))
	syncer.outbox = newOutboxStore(db)
	if err := syncer.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync: %v", err)
	}

	article := blockerArticle{ID: 1, Title: "created"}
	if err := db.WithContext(t.Context()).Create(&article).Error; err != nil {
		t.Fatalf("create article: %v", err)
	}
	if err := db.WithContext(t.Context()).Model(&article).Update("title", "updated").Error; err != nil {
		t.Fatalf("update article: %v", err)
	}
	if err := db.WithContext(t.Context()).Delete(&article).Error; err != nil {
		t.Fatalf("delete article: %v", err)
	}

	var pending int64
	if err := db.WithContext(t.Context()).Model(&OutboxEvent{}).
		Where("status = ?", OutboxStatusPending).Count(&pending).Error; err != nil {
		t.Fatalf("count pending rows: %v", err)
	}
	if pending != 3 {
		t.Fatalf("pending outbox rows = %d, want 3 (create, update, soft delete)", pending)
	}

	result, err := syncer.DrainOutbox(t.Context(), 0)
	if err != nil {
		t.Fatalf("drain outbox: %v", err)
	}
	if result.Processed != 3 || result.Remaining != 0 || result.Dead != 0 {
		t.Fatalf("drain result = %+v, want processed=3 remaining=0 dead=0", result)
	}

	var rows []OutboxEvent
	if err := db.WithContext(t.Context()).Order("id ASC").Find(&rows).Error; err != nil {
		t.Fatalf("list outbox rows: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("outbox rows = %d, want 3", len(rows))
	}
	for _, row := range rows {
		if row.Status != OutboxStatusSent {
			t.Fatalf("row %d status = %q, want %q (last_error=%q)", row.ID, row.Status, OutboxStatusSent, row.LastError)
		}
		if row.SentAt == nil {
			t.Fatalf("row %d has no sent_at", row.ID)
		}
		if row.LeaseToken != "" {
			t.Fatalf("row %d still holds a lease token", row.ID)
		}
		if row.DocumentID != "1" {
			t.Fatalf("row %d document id = %q, want \"1\"", row.ID, row.DocumentID)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 3 {
		t.Fatalf("elasticsearch requests = %v, want 3", requests)
	}
}
