package essyncer

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
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

// closeTrackingIndexer 记录 Close 是否被调用，用于验证关停路径不漏 indexer。
type closeTrackingIndexer struct {
	fakeBulkIndexer
	mu       sync.Mutex
	closeHit bool
}

func (c *closeTrackingIndexer) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeHit = true
	return nil
}

func (c *closeTrackingIndexer) closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeHit
}

// TestShutdown_ClosesIndexerEvenWhenRelayWaitTimesOut 锁定：relay 等待超时是最需要
// 兜底释放 indexer 的时刻。把 Shutdown 改回 waitOutboxRelay 失败即 return，
// 本测试会因为 indexer 未关闭而失败。
func TestShutdown_ClosesIndexerEvenWhenRelayWaitTimesOut(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	indexer := &closeTrackingIndexer{}
	syncer := newBlockerTestSyncer(t, db, indexer)
	syncer.outbox = newOutboxStore(db)

	// 让 relay 卡在一次投递里，使 waitOutboxRelay 必然超时
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-release
		return jsonResponse(req, http.StatusOK, `{}`), nil
	}))
	seed := OutboxEvent{
		TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "1",
		Action: string(actionIndex), Payload: []byte(`{"id":1}`), Status: OutboxStatusPending,
	}
	if err := db.WithContext(t.Context()).Create(&seed).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
	if err := syncer.StartOutboxRelay(t.Context()); err != nil {
		t.Fatalf("start relay: %v", err)
	}
	waitUntil(t, time.Second, func() bool {
		var row OutboxEvent
		if err := db.First(&row, seed.ID).Error; err != nil {
			return false
		}
		return row.Status == OutboxStatusProcessing
	}, "relay to claim the row")

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := syncer.Shutdown(ctx)
	if err == nil {
		t.Fatal("expected shutdown to report the relay wait timeout")
	}
	if !indexer.closed() {
		t.Fatal("indexer must be closed even when the relay wait times out")
	}
}

// TestStartOutboxRelay_PanicIsContainedAndSurfacedByHealth 锁定：库自己起的 relay
// goroutine 里的 panic 不能带走进程，而且必须让 Health 开始报错——否则进程活着、
// HTTP 正常，增量同步已经停了却没有任何信号。去掉 recoverRelayPanic，
// 本测试会让整个测试进程崩溃。
func TestStartOutboxRelay_PanicIsContainedAndSurfacedByHealth(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	syncer.outbox = newOutboxStore(db)
	// Health 的 ping 走同一个 transport，只让投递路径 panic。
	syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodHead {
			return jsonResponse(req, http.StatusOK, `{}`), nil
		}
		panic("boom inside the delivery path")
	}))

	seed := OutboxEvent{
		TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "1",
		Action: string(actionIndex), Payload: []byte(`{"id":1}`), Status: OutboxStatusPending,
	}
	if err := db.WithContext(t.Context()).Create(&seed).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}

	if err := syncer.Health(t.Context()); err != nil {
		t.Fatalf("health must be clean before the panic: %v", err)
	}
	if err := syncer.StartOutboxRelay(t.Context()); err != nil {
		t.Fatalf("start relay: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		return syncer.Health(t.Context()) != nil
	}, "health to report the stopped relay")

	healthErr := syncer.Health(t.Context())
	if !strings.Contains(healthErr.Error(), "panicked") {
		t.Fatalf("health error must name the panic, got %v", healthErr)
	}

	var found bool
	for _, failure := range syncer.RecentFailures(10) {
		if strings.Contains(failure.Error, "panicked") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected a failure sample recording the relay panic")
	}
}

// TestRunOutboxRelay_BlocksUntilContextCancel 锁定阻塞版入口的契约：它在调用方的
// goroutine 上运行，ctx 取消后返回；宿主的 worker 管理器据此接管 panic 与关停。
func TestRunOutboxRelay_BlocksUntilContextCancel(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	syncer.outbox = newOutboxStore(db)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- syncer.RunOutboxRelay(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("relay returned before the context was canceled: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("relay returned %v, want nil after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not return after the context was canceled")
	}
}

// TestRunOutboxRelay_PanicPropagatesToCaller 锁定阻塞版入口不吞 panic：
// 它必须冒泡到宿主的 worker 管理器，由宿主按自己的 Required 语义处理。
func TestRunOutboxRelay_PanicPropagatesToCaller(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	syncer := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	syncer.outbox = newOutboxStore(db)
	syncer.es = newTestElasticsearchClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		panic("boom inside the delivery path")
	}))
	seed := OutboxEvent{
		TableName: "blocker_articles", IndexAlias: "blocker_articles", DocumentID: "1",
		Action: string(actionIndex), Payload: []byte(`{"id":1}`), Status: OutboxStatusPending,
	}
	if err := db.WithContext(t.Context()).Create(&seed).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}

	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_ = syncer.RunOutboxRelay(t.Context())
	}()

	select {
	case recovered := <-panicked:
		if recovered == nil {
			t.Fatal("RunOutboxRelay must not swallow the panic")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic did not reach the caller")
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestNew_ValidatesMappingBeforeAllocatingResources 锁定：mapping 校验必须发生在
// 建立 ES 连接与 BulkIndexer 之前。校验一旦排在 indexer 之后，失败时直接 return
// 会把 indexer 的 worker goroutine 漏掉；此处用一个必定连不上的地址反证顺序——
// 若顺序被改回，返回的会是连接错误而不是 mapping 错误。
func TestNew_ValidatesMappingBeforeAllocatingResources(t *testing.T) {
	badMapping := t.TempDir() + "/bad.json"
	if err := os.WriteFile(badMapping, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write mapping: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Elasticsearch.Addresses = []string{"http://127.0.0.1:1"}
	cfg.Elasticsearch.RequestTimeout = 200 * time.Millisecond
	cfg.Sync.Tables = []TableConfig{{Model: "Article", MappingFile: badMapping}}

	_, err := New(openBlockerTestDB(t), cfg)
	if err == nil {
		t.Fatal("expected New to fail on an invalid mapping file")
	}
	if !strings.Contains(err.Error(), "validate mapping") {
		t.Fatalf("mapping must be validated before any connection is made, got %v", err)
	}
}
