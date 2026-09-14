package essyncer

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const defaultFailureBufferSize = 128

const (
	failureSourceCallback = "callback"
	failureSourceFullSync = "fullsync"
	failureSourceOps      = "ops"
	failureSourceRelay    = "relay"
	failureSourceDead     = "relay_dead"
	failureSourceTxSkip   = "tx_skip"
	// failureSourceUnidentified 标记一次数据库写因为无法确定受影响行的主键而未产生同步事件。
	failureSourceUnidentified = "unidentified_rows"
	actionSkip                = "skip"
)

type FailureEvent struct {
	At         time.Time `json:"at"`
	Source     string    `json:"source"`
	Index      string    `json:"index"`
	Action     string    `json:"action"`
	DocumentID string    `json:"document_id"`
	Status     int       `json:"status"`
	Error      string    `json:"error"`
	Retryable  bool      `json:"retryable"`
}

type failureRecorder struct {
	mu      sync.Mutex
	buf     []FailureEvent
	next    int
	count   int
	last    FailureEvent
	hasLast bool
}

func newFailureRecorder(size int) *failureRecorder {
	if size <= 0 {
		size = defaultFailureBufferSize
	}
	return &failureRecorder{buf: make([]FailureEvent, size)}
}

func (r *failureRecorder) record(event FailureEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.buf) == 0 {
		return
	}

	r.buf[r.next] = event
	r.next = (r.next + 1) % len(r.buf)
	if r.count < len(r.buf) {
		r.count++
	}
	r.last = event
	r.hasLast = true
}

func (r *failureRecorder) recent(limit int) []FailureEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return nil
	}
	if limit <= 0 || limit > r.count {
		limit = r.count
	}

	out := make([]FailureEvent, 0, limit)
	for i := range limit {
		idx := r.next - 1 - i
		if idx < 0 {
			idx += len(r.buf)
		}
		out = append(out, r.buf[idx])
	}
	return out
}

func (r *failureRecorder) summary() (last FailureEvent, hasLast bool, retained int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.hasLast, r.count
}

type flushSummary struct {
	mu           sync.Mutex
	lastAt       time.Time
	lastDuration time.Duration
	lastItems    int64
}

func (f *flushSummary) snapshot() (time.Time, time.Duration, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastAt, f.lastDuration, f.lastItems
}

func (s *Syncer) initObservability() {
	s.failureInit.Do(func() {
		s.failures = newFailureRecorder(s.failureBufferSize)
	})
}

func (s *Syncer) RecentFailures(limit int) []FailureEvent {
	s.initObservability()
	return s.failures.recent(limit)
}

func (s *Syncer) recordFailure(event FailureEvent) {
	if event.At.IsZero() {
		event.At = time.Now()
	}

	s.initObservability()
	s.failures.record(event)

	if event.Source == failureSourceFullSync {
		s.metrics.FullSyncErrorsTotal.Add(1)
	}

	if s.failureHook != nil {
		s.failureHook(event)
	}
}

func classifyRetryable(status int) bool {
	switch status {
	case 429, 502, 503, 504:
		return true
	default:
		return false
	}
}

// recordUnidentifiedRows 记录一次被跳过的写操作：数据库已经写入，但 callback 无法
// 确定受影响行的主键（批量 UPDATE / DELETE 在 callback 里只能看到零值 model），
// 因此没有产生 ES 同步事件。这里不回写 db.Error——同步侧的缺口不应该把业务写变成失败；
// 缺口通过 metrics、失败样本与告警日志暴露，由 FullSyncTable 或 EnqueueDocumentUpdate 补齐。
func (s *Syncer) recordUnidentifiedRows(entry *modelEntry, action actionType) {
	s.metrics.SyncEventsSkippedUnidentified.Add(1)
	s.recordFailure(FailureEvent{
		Source: failureSourceUnidentified,
		Index:  entry.indexName,
		Action: string(action),
		Error: fmt.Sprintf(
			"table %s: bulk %s without loaded primary keys; the database write succeeded but no ES sync event was produced. "+
				"Write by primary key, or run FullSyncTable / EnqueueDocumentUpdate for the affected rows.",
			entry.tableName, action),
	})
	s.logger.Warn("essyncer: write skipped, affected primary keys unknown",
		zap.String("table", entry.tableName),
		zap.String("index", entry.indexName),
		zap.String("action", string(action)),
	)
}
