package essyncer

import (
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8/esutil"
)

// Metrics 提供运行时可观测指标，全 atomic 无锁。
type Metrics struct {
	EnqueuedTotal       atomic.Int64
	DroppedTotal        atomic.Int64
	FullSyncDocs        atomic.Int64
	FullSyncTables      atomic.Int64
	DeadLetters         atomic.Int64
	SyncEventsBuffered  atomic.Int64
	SyncEventsFlushed   atomic.Int64
	SyncEventsSkippedTx atomic.Int64
	FullSyncErrorsTotal atomic.Int64
}

// MetricsSnapshot 用于 JSON 序列化（Gin 健康检查接口）。
type MetricsSnapshot struct {
	EnqueuedTotal  int64 `json:"enqueued_total"`
	DroppedTotal   int64 `json:"dropped_total"`
	FullSyncDocs   int64 `json:"full_sync_docs"`
	FullSyncTables int64 `json:"full_sync_tables"`
	DeadLetters    int64 `json:"dead_letters"`

	LastFlushAtUnixMs      int64  `json:"last_flush_at_unix_ms"`
	LastFlushDurationMs    int64  `json:"last_flush_duration_ms"`
	LastFlushItems         int64  `json:"last_flush_items"`
	LastErrorAtUnixMs      int64  `json:"last_error_at_unix_ms"`
	LastErrorSource        string `json:"last_error_source"`
	LastErrorIndex         string `json:"last_error_index"`
	LastErrorAction        string `json:"last_error_action"`
	LastErrorDocumentID    string `json:"last_error_document_id"`
	LastErrorText          string `json:"last_error_text"`
	SyncEventsBuffered     int64  `json:"sync_events_buffered"`
	SyncEventsFlushed      int64  `json:"sync_events_flushed"`
	SyncEventsSkippedTx    int64  `json:"sync_events_skipped_tx"`
	FullSyncErrorsTotal    int64  `json:"full_sync_errors_total"`
	FailureSamplesRetained int64  `json:"failure_samples_retained"`

	BulkNumAdded   int64 `json:"bulk_num_added"`
	BulkNumFlushed int64 `json:"bulk_num_flushed"`
	BulkNumFailed  int64 `json:"bulk_num_failed"`
	BulkNumIndexed int64 `json:"bulk_num_indexed"`
	BulkNumUpdated int64 `json:"bulk_num_updated"`
	BulkNumDeleted int64 `json:"bulk_num_deleted"`
}

func (m *Metrics) snapshot(stats esutil.BulkIndexerStats) MetricsSnapshot {
	return MetricsSnapshot{
		EnqueuedTotal:       m.EnqueuedTotal.Load(),
		DroppedTotal:        m.DroppedTotal.Load(),
		FullSyncDocs:        m.FullSyncDocs.Load(),
		FullSyncTables:      m.FullSyncTables.Load(),
		DeadLetters:         m.DeadLetters.Load(),
		SyncEventsBuffered:  m.SyncEventsBuffered.Load(),
		SyncEventsFlushed:   m.SyncEventsFlushed.Load(),
		SyncEventsSkippedTx: m.SyncEventsSkippedTx.Load(),
		FullSyncErrorsTotal: m.FullSyncErrorsTotal.Load(),
		BulkNumAdded:        int64(stats.NumAdded),
		BulkNumFlushed:      int64(stats.NumFlushed),
		BulkNumFailed:       int64(stats.NumFailed),
		BulkNumIndexed:      int64(stats.NumIndexed),
		BulkNumUpdated:      int64(stats.NumUpdated),
		BulkNumDeleted:      int64(stats.NumDeleted),
	}
}
