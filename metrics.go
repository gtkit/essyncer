package essyncer

import (
	"math"
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8/esutil"
)

// Metrics 提供运行时可观测指标，全 atomic 无锁。
type Metrics struct {
	EnqueuedTotal         atomic.Int64
	DroppedTotal          atomic.Int64
	FullSyncDocs          atomic.Int64
	FullSyncTables        atomic.Int64
	DeadLetters           atomic.Int64
	OutboxEventsPersisted atomic.Int64
	RelayProcessedTotal   atomic.Int64
	RelayRetriesTotal     atomic.Int64
	RelayDeadTotal        atomic.Int64
	SyncEventsBuffered    atomic.Int64
	SyncEventsFlushed     atomic.Int64
	SyncEventsSkippedTx   atomic.Int64
	FullSyncErrorsTotal   atomic.Int64
}

// MetricsSnapshot 用于 JSON 序列化（Gin 健康检查接口）。
type MetricsSnapshot struct {
	EnqueuedTotal         int64 `json:"enqueued_total"`
	DroppedTotal          int64 `json:"dropped_total"`
	FullSyncDocs          int64 `json:"full_sync_docs"`
	FullSyncTables        int64 `json:"full_sync_tables"`
	DeadLetters           int64 `json:"dead_letters"`
	OutboxEventsPersisted int64 `json:"outbox_events_persisted"`
	RelayProcessedTotal   int64 `json:"relay_processed_total"`
	RelayRetriesTotal     int64 `json:"relay_retries_total"`
	RelayDeadTotal        int64 `json:"relay_dead_total"`

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
		EnqueuedTotal:         m.EnqueuedTotal.Load(),
		DroppedTotal:          m.DroppedTotal.Load(),
		FullSyncDocs:          m.FullSyncDocs.Load(),
		FullSyncTables:        m.FullSyncTables.Load(),
		DeadLetters:           m.DeadLetters.Load(),
		OutboxEventsPersisted: m.OutboxEventsPersisted.Load(),
		RelayProcessedTotal:   m.RelayProcessedTotal.Load(),
		RelayRetriesTotal:     m.RelayRetriesTotal.Load(),
		RelayDeadTotal:        m.RelayDeadTotal.Load(),
		SyncEventsBuffered:    m.SyncEventsBuffered.Load(),
		SyncEventsFlushed:     m.SyncEventsFlushed.Load(),
		SyncEventsSkippedTx:   m.SyncEventsSkippedTx.Load(),
		FullSyncErrorsTotal:   m.FullSyncErrorsTotal.Load(),
		BulkNumAdded:          clampUint64ToInt64(stats.NumAdded),
		BulkNumFlushed:        clampUint64ToInt64(stats.NumFlushed),
		BulkNumFailed:         clampUint64ToInt64(stats.NumFailed),
		BulkNumIndexed:        clampUint64ToInt64(stats.NumIndexed),
		BulkNumUpdated:        clampUint64ToInt64(stats.NumUpdated),
		BulkNumDeleted:        clampUint64ToInt64(stats.NumDeleted),
	}
}

func clampUint64ToInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}
