package essyncer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type actionType string

const (
	actionIndex  actionType = "index"
	actionUpdate actionType = "update"
	actionDelete actionType = "delete"
)

// Syncer 是 GORM → ES8 数据同步器。
// 使用 esutil.BulkIndexer 作为写入引擎（自带 worker pool + flush + 重试）。
type Syncer struct {
	cfg               Config
	db                *gorm.DB
	es                *elasticsearch.Client      // 低级 API（BulkIndexer、Index 管理）
	typed             *elasticsearch.TypedClient // TypedAPI（搜索 DSL）
	indexer           esutil.BulkIndexer
	logger            Logger
	registry          *modelRegistry
	metrics           Metrics
	failures          *failureRecorder
	failureHook       func(FailureEvent)
	failureBufferSize int
	failureInit       sync.Once
	flushState        flushSummary
	stopped           atomic.Bool
	newBulkIndexer    func(esutil.BulkIndexerConfig) (esutil.BulkIndexer, error)
	outbox            *outboxStore
	relayMu           sync.Mutex
	relayCtx          context.Context
	relayCancel       context.CancelFunc
	relayWG           sync.WaitGroup
	relayRunning      bool
}

// New 创建并启动 Syncer。
func New(db *gorm.DB, cfg Config, opts ...Option) (*Syncer, error) {
	cfg = normalizeOutboxConfig(cfg)
	s := &Syncer{
		cfg:      cfg,
		db:       db,
		registry: newModelRegistry(),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = defaultLogger()
	}
	s.initObservability()
	if s.newBulkIndexer == nil {
		s.newBulkIndexer = esutil.NewBulkIndexer
	}
	s.cfg = normalizeOutboxConfig(s.cfg)
	s.outbox = newOutboxStore(s.db)

	// --- 构建 ES8 客户端配置 ---
	esCfg := elasticsearch.Config{
		Addresses:     cfg.Elasticsearch.Addresses,
		RetryOnStatus: cfg.Elasticsearch.RetryOnStatus,
		MaxRetries:    cfg.Elasticsearch.MaxRetries,
	}
	if cfg.Elasticsearch.Username != "" {
		esCfg.Username = cfg.Elasticsearch.Username
		esCfg.Password = cfg.Elasticsearch.Password
	}
	// ES8 TLS 证书支持
	if cfg.Elasticsearch.CACert != "" {
		cert, err := os.ReadFile(cfg.Elasticsearch.CACert)
		if err != nil {
			return nil, fmt.Errorf("essyncer: read ca cert: %w", err)
		}
		esCfg.CACert = cert
	}
	// 仅在显式允许时跳过 TLS 校验。
	if esCfg.CACert == nil && cfg.Elasticsearch.AllowInsecureTLS {
		for _, addr := range cfg.Elasticsearch.Addresses {
			if strings.HasPrefix(addr, "https://") {
				esCfg.Transport = &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				}
				break
			}
		}
	}

	// 创建低级客户端
	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("essyncer: create es client: %w", err)
	}
	// 创建 TypedClient（用于搜索 DSL）
	typedClient, err := elasticsearch.NewTypedClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("essyncer: create typed client: %w", err)
	}

	// Ping 验证
	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("essyncer: es info: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return nil, fmt.Errorf("essyncer: es error: %s", res.String())
	}
	s.es = client
	s.typed = typedClient

	s.logger.Info("essyncer: es8 connected", zap.String("status", res.Status()))

	// --- 创建 BulkIndexer ---
	workers := max(cfg.Sync.Workers, 1)
	flushBytes := cfg.Sync.FlushBytes
	if flushBytes <= 0 {
		flushBytes = 5 << 20
	}
	flushInterval := cfg.Sync.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 2 * time.Second
	}

	indexer, err := s.newBulkIndexer(esutil.BulkIndexerConfig{
		Client:        client,
		NumWorkers:    workers,
		FlushBytes:    flushBytes,
		FlushInterval: flushInterval,
		OnError: func(ctx context.Context, err error) {
			s.recordFailure(FailureEvent{
				Source: failureSourceCallback,
				Error:  err.Error(),
			})
			s.logger.Error("essyncer: bulk indexer error", zap.Error(err))
		},
	})
	if err != nil {
		return nil, fmt.Errorf("essyncer: create bulk indexer: %w", err)
	}
	s.indexer = indexer

	// 校验 mapping 文件
	for _, tc := range cfg.Sync.Tables {
		if tc.MappingFile != "" {
			if _, verr := LoadMappingFromFile(tc.MappingFile); verr != nil {
				return nil, fmt.Errorf("essyncer: validate mapping %s: %w", tc.Model, verr)
			}
		}
	}

	s.logger.Info("essyncer: initialized",
		zap.Int("workers", workers),
		zap.Int("flush_bytes", flushBytes),
		zap.Duration("flush_interval", flushInterval),
	)
	return s, nil
}

func normalizeOutboxConfig(cfg Config) Config {
	if cfg.Outbox.PollInterval <= 0 {
		cfg.Outbox.PollInterval = 2 * time.Second
	}
	if cfg.Outbox.BatchSize <= 0 {
		cfg.Outbox.BatchSize = 100
	}
	if cfg.Outbox.MaxAttempts <= 0 {
		cfg.Outbox.MaxAttempts = 8
	}
	if cfg.Outbox.Lease <= 0 {
		cfg.Outbox.Lease = 30 * time.Second
	}
	return cfg
}

func (s *Syncer) Register(model Syncable, opts ...RegisterOption) error {
	return s.registry.register(s.db, model, s.cfg.Sync.DefaultBatchSize, opts...)
}

func (s *Syncer) Unregister(model Syncable) { s.registry.unregister(s.db, model) }

// ESClient 返回低级 ES 客户端。
func (s *Syncer) ESClient() *elasticsearch.Client { return s.es }

// TypedClient 返回 TypedAPI 客户端（用于搜索 DSL）。
func (s *Syncer) TypedClient() *elasticsearch.TypedClient { return s.typed }

func (s *Syncer) GetMetrics() MetricsSnapshot {
	s.initObservability()

	var stats esutil.BulkIndexerStats
	if s.indexer != nil {
		stats = s.indexer.Stats()
	}

	snapshot := s.metrics.snapshot(stats)
	lastFailure, hasLastFailure, retained := s.failures.summary()
	if hasLastFailure {
		snapshot.LastErrorAtUnixMs = lastFailure.At.UnixMilli()
		snapshot.LastErrorSource = lastFailure.Source
		snapshot.LastErrorIndex = lastFailure.Index
		snapshot.LastErrorAction = lastFailure.Action
		snapshot.LastErrorDocumentID = lastFailure.DocumentID
		snapshot.LastErrorText = lastFailure.Error
	}
	snapshot.FailureSamplesRetained = int64(retained)

	lastFlushAt, lastFlushDuration, lastFlushItems := s.flushState.snapshot()
	if !lastFlushAt.IsZero() {
		snapshot.LastFlushAtUnixMs = lastFlushAt.UnixMilli()
		snapshot.LastFlushDurationMs = lastFlushDuration.Milliseconds()
		snapshot.LastFlushItems = lastFlushItems
	}

	return snapshot
}

func (s *Syncer) Health(ctx context.Context) error {
	if s.stopped.Load() {
		return fmt.Errorf("essyncer: stopped")
	}
	res, err := s.es.Ping(s.es.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("essyncer: ping: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return fmt.Errorf("essyncer: unhealthy: %s", res.Status())
	}
	return nil
}

func (s *Syncer) makeBulkIndexer(cfg esutil.BulkIndexerConfig) (esutil.BulkIndexer, error) {
	if s.newBulkIndexer != nil {
		return s.newBulkIndexer(cfg)
	}
	return esutil.NewBulkIndexer(cfg)
}

func (s *Syncer) Shutdown(ctx context.Context) error {
	s.stopped.Store(true)
	s.stopOutboxRelay()
	if err := s.waitOutboxRelay(ctx); err != nil {
		return err
	}
	if s.indexer == nil {
		return nil
	}
	if err := s.indexer.Close(ctx); err != nil {
		s.logger.Error("essyncer: close indexer", zap.Error(err))
		return fmt.Errorf("essyncer: shutdown: %w", err)
	}
	stats := s.indexer.Stats()
	s.logger.Info("essyncer: shutdown done",
		zap.Uint64("flushed", stats.NumFlushed),
		zap.Uint64("failed", stats.NumFailed),
	)
	return nil
}

// --- Index 管理 ---

func (s *Syncer) EnsureIndex(ctx context.Context, entry *modelEntry) error {
	state, err := s.resolveAliasState(ctx, entry.indexName)
	if err != nil {
		return err
	}
	if len(state.targets) > 0 {
		return nil
	}

	bootstrapIndex := entry.newBootstrapIndexName(time.Now().UTC())
	if err := s.createManagedIndex(ctx, entry, bootstrapIndex, entry.indexName); err != nil {
		return err
	}
	return nil
}

func (s *Syncer) EnsureAllIndices(ctx context.Context) error {
	var firstErr error
	s.registry.forEach(func(tableName string, entry *modelEntry) bool {
		if err := s.EnsureIndex(ctx, entry); err != nil {
			s.logger.Error("essyncer: ensure index", zap.String("table", tableName), zap.Error(err))
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

// --- 配置注册 ---

func (s *Syncer) RegisterFromConfig(models map[string]Syncable) error {
	for _, tc := range s.cfg.Sync.Tables {
		model, ok := models[tc.Model]
		if !ok {
			s.logger.Warn("essyncer: model not found", zap.String("model", tc.Model))
			continue
		}
		var opts []RegisterOption
		if tc.IndexName != "" {
			opts = append(opts, WithIndexName(tc.IndexName))
		}
		if tc.MappingFile != "" {
			opts = append(opts, WithMappingFile(tc.MappingFile))
		}
		if tc.BatchSize > 0 {
			opts = append(opts, WithBatchSize(tc.BatchSize))
		}
		if tc.AutoSync != nil {
			opts = append(opts, WithAutoSync(*tc.AutoSync))
		}
		if tc.SoftDeleteMode != "" {
			opts = append(opts, WithSoftDeleteMode(tc.SoftDeleteMode))
		}
		opts = append(opts, WithFullDocUpdate(tc.FullDocUpdate))

		if err := s.Register(model, opts...); err != nil {
			return fmt.Errorf("essyncer: register %s: %w", tc.Model, err)
		}
	}
	return nil
}

func (s *Syncer) GetTableConfig(modelName string) (TableConfig, bool) {
	for _, tc := range s.cfg.Sync.Tables {
		if tc.Model == modelName {
			return tc, true
		}
	}
	return TableConfig{}, false
}
