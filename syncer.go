package essyncer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type actionType string

const (
	actionIndex  actionType = "index"
	actionUpdate actionType = "update"
	actionDelete actionType = "delete"
)

// Syncer 是 GORM → ES9 数据同步器。
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
	stopped           atomic.Bool
	newBulkIndexer    func(esutil.BulkIndexerConfig) (esutil.BulkIndexer, error)
	outbox            *outboxStore
	relayMu           sync.Mutex
	relayCancel       context.CancelFunc
	relayWG           sync.WaitGroup
	relayRunning      bool
	relayPanic        atomic.Pointer[string]
}

// New 创建并启动 Syncer。
//
//nolint:cyclop // Constructor setup validates several independent configuration branches and keeps the exported API stable.
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
	effectiveCfg := s.cfg

	// mapping 校验只读本地文件，放在建立任何连接与 BulkIndexer 之前：
	// 之前它在 indexer 创建之后，校验失败直接 return 会把 indexer 的 worker
	// goroutine 连同它持有的连接一起漏掉。
	for _, tc := range effectiveCfg.Sync.Tables {
		if tc.MappingFile != "" {
			if _, verr := LoadMappingFromFile(tc.MappingFile); verr != nil {
				return nil, fmt.Errorf("essyncer: validate mapping %s: %w", tc.Model, verr)
			}
		}
	}

	// --- 构建 ES9 客户端配置 ---
	// v9 把 Config 与 NewClient / NewTypedClient 标记为 Deprecated，改推函数式选项，
	// 但其废弃说明写明这些仍完全可用。切换构造器属于独立改动，不夹进本次主线升级。
	//nolint:staticcheck // SA1019: v9 的 Config 仍完全可用，构造器迁移另行处理
	esCfg := elasticsearch.Config{
		Addresses:     effectiveCfg.Elasticsearch.Addresses,
		RetryOnStatus: effectiveCfg.Elasticsearch.RetryOnStatus,
		MaxRetries:    effectiveCfg.Elasticsearch.MaxRetries,
	}
	if effectiveCfg.Elasticsearch.Username != "" {
		esCfg.Username = effectiveCfg.Elasticsearch.Username
		esCfg.Password = effectiveCfg.Elasticsearch.Password
	}
	// ES9 TLS 证书支持
	if effectiveCfg.Elasticsearch.CACert != "" {
		cert, err := os.ReadFile(effectiveCfg.Elasticsearch.CACert)
		if err != nil {
			return nil, fmt.Errorf("essyncer: read ca cert: %w", err)
		}
		esCfg.CACert = cert
	}
	transport, err := newElasticsearchTransport(effectiveCfg.Elasticsearch)
	if err != nil {
		return nil, err
	}
	esCfg.Transport = transport

	// 创建低级客户端
	//nolint:staticcheck // SA1019: 同上，与 Config 一并迁移
	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("essyncer: create es client: %w", err)
	}
	// 创建 TypedClient（用于搜索 DSL）
	//nolint:staticcheck // SA1019: 同上，与 Config 一并迁移
	typedClient, err := elasticsearch.NewTypedClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("essyncer: create typed client: %w", err)
	}

	// Ping 验证
	infoCtx, cancel := newStartupContext(effectiveCfg.Elasticsearch.RequestTimeout)
	defer cancel()

	res, err := client.Info(client.Info.WithContext(infoCtx))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("essyncer: es info timeout: %w", err)
		}
		return nil, fmt.Errorf("essyncer: es info: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return nil, fmt.Errorf("essyncer: es error: %s", res.String())
	}
	s.es = client
	s.typed = typedClient

	s.logger.Info("essyncer: es9 connected", zap.String("status", res.Status()))

	// --- 创建 BulkIndexer ---
	workers := max(effectiveCfg.Sync.Workers, 1)
	flushBytes := effectiveCfg.Sync.FlushBytes
	if flushBytes <= 0 {
		flushBytes = 5 << 20
	}
	flushInterval := effectiveCfg.Sync.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 2 * time.Second
	}

	indexer, err := s.newBulkIndexer(esutil.BulkIndexerConfig{
		Client:        client,
		NumWorkers:    workers,
		FlushBytes:    flushBytes,
		FlushInterval: flushInterval,
		OnError: func(_ context.Context, err error) {
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

	s.logger.Info("essyncer: initialized",
		zap.Int("workers", workers),
		zap.Int("flush_bytes", flushBytes),
		zap.Duration("flush_interval", flushInterval),
	)
	return s, nil
}

func newElasticsearchTransport(cfg ESConfig) (*http.Transport, error) {
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("essyncer: unexpected default transport type %T", http.DefaultTransport)
	}
	transport := baseTransport.Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.TLSHandshakeTimeout = timeout
	transport.ResponseHeaderTimeout = timeout

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.AllowInsecureTLS {
		for _, addr := range cfg.Addresses {
			if strings.HasPrefix(addr, "https://") {
				tlsConfig.InsecureSkipVerify = true
				break
			}
		}
	}
	transport.TLSClientConfig = tlsConfig

	return transport, nil
}

func newStartupContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
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

func (s *Syncer) ensureOutboxStore() error {
	if s.outbox != nil {
		return nil
	}
	if s.db == nil {
		return errors.New("essyncer: nil db")
	}
	s.outbox = newOutboxStore(s.db)
	return nil
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

	return snapshot
}

func (s *Syncer) Health(ctx context.Context) error {
	if s.stopped.Load() {
		return errors.New("essyncer: stopped")
	}
	if message := s.relayPanic.Load(); message != nil {
		return errors.New("essyncer: " + *message)
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
	indexer, err := esutil.NewBulkIndexer(cfg)
	if err != nil {
		return nil, fmt.Errorf("essyncer: new bulk indexer: %w", err)
	}
	return indexer, nil
}

// Shutdown 停止 relay 并关闭 indexer。两步都会执行：relay 等待超时是最需要
// 兜底释放 indexer 的时候，提前 return 会把它的 worker goroutine 留在原地。
// 多次调用是安全的。
func (s *Syncer) Shutdown(ctx context.Context) error {
	s.stopped.Store(true)
	s.stopOutboxRelay()
	waitErr := s.waitOutboxRelay(ctx)

	if s.indexer == nil {
		return waitErr
	}

	closeErr := s.indexer.Close(ctx)
	if closeErr != nil {
		s.logger.Error("essyncer: close indexer", zap.Error(closeErr))
		closeErr = fmt.Errorf("essyncer: shutdown: %w", closeErr)
	} else {
		stats := s.indexer.Stats()
		s.logger.Info("essyncer: shutdown done",
			zap.Uint64("flushed", stats.NumFlushed),
			zap.Uint64("failed", stats.NumFailed),
		)
	}
	return errors.Join(waitErr, closeErr)
}

// --- Index 管理 ---

// EnsureIndex 为已注册的模型确保 alias 与底层物理索引存在。
func (s *Syncer) EnsureIndex(ctx context.Context, model Syncable) error {
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return errors.New("essyncer: model not registered")
	}
	return s.ensureIndexEntry(ctx, entry)
}

func (s *Syncer) ensureIndexEntry(ctx context.Context, entry *modelEntry) error {
	state, err := s.resolveAliasState(ctx, entry.indexName)
	if err != nil {
		return err
	}
	if len(state.targets) > 0 {
		return nil
	}

	bootstrapIndex := entry.newBootstrapIndexName(time.Now().UTC())
	return s.createManagedIndex(ctx, entry, bootstrapIndex, entry.indexName)
}

func (s *Syncer) EnsureAllIndices(ctx context.Context) error {
	var firstErr error
	s.registry.forEach(func(tableName string, entry *modelEntry) bool {
		if err := s.ensureIndexEntry(ctx, entry); err != nil {
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
