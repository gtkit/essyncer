package essyncer

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
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
}

// New 创建并启动 Syncer。
func New(db *gorm.DB, cfg Config, opts ...Option) (*Syncer, error) {
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
	defer res.Body.Close()
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
	defer res.Body.Close()
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

// enqueue 添加操作到 BulkIndexer，带 copier 深拷贝。
func (s *Syncer) enqueue(ctx context.Context, source string, action actionType, indexName, docID string, doc any) {
	if s.stopped.Load() {
		return
	}
	s.metrics.EnqueuedTotal.Add(1)

	if doc != nil {
		if copied, err := deepCopyModel(doc); err == nil {
			doc = copied
		}
	}

	item := esutil.BulkIndexerItem{
		Index:      indexName,
		Action:     string(action),
		DocumentID: docID,
		OnFailure: func(_ context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
			s.metrics.DeadLetters.Add(1)
			reason := resp.Error.Reason
			if err != nil {
				reason = err.Error()
			}
			s.recordFailure(FailureEvent{
				Source:     source,
				Index:      item.Index,
				Action:     item.Action,
				DocumentID: item.DocumentID,
				Status:     resp.Status,
				Error:      reason,
				Retryable:  classifyRetryable(resp.Status),
			})
			s.logger.Error("essyncer: bulk item failed",
				zap.String("index", item.Index),
				zap.String("id", item.DocumentID),
				zap.Int("status", resp.Status),
				zap.String("error", resp.Error.Reason),
			)
		},
	}

	if doc != nil {
		var data []byte
		var marshalErr error

		switch action {
		case actionUpdate:
			data, marshalErr = json.Marshal(map[string]any{"doc": doc})
		default:
			data, marshalErr = json.Marshal(doc)
		}

		if marshalErr != nil {
			s.metrics.DroppedTotal.Add(1)
			s.recordFailure(FailureEvent{
				Source:     source,
				Index:      indexName,
				Action:     string(action),
				DocumentID: docID,
				Error:      marshalErr.Error(),
			})
			s.logger.Error("essyncer: marshal", zap.Error(marshalErr))
			return
		}
		item.Body = bytes.NewReader(data)
	}

	if err := s.indexer.Add(ctx, item); err != nil {
		s.metrics.DroppedTotal.Add(1)
		s.recordFailure(FailureEvent{
			Source:     source,
			Index:      indexName,
			Action:     string(action),
			DocumentID: docID,
			Error:      err.Error(),
		})
		s.logger.Warn("essyncer: add to indexer", zap.String("id", docID), zap.Error(err))
	}
}

func (s *Syncer) Shutdown(ctx context.Context) error {
	s.stopped.Store(true)
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
	res, err := s.es.Indices.Exists([]string{entry.indexName}, s.es.Indices.Exists.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("essyncer: check index %s: %w", entry.indexName, err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return nil
	}

	if entry.mapping != nil {
		r, err := s.es.Indices.Create(entry.indexName,
			s.es.Indices.Create.WithBody(strings.NewReader(string(entry.mapping))),
			s.es.Indices.Create.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("essyncer: create index %s: %w", entry.indexName, err)
		}
		defer r.Body.Close()
		if r.IsError() {
			return fmt.Errorf("essyncer: create index %s: %s", entry.indexName, r.String())
		}
	} else {
		r, err := s.es.Indices.Create(entry.indexName, s.es.Indices.Create.WithContext(ctx))
		if err != nil {
			return fmt.Errorf("essyncer: create index %s: %w", entry.indexName, err)
		}
		defer r.Body.Close()
		if r.IsError() {
			return fmt.Errorf("essyncer: create index %s: %s", entry.indexName, r.String())
		}
	}

	s.logger.Info("essyncer: index created",
		zap.String("index", entry.indexName),
		zap.Bool("has_mapping", entry.mapping != nil),
	)
	return nil
}

func (s *Syncer) EnsureAllIndices(ctx context.Context) error {
	var firstErr error
	s.registry.forEach(func(tableName string, entry *modelEntry) bool {
		if err := s.EnsureIndex(ctx, entry); err != nil {
			s.logger.Error("essyncer: ensure index", zap.String("table", tableName), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
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
