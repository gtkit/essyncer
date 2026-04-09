package essyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"go.uber.org/zap"
)

type FullSyncResult struct {
	Table       string
	IndexName   string
	TotalSynced int64
	Err         error
}

func (s *Syncer) FullSync(ctx context.Context) []FullSyncResult {
	entries := s.registry.allEntries()
	if len(entries) == 0 {
		s.logger.Warn("essyncer: no models registered")
		return nil
	}
	if err := s.EnsureAllIndices(ctx); err != nil {
		results := make([]FullSyncResult, 0, len(entries))
		for _, entry := range entries {
			s.recordFailure(FailureEvent{
				Source: failureSourceFullSync,
				Index:  entry.indexName,
				Action: string(actionIndex),
				Error:  err.Error(),
			})
			results = append(results, FullSyncResult{
				Table:     entry.tableName,
				IndexName: entry.indexName,
				Err:       err,
			})
		}
		return results
	}

	workers := max(s.cfg.Sync.Workers, 1)
	taskCh := make(chan *modelEntry, len(entries))
	for _, e := range entries {
		taskCh <- e
	}
	close(taskCh)

	resultCh := make(chan FullSyncResult, len(entries))
	var wg sync.WaitGroup
	for range min(workers, len(entries)) {
		wg.Go(func() {
			for entry := range taskCh {
				select {
				case <-ctx.Done():
					resultCh <- FullSyncResult{Table: entry.tableName, Err: ctx.Err()}
					return
				default:
				}
				total, err := s.fullSyncTable(ctx, entry)
				resultCh <- FullSyncResult{
					Table: entry.tableName, IndexName: entry.indexName,
					TotalSynced: total, Err: err,
				}
			}
		})
	}
	go func() { wg.Wait(); close(resultCh) }()

	var results []FullSyncResult
	for r := range resultCh {
		if r.Err != nil {
			s.logger.Error("essyncer: full sync failed", zap.String("table", r.Table), zap.Error(r.Err))
		} else {
			s.logger.Info("essyncer: full sync done", zap.String("table", r.Table), zap.Int64("synced", r.TotalSynced))
			s.metrics.FullSyncTables.Add(1)
		}
		results = append(results, r)
	}
	return results
}

func (s *Syncer) FullSyncTable(ctx context.Context, model Syncable) (int64, error) {
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return 0, fmt.Errorf("essyncer: model not registered")
	}
	if err := s.EnsureIndex(ctx, entry); err != nil {
		return 0, err
	}
	return s.fullSyncFrom(ctx, entry, 0)
}

func (s *Syncer) fullSyncTable(ctx context.Context, entry *modelEntry) (int64, error) {
	return s.fullSyncFrom(ctx, entry, 0)
}

func (s *Syncer) fullSyncFrom(ctx context.Context, entry *modelEntry, startID int64) (int64, error) {
	batchSize := entry.batchSize
	if batchSize <= 0 {
		batchSize = s.cfg.Sync.DefaultBatchSize
	}

	// 全量同步用独立 BulkIndexer，不与增量混用
	indexer, err := s.makeBulkIndexer(esutil.BulkIndexerConfig{
		Client:     s.es,
		Index:      entry.indexName,
		NumWorkers: 2,
		FlushBytes: 5 << 20,
		OnError: func(_ context.Context, err error) {
			s.recordFailure(FailureEvent{
				Source: failureSourceFullSync,
				Index:  entry.indexName,
				Action: string(actionIndex),
				Error:  err.Error(),
			})
			s.logger.Error("essyncer: full sync bulk error", zap.String("table", entry.tableName), zap.Error(err))
		},
	})
	if err != nil {
		return 0, fmt.Errorf("essyncer: create full sync indexer: %w", err)
	}

	var (
		checkpoint  atomic.Int64
		totalSynced int64
	)
	checkpoint.Store(startID)

	for {
		select {
		case <-ctx.Done():
			indexer.Close(ctx)
			return totalSynced, ctx.Err()
		default:
		}

		lastID := checkpoint.Load()
		slicePtr := newModelSlice(entry)

		result := s.db.WithContext(ctx).
			Unscoped().
			Table(entry.tableName).
			Where("id > ?", lastID).
			Order("id ASC").
			Limit(batchSize).
			Find(slicePtr)

		if result.Error != nil {
			indexer.Close(ctx)
			s.recordFailure(FailureEvent{
				Source: failureSourceFullSync,
				Index:  entry.indexName,
				Action: string(actionIndex),
				Error:  result.Error.Error(),
			})
			return totalSynced, fmt.Errorf("essyncer: query %s: %w", entry.tableName, result.Error)
		}

		sliceVal := reflect.ValueOf(slicePtr).Elem()
		rowCount := sliceVal.Len()
		if rowCount == 0 {
			break
		}

		var maxID int64
		for i := range rowCount {
			elem := sliceVal.Index(i).Addr().Interface()
			syncable, ok := elem.(Syncable)
			if !ok {
				indexer.Close(ctx)
				s.recordFailure(FailureEvent{
					Source: failureSourceFullSync,
					Index:  entry.indexName,
					Action: string(actionIndex),
					Error:  fmt.Sprintf("model %s does not implement Syncable", entry.tableName),
				})
				return totalSynced, fmt.Errorf("essyncer: model %s does not implement Syncable", entry.tableName)
			}
			data, err := json.Marshal(elem)
			if err != nil {
				indexer.Close(ctx)
				s.recordFailure(FailureEvent{
					Source:     failureSourceFullSync,
					Index:      entry.indexName,
					Action:     string(actionIndex),
					DocumentID: syncable.GetID(),
					Error:      err.Error(),
				})
				return totalSynced, fmt.Errorf("essyncer: marshal %s id %s: %w", entry.tableName, syncable.GetID(), err)
			}
			if err := indexer.Add(ctx, esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: syncable.GetID(),
				Body:       bytes.NewReader(data),
			}); err != nil {
				indexer.Close(ctx)
				s.recordFailure(FailureEvent{
					Source:     failureSourceFullSync,
					Index:      entry.indexName,
					Action:     string(actionIndex),
					DocumentID: syncable.GetID(),
					Error:      err.Error(),
				})
				return totalSynced, fmt.Errorf("essyncer: add to full sync indexer %s id %s: %w", entry.tableName, syncable.GetID(), err)
			}
			totalSynced++
			s.metrics.FullSyncDocs.Add(1)
			if id := extractID(sliceVal.Index(i)); id > maxID {
				maxID = id
			}
		}

		checkpoint.Store(maxID)

		if rowCount < batchSize {
			break
		}
	}

	if err := indexer.Close(ctx); err != nil {
		return totalSynced, fmt.Errorf("essyncer: close full sync indexer: %w", err)
	}
	return totalSynced, nil
}

func (s *Syncer) FullSyncWithCheckpoint(ctx context.Context, model Syncable, startID int64) (int64, error) {
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return 0, fmt.Errorf("essyncer: model not registered")
	}
	if err := s.EnsureIndex(ctx, entry); err != nil {
		return 0, err
	}
	return s.fullSyncFrom(ctx, entry, startID)
}
