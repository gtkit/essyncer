package essyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"go.uber.org/zap"
)

type FullSyncResult struct {
	Table       string
	IndexName   string
	TotalSynced int64
	Err         error
}

func (s *Syncer) FullSync(ctx context.Context, models ...Syncable) []FullSyncResult {
	entries, err := s.registry.entriesForModels(s.db, models...)
	if err != nil {
		return []FullSyncResult{{Err: err}}
	}
	if len(entries) == 0 {
		s.logger.Warn("essyncer: no models registered")
		return nil
	}
	if entry, err := s.validateFullSyncEntries(entries, 0); err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return []FullSyncResult{{
			Table:     entry.tableName,
			IndexName: entry.indexName,
			Err:       err,
		}}
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

func (s *Syncer) validateFullSyncEntries(entries []*modelEntry, startID int64) (*modelEntry, error) {
	for _, entry := range entries {
		if _, err := entry.fullSyncCursor(startID); err != nil {
			return entry, err
		}
	}
	return nil, nil
}

func (s *Syncer) ensureIndices(ctx context.Context, entries []*modelEntry) (*modelEntry, error) {
	for _, entry := range entries {
		if err := s.EnsureIndex(ctx, entry); err != nil {
			return entry, err
		}
	}
	return nil, nil
}

func (s *Syncer) FullSyncTable(ctx context.Context, model Syncable) (int64, error) {
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return 0, fmt.Errorf("essyncer: model not registered")
	}
	return s.fullSyncTableAt(ctx, entry, 0)
}

func (s *Syncer) fullSyncTable(ctx context.Context, entry *modelEntry) (int64, error) {
	return s.fullSyncTableAt(ctx, entry, 0)
}

func (s *Syncer) fullSyncTableAt(ctx context.Context, entry *modelEntry, startID int64) (int64, error) {
	if _, err := entry.fullSyncCursor(startID); err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return 0, err
	}

	aliasState, err := s.resolveAliasState(ctx, entry.indexName)
	if err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return 0, err
	}

	rebuildIndex := entry.newRebuildIndexName(time.Now().UTC())
	if err := s.createManagedIndex(ctx, entry, rebuildIndex); err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return 0, err
	}

	cursor, err := entry.fullSyncCursor(startID)
	if err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return 0, err
	}

	totalSynced, err := s.fullSyncFrom(ctx, entry, rebuildIndex, cursor)
	if err != nil {
		s.cleanupDetachedIndex(ctx, rebuildIndex)
		return totalSynced, err
	}

	if err := s.switchAliasToIndex(ctx, aliasState, rebuildIndex); err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return totalSynced, err
	}

	if err := s.deleteReplacedIndices(ctx, aliasState); err != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  err.Error(),
		})
		return totalSynced, err
	}

	return totalSynced, nil
}

func (s *Syncer) fullSyncFrom(ctx context.Context, entry *modelEntry, targetIndex string, cursor FullSyncScanCursor) (int64, error) {
	batchSize := entry.batchSize
	if batchSize <= 0 {
		batchSize = s.cfg.Sync.DefaultBatchSize
	}

	// 全量同步用独立 BulkIndexer，不与增量混用
	indexer, err := s.makeBulkIndexer(esutil.BulkIndexerConfig{
		Client:     s.es,
		Index:      targetIndex,
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

	var totalSynced int64

	for {
		select {
		case <-ctx.Done():
			_ = indexer.Close(ctx)
			return totalSynced, ctx.Err()
		default:
		}

		slicePtr := newModelSlice(entry)

		query := s.db.WithContext(ctx)
		if entry.hasSoftDelete && entry.softDeleteMode == SoftDeleteModeUpdate {
			query = query.Unscoped()
		}

		beforeCheckpoint := cursor.Checkpoint()
		result := cursor.Scope(query.Table(entry.tableName), batchSize).Find(slicePtr)

		if result.Error != nil {
			_ = indexer.Close(ctx)
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

		for i := range rowCount {
			elem := sliceVal.Index(i).Addr().Interface()
			syncable, ok := elem.(Syncable)
			if !ok {
				_ = indexer.Close(ctx)
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
				_ = indexer.Close(ctx)
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
				Index:      targetIndex,
				Action:     "index",
				DocumentID: syncable.GetID(),
				Body:       bytes.NewReader(data),
			}); err != nil {
				_ = indexer.Close(ctx)
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
			if err := cursor.Advance(ctx, elem); err != nil {
				_ = indexer.Close(ctx)
				s.recordFailure(FailureEvent{
					Source:     failureSourceFullSync,
					Index:      entry.indexName,
					Action:     string(actionIndex),
					DocumentID: syncable.GetID(),
					Error:      err.Error(),
				})
				return totalSynced, fmt.Errorf("essyncer: advance full sync cursor %s id %s: %w", entry.tableName, syncable.GetID(), err)
			}
		}
		if rowCount == batchSize && reflect.DeepEqual(beforeCheckpoint, cursor.Checkpoint()) {
			_ = indexer.Close(ctx)
			err := fmt.Errorf("essyncer: full sync scan strategy for %s did not advance", entry.tableName)
			s.recordFailure(FailureEvent{
				Source: failureSourceFullSync,
				Index:  entry.indexName,
				Action: string(actionIndex),
				Error:  err.Error(),
			})
			return totalSynced, err
		}

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
	return s.fullSyncTableAt(ctx, entry, startID)
}
