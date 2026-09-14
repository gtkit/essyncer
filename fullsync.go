package essyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
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
				resultCh <- s.fullSyncTableGuarded(ctx, entry)
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

// fullSyncTableGuarded 把一个模型的全量同步 panic 收敛成该模型的失败结果。
// 这些 worker 跑在库自己起的 goroutine 上，调用方 recover 不到；不拦住的话
// 一个模型的编程错误会带走整个进程，其余模型的同步结果也一并丢失。
func (s *Syncer) fullSyncTableGuarded(ctx context.Context, entry *modelEntry) (result FullSyncResult) {
	result = FullSyncResult{Table: entry.tableName, IndexName: entry.indexName}
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		result.Err = fmt.Errorf("essyncer: full sync %s panicked: %v", entry.tableName, recovered)
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  result.Err.Error(),
		})
		s.logger.Error("essyncer: full sync panicked",
			zap.String("table", entry.tableName),
			zap.Any("panic", recovered),
			zap.String("stack", string(debug.Stack())),
		)
	}()

	result.TotalSynced, result.Err = s.fullSyncTable(ctx, entry)
	return result
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
		if err := s.ensureIndexEntry(ctx, entry); err != nil {
			return entry, err
		}
	}
	return nil, nil
}

func (s *Syncer) FullSyncTable(ctx context.Context, model Syncable) (int64, error) {
	entry, ok := s.registry.getByModel(s.db, model)
	if !ok {
		return 0, errors.New("essyncer: model not registered")
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
	if createErr := s.createManagedIndex(ctx, entry, rebuildIndex); createErr != nil {
		s.recordFailure(FailureEvent{
			Source: failureSourceFullSync,
			Index:  entry.indexName,
			Action: string(actionIndex),
			Error:  createErr.Error(),
		})
		return 0, createErr
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
			return totalSynced, fmt.Errorf("essyncer: full sync canceled for %s: %w", entry.tableName, ctx.Err())
		default:
		}

		slicePtr := newModelSlice(entry)
		beforeCheckpoint := cursor.Checkpoint()
		result := cursor.Scope(s.fullSyncQuery(ctx, entry).Table(entry.tableName), batchSize).Find(slicePtr)

		if result.Error != nil {
			_ = indexer.Close(ctx)
			s.recordFullSyncFailure(entry, "", result.Error)
			return totalSynced, fmt.Errorf("essyncer: query %s: %w", entry.tableName, result.Error)
		}

		sliceVal := reflect.ValueOf(slicePtr).Elem()
		rowCount := sliceVal.Len()
		if rowCount == 0 {
			break
		}

		syncedCount, err := s.syncFullSyncBatch(ctx, entry, targetIndex, cursor, indexer, sliceVal)
		totalSynced += syncedCount
		if err != nil {
			_ = indexer.Close(ctx)
			return totalSynced, err
		}
		if rowCount == batchSize && reflect.DeepEqual(beforeCheckpoint, cursor.Checkpoint()) {
			_ = indexer.Close(ctx)
			stallErr := fmt.Errorf("essyncer: full sync scan strategy for %s did not advance", entry.tableName)
			s.recordFailure(FailureEvent{
				Source: failureSourceFullSync,
				Index:  entry.indexName,
				Action: string(actionIndex),
				Error:  stallErr.Error(),
			})
			return totalSynced, stallErr
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
		return 0, errors.New("essyncer: model not registered")
	}
	return s.fullSyncTableAt(ctx, entry, startID)
}

func (s *Syncer) fullSyncQuery(ctx context.Context, entry *modelEntry) *gorm.DB {
	query := s.db.WithContext(ctx)
	if entry.hasSoftDelete && entry.softDeleteMode == SoftDeleteModeUpdate {
		return query.Unscoped()
	}
	return query
}

func (s *Syncer) recordFullSyncFailure(entry *modelEntry, documentID string, err error) {
	if err == nil {
		return
	}
	s.recordFailure(FailureEvent{
		Source:     failureSourceFullSync,
		Index:      entry.indexName,
		Action:     string(actionIndex),
		DocumentID: documentID,
		Error:      err.Error(),
	})
}

func (s *Syncer) syncFullSyncBatch(
	ctx context.Context,
	entry *modelEntry,
	targetIndex string,
	cursor FullSyncScanCursor,
	indexer esutil.BulkIndexer,
	sliceVal reflect.Value,
) (int64, error) {
	var syncedCount int64

	for i := range sliceVal.Len() {
		elem := sliceVal.Index(i).Addr().Interface()
		syncable, ok := elem.(Syncable)
		if !ok {
			err := fmt.Errorf("essyncer: model %s does not implement Syncable", entry.tableName)
			s.recordFullSyncFailure(entry, "", err)
			return syncedCount, err
		}

		data, err := json.Marshal(elem)
		if err != nil {
			s.recordFullSyncFailure(entry, syncable.GetID(), err)
			return syncedCount, fmt.Errorf("essyncer: marshal %s id %s: %w", entry.tableName, syncable.GetID(), err)
		}

		if err := indexer.Add(ctx, esutil.BulkIndexerItem{
			Index:      targetIndex,
			Action:     "index",
			DocumentID: syncable.GetID(),
			Body:       bytes.NewReader(data),
		}); err != nil {
			s.recordFullSyncFailure(entry, syncable.GetID(), err)
			return syncedCount, fmt.Errorf("essyncer: add to full sync indexer %s id %s: %w", entry.tableName, syncable.GetID(), err)
		}

		syncedCount++
		s.metrics.FullSyncDocs.Add(1)

		if err := cursor.Advance(ctx, elem); err != nil {
			s.recordFullSyncFailure(entry, syncable.GetID(), err)
			return syncedCount, fmt.Errorf("essyncer: advance full sync cursor %s id %s: %w", entry.tableName, syncable.GetID(), err)
		}
	}

	return syncedCount, nil
}
