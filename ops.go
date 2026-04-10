package essyncer

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	json "github.com/gtkit/json"
)

type OutboxListOptions struct {
	Statuses []string
	Limit    int
}

type OutboxCleanupOptions struct {
	Statuses  []string
	OlderThan time.Duration
	Limit     int
}

type OutboxReplayOptions struct {
	IDs     []int64
	AllDead bool
	Limit   int
}

type OutboxDrainResult struct {
	Batches   int   `json:"batches"`
	Claimed   int64 `json:"claimed"`
	Processed int64 `json:"processed"`
	Retried   int64 `json:"retried"`
	Dead      int64 `json:"dead"`
	Remaining int64 `json:"remaining"`
}

type ReconcileCountResult struct {
	Model string `json:"model"`
	Table string `json:"table"`
	Alias string `json:"alias"`

	DBCount int64 `json:"db_count"`
	ESCount int64 `json:"es_count"`
	Match   bool  `json:"match"`

	Err error `json:"-"`
}

func (s *Syncer) ListOutbox(ctx context.Context, opts OutboxListOptions) ([]OutboxEvent, error) {
	if err := s.ensureOutboxStore(); err != nil {
		return nil, fmt.Errorf("list outbox rows: %w", err)
	}
	query := s.db.WithContext(ctx).Model(&OutboxEvent{}).Order("id DESC")
	if len(opts.Statuses) > 0 {
		query = query.Where("status IN ?", normalizeOutboxStatuses(opts.Statuses))
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	query = query.Limit(limit)

	var rows []OutboxEvent
	if err := query.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list outbox rows: %w", err)
	}
	return rows, nil
}

func (s *Syncer) CleanupOutbox(ctx context.Context, opts OutboxCleanupOptions) (int64, error) {
	if err := s.ensureOutboxStore(); err != nil {
		return 0, fmt.Errorf("cleanup outbox rows: %w", err)
	}
	statuses := normalizeOutboxStatuses(opts.Statuses)
	if len(statuses) == 0 {
		statuses = []string{OutboxStatusDead}
	}

	query := s.db.WithContext(ctx).Model(&OutboxEvent{}).Where("status IN ?", statuses)
	if opts.OlderThan > 0 {
		cutoff := time.Now().UTC().Add(-opts.OlderThan)
		query = query.Where("created_at <= ?", cutoff)
	}

	if opts.Limit > 0 {
		var ids []int64
		if err := query.Order("id ASC").Limit(opts.Limit).Pluck("id", &ids).Error; err != nil {
			return 0, fmt.Errorf("select outbox cleanup ids: %w", err)
		}
		if len(ids) == 0 {
			return 0, nil
		}
		result := s.db.WithContext(ctx).Where("id IN ?", ids).Delete(&OutboxEvent{})
		if result.Error != nil {
			return 0, fmt.Errorf("cleanup outbox rows: %w", result.Error)
		}
		return result.RowsAffected, nil
	}

	result := query.Delete(&OutboxEvent{})
	if result.Error != nil {
		return 0, fmt.Errorf("cleanup outbox rows: %w", result.Error)
	}
	return result.RowsAffected, nil
}

func (s *Syncer) ReplayOutbox(ctx context.Context, opts OutboxReplayOptions) (int64, error) {
	if err := s.ensureOutboxStore(); err != nil {
		return 0, fmt.Errorf("replay outbox rows: %w", err)
	}
	query := s.db.WithContext(ctx).Model(&OutboxEvent{})
	switch {
	case len(opts.IDs) > 0:
		query = query.Where("id IN ?", opts.IDs)
	case opts.AllDead:
		query = query.Where("status = ?", OutboxStatusDead)
	default:
		return 0, fmt.Errorf("replay outbox rows: no ids provided and all_dead disabled")
	}

	if opts.Limit > 0 {
		var ids []int64
		if err := query.Order("id ASC").Limit(opts.Limit).Pluck("id", &ids).Error; err != nil {
			return 0, fmt.Errorf("select outbox replay ids: %w", err)
		}
		if len(ids) == 0 {
			return 0, nil
		}
		query = s.db.WithContext(ctx).Model(&OutboxEvent{}).Where("id IN ?", ids)
	}

	result := query.Updates(map[string]any{
		"status":        OutboxStatusPending,
		"attempts":      0,
		"next_retry_at": nil,
		"last_error":    "",
		"leased_until":  nil,
		"sent_at":       nil,
	})
	if result.Error != nil {
		return 0, fmt.Errorf("replay outbox rows: %w", result.Error)
	}
	return result.RowsAffected, nil
}

func (s *Syncer) DrainOutbox(ctx context.Context, maxBatches int) (OutboxDrainResult, error) {
	var result OutboxDrainResult
	if err := s.ensureOutboxStore(); err != nil {
		return result, fmt.Errorf("drain outbox rows: %w", err)
	}

	for maxBatches <= 0 || result.Batches < maxBatches {
		batch, err := s.runOutboxBatch(ctx)
		if err != nil {
			return result, err
		}
		if batch.Claimed == 0 {
			break
		}
		result.Batches++
		result.Claimed += int64(batch.Claimed)
		result.Processed += batch.Processed
		result.Retried += batch.Retried
		result.Dead += batch.Dead
	}

	var remaining int64
	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("status = ? OR status = ?", OutboxStatusPending, OutboxStatusProcessing).
		Count(&remaining).Error; err != nil {
		return result, fmt.Errorf("count remaining outbox rows: %w", err)
	}
	result.Remaining = remaining
	return result, nil
}

func (s *Syncer) ReconcileCounts(ctx context.Context, models ...Syncable) ([]ReconcileCountResult, error) {
	entries, err := s.registry.entriesForModels(s.db, models...)
	if err != nil {
		return nil, err
	}

	results := make([]ReconcileCountResult, 0, len(entries))
	for _, entry := range entries {
		item := ReconcileCountResult{
			Model: entry.modelType.Name(),
			Table: entry.tableName,
			Alias: entry.indexName,
		}

		dbQuery := s.db.WithContext(ctx).Table(entry.tableName)
		if entry.hasSoftDelete && entry.softDeleteMode == SoftDeleteModeUpdate {
			dbQuery = dbQuery.Unscoped()
		}
		if err := dbQuery.Count(&item.DBCount).Error; err != nil {
			item.Err = fmt.Errorf("count db rows: %w", err)
			results = append(results, item)
			continue
		}

		resp, err := s.es.Count(
			s.es.Count.WithContext(ctx),
			s.es.Count.WithIndex(entry.indexName),
		)
		if err != nil {
			item.Err = fmt.Errorf("count es docs: %w", err)
			results = append(results, item)
			continue
		}
		func() {
			defer func() { _ = resp.Body.Close() }()
			if resp.IsError() {
				item.Err = fmt.Errorf("count es docs: %s", resp.Status())
				return
			}
			var payload struct {
				Count int64 `json:"count"`
			}
			if err := jsonUnmarshal(resp.Body, &payload); err != nil {
				item.Err = fmt.Errorf("decode es count: %w", err)
				return
			}
			item.ESCount = payload.Count
			item.Match = item.DBCount == item.ESCount
		}()
		results = append(results, item)
	}
	return results, nil
}

func normalizeOutboxStatuses(statuses []string) []string {
	if len(statuses) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(statuses))
	seen := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		s := strings.TrimSpace(strings.ToLower(status))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		normalized = append(normalized, s)
	}
	slices.Sort(normalized)
	return normalized
}

func jsonUnmarshal(r io.Reader, dst any) error {
	decoder := json.NewDecoder(r)
	return decoder.Decode(dst)
}
