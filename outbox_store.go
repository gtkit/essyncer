package essyncer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	json "github.com/gtkit/json"
	"gorm.io/gorm"
)

type outboxStore struct {
	db  *gorm.DB
	now func() time.Time
}

type outboxWriteEvent struct {
	TableName  string
	IndexAlias string
	DocumentID string
	Action     actionType
	Doc        any
}

func newOutboxStore(db *gorm.DB) *outboxStore {
	return &outboxStore{
		db:  db,
		now: time.Now,
	}
}

func (s *outboxStore) insertFromEvents(ctx context.Context, tx *gorm.DB, events []outboxWriteEvent) error {
	if len(events) == 0 {
		return nil
	}
	if tx == nil {
		return errors.New("insert outbox events: nil transaction db")
	}

	rows := make([]OutboxEvent, 0, len(events))
	for _, event := range events {
		if event.TableName == "" {
			return errors.New("insert outbox events: empty table name")
		}
		if event.IndexAlias == "" {
			return errors.New("insert outbox events: empty index alias")
		}
		if event.DocumentID == "" {
			return errors.New("insert outbox events: empty document id")
		}
		payload, err := marshalOutboxPayload(event.Action, event.Doc)
		if err != nil {
			return fmt.Errorf("insert outbox events: marshal payload: %w", err)
		}
		rows = append(rows, OutboxEvent{
			TableName:  event.TableName,
			IndexAlias: event.IndexAlias,
			DocumentID: event.DocumentID,
			Action:     string(event.Action),
			Payload:    payload,
			Status:     OutboxStatusPending,
			Attempts:   0,
		})
	}

	if err := tx.WithContext(ctx).CreateInBatches(rows, 100).Error; err != nil {
		return fmt.Errorf("insert outbox events: create rows: %w", err)
	}
	return nil
}

func marshalOutboxPayload(action actionType, doc any) ([]byte, error) {
	if doc == nil {
		return []byte("{}"), nil
	}
	switch action {
	case actionUpdate:
		payload, err := json.Marshal(map[string]any{"doc": doc})
		if err != nil {
			return nil, fmt.Errorf("marshal update payload: %w", err)
		}
		return payload, nil
	default:
		payload, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		return payload, nil
	}
}

func (s *outboxStore) claimPending(ctx context.Context, batchSize int, leaseDuration time.Duration) ([]OutboxEvent, error) {
	if batchSize <= 0 {
		return nil, nil
	}
	if leaseDuration <= 0 {
		return nil, errors.New("claim pending outbox rows: invalid lease duration")
	}
	now := s.now().UTC()
	leaseUntil := now.Add(leaseDuration)
	claimed := make([]OutboxEvent, 0, batchSize)
	candidateLimit := batchSize * 4
	if candidateLimit < batchSize {
		candidateLimit = batchSize
	}
	var candidates []OutboxEvent

	if err := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("((status = ? AND (leased_until IS NULL OR leased_until <= ?)) OR (status = ? AND leased_until <= ?))",
			OutboxStatusPending, now, OutboxStatusProcessing, now).
		Where("next_retry_at IS NULL OR next_retry_at <= ?", now).
		Order("id ASC").
		Limit(candidateLimit).
		Find(&candidates).Error; err != nil {
		return nil, fmt.Errorf("claim pending outbox rows: query candidates: %w", err)
	}

	for _, candidate := range candidates {
		if len(claimed) >= batchSize {
			break
		}
		token := newLeaseToken()
		ok, err := s.tryClaimRow(ctx, candidate.ID, now, leaseUntil, token)
		if err != nil {
			return nil, fmt.Errorf("claim pending outbox rows: claim id %d: %w", candidate.ID, err)
		}
		if !ok {
			continue
		}
		candidate.Status = OutboxStatusProcessing
		candidate.LeasedUntil = &leaseUntil
		candidate.LeaseToken = token
		claimed = append(claimed, candidate)
	}

	return claimed, nil
}

// newLeaseToken 生成一次租约的 fencing 凭证。crypto/rand.Read 自 Go 1.24 起
// 保证填满缓冲且不返回错误。
func newLeaseToken() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func (s *outboxStore) tryClaimRow(ctx context.Context, id int64, now, leaseUntil time.Time, token string) (bool, error) {
	updates := map[string]any{
		"status":       OutboxStatusProcessing,
		"leased_until": leaseUntil,
		"lease_token":  token,
	}
	result := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("id = ?", id).
		Where("next_retry_at IS NULL OR next_retry_at <= ?", now).
		Where("((status = ? AND (leased_until IS NULL OR leased_until <= ?)) OR (status = ? AND leased_until <= ?))",
			OutboxStatusPending, now, OutboxStatusProcessing, now).
		Updates(updates)
	if result.Error != nil {
		return false, fmt.Errorf("try claim row: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (s *outboxStore) markSent(ctx context.Context, row OutboxEvent) error {
	token, err := rowLeaseToken(row)
	if err != nil {
		return fmt.Errorf("mark outbox row sent: %w", err)
	}
	now := s.now().UTC()
	updates := map[string]any{
		"status":        OutboxStatusSent,
		"sent_at":       now,
		"leased_until":  nil,
		"lease_token":   "",
		"last_error":    "",
		"next_retry_at": nil,
	}
	if err := s.updateClaimedRow(ctx, row.ID, token, updates); err != nil {
		return fmt.Errorf("mark outbox row sent: %w", err)
	}
	return nil
}

func (s *outboxStore) markRetry(ctx context.Context, row OutboxEvent, retryAt time.Time, reason error) error {
	token, err := rowLeaseToken(row)
	if err != nil {
		return fmt.Errorf("mark outbox row retry: %w", err)
	}
	nextAttempts := row.Attempts + 1
	errText := ""
	if reason != nil {
		errText = reason.Error()
	}

	updates := map[string]any{
		"status":        OutboxStatusPending,
		"attempts":      nextAttempts,
		"next_retry_at": retryAt.UTC(),
		"last_error":    errText,
		"leased_until":  nil,
		"lease_token":   "",
	}
	if err := s.updateClaimedRow(ctx, row.ID, token, updates); err != nil {
		return fmt.Errorf("mark outbox row retry: %w", err)
	}
	return nil
}

func (s *outboxStore) markDead(ctx context.Context, row OutboxEvent, reason error) error {
	token, err := rowLeaseToken(row)
	if err != nil {
		return fmt.Errorf("mark outbox row dead: %w", err)
	}
	nextAttempts := row.Attempts + 1
	errText := ""
	if reason != nil {
		errText = reason.Error()
	}
	updates := map[string]any{
		"status":        OutboxStatusDead,
		"attempts":      nextAttempts,
		"last_error":    errText,
		"leased_until":  nil,
		"lease_token":   "",
		"next_retry_at": nil,
	}
	if err := s.updateClaimedRow(ctx, row.ID, token, updates); err != nil {
		return fmt.Errorf("mark outbox row dead: %w", err)
	}
	return nil
}

// rowLeaseToken 取出行的租约凭证。空 token 必须被拒绝：否则
// updateClaimedRow 的 lease_token = "" 条件会匹配到所有已结束租约的行。
func rowLeaseToken(row OutboxEvent) (string, error) {
	if row.ID <= 0 {
		return "", errors.New("invalid row id")
	}
	if row.LeaseToken == "" {
		return "", errors.New("missing lease token")
	}
	return row.LeaseToken, nil
}

func (s *outboxStore) updateClaimedRow(ctx context.Context, id int64, token string, updates map[string]any) error {
	result := s.db.WithContext(ctx).Model(&OutboxEvent{}).
		Where("id = ?", id).
		Where("status = ?", OutboxStatusProcessing).
		Where("lease_token = ?", token).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update claimed row: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return errors.New("lease mismatch or row not claimed")
	}
	return nil
}
