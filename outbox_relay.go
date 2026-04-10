package essyncer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	json "github.com/gtkit/json"
	"go.uber.org/zap"
)

func (s *Syncer) StartOutboxRelay(ctx context.Context) error {
	s.relayMu.Lock()
	defer s.relayMu.Unlock()

	if s.stopped.Load() {
		return fmt.Errorf("essyncer: stopped")
	}
	if s.relayRunning {
		return nil
	}
	if s.db == nil {
		return fmt.Errorf("essyncer: start outbox relay: nil db")
	}
	if s.outbox == nil {
		s.outbox = newOutboxStore(s.db)
	}

	relayCtx := ctx
	if relayCtx == nil {
		relayCtx = context.Background()
	}
	relayCtx, cancel := context.WithCancel(relayCtx)
	s.relayCtx = relayCtx
	s.relayCancel = cancel
	s.relayRunning = true
	s.relayWG.Add(1)

	go func() {
		defer s.relayWG.Done()
		s.runOutboxRelay(relayCtx)

		s.relayMu.Lock()
		s.relayRunning = false
		s.relayCtx = nil
		s.relayCancel = nil
		s.relayMu.Unlock()
	}()

	return nil
}

func (s *Syncer) stopOutboxRelay() {
	s.relayMu.Lock()
	cancel := s.relayCancel
	s.relayMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Syncer) waitOutboxRelay(ctx context.Context) error {
	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.relayWG.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("essyncer: shutdown relay: %w", waitCtx.Err())
	}
}

func (s *Syncer) runOutboxRelay(ctx context.Context) {
	pollInterval := s.cfg.Outbox.PollInterval
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		s.runOutboxBatch(ctx)
		timer.Reset(pollInterval)
	}
}

func (s *Syncer) runOutboxBatch(ctx context.Context) {
	rows, err := s.outbox.claimPending(ctx, s.cfg.Outbox.BatchSize, s.cfg.Outbox.Lease)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.recordFailure(FailureEvent{
			Source: failureSourceRelay,
			Error:  err.Error(),
		})
		s.logger.Warn("essyncer: relay claim pending", zap.Error(err))
		return
	}
	for _, row := range rows {
		s.processOutboxRow(ctx, row)
	}
}

func (s *Syncer) processOutboxRow(ctx context.Context, row OutboxEvent) {
	rowCtx, cancel := context.WithTimeout(ctx, s.outboxSendTimeout())
	defer cancel()
	storeCtx := flushContext(ctx)

	status, sendErr := s.sendOutboxRow(rowCtx, row)
	if sendErr == nil {
		if err := s.outbox.markSent(storeCtx, row); err != nil {
			s.recordFailure(FailureEvent{
				Source:     failureSourceRelay,
				Index:      row.IndexAlias,
				Action:     row.Action,
				DocumentID: row.DocumentID,
				Error:      err.Error(),
			})
			return
		}
		s.metrics.RelayProcessedTotal.Add(1)
		return
	}

	retryable := status == 0 || classifyRetryable(status)
	s.recordFailure(FailureEvent{
		Source:     failureSourceRelay,
		Index:      row.IndexAlias,
		Action:     row.Action,
		DocumentID: row.DocumentID,
		Status:     status,
		Error:      sendErr.Error(),
		Retryable:  retryable,
	})

	if retryable && row.Attempts < s.cfg.Outbox.MaxAttempts {
		retryAt := time.Now().UTC().Add(s.relayBackoff(row.Attempts + 1))
		if err := s.outbox.markRetry(storeCtx, row, retryAt, sendErr); err != nil {
			s.recordFailure(FailureEvent{
				Source:     failureSourceRelay,
				Index:      row.IndexAlias,
				Action:     row.Action,
				DocumentID: row.DocumentID,
				Status:     status,
				Error:      err.Error(),
			})
			return
		}
		s.metrics.RelayRetriesTotal.Add(1)
		return
	}

	if err := s.outbox.markDead(storeCtx, row, sendErr); err != nil {
		s.recordFailure(FailureEvent{
			Source:     failureSourceRelay,
			Index:      row.IndexAlias,
			Action:     row.Action,
			DocumentID: row.DocumentID,
			Status:     status,
			Error:      err.Error(),
		})
		return
	}

	s.metrics.DeadLetters.Add(1)
	s.metrics.RelayDeadTotal.Add(1)
	s.recordFailure(FailureEvent{
		Source:     failureSourceDead,
		Index:      row.IndexAlias,
		Action:     row.Action,
		DocumentID: row.DocumentID,
		Status:     status,
		Error:      sendErr.Error(),
		Retryable:  false,
	})
}

func (s *Syncer) outboxSendTimeout() time.Duration {
	if s.cfg.Outbox.Lease > 0 {
		return s.cfg.Outbox.Lease
	}
	return 30 * time.Second
}

func (s *Syncer) relayBackoff(attempt int) time.Duration {
	base := s.cfg.Outbox.PollInterval
	if base <= 0 {
		base = 2 * time.Second
	}
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 8 {
		shift = 8
	}
	return base * time.Duration(1<<shift)
}

func (s *Syncer) sendOutboxRow(ctx context.Context, row OutboxEvent) (int, error) {
	var (
		res *esapi.Response
		err error
	)

	switch row.Action {
	case string(actionIndex):
		req := esapi.IndexRequest{
			Index:      row.IndexAlias,
			DocumentID: row.DocumentID,
			Body:       bytes.NewReader(row.Payload),
		}
		res, err = req.Do(ctx, s.es)
	case string(actionUpdate):
		req := esapi.UpdateRequest{
			Index:      row.IndexAlias,
			DocumentID: row.DocumentID,
			Body:       bytes.NewReader(row.Payload),
		}
		res, err = req.Do(ctx, s.es)
	case string(actionDelete):
		req := esapi.DeleteRequest{
			Index:      row.IndexAlias,
			DocumentID: row.DocumentID,
		}
		res, err = req.Do(ctx, s.es)
	default:
		return 0, fmt.Errorf("unsupported outbox action %q", row.Action)
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Body.Close() }()

	if !res.IsError() {
		return res.StatusCode, nil
	}
	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		return res.StatusCode, fmt.Errorf("es status %d: read body: %w", res.StatusCode, readErr)
	}
	return res.StatusCode, fmt.Errorf("es status %d: %s", res.StatusCode, relayErrorReason(body))
}

func relayErrorReason(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "unknown"
	}

	var payload struct {
		Error struct {
			Reason string `json:"reason"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Error.Reason != "" {
		return payload.Error.Reason
	}
	return trimmed
}
