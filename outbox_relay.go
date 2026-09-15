package essyncer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9/esapi"
	json "github.com/gtkit/json/v2"
	"go.uber.org/zap"
)

// errUnsupportedOutboxAction 标记一条 action 字段无法识别的 outbox 行。它和
// "没拿到 HTTP 响应" 同样返回 status 0，但属于这一行自己的问题，必须计入重试预算
// 并最终进 dead，否则毒丸行会永久占用 relay。
var errUnsupportedOutboxAction = errors.New("unsupported outbox action")

type outboxBatchResult struct {
	Claimed   int
	Processed int64
	Retried   int64
	Dead      int64
}

func (s *Syncer) StartOutboxRelay(ctx context.Context) error {
	s.relayMu.Lock()
	defer s.relayMu.Unlock()

	if s.stopped.Load() {
		return errors.New("essyncer: stopped")
	}
	if s.relayRunning {
		return nil
	}
	if ctx == nil {
		return errors.New("essyncer: start outbox relay: nil context")
	}
	if s.db == nil {
		return errors.New("essyncer: start outbox relay: nil db")
	}
	if err := s.ensureOutboxStore(); err != nil {
		return fmt.Errorf("essyncer: start outbox relay: %w", err)
	}

	relayCtx, cancel := context.WithCancel(ctx)
	s.relayCancel = cancel
	s.relayRunning = true

	s.relayWG.Go(func() {
		defer func() {
			s.relayMu.Lock()
			s.relayRunning = false
			s.relayCancel = nil
			s.relayMu.Unlock()
		}()
		// relay 跑在库自己起的 goroutine 上，宿主的 recover 覆盖不到这里：
		// 不拦住 panic，一次投递路径上的编程错误就会带走接入方的整个进程。
		defer s.recoverRelayPanic()
		_ = s.runOutboxRelay(relayCtx)
	})

	return nil
}

// RunOutboxRelay 在调用方的 goroutine 里阻塞运行 relay 循环，直到 ctx 取消后返回 nil。
// 它不拦截 panic：交给宿主的 worker 管理器统一处理 panic、重启与关停顺序。
// 需要库自行托管后台 goroutine 时用 StartOutboxRelay。
func (s *Syncer) RunOutboxRelay(ctx context.Context) error {
	if s.stopped.Load() {
		return errors.New("essyncer: stopped")
	}
	if ctx == nil {
		return errors.New("essyncer: run outbox relay: nil context")
	}
	if s.db == nil {
		return errors.New("essyncer: run outbox relay: nil db")
	}
	if err := s.ensureOutboxStore(); err != nil {
		return fmt.Errorf("essyncer: run outbox relay: %w", err)
	}

	s.relayMu.Lock()
	if s.relayRunning {
		s.relayMu.Unlock()
		return errors.New("essyncer: outbox relay already running")
	}
	relayCtx, cancel := context.WithCancel(ctx)
	s.relayCancel = cancel
	s.relayRunning = true
	s.relayWG.Add(1)
	s.relayMu.Unlock()

	defer s.relayWG.Done()
	defer func() {
		s.relayMu.Lock()
		s.relayRunning = false
		s.relayCancel = nil
		s.relayMu.Unlock()
		cancel()
	}()

	return s.runOutboxRelay(relayCtx)
}

// recoverRelayPanic 把 relay goroutine 的 panic 转成一条可观测的失败事件。
// relay 就此停止，Health 会开始报错——否则进程活着、HTTP 正常，而增量同步
// 已经不工作了，没有任何信号。
func (s *Syncer) recoverRelayPanic() {
	recovered := recover()
	if recovered == nil {
		return
	}
	stack := string(debug.Stack())
	message := fmt.Sprintf("outbox relay panicked: %v", recovered)
	s.relayPanic.Store(&message)
	s.recordFailure(FailureEvent{
		Source: failureSourceRelay,
		Error:  message,
	})
	s.logger.Error("essyncer: outbox relay panicked, relay stopped",
		zap.Any("panic", recovered),
		zap.String("stack", stack),
	)
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
	if ctx == nil {
		return errors.New("essyncer: shutdown relay: nil context")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.relayWG.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("essyncer: shutdown relay: %w", ctx.Err())
	}
}

func (s *Syncer) runOutboxRelay(ctx context.Context) error {
	pollInterval := s.cfg.Outbox.PollInterval
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		if _, err := s.runOutboxBatch(ctx); err != nil && ctx.Err() == nil {
			s.recordFailure(FailureEvent{
				Source: failureSourceRelay,
				Error:  err.Error(),
			})
			s.logger.Warn("essyncer: relay claim pending", zap.Error(err))
		}
		timer.Reset(pollInterval)
	}
}

func (s *Syncer) runOutboxBatch(ctx context.Context) (outboxBatchResult, error) {
	var batch outboxBatchResult
	rows, err := s.outbox.claimPending(ctx, s.cfg.Outbox.BatchSize, s.cfg.Outbox.Lease)
	if err != nil {
		if ctx.Err() != nil {
			return batch, fmt.Errorf("relay claim pending: %w", ctx.Err())
		}
		return batch, fmt.Errorf("relay claim pending: %w", err)
	}
	batch.Claimed = len(rows)
	for _, row := range rows {
		outcome, err := s.processOutboxRow(ctx, row)
		if err != nil {
			return batch, err
		}
		switch outcome {
		case OutboxStatusSent:
			batch.Processed++
		case OutboxStatusPending:
			batch.Retried++
		case OutboxStatusDead:
			batch.Dead++
		}
	}
	return batch, nil
}

func (s *Syncer) processOutboxRow(ctx context.Context, row OutboxEvent) (string, error) {
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
			return "", err
		}
		s.metrics.RelayProcessedTotal.Add(1)
		return OutboxStatusSent, nil
	}

	// status == 0 表示请求没有拿到 HTTP 响应（连接失败 / 超时），是 ES 整体不可达，
	// 不是这一行的问题。这类失败不消耗 MaxAttempts 预算：否则一次超过
	// PollInterval*2^MaxAttempts 的 ES 停机会把全部待投递行冲进 dead，只能人工 replay。
	transportFailure := status == 0 && !errors.Is(sendErr, errUnsupportedOutboxAction)
	retryable := transportFailure || classifyRetryable(status)
	s.recordFailure(FailureEvent{
		Source:     failureSourceRelay,
		Index:      row.IndexAlias,
		Action:     row.Action,
		DocumentID: row.DocumentID,
		Status:     status,
		Error:      sendErr.Error(),
		Retryable:  retryable,
	})

	if retryable && (transportFailure || row.Attempts < s.cfg.Outbox.MaxAttempts) {
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
			return "", err
		}
		s.metrics.RelayRetriesTotal.Add(1)
		return OutboxStatusPending, nil
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
		return "", err
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
	return OutboxStatusDead, nil
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
	shift := min(max(attempt-1, 0), 8)
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
		return 0, fmt.Errorf("%w %q", errUnsupportedOutboxAction, row.Action)
	}
	if err != nil {
		return 0, fmt.Errorf("send outbox row %s %s: %w", row.Action, row.DocumentID, err)
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
