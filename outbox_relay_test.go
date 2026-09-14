package essyncer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOutboxRelay_StatusTransitions(t *testing.T) {
	tests := []struct {
		name               string
		seedAttempts       int
		maxAttempts        *int
		transportCode      int
		action             string
		payload            []byte
		wantStatus         string
		wantAttempts       int
		wantRetryAtPresent bool
		wantMethod         string
		wantPath           string
	}{
		{
			name:               "pending to sent on success",
			seedAttempts:       0,
			maxAttempts:        nil,
			transportCode:      http.StatusOK,
			action:             string(actionIndex),
			payload:            []byte(`{"id":1,"title":"relay-index"}`),
			wantStatus:         "sent",
			wantAttempts:       0,
			wantRetryAtPresent: false,
			wantMethod:         http.MethodPut,
			wantPath:           "/blocker_articles/_doc/1",
		},
		{
			name:               "retryable failure increments attempts and schedules retry",
			seedAttempts:       0,
			maxAttempts:        nil,
			transportCode:      http.StatusTooManyRequests,
			action:             string(actionUpdate),
			payload:            []byte(`{"doc":{"title":"relay-update"}}`),
			wantStatus:         "pending",
			wantAttempts:       1,
			wantRetryAtPresent: true,
			wantMethod:         http.MethodPost,
			wantPath:           "/blocker_articles/_update/1",
		},
		{
			name:               "exhausted failure transitions to dead when attempts reach configured max",
			seedAttempts:       2,
			maxAttempts:        ptrInt(2),
			transportCode:      http.StatusTooManyRequests,
			action:             string(actionDelete),
			payload:            nil,
			wantStatus:         "dead",
			wantAttempts:       3,
			wantRetryAtPresent: false,
			wantMethod:         http.MethodDelete,
			wantPath:           "/blocker_articles/_doc/1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			if tt.maxAttempts != nil {
				if err := setOutboxMaxAttemptsByReflection(s, *tt.maxAttempts); err != nil {
					t.Fatalf("configure outbox max attempts: %v", err)
				}
			}

			var reqMu sync.Mutex
			var gotMethod, gotPath string
			s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				reqMu.Lock()
				gotMethod, gotPath = req.Method, req.URL.Path
				reqMu.Unlock()
				return jsonResponse(req, tt.transportCode, `{"result":"ok"}`), nil
			}))

			row := blockerOutboxEvent{
				Table:      "blocker_articles",
				IndexAlias: "blocker_articles",
				DocumentID: "1",
				Action:     tt.action,
				Payload:    tt.payload,
				Status:     "pending",
				Attempts:   tt.seedAttempts,
			}
			if err := db.Create(&row).Error; err != nil {
				t.Fatalf("seed outbox row: %v", err)
			}

			relayCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := startOutboxRelayByReflection(relayCtx, s); err != nil {
				t.Fatalf("start outbox relay: %v", err)
			}

			if err := waitFor(t.Context(), 2*time.Second, func() (bool, error) {
				var got blockerOutboxEvent
				if err := db.First(&got, row.ID).Error; err != nil {
					return false, err
				}
				if got.Status != tt.wantStatus {
					return false, nil
				}
				if got.Attempts != tt.wantAttempts {
					return false, nil
				}
				if tt.wantRetryAtPresent && got.NextRetryAt == nil {
					return false, nil
				}
				if !tt.wantRetryAtPresent && got.NextRetryAt != nil {
					return false, nil
				}
				return true, nil
			}); err != nil {
				t.Fatalf("wait relay transition: %v", err)
			}

			reqMu.Lock()
			method, path := gotMethod, gotPath
			reqMu.Unlock()
			if method != tt.wantMethod || path != tt.wantPath {
				t.Fatalf("relay endpoint = %s %s, want %s %s", method, path, tt.wantMethod, tt.wantPath)
			}
		})
	}
}

func TestOutboxRelay_LeasePreventsDuplicateProcessing(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "two relays do not process the same pending row twice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)

			row := blockerOutboxEvent{
				Table:      "blocker_articles",
				IndexAlias: "blocker_articles",
				DocumentID: "1",
				Action:     string(actionUpdate),
				Payload:    []byte(`{"doc":{"title":"lease"}}`),
				Status:     "pending",
			}
			if err := db.Create(&row).Error; err != nil {
				t.Fatalf("seed outbox row: %v", err)
			}

			var calls atomic.Int64
			firstStarted := make(chan struct{}, 1)
			secondStarted := make(chan struct{}, 1)
			release := make(chan struct{})
			client := newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					select {
					case firstStarted <- struct{}{}:
					default:
					}
				} else {
					select {
					case secondStarted <- struct{}{}:
					default:
					}
				}
				<-release
				return jsonResponse(req, http.StatusOK, `{"result":"ok"}`), nil
			}))

			s1 := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			s2 := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
			if err := setOutboxPollIntervalByReflection(s1, 10*time.Millisecond); err != nil {
				t.Fatalf("configure first relay poll interval: %v", err)
			}
			if err := setOutboxPollIntervalByReflection(s2, 10*time.Millisecond); err != nil {
				t.Fatalf("configure second relay poll interval: %v", err)
			}
			s1.es = client
			s2.es = client

			ctx1, cancel1 := context.WithCancel(t.Context())
			defer cancel1()
			ctx2, cancel2 := context.WithCancel(t.Context())
			defer cancel2()

			if err := startOutboxRelayByReflection(ctx1, s1); err != nil {
				t.Fatalf("start first relay: %v", err)
			}
			if err := startOutboxRelayByReflection(ctx2, s2); err != nil {
				t.Fatalf("start second relay: %v", err)
			}

			select {
			case <-firstStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("expected first relay request to start")
			}

			select {
			case <-secondStarted:
				t.Fatal("expected lease to prevent duplicate in-flight processing")
			case <-time.After(500 * time.Millisecond):
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("expected exactly one call while first relay holds lease, got %d", got)
			}

			close(release)
		})
	}
}

func TestOutboxRelay_ShutdownWaitsForInFlightBatch(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "shutdown blocks until in-flight relay request finishes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

			started := make(chan struct{}, 1)
			release := make(chan struct{})
			s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				select {
				case started <- struct{}{}:
				default:
				}
				<-release
				return jsonResponse(req, http.StatusOK, `{"result":"ok"}`), nil
			}))

			row := blockerOutboxEvent{
				Table:      "blocker_articles",
				IndexAlias: "blocker_articles",
				DocumentID: "1",
				Action:     string(actionIndex),
				Payload:    []byte(`{"id":1,"title":"shutdown"}`),
				Status:     "pending",
			}
			if err := db.Create(&row).Error; err != nil {
				t.Fatalf("seed outbox row: %v", err)
			}

			relayCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := startOutboxRelayByReflection(relayCtx, s); err != nil {
				t.Fatalf("start outbox relay: %v", err)
			}

			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("expected relay request to start")
			}

			done := make(chan error, 1)
			go func() {
				done <- s.Shutdown(t.Context())
			}()

			select {
			case err := <-done:
				t.Fatalf("shutdown returned before in-flight request finished: %v", err)
			case <-time.After(150 * time.Millisecond):
			}

			close(release)

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("shutdown after release: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown did not finish after releasing in-flight request")
			}
		})
	}
}

func TestOutboxPayload_MatchesRelayRequestBody(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		payload    []byte
		wantMethod string
		wantPath   string
	}{
		{
			name:       "update payload body is forwarded as exact request body",
			action:     string(actionUpdate),
			payload:    []byte(`{"doc":{"title":"from-outbox"}}`),
			wantMethod: http.MethodPost,
			wantPath:   "/blocker_articles/_update/1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			setupOutboxTable(t, db)
			s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

			var reqMu sync.Mutex
			var gotMethod, gotPath string
			var gotBody []byte
			s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				reqMu.Lock()
				gotMethod, gotPath = req.Method, req.URL.Path
				gotBody = body
				reqMu.Unlock()
				return jsonResponse(req, http.StatusOK, `{"result":"ok"}`), nil
			}))

			row := blockerOutboxEvent{
				Table:      "blocker_articles",
				IndexAlias: "blocker_articles",
				DocumentID: "1",
				Action:     tt.action,
				Payload:    tt.payload,
				Status:     "pending",
			}
			if err := db.Create(&row).Error; err != nil {
				t.Fatalf("seed outbox row: %v", err)
			}

			relayCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := startOutboxRelayByReflection(relayCtx, s); err != nil {
				t.Fatalf("start outbox relay: %v", err)
			}

			if err := waitFor(t.Context(), 2*time.Second, func() (bool, error) {
				reqMu.Lock()
				defer reqMu.Unlock()
				return len(gotBody) > 0, nil
			}); err != nil {
				t.Fatalf("wait relay body capture: %v", err)
			}

			reqMu.Lock()
			method, path, body := gotMethod, gotPath, string(gotBody)
			reqMu.Unlock()
			if method != tt.wantMethod || path != tt.wantPath {
				t.Fatalf("relay endpoint = %s %s, want %s %s", method, path, tt.wantMethod, tt.wantPath)
			}
			if body != string(tt.payload) {
				t.Fatalf("relay body = %q, want exact %q", body, string(tt.payload))
			}
		})
	}
}

func TestOutboxPayload_RequiresGTKitJSONInOutboxPath(t *testing.T) {
	tests := []struct {
		name         string
		file         string
		expectImport string
		denyImport   string
	}{
		{
			name:         "outbox store uses gtkit json",
			file:         "outbox_store.go",
			expectImport: `"github.com/gtkit/json/v2"`,
			denyImport:   `"encoding/json"`,
		},
		{
			name:         "outbox relay uses gtkit json",
			file:         "outbox_relay.go",
			expectImport: `"github.com/gtkit/json/v2"`,
			denyImport:   `"encoding/json"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Clean(tt.file)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("outbox path file %s is required for Task 1 red baseline: %v", tt.file, err)
			}
			content := string(data)
			if !strings.Contains(content, tt.expectImport) {
				t.Fatalf("file %s must import %s for outbox payload serialization", tt.file, tt.expectImport)
			}
			if strings.Contains(content, tt.denyImport) {
				t.Fatalf("file %s must not import %s in outbox payload path", tt.file, tt.denyImport)
			}
		})
	}
}

func startOutboxRelayByReflection(ctx context.Context, s *Syncer) error {
	method := reflect.ValueOf(s).MethodByName("StartOutboxRelay")
	if !method.IsValid() {
		return errors.New("StartOutboxRelay is not implemented")
	}
	out := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
	if len(out) != 1 {
		return fmt.Errorf("StartOutboxRelay returned %d values, want 1", len(out))
	}
	if out[0].IsNil() {
		return nil
	}
	err, ok := out[0].Interface().(error)
	if !ok {
		return fmt.Errorf("StartOutboxRelay returned non-error type %T", out[0].Interface())
	}
	return err
}

func setOutboxMaxAttemptsByReflection(s *Syncer, maxAttempts int) error {
	if s == nil {
		return errors.New("syncer is nil")
	}
	if maxAttempts <= 0 {
		return errors.New("max attempts must be > 0")
	}
	s.cfg.Outbox.MaxAttempts = maxAttempts
	return nil
}

func setOutboxPollIntervalByReflection(s *Syncer, d time.Duration) error {
	if s == nil {
		return errors.New("syncer is nil")
	}
	if d <= 0 {
		return errors.New("poll interval must be > 0")
	}
	s.cfg.Outbox.PollInterval = d
	return nil
}

func waitFor(ctx context.Context, timeout time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func ptrInt(n int) *int {
	return &n
}
