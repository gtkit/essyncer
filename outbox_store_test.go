package essyncer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestOutboxStore_InsertFromEvents_PreservesTableAndDeletePayload(t *testing.T) {
	tests := []struct {
		name        string
		event       outboxWriteEvent
		wantPayload string
	}{
		{
			name: "delete event persists source table and non-null payload",
			event: outboxWriteEvent{
				TableName:  "source_articles",
				IndexAlias: "articles_alias",
				DocumentID: "42",
				Action:     actionDelete,
				Doc:        nil,
			},
			wantPayload: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openOutboxStoreTestDB(t)
			store := newOutboxStore(db)

			err := db.WithContext(t.Context()).Transaction(func(tx *gorm.DB) error {
				return store.insertFromEvents(t.Context(), tx, []outboxWriteEvent{tt.event})
			})
			if err != nil {
				t.Fatalf("insert outbox events: %v", err)
			}

			var got OutboxEvent
			if err := db.WithContext(t.Context()).First(&got).Error; err != nil {
				t.Fatalf("load outbox row: %v", err)
			}
			if got.TableName != tt.event.TableName {
				t.Fatalf("table_name = %q, want %q", got.TableName, tt.event.TableName)
			}
			if got.IndexAlias != tt.event.IndexAlias {
				t.Fatalf("index_alias = %q, want %q", got.IndexAlias, tt.event.IndexAlias)
			}
			if string(got.Payload) != tt.wantPayload {
				t.Fatalf("payload = %q, want %q", string(got.Payload), tt.wantPayload)
			}
		})
	}
}

func TestOutboxStore_ClaimPending_ReclaimsExpiredAndClaimsExclusively(t *testing.T) {
	tests := []struct {
		name      string
		seed      func(now time.Time) []OutboxEvent
		assertion func(t *testing.T, storeA, storeB *outboxStore)
	}{
		{
			name: "pending row is claimed by at most one concurrent claimant",
			seed: func(time.Time) []OutboxEvent {
				return []OutboxEvent{
					{
						TableName:  "source_articles",
						IndexAlias: "articles_alias",
						DocumentID: "1",
						Action:     string(actionIndex),
						Payload:    []byte(`{"id":1}`),
						Status:     OutboxStatusPending,
					},
				}
			},
			assertion: func(t *testing.T, storeA, storeB *outboxStore) {
				t.Helper()
				first, err := storeA.claimPending(t.Context(), 1, 30*time.Second)
				if err != nil {
					t.Fatalf("first claim: %v", err)
				}
				second, err := storeB.claimPending(t.Context(), 1, 30*time.Second)
				if err != nil {
					t.Fatalf("second claim: %v", err)
				}
				if len(first) != 1 || len(second) != 0 {
					t.Fatalf("expected claims len(first)=1 len(second)=0, got %d %d", len(first), len(second))
				}

				now := time.Now().UTC()
				leaseUntil := now.Add(30 * time.Second)
				ok, err := storeB.tryClaimRow(t.Context(), first[0].ID, now, leaseUntil, newLeaseToken())
				if err != nil {
					t.Fatalf("second cas claim check: %v", err)
				}
				if ok {
					t.Fatal("expected compare-and-swap claim to reject already claimed row")
				}
			},
		},
		{
			name: "expired processing row is reclaimable",
			seed: func(now time.Time) []OutboxEvent {
				expired := now.Add(-1 * time.Minute)
				return []OutboxEvent{
					{
						TableName:   "source_articles",
						IndexAlias:  "articles_alias",
						DocumentID:  "2",
						Action:      string(actionUpdate),
						Payload:     []byte(`{"doc":{"title":"stale"}}`),
						Status:      OutboxStatusProcessing,
						LeasedUntil: &expired,
					},
				}
			},
			assertion: func(t *testing.T, storeA, _ *outboxStore) {
				t.Helper()
				claimed, err := storeA.claimPending(t.Context(), 1, 30*time.Second)
				if err != nil {
					t.Fatalf("claim expired row: %v", err)
				}
				if len(claimed) != 1 {
					t.Fatalf("expected expired processing row reclaimed once, got %d", len(claimed))
				}
				if claimed[0].Status != OutboxStatusProcessing || claimed[0].LeasedUntil == nil {
					t.Fatalf("expected reclaimed row to be processing with lease, got %#v", claimed[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openOutboxStoreTestDB(t)
			storeA := newOutboxStore(db)
			storeB := newOutboxStore(db)
			now := time.Now().UTC()
			storeA.now = func() time.Time { return now }
			storeB.now = func() time.Time { return now }

			rows := tt.seed(now)
			if err := db.WithContext(t.Context()).Create(&rows).Error; err != nil {
				t.Fatalf("seed outbox rows: %v", err)
			}
			tt.assertion(t, storeA, storeB)
		})
	}
}

func TestOutboxStore_MarkUpdates_RequireMatchingLease(t *testing.T) {
	tests := []struct {
		name   string
		update func(ctx context.Context, store *outboxStore, claimed OutboxEvent) error
	}{
		{
			name: "markSent rejects stale lease",
			update: func(ctx context.Context, store *outboxStore, claimed OutboxEvent) error {
				return store.markSent(ctx, claimed)
			},
		},
		{
			name: "markRetry rejects stale lease",
			update: func(ctx context.Context, store *outboxStore, claimed OutboxEvent) error {
				return store.markRetry(ctx, claimed, time.Now().UTC().Add(time.Minute), errors.New("retry"))
			},
		},
		{
			name: "markDead rejects stale lease",
			update: func(ctx context.Context, store *outboxStore, claimed OutboxEvent) error {
				return store.markDead(ctx, claimed, errors.New("dead"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openOutboxStoreTestDB(t)
			store := newOutboxStore(db)

			seed := OutboxEvent{
				TableName:  "source_articles",
				IndexAlias: "articles_alias",
				DocumentID: "3",
				Action:     string(actionUpdate),
				Payload:    []byte(`{"doc":{"title":"lease"}}`),
				Status:     OutboxStatusPending,
			}
			if err := db.WithContext(t.Context()).Create(&seed).Error; err != nil {
				t.Fatalf("seed row: %v", err)
			}

			claimed, err := store.claimPending(t.Context(), 1, 30*time.Second)
			if err != nil {
				t.Fatalf("claim row: %v", err)
			}
			if len(claimed) != 1 {
				t.Fatalf("expected 1 claimed row, got %d", len(claimed))
			}

			// 模拟另一个 relay 抢走租约：lease_token 是 fencing 凭证，
			// 原持有者手里的旧 token 必须再也更新不了这一行。
			if updateErr := db.WithContext(t.Context()).
				Model(&OutboxEvent{}).
				Where("id = ?", claimed[0].ID).
				Updates(map[string]any{
					"leased_until": time.Now().UTC().Add(time.Minute),
					"lease_token":  newLeaseToken(),
				}).Error; updateErr != nil {
				t.Fatalf("simulate lease handoff: %v", updateErr)
			}

			err = tt.update(t.Context(), store, claimed[0])
			if err == nil {
				t.Fatal("expected stale lease error")
			}
			if !strings.Contains(err.Error(), "lease mismatch") {
				t.Fatalf("unexpected error: %v", err)
			}

			var got OutboxEvent
			if err := db.WithContext(t.Context()).First(&got, claimed[0].ID).Error; err != nil {
				t.Fatalf("reload row: %v", err)
			}
			if got.Status != OutboxStatusProcessing {
				t.Fatalf("status = %s, want %s", got.Status, OutboxStatusProcessing)
			}
		})
	}
}

func openOutboxStoreTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_busy_timeout=5000", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&OutboxEvent{}); err != nil {
		t.Fatalf("migrate outbox table: %v", err)
	}
	return db
}
