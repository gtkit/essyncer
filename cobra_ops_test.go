package essyncer

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestOpsCommand_FullSync(t *testing.T) {
	db := openScopedTestDB(t)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
		t.Fatalf("register blocker user: %v", err)
	}

	cmd := NewOpsCommand(CobraOpsOptions{
		Syncer: s,
		Models: map[string]Syncable{
			"User": &blockerUser{},
		},
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"full-sync", "User"})

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute full-sync command: %v", err)
	}
	if !strings.Contains(out.String(), "blocker_users synced=1") {
		t.Fatalf("unexpected full-sync output: %q", out.String())
	}
}

func TestOpsCommand_DocCreateUpdateDeleteGet(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocker_articles/_doc/1":
			return jsonResponse(req, http.StatusOK, `{"found":true}`), nil
		default:
			return jsonResponse(req, http.StatusOK, `{}`), nil
		}
	}))

	opts := CobraOpsOptions{
		Syncer: s,
		Models: map[string]Syncable{
			"Article": &blockerArticle{},
		},
	}

	createCmd := NewOpsCommand(opts)
	var createOut bytes.Buffer
	createCmd.SetOut(&createOut)
	createCmd.SetErr(&createOut)
	createCmd.SetArgs([]string{"doc", "create", "--model", "Article", "--pk", "1"})
	if err := createCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute doc create command: %v", err)
	}
	rows := listOutboxEvents(t, db)
	if len(rows) != 1 || rows[0].Action != string(actionIndex) {
		t.Fatalf("expected index outbox row after doc create, got %#v", rows)
	}

	if _, err := s.CleanupOutbox(context.Background(), OutboxCleanupOptions{Statuses: []string{OutboxStatusPending}}); err != nil {
		t.Fatalf("cleanup outbox rows: %v", err)
	}

	updateCmd := NewOpsCommand(opts)
	var updateOut bytes.Buffer
	updateCmd.SetOut(&updateOut)
	updateCmd.SetErr(&updateOut)
	updateCmd.SetArgs([]string{"doc", "update", "--model", "Article", "--pk", "1"})
	if err := updateCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute doc update command: %v", err)
	}
	rows = listOutboxEvents(t, db)
	if len(rows) != 1 || rows[0].Action != string(actionUpdate) {
		t.Fatalf("expected update outbox row after doc update, got %#v", rows)
	}

	if _, err := s.CleanupOutbox(context.Background(), OutboxCleanupOptions{Statuses: []string{OutboxStatusPending}}); err != nil {
		t.Fatalf("cleanup outbox rows: %v", err)
	}

	deleteCmd := NewOpsCommand(opts)
	var deleteOut bytes.Buffer
	deleteCmd.SetOut(&deleteOut)
	deleteCmd.SetErr(&deleteOut)
	deleteCmd.SetArgs([]string{"doc", "delete", "--model", "Article", "--id", "1"})
	if err := deleteCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute doc delete command: %v", err)
	}
	rows = listOutboxEvents(t, db)
	if len(rows) != 1 || rows[0].Action != string(actionDelete) {
		t.Fatalf("expected delete outbox row after doc delete, got %#v", rows)
	}

	getCmd := NewOpsCommand(opts)
	var getOut bytes.Buffer
	getCmd.SetOut(&getOut)
	getCmd.SetErr(&getOut)
	getCmd.SetArgs([]string{"doc", "get", "--model", "Article", "--pk", "1"})
	if err := getCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute doc get command: %v", err)
	}
	if !strings.Contains(getOut.String(), "db_found=true") || !strings.Contains(getOut.String(), "es_found=true") {
		t.Fatalf("unexpected doc get output: %q", getOut.String())
	}
}

func TestOpsCommand_OutboxListCleanupReplay(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

	seed := blockerOutboxEvent{
		Table:      "blocker_articles",
		IndexAlias: "blocker_articles",
		DocumentID: "1",
		Action:     string(actionUpdate),
		Payload:    []byte(`{"doc":{"title":"dead"}}`),
		Status:     OutboxStatusDead,
		Attempts:   3,
		LastError:  "boom",
	}
	if err := db.Create(&seed).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}

	listCmd := NewOpsCommand(CobraOpsOptions{Syncer: s})
	var listOut bytes.Buffer
	listCmd.SetOut(&listOut)
	listCmd.SetErr(&listOut)
	listCmd.SetArgs([]string{"outbox", "list", "--status", "dead"})
	if err := listCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute outbox list command: %v", err)
	}
	if !strings.Contains(listOut.String(), "status=dead") {
		t.Fatalf("unexpected outbox list output: %q", listOut.String())
	}

	replayCmd := NewOpsCommand(CobraOpsOptions{Syncer: s})
	var replayOut bytes.Buffer
	replayCmd.SetOut(&replayOut)
	replayCmd.SetErr(&replayOut)
	replayCmd.SetArgs([]string{"outbox", "replay", "--all-dead"})
	if err := replayCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute outbox replay command: %v", err)
	}
	if !strings.Contains(replayOut.String(), "replayed=1") {
		t.Fatalf("unexpected replay output: %q", replayOut.String())
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 || rows[0].Status != OutboxStatusPending {
		t.Fatalf("expected dead row replayed to pending, got %#v", rows)
	}

	if err := db.Model(&blockerOutboxEvent{}).Where("id = ?", rows[0].ID).Update("status", OutboxStatusDead).Error; err != nil {
		t.Fatalf("reset row to dead: %v", err)
	}

	cleanupCmd := NewOpsCommand(CobraOpsOptions{Syncer: s})
	var cleanupOut bytes.Buffer
	cleanupCmd.SetOut(&cleanupOut)
	cleanupCmd.SetErr(&cleanupOut)
	cleanupCmd.SetArgs([]string{"outbox", "cleanup", "--status", "dead"})
	if err := cleanupCmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute outbox cleanup command: %v", err)
	}
	if !strings.Contains(cleanupOut.String(), "deleted=1") {
		t.Fatalf("unexpected cleanup output: %q", cleanupOut.String())
	}
	if rows := listOutboxEvents(t, db); len(rows) != 0 {
		t.Fatalf("expected dead row cleanup, got %#v", rows)
	}
}

func TestOpsCommand_RelayDrain(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, `{"result":"ok"}`), nil
	}))

	row := blockerOutboxEvent{
		Table:      "blocker_articles",
		IndexAlias: "blocker_articles",
		DocumentID: "1",
		Action:     string(actionIndex),
		Payload:    []byte(`{"id":1,"title":"drain"}`),
		Status:     OutboxStatusPending,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}

	cmd := NewOpsCommand(CobraOpsOptions{Syncer: s})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"relay", "drain", "--max-batches", "1"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute relay drain command: %v", err)
	}
	if !strings.Contains(out.String(), "processed=1") {
		t.Fatalf("unexpected drain output: %q", out.String())
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 || rows[0].Status != OutboxStatusSent {
		t.Fatalf("expected drained row to be sent, got %#v", rows)
	}
}

func TestOpsCommand_ReconcileCounts(t *testing.T) {
	db := openScopedTestDB(t)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})
	if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
		t.Fatalf("register blocker user: %v", err)
	}
	s.es = newTestElasticsearchClient(t, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocker_users/_count":
			return jsonResponse(req, http.StatusOK, `{"count":1}`), nil
		default:
			return jsonResponse(req, http.StatusOK, `{}`), nil
		}
	}))

	cmd := NewOpsCommand(CobraOpsOptions{
		Syncer: s,
		Models: map[string]Syncable{
			"User": &blockerUser{},
		},
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"reconcile", "User"})

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute reconcile command: %v", err)
	}
	if !strings.Contains(out.String(), "match=true") {
		t.Fatalf("unexpected reconcile output: %q", out.String())
	}
}

func TestOpsCommand_OutboxCleanupOlderThan(t *testing.T) {
	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	s := newBlockerTestSyncer(t, db, &fakeBulkIndexer{})

	oldTime := time.Now().UTC().Add(-2 * time.Hour)
	old := blockerOutboxEvent{
		Table:      "blocker_articles",
		IndexAlias: "blocker_articles",
		DocumentID: "1",
		Action:     string(actionDelete),
		Payload:    []byte(`{}`),
		Status:     OutboxStatusDead,
		CreatedAt:  oldTime,
	}
	recent := blockerOutboxEvent{
		Table:      "blocker_articles",
		IndexAlias: "blocker_articles",
		DocumentID: "2",
		Action:     string(actionDelete),
		Payload:    []byte(`{}`),
		Status:     OutboxStatusDead,
		CreatedAt:  time.Now().UTC(),
	}
	if err := db.Create(&old).Error; err != nil {
		t.Fatalf("seed old dead row: %v", err)
	}
	if err := db.Create(&recent).Error; err != nil {
		t.Fatalf("seed recent dead row: %v", err)
	}

	cmd := NewOpsCommand(CobraOpsOptions{Syncer: s})
	cmd.SetArgs([]string{"outbox", "cleanup", "--status", "dead", "--older-than", "1h"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute cleanup older-than command: %v", err)
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 || rows[0].DocumentID != "2" {
		t.Fatalf("expected only recent dead row to remain, got %#v", rows)
	}
}
