package essyncer

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type blockerUser struct {
	ID   int64  `json:"id"   gorm:"primaryKey"`
	Name string `json:"name"`
}

func (blockerUser) TableName() string { return "blocker_users" }
func (u blockerUser) GetID() string   { return strconv.FormatInt(u.ID, 10) }

func openScopedTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db := openBlockerTestDB(t)
	setupOutboxTable(t, db)
	if err := db.AutoMigrate(&blockerUser{}); err != nil {
		t.Fatalf("migrate blocker user: %v", err)
	}
	if err := db.Create(&blockerUser{ID: 1, Name: "before"}).Error; err != nil {
		t.Fatalf("seed blocker user: %v", err)
	}
	return db
}

func TestEnableAutoSync_SelectedModelsOnly(t *testing.T) {
	db := openScopedTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)
	if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
		t.Fatalf("register blocker user: %v", err)
	}

	if err := s.EnableAutoSync(db, &blockerArticle{}); err != nil {
		t.Fatalf("enable scoped auto sync: %v", err)
	}
	if err := db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "after"}).Error; err != nil {
		t.Fatalf("update article: %v", err)
	}
	if err := db.Model(&blockerUser{ID: 1}).Updates(map[string]any{"name": "after"}).Error; err != nil {
		t.Fatalf("update user: %v", err)
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 1 {
		t.Fatalf("expected only selected model to write one outbox row, got %d", len(rows))
	}
	if rows[0].IndexAlias != "blocker_articles" || rows[0].Table != "blocker_articles" {
		t.Fatalf("expected only article model to write outbox row, got %#v", rows[0])
	}
}

func TestEnableAutoSync_AllModelsWhenNoModelsSpecified(t *testing.T) {
	db := openScopedTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)
	if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
		t.Fatalf("register blocker user: %v", err)
	}

	if err := s.EnableAutoSync(db); err != nil {
		t.Fatalf("enable auto sync for all: %v", err)
	}
	if err := db.Model(&blockerArticle{ID: 1}).Updates(map[string]any{"title": "after"}).Error; err != nil {
		t.Fatalf("update article: %v", err)
	}
	if err := db.Model(&blockerUser{ID: 1}).Updates(map[string]any{"name": "after"}).Error; err != nil {
		t.Fatalf("update user: %v", err)
	}

	rows := listOutboxEvents(t, db)
	if len(rows) != 2 {
		t.Fatalf("expected both models to write outbox rows, got %d", len(rows))
	}
	gotAliases := []string{rows[0].IndexAlias, rows[1].IndexAlias}
	slices.Sort(gotAliases)
	if !slices.Equal(gotAliases, []string{"blocker_articles", "blocker_users"}) {
		t.Fatalf("expected both aliases in outbox rows, got %#v", gotAliases)
	}
}

func TestFullSync_SelectedModelsOnly(t *testing.T) {
	db := openScopedTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)
	if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
		t.Fatalf("register blocker user: %v", err)
	}

	results := s.FullSync(t.Context(), &blockerUser{})
	if len(results) != 1 {
		t.Fatalf("expected one full sync result, got %d", len(results))
	}
	if results[0].Table != "blocker_users" || results[0].Err != nil {
		t.Fatalf("unexpected full sync result: %#v err=%v", results[0], results[0].Err)
	}
	if len(indexer.items) != 1 || !strings.HasPrefix(indexer.items[0].Index, "blocker_users__fullsync__") {
		t.Fatalf("expected only selected model docs to sync, got %#v", indexer.items)
	}
}

func TestFullSync_AllModelsWhenNoModelsSpecified(t *testing.T) {
	db := openScopedTestDB(t)
	indexer := &fakeBulkIndexer{}
	s := newBlockerTestSyncer(t, db, indexer)
	if err := s.Register(&blockerUser{}, WithIndexName("blocker_users")); err != nil {
		t.Fatalf("register blocker user: %v", err)
	}

	results := s.FullSync(t.Context())
	if len(results) != 2 {
		t.Fatalf("expected two full sync results, got %d", len(results))
	}
	if len(indexer.items) != 2 {
		t.Fatalf("expected both models to full sync, got %#v", indexer.items)
	}
}
