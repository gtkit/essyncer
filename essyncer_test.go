package essyncer

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// --- 测试模型 ---

type testArticle struct {
	ID        int64          `json:"id"                  gorm:"primaryKey"`
	Title     string         `json:"title"`
	DeletedAt gorm.DeletedAt `json:"deleted_at,omitzero" gorm:"index"`
}

func (testArticle) TableName() string { return "articles" }
func (testArticle) GetID() string     { return "1" }

type testUser struct {
	ID   int64  `json:"id"   gorm:"primaryKey"`
	Name string `json:"name"`
}

func (testUser) TableName() string { return "users" }
func (testUser) GetID() string     { return "1" }

// --- Config ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.Elasticsearch.Addresses) != 1 {
		t.Errorf("expected 1 address, got %d", len(cfg.Elasticsearch.Addresses))
	}
	if cfg.Sync.Workers != 4 {
		t.Errorf("expected 4 workers, got %d", cfg.Sync.Workers)
	}
	if cfg.Sync.FlushInterval != 2*time.Second {
		t.Errorf("expected 2s flush, got %v", cfg.Sync.FlushInterval)
	}
}

func TestTableConfig_IsAutoSync(t *testing.T) {
	tests := []struct {
		name string
		tc   TableConfig
		want bool
	}{
		{"nil defaults true", TableConfig{}, true},
		{"true", TableConfig{AutoSync: new(true)}, true},
		{"false", TableConfig{AutoSync: new(false)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tc.IsAutoSync(); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// --- Soft Delete Detection ---

func TestHasSoftDeleteField(t *testing.T) {
	tests := []struct {
		name  string
		model any
		want  bool
	}{
		{"with DeletedAt", &testArticle{}, true},
		{"without DeletedAt", &testUser{}, false},
		{"embedded gorm.Model", &struct{ gorm.Model }{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasSoftDeleteField(tt.model); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// --- Full Sync Key ---

func TestNewFullSyncKey(t *testing.T) {
	tests := []struct {
		name            string
		model           any
		wantColumn      string
		wantUnsigned    bool
		wantErrContains string
	}{
		{
			name:         "int64 id is supported",
			model:        &testArticle{},
			wantColumn:   "id",
			wantUnsigned: false,
		},
		{
			name: "uint id is supported",
			model: &struct {
				ID uint `gorm:"primaryKey"`
			}{},
			wantColumn:   "id",
			wantUnsigned: true,
		},
		{
			name: "string id is rejected",
			model: &struct {
				ID string `gorm:"primaryKey"`
			}{},
			wantErrContains: "requires exactly one integer primary key",
		},
		{
			name: "composite primary key is rejected",
			model: &struct {
				TenantID int64 `gorm:"primaryKey"`
				ID       int64 `gorm:"primaryKey"`
			}{},
			wantErrContains: "requires exactly one integer primary key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openBlockerTestDB(t)
			stmt := &gorm.Statement{DB: db}
			if err := stmt.Parse(tt.model); err != nil {
				t.Fatalf("parse model: %v", err)
			}

			key, err := newFullSyncKey(stmt.Schema)
			if tt.wantErrContains != "" {
				if err == nil {
					t.Fatal("expected full sync key error")
				}
				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tt.wantErrContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("new full sync key: %v", err)
			}
			if key.columnName != tt.wantColumn {
				t.Fatalf("column name = %q, want %q", key.columnName, tt.wantColumn)
			}
			if key.unsigned != tt.wantUnsigned {
				t.Fatalf("unsigned = %v, want %v", key.unsigned, tt.wantUnsigned)
			}
		})
	}
}

// --- Model Slice ---

func TestNewModelSlice(t *testing.T) {
	entry := &modelEntry{modelType: reflect.TypeFor[testArticle]()}
	slicePtr := newModelSlice(entry)
	v := reflect.ValueOf(slicePtr)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Slice {
		t.Fatal("expected pointer to slice")
	}
	if v.Elem().Type().Elem() != reflect.TypeFor[testArticle]() {
		t.Errorf("unexpected element type: %v", v.Elem().Type().Elem())
	}
}

// --- Deep Copy ---

func TestDeepCopyModel_Map(t *testing.T) {
	src := map[string]any{"title": "hello"}
	dst, err := deepCopyModel(src)
	if err != nil {
		t.Fatal(err)
	}
	// map 应直接返回
	if dst.(map[string]any)["title"] != "hello" {
		t.Error("map should pass through")
	}
}

func TestDeepCopyModel_Struct(t *testing.T) {
	src := &testArticle{ID: 42, Title: "original"}
	dst, err := deepCopyModel(src)
	if err != nil {
		t.Fatal(err)
	}
	copied, ok := dst.(*testArticle)
	if !ok {
		t.Fatalf("expected *testArticle, got %T", dst)
	}
	if copied.ID != 42 || copied.Title != "original" {
		t.Error("copy fields mismatch")
	}
	// 修改原始不影响拷贝
	src.Title = "modified"
	if copied.Title != "original" {
		t.Error("deep copy should be independent")
	}
}

// --- Mapping ---

func TestLoadMappingFromFile_ValidJSON(t *testing.T) {
	tmp := t.TempDir() + "/m.json"
	data, err := json.Marshal(map[string]any{"mappings": map[string]any{}})
	if err != nil {
		t.Fatalf("marshal valid mapping: %v", err)
	}
	if writeErr := os.WriteFile(tmp, data, 0o644); writeErr != nil {
		t.Fatalf("write valid mapping: %v", writeErr)
	}

	result, err := LoadMappingFromFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(result) {
		t.Error("not valid JSON")
	}
}

func TestLoadMappingFromFile_InvalidJSON(t *testing.T) {
	tmp := t.TempDir() + "/bad.json"
	if err := os.WriteFile(tmp, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write invalid mapping: %v", err)
	}
	_, err := LoadMappingFromFile(tmp)
	if err == nil {
		t.Error("expected error")
	}
}

func TestLoadMappingFromFile_NotExists(t *testing.T) {
	_, err := LoadMappingFromFile("/nonexistent/file.json")
	if err == nil {
		t.Error("expected error")
	}
}

// --- Options ---

func TestRegisterOptions(t *testing.T) {
	entry := &modelEntry{batchSize: 1000, autoSync: true, softDeleteMode: SoftDeleteModeUpdate}

	WithIndexName("my_index")(entry)
	if entry.indexName != "my_index" {
		t.Errorf("got %s", entry.indexName)
	}
	WithBatchSize(500)(entry)
	if entry.batchSize != 500 {
		t.Errorf("got %d", entry.batchSize)
	}
	WithAutoSync(false)(entry)
	if entry.autoSync {
		t.Error("expected false")
	}
	WithSoftDeleteMode(SoftDeleteModeDelete)(entry)
	if entry.softDeleteMode != SoftDeleteModeDelete {
		t.Errorf("got %s", entry.softDeleteMode)
	}
	WithFullDocUpdate(true)(entry)
	if !entry.fullDocUpdate {
		t.Error("expected true")
	}
	// 0 should not change
	WithBatchSize(0)(entry)
	if entry.batchSize != 500 {
		t.Errorf("got %d", entry.batchSize)
	}
}

func TestRegister_WithMappingFileReturnsError(t *testing.T) {
	db := openBlockerTestDB(t)
	s := &Syncer{
		db:       db,
		registry: newModelRegistry(),
	}

	err := s.Register(&testArticle{}, WithMappingFile("/definitely/missing/mapping.json"))
	if err == nil {
		t.Fatal("expected register to fail when mapping file cannot be loaded")
	}
}

// --- Logger ---

func TestDefaultLogger(t *testing.T) {
	logger := defaultLogger()
	if logger == nil {
		t.Fatal("nil logger")
	}
	// 应该不 panic
	logger.Info("test info")
	logger.Warn("test warn")
	logger.Error("test error")
	logger.Debug("test debug")
}
