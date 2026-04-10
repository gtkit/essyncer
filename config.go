package essyncer

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type SoftDeleteMode string

const (
	SoftDeleteModeUpdate SoftDeleteMode = "update"
	SoftDeleteModeDelete SoftDeleteMode = "delete"
)

type Config struct {
	Elasticsearch ESConfig     `yaml:"elasticsearch"`
	Sync          SyncConfig   `yaml:"sync"`
	Outbox        OutboxConfig `yaml:"outbox"`
}

type ESConfig struct {
	Addresses        []string `yaml:"addresses"`
	Username         string   `yaml:"username"`
	Password         string   `yaml:"password"`
	MaxRetries       int      `yaml:"max_retries"`
	RetryOnStatus    []int    `yaml:"retry_on_status"`
	CACert           string   `yaml:"ca_cert"`            // ES8 TLS 证书路径（可选）
	AllowInsecureTLS bool     `yaml:"allow_insecure_tls"` // 仅开发调试时显式启用
}

type SyncConfig struct {
	Workers          int           `yaml:"workers"`
	FlushBytes       int           `yaml:"flush_bytes"`
	FlushInterval    time.Duration `yaml:"flush_interval"`
	DefaultBatchSize int           `yaml:"default_batch_size"`
	Tables           []TableConfig `yaml:"tables"`
}

type OutboxConfig struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	BatchSize    int           `yaml:"batch_size"`
	MaxAttempts  int           `yaml:"max_attempts"`
	Lease        time.Duration `yaml:"lease_duration"`
}

type TableConfig struct {
	Model          string         `yaml:"model"`
	IndexName      string         `yaml:"index_name"`
	MappingFile    string         `yaml:"mapping_file"`
	BatchSize      int            `yaml:"batch_size"`
	AutoSync       *bool          `yaml:"auto_sync"`
	SoftDeleteMode SoftDeleteMode `yaml:"soft_delete_mode"`
	FullDocUpdate  bool           `yaml:"full_doc_update"`
}

func (tc TableConfig) IsAutoSync() bool {
	if tc.AutoSync == nil {
		return true
	}
	return *tc.AutoSync
}

func DefaultConfig() Config {
	return Config{
		Elasticsearch: ESConfig{
			Addresses:     []string{"http://localhost:9200"},
			MaxRetries:    3,
			RetryOnStatus: []int{502, 503, 504, 429},
		},
		Sync: SyncConfig{
			Workers:          4,
			FlushBytes:       5 << 20,
			FlushInterval:    2 * time.Second,
			DefaultBatchSize: 1000,
		},
		Outbox: OutboxConfig{
			PollInterval: 2 * time.Second,
			BatchSize:    100,
			MaxAttempts:  8,
			Lease:        30 * time.Second,
		},
	}
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("essyncer: read config %s: %w", path, err)
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("essyncer: parse config: %w", err)
	}
	return cfg, nil
}

func LoadMappingFromFile(path string) (json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("essyncer: read mapping %s: %w", path, err)
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("essyncer: invalid JSON in mapping %s", path)
	}
	return data, nil
}
