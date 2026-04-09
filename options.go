package essyncer

import (
	"encoding/json"
	"time"
)

type Option func(*Syncer)

func WithLogger(logger Logger) Option {
	return func(s *Syncer) { s.logger = logger }
}

func WithFailureHook(fn func(FailureEvent)) Option {
	return func(s *Syncer) { s.failureHook = fn }
}

func WithFailureBufferSize(n int) Option {
	return func(s *Syncer) {
		if n > 0 {
			s.failureBufferSize = n
		}
	}
}

func WithWorkers(n int) Option {
	return func(s *Syncer) {
		if n > 0 {
			s.cfg.Sync.Workers = n
		}
	}
}

func WithFlushBytes(bytes int) Option {
	return func(s *Syncer) {
		if bytes > 0 {
			s.cfg.Sync.FlushBytes = bytes
		}
	}
}

func WithFlushInterval(d time.Duration) Option {
	return func(s *Syncer) {
		if d > 0 {
			s.cfg.Sync.FlushInterval = d
		}
	}
}

func WithDefaultBatchSize(size int) Option {
	return func(s *Syncer) {
		if size > 0 {
			s.cfg.Sync.DefaultBatchSize = size
		}
	}
}

// --- RegisterOption ---

type RegisterOption func(*modelEntry)

func setOptionError(e *modelEntry, err error) {
	if err == nil || e.optionErr != nil {
		return
	}
	e.optionErr = err
}

func WithIndexName(name string) RegisterOption     { return func(e *modelEntry) { e.indexName = name } }
func WithMapping(m json.RawMessage) RegisterOption { return func(e *modelEntry) { e.mapping = m } }
func WithBatchSize(size int) RegisterOption {
	return func(e *modelEntry) {
		if size > 0 {
			e.batchSize = size
		}
	}
}
func WithAutoSync(enabled bool) RegisterOption { return func(e *modelEntry) { e.autoSync = enabled } }
func WithSoftDeleteMode(mode SoftDeleteMode) RegisterOption {
	return func(e *modelEntry) { e.softDeleteMode = mode }
}
func WithFullDocUpdate(enabled bool) RegisterOption {
	return func(e *modelEntry) { e.fullDocUpdate = enabled }
}

func WithMappingMap(m map[string]any) RegisterOption {
	return func(e *modelEntry) {
		data, err := json.Marshal(m)
		if err != nil {
			setOptionError(e, err)
			return
		}
		e.mapping = data
	}
}

func WithMappingFile(path string) RegisterOption {
	return func(e *modelEntry) {
		data, err := LoadMappingFromFile(path)
		if err != nil {
			setOptionError(e, err)
			return
		}
		e.mapping = data
	}
}
