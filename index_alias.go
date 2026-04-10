package essyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
)

type aliasState struct {
	aliasName         string
	targets           []string
	legacyConcreteIdx bool
}

func (e *modelEntry) newBootstrapIndexName(now time.Time) string {
	return e.newManagedIndexName("bootstrap", now)
}

func (e *modelEntry) newRebuildIndexName(now time.Time) string {
	return e.newManagedIndexName("fullsync", now)
}

func (s *Syncer) resolveAliasState(ctx context.Context, alias string) (aliasState, error) {
	state := aliasState{aliasName: alias}

	res, err := s.es.Indices.GetAlias(
		s.es.Indices.GetAlias.WithName(alias),
		s.es.Indices.GetAlias.WithContext(ctx),
	)
	if err != nil {
		return state, fmt.Errorf("essyncer: get alias %s: %w", alias, err)
	}
	defer func() { _ = res.Body.Close() }()

	switch res.StatusCode {
	case 200:
		var payload map[string]struct {
			Aliases map[string]json.RawMessage `json:"aliases"`
		}
		if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
			return state, fmt.Errorf("essyncer: decode alias %s: %w", alias, err)
		}
		for indexName, details := range payload {
			if _, ok := details.Aliases[alias]; ok {
				state.targets = append(state.targets, indexName)
			}
		}
		slices.Sort(state.targets)
		return state, nil
	case 404:
		exists, err := s.indexExists(ctx, alias)
		if err != nil {
			return state, err
		}
		if exists {
			state.targets = []string{alias}
			state.legacyConcreteIdx = true
		}
		return state, nil
	default:
		return state, fmt.Errorf("essyncer: get alias %s: %s", alias, res.String())
	}
}

func (s *Syncer) indexExists(ctx context.Context, index string) (bool, error) {
	res, err := s.es.Indices.Exists([]string{index}, s.es.Indices.Exists.WithContext(ctx))
	if err != nil {
		return false, fmt.Errorf("essyncer: check index %s: %w", index, err)
	}
	defer func() { _ = res.Body.Close() }()

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("essyncer: check index %s: %s", index, res.String())
	}
}

func (s *Syncer) createManagedIndex(ctx context.Context, entry *modelEntry, physicalIndex string, aliases ...string) error {
	body, err := buildIndexCreateBody(entry.mapping, aliases...)
	if err != nil {
		return fmt.Errorf("essyncer: build index body for %s: %w", physicalIndex, err)
	}

	opts := []func(*esapi.IndicesCreateRequest){
		s.es.Indices.Create.WithContext(ctx),
	}
	if body != nil {
		opts = append(opts, s.es.Indices.Create.WithBody(body))
	}
	res, err := s.es.Indices.Create(physicalIndex, opts...)
	if err != nil {
		return fmt.Errorf("essyncer: create index %s: %w", physicalIndex, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return fmt.Errorf("essyncer: create index %s: %s", physicalIndex, res.String())
	}

	s.logger.Info("essyncer: index created",
		zap.String("index", physicalIndex),
		zap.String("alias", entry.indexName),
		zap.Bool("alias_attached", len(aliases) > 0),
		zap.Bool("has_mapping", entry.mapping != nil),
	)
	return nil
}

func buildIndexCreateBody(mapping json.RawMessage, aliases ...string) (io.Reader, error) {
	if mapping == nil && len(aliases) == 0 {
		return nil, nil
	}

	payload := make(map[string]any)
	if mapping != nil {
		if err := json.Unmarshal(mapping, &payload); err != nil {
			return nil, err
		}
	}
	if len(aliases) > 0 {
		aliasPayload := make(map[string]any)
		if existing, ok := payload["aliases"].(map[string]any); ok {
			maps.Copy(aliasPayload, existing)
		}
		for _, alias := range aliases {
			aliasPayload[alias] = map[string]any{"is_write_index": true}
		}
		payload["aliases"] = aliasPayload
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func (s *Syncer) switchAliasToIndex(ctx context.Context, state aliasState, newIndex string) error {
	body, err := buildAliasUpdateBody(state, newIndex)
	if err != nil {
		return fmt.Errorf("essyncer: build alias update for %s: %w", state.aliasName, err)
	}

	res, err := s.es.Indices.UpdateAliases(
		bytes.NewReader(body),
		s.es.Indices.UpdateAliases.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("essyncer: switch alias %s to %s: %w", state.aliasName, newIndex, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return fmt.Errorf("essyncer: switch alias %s to %s: %s", state.aliasName, newIndex, res.String())
	}

	return nil
}

func buildAliasUpdateBody(state aliasState, newIndex string) ([]byte, error) {
	actions := make([]map[string]any, 0, len(state.targets)+1)
	for _, current := range state.targets {
		if state.legacyConcreteIdx && current == state.aliasName {
			actions = append(actions, map[string]any{
				"remove_index": map[string]any{"index": current},
			})
			continue
		}
		actions = append(actions, map[string]any{
			"remove": map[string]any{
				"index": current,
				"alias": state.aliasName,
			},
		})
	}
	actions = append(actions, map[string]any{
		"add": map[string]any{
			"index":          newIndex,
			"alias":          state.aliasName,
			"is_write_index": true,
		},
	})

	return json.Marshal(map[string]any{"actions": actions})
}

func (s *Syncer) deleteIndices(ctx context.Context, indices ...string) error {
	if len(indices) == 0 {
		return nil
	}

	res, err := s.es.Indices.Delete(indices, s.es.Indices.Delete.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("essyncer: delete indices %v: %w", indices, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return fmt.Errorf("essyncer: delete indices %v: %s", indices, res.String())
	}
	return nil
}

func (s *Syncer) deleteReplacedIndices(ctx context.Context, state aliasState) error {
	if state.legacyConcreteIdx || len(state.targets) == 0 {
		return nil
	}
	return s.deleteIndices(ctx, state.targets...)
}

func (s *Syncer) cleanupDetachedIndex(ctx context.Context, index string) {
	if index == "" {
		return
	}
	if err := s.deleteIndices(ctx, index); err != nil {
		s.logger.Warn("essyncer: cleanup detached rebuild index",
			zap.String("index", index),
			zap.Error(err),
		)
	}
}
