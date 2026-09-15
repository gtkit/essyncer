package searcher

import (
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
)

// SearchResult 是泛型搜索结果。
type SearchResult[T any] struct {
	Total      int64                   `json:"total"`
	Items      []T                     `json:"items"`
	MaxScore   *types.Float64          `json:"max_score,omitzero"`
	ScrollID   string                  `json:"scroll_id,omitzero"`
	Highlights map[string]HitHighlight `json:"highlights,omitzero"`
}

// HitHighlight 存储单个文档的高亮片段。
type HitHighlight struct {
	Fields map[string][]string `json:"fields"`
}

// AggResult 是聚合查询结果。
type AggResult struct {
	Aggregations map[string]types.Aggregate `json:"aggregations"`
}

// parseTypedSearchResult 将 ES9 TypedAPI 响应解析为泛型结果。
func parseTypedSearchResult[T any](resp *search.Response) (*SearchResult[T], error) {
	result := &SearchResult[T]{
		Highlights: make(map[string]HitHighlight),
	}

	if resp.Hits.Total != nil {
		result.Total = resp.Hits.Total.Value
	}
	result.MaxScore = resp.Hits.MaxScore
	if resp.ScrollId_ != nil {
		result.ScrollID = *resp.ScrollId_
	}

	result.Items = make([]T, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		docID := "<unknown>"
		if hit.Id_ != nil {
			docID = *hit.Id_
		}

		var item T
		if err := json.Unmarshal(hit.Source_, &item); err != nil {
			return nil, fmt.Errorf("searcher: unmarshal doc %s: %w", docID, err)
		}
		result.Items = append(result.Items, item)

		// 高亮
		if len(hit.Highlight) > 0 && hit.Id_ != nil {
			hl := HitHighlight{Fields: make(map[string][]string, len(hit.Highlight))}
			for field, fragments := range hit.Highlight {
				hl.Fields[field] = fragments
			}
			result.Highlights[*hit.Id_] = hl
		}
	}

	return result, nil
}
