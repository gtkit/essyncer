package searcher

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"golang.org/x/sync/singleflight"
)

// 全局 singleflight group，合并相同查询条件的并发请求。
var sfGroup singleflight.Group

// Search 是泛型链式 ES8 查询构建器。
// 默认自动过滤软删除文档，可通过 Unscoped() 取消。
type Search[T any] struct {
	client *elasticsearch.TypedClient
	index  string

	must    []types.Query
	should  []types.Query
	mustNot []types.Query
	filter  []types.Query

	sorts       []types.SortCombinations
	from        *int
	size        *int
	searchAfter []types.FieldValue

	highlightFields []string

	aggs map[string]types.Aggregations

	unscoped        bool
	minShouldMatch  *int
	includes        []string
	excludes        []string
	useSingleflight bool // 是否启用 singleflight 合并
}

// NewSearch 创建泛型搜索构建器。
func NewSearch[T any](client *elasticsearch.TypedClient, index string) *Search[T] {
	return &Search[T]{
		client: client,
		index:  index,
		aggs:   make(map[string]types.Aggregations),
	}
}

// --- 布尔查询 ---

func (s *Search[T]) Must(queries ...types.Query) *Search[T] {
	s.must = append(s.must, queries...)
	return s
}

func (s *Search[T]) Should(queries ...types.Query) *Search[T] {
	s.should = append(s.should, queries...)
	return s
}

func (s *Search[T]) MustNot(queries ...types.Query) *Search[T] {
	s.mustNot = append(s.mustNot, queries...)
	return s
}

func (s *Search[T]) Filter(queries ...types.Query) *Search[T] {
	s.filter = append(s.filter, queries...)
	return s
}

func (s *Search[T]) Where(queries ...types.Query) *Search[T] {
	return s.Filter(queries...)
}

func (s *Search[T]) MinimumShouldMatch(n int) *Search[T] {
	s.minShouldMatch = &n
	return s
}

// --- 排序 ---

func (s *Search[T]) Sort(field string, ascending bool) *Search[T] {
	var order sortorder.SortOrder
	if ascending {
		order = sortorder.Asc
	} else {
		order = sortorder.Desc
	}
	s.sorts = append(s.sorts, types.SortOptions{
		SortOptions: map[string]types.FieldSort{
			field: {Order: &order},
		},
	})
	return s
}

// --- 分页 ---

func (s *Search[T]) Page(page, pageSize int) *Search[T] {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	from := (page - 1) * pageSize
	s.from = &from
	s.size = &pageSize
	return s
}

func (s *Search[T]) Offset(from, size int) *Search[T] {
	s.from = &from
	s.size = &size
	return s
}

func (s *Search[T]) SearchAfter(values ...types.FieldValue) *Search[T] {
	s.searchAfter = values
	return s
}

// --- 高亮 ---

func (s *Search[T]) Highlight(fields ...string) *Search[T] {
	s.highlightFields = append(s.highlightFields, fields...)
	return s
}

// --- 聚合 ---

func (s *Search[T]) Agg(name string, agg types.Aggregations) *Search[T] {
	s.aggs[name] = agg
	return s
}

// --- 软删除 ---

func (s *Search[T]) Unscoped() *Search[T] {
	s.unscoped = true
	return s
}

// --- 字段选择 ---

func (s *Search[T]) Select(fields ...string) *Search[T] {
	s.includes = append(s.includes, fields...)
	return s
}

func (s *Search[T]) Exclude(fields ...string) *Search[T] {
	s.excludes = append(s.excludes, fields...)
	return s
}

// --- Singleflight ---

// WithSingleflight 启用 singleflight：相同查询条件的并发请求合并为一次 ES 调用。
func (s *Search[T]) WithSingleflight() *Search[T] {
	s.useSingleflight = true
	return s
}

// --- 执行 ---

func (s *Search[T]) Do(ctx context.Context) (*SearchResult[T], error) {
	if s.useSingleflight {
		key := s.cacheKey()
		v, err, _ := sfGroup.Do(key, func() (any, error) {
			return s.doSearch(ctx)
		})
		if err != nil {
			return nil, err
		}
		return v.(*SearchResult[T]), nil
	}
	return s.doSearch(ctx)
}

func (s *Search[T]) doSearch(ctx context.Context) (*SearchResult[T], error) {
	req := s.buildRequest()

	resp, err := s.client.Search().
		Index(s.index).
		Request(req).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("searcher: search: %w", err)
	}

	return parseTypedSearchResult[T](resp)
}

// DoAgg 执行聚合查询（size=0，不返回文档）。
func (s *Search[T]) DoAgg(ctx context.Context) (*AggResult, error) {
	req := s.buildRequest()
	zero := 0
	req.Size = &zero

	resp, err := s.client.Search().
		Index(s.index).
		Request(req).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("searcher: agg: %w", err)
	}

	return &AggResult{Aggregations: resp.Aggregations}, nil
}

// Count 仅返回匹配文档总数。
func (s *Search[T]) Count(ctx context.Context) (int64, error) {
	query := s.buildQuery()
	resp, err := s.client.Count().
		Index(s.index).
		Query(query).
		Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("searcher: count: %w", err)
	}
	return resp.Count, nil
}

// --- 内部构建 ---

func (s *Search[T]) buildQuery() *types.Query {
	boolQ := &types.BoolQuery{}

	if len(s.must) > 0 {
		boolQ.Must = s.must
	}
	if len(s.should) > 0 {
		boolQ.Should = s.should
	}
	if len(s.mustNot) > 0 {
		boolQ.MustNot = s.mustNot
	}
	if len(s.filter) > 0 {
		boolQ.Filter = s.filter
	}
	if s.minShouldMatch != nil {
		msm := fmt.Sprintf("%d", *s.minShouldMatch)
		boolQ.MinimumShouldMatch = &msm
	}

	// 默认过滤软删除
	if !s.unscoped {
		boolQ.MustNot = append(boolQ.MustNot, types.Query{
			Exists: &types.ExistsQuery{Field: "deleted_at"},
		})
	}

	return &types.Query{Bool: boolQ}
}

func (s *Search[T]) buildRequest() *search.Request {
	req := &search.Request{
		Query: s.buildQuery(),
	}

	if s.from != nil {
		req.From = s.from
	}
	if s.size != nil {
		req.Size = s.size
	}

	if len(s.sorts) > 0 {
		req.Sort = s.sorts
	}

	if len(s.searchAfter) > 0 {
		req.SearchAfter = s.searchAfter
	}

	// 高亮
	if len(s.highlightFields) > 0 {
		fields := make(map[string]types.HighlightField, len(s.highlightFields))
		for _, f := range s.highlightFields {
			fields[f] = types.HighlightField{}
		}
		req.Highlight = &types.Highlight{Fields: fields}
	}

	// 聚合
	if len(s.aggs) > 0 {
		req.Aggregations = s.aggs
	}

	// _source filtering
	if len(s.includes) > 0 || len(s.excludes) > 0 {
		filter := &types.SourceFilter{}
		if len(s.includes) > 0 {
			filter.Includes = s.includes
		}
		if len(s.excludes) > 0 {
			filter.Excludes = s.excludes
		}
		req.Source_ = filter
	}

	return req
}

// cacheKey 生成查询的唯一标识，用于 singleflight 去重。
func (s *Search[T]) cacheKey() string {
	req := s.buildRequest()
	data, _ := json.Marshal(req)
	h := sha256.Sum256(data)
	return fmt.Sprintf("%p:%s:%x", s.client, s.index, h[:8])
}
