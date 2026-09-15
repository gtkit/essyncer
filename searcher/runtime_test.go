package searcher

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
)

func TestAdvancedQueryBuilders(t *testing.T) {
	tests := []struct {
		name  string
		query types.Query
		check func(*testing.T, types.Query)
	}{
		{
			name:  "match phrase builder stores field and text",
			query: MatchPhrase("title", "hello world"),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.MatchPhrase == nil {
					t.Fatal("expected match_phrase query")
				}
				if query.MatchPhrase["title"].Query != "hello world" {
					t.Fatalf("unexpected match_phrase query: %#v", query.MatchPhrase)
				}
			},
		},
		{
			name:  "term int builder stores integer value",
			query: TermInt("tenant_id", 42),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.Term == nil {
					t.Fatal("expected term query")
				}
				if query.Term["tenant_id"].Value != int64(42) {
					t.Fatalf("unexpected term value: %#v", query.Term["tenant_id"].Value)
				}
			},
		},
		{
			name:  "wildcard builder stores pattern",
			query: Wildcard("title", "go*"),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.Wildcard == nil {
					t.Fatal("expected wildcard query")
				}
				pattern := query.Wildcard["title"].Value
				if pattern == nil || *pattern != "go*" {
					t.Fatalf("unexpected wildcard pattern: %#v", pattern)
				}
			},
		},
		{
			name:  "prefix builder stores prefix",
			query: Prefix("title", "go"),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.Prefix == nil {
					t.Fatal("expected prefix query")
				}
				if query.Prefix["title"].Value != "go" {
					t.Fatalf("unexpected prefix value: %#v", query.Prefix)
				}
			},
		},
		{
			name:  "number range supports gt and lte",
			query: RangeNum("score").Gt(1.5).Lte(9.5).Build(),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.Range == nil {
					t.Fatal("expected range query")
				}
				rangeQuery, ok := query.Range["score"].(types.NumberRangeQuery)
				if !ok {
					t.Fatalf("unexpected number range type: %T", query.Range["score"])
				}
				if rangeQuery.Gt == nil || float64(*rangeQuery.Gt) != 1.5 {
					t.Fatalf("unexpected gt value: %#v", rangeQuery.Gt)
				}
				if rangeQuery.Lte == nil || float64(*rangeQuery.Lte) != 9.5 {
					t.Fatalf("unexpected lte value: %#v", rangeQuery.Lte)
				}
			},
		},
		{
			name:  "date range supports gt lte and format",
			query: RangeDate("created_at").Gt("2024-01-01").Lte("2024-12-31").Format("strict_date").Build(),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.Range == nil {
					t.Fatal("expected range query")
				}
				rangeQuery, ok := query.Range["created_at"].(types.DateRangeQuery)
				if !ok {
					t.Fatalf("unexpected date range type: %T", query.Range["created_at"])
				}
				if rangeQuery.Gt == nil || *rangeQuery.Gt != "2024-01-01" {
					t.Fatalf("unexpected gt value: %#v", rangeQuery.Gt)
				}
				if rangeQuery.Lte == nil || *rangeQuery.Lte != "2024-12-31" {
					t.Fatalf("unexpected lte value: %#v", rangeQuery.Lte)
				}
				if rangeQuery.Format == nil || *rangeQuery.Format != "strict_date" {
					t.Fatalf("unexpected format: %#v", rangeQuery.Format)
				}
			},
		},
		{
			name: "bool builder supports should clauses",
			query: Bool().
				Must(Match("title", "go")).
				Should(Term("status", "published")).
				Build(),
			check: func(t *testing.T, query types.Query) {
				t.Helper()
				if query.Bool == nil {
					t.Fatal("expected bool query")
				}
				if len(query.Bool.Should) != 1 {
					t.Fatalf("unexpected should clauses: %#v", query.Bool.Should)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.query)
		})
	}
}

func TestAdvancedAggBuilders(t *testing.T) {
	tests := []struct {
		name  string
		agg   types.Aggregations
		check func(*testing.T, types.Aggregations)
	}{
		{
			name: "histogram agg stores interval",
			agg:  HistogramAgg("score", 5),
			check: func(t *testing.T, agg types.Aggregations) {
				t.Helper()
				if agg.Histogram == nil || agg.Histogram.Interval == nil || float64(*agg.Histogram.Interval) != 5 {
					t.Fatalf("unexpected histogram agg: %#v", agg.Histogram)
				}
			},
		},
		{
			name: "sum agg stores field",
			agg:  SumAgg("score"),
			check: func(t *testing.T, agg types.Aggregations) {
				t.Helper()
				if agg.Sum == nil || agg.Sum.Field == nil || *agg.Sum.Field != "score" {
					t.Fatalf("unexpected sum agg: %#v", agg.Sum)
				}
			},
		},
		{
			name: "min agg stores field",
			agg:  MinAgg("score"),
			check: func(t *testing.T, agg types.Aggregations) {
				t.Helper()
				if agg.Min == nil || agg.Min.Field == nil || *agg.Min.Field != "score" {
					t.Fatalf("unexpected min agg: %#v", agg.Min)
				}
			},
		},
		{
			name: "max agg stores field",
			agg:  MaxAgg("score"),
			check: func(t *testing.T, agg types.Aggregations) {
				t.Helper()
				if agg.Max == nil || agg.Max.Field == nil || *agg.Max.Field != "score" {
					t.Fatalf("unexpected max agg: %#v", agg.Max)
				}
			},
		},
		{
			name: "cardinality agg stores field",
			agg:  CardinalityAgg("author_id"),
			check: func(t *testing.T, agg types.Aggregations) {
				t.Helper()
				if agg.Cardinality == nil || agg.Cardinality.Field == nil || *agg.Cardinality.Field != "author_id" {
					t.Fatalf("unexpected cardinality agg: %#v", agg.Cardinality)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.agg)
		})
	}
}

func TestSearch_BuildRequest_AdvancedOptions(t *testing.T) {
	tests := []struct {
		name  string
		build func() *Search[searcherTestDoc]
		check func(*testing.T, *search.Request)
	}{
		{
			name: "where minimum should match offset and search after are encoded",
			build: func() *Search[searcherTestDoc] {
				return NewSearch[searcherTestDoc](nil, "articles").
					Where(Term("status", "published")).
					Should(Match("title", "go")).
					MinimumShouldMatch(1).
					Offset(3, 7).
					SearchAfter("cursor-1")
			},
			check: func(t *testing.T, req *search.Request) {
				t.Helper()
				if req.From == nil || *req.From != 3 {
					t.Fatalf("unexpected offset: %#v", req.From)
				}
				if req.Size == nil || *req.Size != 7 {
					t.Fatalf("unexpected size: %#v", req.Size)
				}
				if len(req.SearchAfter) != 1 || req.SearchAfter[0] != "cursor-1" {
					t.Fatalf("unexpected search_after: %#v", req.SearchAfter)
				}
				if req.Query == nil || req.Query.Bool == nil {
					t.Fatal("expected bool query")
				}
				switch value := req.Query.Bool.MinimumShouldMatch.(type) {
				case *string:
					if value == nil || *value != "1" {
						t.Fatalf("unexpected minimum_should_match: %#v", req.Query.Bool.MinimumShouldMatch)
					}
				case string:
					if value != "1" {
						t.Fatalf("unexpected minimum_should_match: %#v", req.Query.Bool.MinimumShouldMatch)
					}
				default:
					t.Fatalf("unexpected minimum_should_match type: %T", req.Query.Bool.MinimumShouldMatch)
				}
				if len(req.Query.Bool.Filter) != 1 {
					t.Fatalf("unexpected filters: %#v", req.Query.Bool.Filter)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.build().buildRequest()
			tt.check(t, req)
		})
	}
}

func TestSearch_RuntimeOperations(t *testing.T) {
	tests := []struct {
		name       string
		build      func(*elasticsearch.TypedClient) any
		assertion  func(*testing.T, any, *capturedRequest)
		wantPath   string
		response   string
		statusCode int
	}{
		{
			name: "do returns parsed items and request body",
			build: func(client *elasticsearch.TypedClient) any {
				return func(ctx context.Context) (*SearchResult[searcherTestDoc], error) {
					return NewSearch[searcherTestDoc](client, "articles").
						Must(Match("title", "go")).
						Do(ctx)
				}
			},
			assertion: func(t *testing.T, value any, req *capturedRequest) {
				t.Helper()
				call := value.(func(context.Context) (*SearchResult[searcherTestDoc], error))
				result, err := call(t.Context())
				if err != nil {
					t.Fatalf("do search: %v", err)
				}
				if result.Total != 1 || len(result.Items) != 1 || result.Items[0].Name != "alice" {
					t.Fatalf("unexpected search result: %#v", result)
				}
				if !strings.Contains(req.Body, `"match":{"title":{"query":"go"}}`) {
					t.Fatalf("unexpected request body: %s", req.Body)
				}
			},
			wantPath:   "/articles/_search",
			statusCode: http.StatusOK,
			response:   `{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","_source":{"id":"1","name":"alice"}}]}}`,
		},
		{
			name: "do agg forces size zero and returns aggregations",
			build: func(client *elasticsearch.TypedClient) any {
				return func(ctx context.Context) (*AggResult, error) {
					return NewSearch[searcherTestDoc](client, "articles").
						Agg("by_status", TermsAgg("status", 5)).
						DoAgg(ctx)
				}
			},
			assertion: func(t *testing.T, value any, req *capturedRequest) {
				t.Helper()
				call := value.(func(context.Context) (*AggResult, error))
				result, err := call(t.Context())
				if err != nil {
					t.Fatalf("do agg: %v", err)
				}
				if len(result.Aggregations) != 1 {
					t.Fatalf("unexpected aggregations: %#v", result.Aggregations)
				}
				if !strings.Contains(req.Body, `"size":0`) {
					t.Fatalf("expected size=0 request body, got %s", req.Body)
				}
			},
			wantPath:   "/articles/_search",
			statusCode: http.StatusOK,
			response:   `{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]},"aggregations":{"by_status":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}}}`,
		},
		{
			name: "count returns elasticsearch count payload",
			build: func(client *elasticsearch.TypedClient) any {
				return func(ctx context.Context) (int64, error) {
					return NewSearch[searcherTestDoc](client, "articles").
						Filter(Term("status", "published")).
						Count(ctx)
				}
			},
			assertion: func(t *testing.T, value any, req *capturedRequest) {
				t.Helper()
				call := value.(func(context.Context) (int64, error))
				count, err := call(t.Context())
				if err != nil {
					t.Fatalf("count: %v", err)
				}
				if count != 7 {
					t.Fatalf("unexpected count: %d", count)
				}
				if !strings.Contains(req.Body, `"term":{"status":{"value":"published"}}`) {
					t.Fatalf("unexpected count request body: %s", req.Body)
				}
			},
			wantPath:   "/articles/_count",
			statusCode: http.StatusOK,
			response:   `{"count":7}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req capturedRequest
			client := newTestTypedClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				req = capturedRequest{Path: request.URL.Path, Body: string(body)}
				return searchJSONResponse(request, tt.statusCode, tt.response), nil
			}))

			tt.assertion(t, tt.build(client), &req)
			if req.Path != tt.wantPath {
				t.Fatalf("unexpected request path: got %s want %s", req.Path, tt.wantPath)
			}
		})
	}
}

func TestParseTypedSearchResult_WithHighlights(t *testing.T) {
	tests := []struct {
		name string
		resp *search.Response
	}{
		{
			name: "highlight fragments are copied by document id",
			resp: &search.Response{
				Hits: types.HitsMetadata{
					Total:    &types.TotalHits{Value: 1},
					MaxScore: ptrFloat64(types.Float64(1.0)),
					Hits: []types.Hit{
						{
							Id_:       ptrString("1"),
							Source_:   json.RawMessage(`{"id":"1","name":"alice"}`),
							Highlight: map[string][]string{"title": {"<em>alice</em>"}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTypedSearchResult[searcherTestDoc](tt.resp)
			if err != nil {
				t.Fatalf("parse typed search result: %v", err)
			}
			if len(result.Highlights) != 1 || result.Highlights["1"].Fields["title"][0] != "<em>alice</em>" {
				t.Fatalf("unexpected highlights: %#v", result.Highlights)
			}
		})
	}
}

type capturedRequest struct {
	Path string
	Body string
}

func ptrString(value string) *string {
	return &value
}

func ptrFloat64(value types.Float64) *types.Float64 {
	return &value
}
