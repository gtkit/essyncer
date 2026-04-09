package searcher

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

func TestMatch(t *testing.T) {
	q := Match("title", "hello")
	if q.Match == nil {
		t.Fatal("Match returned empty")
	}
	if _, ok := q.Match["title"]; !ok {
		t.Error("missing title key")
	}
}

func TestTerm(t *testing.T) {
	q := Term("status", "published")
	if q.Term == nil {
		t.Fatal("Term returned empty")
	}
}

func TestExists(t *testing.T) {
	q := Exists("deleted_at")
	if q.Exists == nil {
		t.Fatal("Exists returned empty")
	}
	if q.Exists.Field != "deleted_at" {
		t.Errorf("got field %s", q.Exists.Field)
	}
}

func TestMultiMatch(t *testing.T) {
	q := MultiMatch("hello", "title", "content")
	if q.MultiMatch == nil {
		t.Fatal("MultiMatch returned empty")
	}
	if len(q.MultiMatch.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(q.MultiMatch.Fields))
	}
}

func TestRangeDate(t *testing.T) {
	q := RangeDate("created_at").Gte("2024-01-01").Lt("2025-01-01").Build()
	if q.Range == nil {
		t.Fatal("Range returned empty")
	}
	if _, ok := q.Range["created_at"]; !ok {
		t.Error("missing created_at key")
	}
}

func TestRangeNum(t *testing.T) {
	q := RangeNum("age").Gte(18).Lt(65).Build()
	if q.Range == nil {
		t.Fatal("Range returned empty")
	}
}

func TestBool(t *testing.T) {
	q := Bool().
		Must(Match("title", "go")).
		Filter(Term("status", "active")).
		MustNot(Exists("deleted_at")).
		Build()
	if q.Bool == nil {
		t.Fatal("Bool returned empty")
	}
	if len(q.Bool.Must) != 1 {
		t.Errorf("expected 1 must, got %d", len(q.Bool.Must))
	}
	if len(q.Bool.Filter) != 1 {
		t.Errorf("expected 1 filter, got %d", len(q.Bool.Filter))
	}
	if len(q.Bool.MustNot) != 1 {
		t.Errorf("expected 1 must_not, got %d", len(q.Bool.MustNot))
	}
}

// --- Search Builder ---

func TestSearch_Chain(t *testing.T) {
	s := NewSearch[any](nil, "test").
		Must(Match("title", "hello")).
		Filter(Term("status", "active")).
		Should(Match("content", "world")).
		MustNot(Exists("deleted_at")).
		Sort("created_at", false).
		Page(2, 20).
		Highlight("title", "content")

	if len(s.must) != 1 {
		t.Errorf("must: got %d", len(s.must))
	}
	if len(s.filter) != 1 {
		t.Errorf("filter: got %d", len(s.filter))
	}
	if len(s.should) != 1 {
		t.Errorf("should: got %d", len(s.should))
	}
	if len(s.mustNot) != 1 {
		t.Errorf("must_not: got %d", len(s.mustNot))
	}
	if *s.from != 20 {
		t.Errorf("from: got %d", *s.from)
	}
	if *s.size != 20 {
		t.Errorf("size: got %d", *s.size)
	}
	if len(s.highlightFields) != 2 {
		t.Errorf("highlights: got %d", len(s.highlightFields))
	}
}

func TestSearch_Unscoped(t *testing.T) {
	s := NewSearch[any](nil, "test")
	if s.unscoped {
		t.Error("should default to scoped")
	}
	s.Unscoped()
	if !s.unscoped {
		t.Error("should be unscoped")
	}
}

func TestSearch_Singleflight(t *testing.T) {
	s := NewSearch[any](nil, "test").WithSingleflight()
	if !s.useSingleflight {
		t.Error("should be enabled")
	}
}

func TestSearch_BuildQuery_SoftDeleteFilter(t *testing.T) {
	s := NewSearch[any](nil, "test").Must(MatchAll())
	q := s.buildQuery()
	if q.Bool == nil {
		t.Fatal("expected bool query")
	}
	// 默认应有 must_not exists deleted_at
	found := false
	for _, mn := range q.Bool.MustNot {
		if mn.Exists != nil && mn.Exists.Field == "deleted_at" {
			found = true
		}
	}
	if !found {
		t.Error("should have deleted_at must_not filter")
	}
}

func TestSearch_BuildQuery_UnscopedNoFilter(t *testing.T) {
	s := NewSearch[any](nil, "test").Unscoped().Must(MatchAll())
	q := s.buildQuery()
	for _, mn := range q.Bool.MustNot {
		if mn.Exists != nil && mn.Exists.Field == "deleted_at" {
			t.Error("unscoped should not filter deleted_at")
		}
	}
}

func TestSearch_Agg(t *testing.T) {
	s := NewSearch[any](nil, "test").
		Agg("by_cat", TermsAgg("category", 10)).
		Agg("avg_score", AvgAgg("score"))
	if len(s.aggs) != 2 {
		t.Errorf("expected 2 aggs, got %d", len(s.aggs))
	}
}

func TestSearch_Page_Defaults(t *testing.T) {
	s := NewSearch[any](nil, "test").Page(0, -1)
	if *s.from != 0 {
		t.Errorf("from: got %d", *s.from)
	}
	if *s.size != 20 {
		t.Errorf("size: got %d", *s.size)
	}
}

func TestSearch_Select_Exclude(t *testing.T) {
	s := NewSearch[any](nil, "test").
		Select("id", "title").
		Exclude("content")
	if len(s.includes) != 2 {
		t.Errorf("includes: got %d", len(s.includes))
	}
	if len(s.excludes) != 1 {
		t.Errorf("excludes: got %d", len(s.excludes))
	}
}

func TestSearch_CacheKeyIncludesClientIdentity(t *testing.T) {
	clientA := &elasticsearch.TypedClient{}
	clientB := &elasticsearch.TypedClient{}

	keyA := NewSearch[any](clientA, "articles").
		Must(Match("title", "hello")).
		Filter(Term("status", "published")).
		Page(1, 20).
		cacheKey()

	keyB := NewSearch[any](clientB, "articles").
		Must(Match("title", "hello")).
		Filter(Term("status", "published")).
		Page(1, 20).
		cacheKey()

	if keyA == keyB {
		t.Fatalf("expected cache keys to differ for different clients, got %q", keyA)
	}
}

// --- Agg Builders ---

func TestTermsAgg(t *testing.T) {
	a := TermsAgg("category", 10)
	if a.Terms == nil {
		t.Fatal("nil")
	}
}

func TestAvgAgg(t *testing.T) {
	a := AvgAgg("score")
	if a.Avg == nil {
		t.Fatal("nil")
	}
}

func TestDateHistogramAgg(t *testing.T) {
	a := DateHistogramAgg("created_at", "month")
	if a.DateHistogram == nil {
		t.Fatal("nil")
	}
}

// prevent unused import
var _ types.Query
