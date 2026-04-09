package searcher

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// 返回 types.Query，可直接传入 Search.Must/Should/Filter/MustNot。

func MatchAll() types.Query {
	return types.Query{MatchAll: &types.MatchAllQuery{}}
}

func Match(field, text string) types.Query {
	return types.Query{
		Match: map[string]types.MatchQuery{
			field: {Query: text},
		},
	}
}

func MatchPhrase(field, text string) types.Query {
	return types.Query{
		MatchPhrase: map[string]types.MatchPhraseQuery{
			field: {Query: text},
		},
	}
}

func MultiMatch(text string, fields ...string) types.Query {
	return types.Query{
		MultiMatch: &types.MultiMatchQuery{
			Query:  text,
			Fields: fields,
		},
	}
}

func Term(field string, value string) types.Query {
	return types.Query{
		Term: map[string]types.TermQuery{
			field: {Value: value},
		},
	}
}

func TermInt(field string, value int64) types.Query {
	return types.Query{
		Term: map[string]types.TermQuery{
			field: {Value: value},
		},
	}
}

func Exists(field string) types.Query {
	return types.Query{
		Exists: &types.ExistsQuery{Field: field},
	}
}

func Wildcard(field, pattern string) types.Query {
	return types.Query{
		Wildcard: map[string]types.WildcardQuery{
			field: {Value: &pattern},
		},
	}
}

func Prefix(field, prefix string) types.Query {
	return types.Query{
		Prefix: map[string]types.PrefixQuery{
			field: {Value: prefix},
		},
	}
}

// --- Range 查询 ---

func RangeNum(field string) *NumRangeBuilder {
	return &NumRangeBuilder{field: field, q: types.NumberRangeQuery{}}
}

type NumRangeBuilder struct {
	field string
	q     types.NumberRangeQuery
}

func (b *NumRangeBuilder) Gt(v float64) *NumRangeBuilder  { f := types.Float64(v); b.q.Gt = &f; return b }
func (b *NumRangeBuilder) Gte(v float64) *NumRangeBuilder { f := types.Float64(v); b.q.Gte = &f; return b }
func (b *NumRangeBuilder) Lt(v float64) *NumRangeBuilder  { f := types.Float64(v); b.q.Lt = &f; return b }
func (b *NumRangeBuilder) Lte(v float64) *NumRangeBuilder { f := types.Float64(v); b.q.Lte = &f; return b }

func (b *NumRangeBuilder) Build() types.Query {
	return types.Query{
		Range: map[string]types.RangeQuery{b.field: b.q},
	}
}

func RangeDate(field string) *DateRangeBuilder {
	return &DateRangeBuilder{field: field, q: types.DateRangeQuery{}}
}

type DateRangeBuilder struct {
	field string
	q     types.DateRangeQuery
}

func (b *DateRangeBuilder) Gt(v string) *DateRangeBuilder     { b.q.Gt = &v; return b }
func (b *DateRangeBuilder) Gte(v string) *DateRangeBuilder    { b.q.Gte = &v; return b }
func (b *DateRangeBuilder) Lt(v string) *DateRangeBuilder     { b.q.Lt = &v; return b }
func (b *DateRangeBuilder) Lte(v string) *DateRangeBuilder    { b.q.Lte = &v; return b }
func (b *DateRangeBuilder) Format(f string) *DateRangeBuilder { b.q.Format = &f; return b }

func (b *DateRangeBuilder) Build() types.Query {
	return types.Query{
		Range: map[string]types.RangeQuery{b.field: b.q},
	}
}

// --- Bool 嵌套查询 ---

func Bool() *BoolBuilder { return &BoolBuilder{q: &types.BoolQuery{}} }

type BoolBuilder struct{ q *types.BoolQuery }

func (b *BoolBuilder) Must(queries ...types.Query) *BoolBuilder {
	b.q.Must = append(b.q.Must, queries...)
	return b
}
func (b *BoolBuilder) Should(queries ...types.Query) *BoolBuilder {
	b.q.Should = append(b.q.Should, queries...)
	return b
}
func (b *BoolBuilder) MustNot(queries ...types.Query) *BoolBuilder {
	b.q.MustNot = append(b.q.MustNot, queries...)
	return b
}
func (b *BoolBuilder) Filter(queries ...types.Query) *BoolBuilder {
	b.q.Filter = append(b.q.Filter, queries...)
	return b
}
func (b *BoolBuilder) Build() types.Query { return types.Query{Bool: b.q} }
