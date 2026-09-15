package searcher

import (
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types/enums/calendarinterval"
)

// TermsAgg 创建 Terms 聚合。
func TermsAgg(field string, size int) types.Aggregations {
	return types.Aggregations{
		Terms: &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		},
	}
}

// DateHistogramAgg 创建日期直方图聚合。
func DateHistogramAgg(field, calendarInterval string) types.Aggregations {
	interval := calendarinterval.CalendarInterval{Name: calendarInterval}
	return types.Aggregations{
		DateHistogram: &types.DateHistogramAggregation{
			Field:            &field,
			CalendarInterval: &interval,
		},
	}
}

// HistogramAgg 创建数值直方图聚合。
func HistogramAgg(field string, interval float64) types.Aggregations {
	f := types.Float64(interval)
	return types.Aggregations{
		Histogram: &types.HistogramAggregation{
			Field:    &field,
			Interval: &f,
		},
	}
}

// AvgAgg 创建平均值聚合。
func AvgAgg(field string) types.Aggregations {
	return types.Aggregations{
		Avg: &types.AverageAggregation{Field: &field},
	}
}

// SumAgg 创建求和聚合。
func SumAgg(field string) types.Aggregations {
	return types.Aggregations{
		Sum: &types.SumAggregation{Field: &field},
	}
}

// MinAgg 创建最小值聚合。
func MinAgg(field string) types.Aggregations {
	return types.Aggregations{
		Min: &types.MinAggregation{Field: &field},
	}
}

// MaxAgg 创建最大值聚合。
func MaxAgg(field string) types.Aggregations {
	return types.Aggregations{
		Max: &types.MaxAggregation{Field: &field},
	}
}

// CardinalityAgg 创建基数聚合（去重计数）。
func CardinalityAgg(field string) types.Aggregations {
	return types.Aggregations{
		Cardinality: &types.CardinalityAggregation{Field: &field},
	}
}
