package engine_test

import (
	"fpetkovski/promql-engine/engine"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestRangeQuery(t *testing.T) {
	cases := []struct {
		Name     string
		Load     string
		Query    string
		Result   parser.Value
		Start    time.Time
		End      time.Time
		Interval time.Duration
	}{
		{
			Name: "sum_over_time with all values",
			Load: `load 30s
              bar 0 1 10 100 1000`,
			Query: "sum_over_time(bar[30s])",
			Result: promql.Matrix{
				promql.Series{
					Points: []promql.Point{{V: 0, T: 0}, {V: 11, T: 60000}, {V: 1100, T: 120000}},
					Metric: labels.Labels{},
				},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(120, 0),
			Interval: 60 * time.Second,
		},
		{
			Name: "sum_over_time with trailing values",
			Load: `load 30s
              bar 0 1 10 100 1000 0 0 0 0`,
			Query: "sum_over_time(bar[30s])",
			Result: promql.Matrix{
				promql.Series{
					Points: []promql.Point{{V: 0, T: 0}, {V: 11, T: 60000}, {V: 1100, T: 120000}},
					Metric: labels.Labels{},
				},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(120, 0),
			Interval: 60 * time.Second,
		},
		{
			Name: "sum_over_time with all values long",
			Load: `load 30s
              bar 0 1 10 100 1000 10000 100000 1000000 10000000`,
			Query: "sum_over_time(bar[30s])",
			Result: promql.Matrix{
				promql.Series{
					Points: []promql.Point{{V: 0, T: 0}, {V: 11, T: 60000}, {V: 1100, T: 120000}, {V: 110000, T: 180000}, {V: 11000000, T: 240000}},
					Metric: labels.Labels{},
				},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(240, 0),
			Interval: 60 * time.Second,
		},
		{
			Name: "sum_over_time with all values random",
			Load: `load 30s
              bar 5 17 42 2 7 905 51`,
			Query: "sum_over_time(bar[30s])",
			Result: promql.Matrix{
				promql.Series{
					Points: []promql.Point{{V: 5, T: 0}, {V: 59, T: 60000}, {V: 9, T: 120000}, {V: 956, T: 180000}},
					Metric: labels.Labels{},
				},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(180, 0),
			Interval: 60 * time.Second,
		},
		{
			Name: "metric query",
			Load: `load 30s
              metric 1+1x4`,
			Query: "metric",
			Result: promql.Matrix{
				promql.Series{
					Points: []promql.Point{{V: 1, T: 0}, {V: 3, T: 60000}, {V: 5, T: 120000}},
					Metric: labels.Labels{labels.Label{Name: "__name__", Value: "metric"}},
				},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(120, 0),
			Interval: 1 * time.Minute,
		},
		{
			Name: "metric query with trailing values",
			Load: `load 30s
              metric 1+1x8`,
			Query: "metric",
			Result: promql.Matrix{
				promql.Series{
					Points: []promql.Point{{V: 1, T: 0}, {V: 3, T: 60000}, {V: 5, T: 120000}},
					Metric: labels.Labels{labels.Label{Name: "__name__", Value: "metric"}},
				},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(120, 0),
			Interval: 1 * time.Minute,
		},
	}

	ng := engine.New()

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			test, err := promql.NewTest(t, c.Load)
			require.NoError(t, err)
			defer test.Close()

			err = test.Run()
			require.NoError(t, err)
			qry, err := ng.NewRangeQuery(test.Queryable(), nil, c.Query, c.Start, c.End, c.Interval)
			require.NoError(t, err)

			res := qry.Exec(test.Context())
			require.NoError(t, res.Err)
			require.Equal(t, c.Result, res.Value)
		})
	}
}
