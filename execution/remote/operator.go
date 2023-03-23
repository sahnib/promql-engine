// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package remote

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/scan"
	engstore "github.com/thanos-community/promql-engine/execution/storage"
	"github.com/thanos-community/promql-engine/query"
)

type Execution struct {
	storage        *storageAdapter
	query          promql.Query
	opts           *query.Options
	vectorSelector model.VectorOperator
}

func NewExecution(query promql.Query, pool *model.VectorPool, opts *query.Options) *Execution {
	storage := newStorageFromQuery(query, opts)
	return &Execution{
		storage:        storage,
		query:          query,
		opts:           opts,
		vectorSelector: scan.NewVectorSelector(pool, storage, opts, 0, 0, 1),
	}
}

func (e *Execution) Series(ctx context.Context, tracer *model.OperatorTracer) ([]labels.Labels, error) {
	return e.vectorSelector.Series(ctx, tracer)
}

func (e *Execution) Next(ctx context.Context, tracer *model.OperatorTracer) ([]model.StepVector, error) {
	next, err := e.vectorSelector.Next(ctx, tracer)
	if next == nil {
		// Closing the storage prematurely can lead to results from the query
		// engine to be recycled. Because of this, we close the storage only
		// when we are done with processing all samples returned by the query.
		e.storage.Close()
	}
	return next, err
}

func (e *Execution) GetPool() *model.VectorPool {
	return e.vectorSelector.GetPool()
}

func (e *Execution) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*remoteExec] %s (%d, %d)", e.query, e.opts.Start.Unix(), e.opts.End.Unix()), nil
}

type storageAdapter struct {
	query promql.Query
	opts  *query.Options

	once   sync.Once
	err    error
	series []engstore.SignedSeries
}

func newStorageFromQuery(query promql.Query, opts *query.Options) *storageAdapter {
	return &storageAdapter{
		query: query,
		opts:  opts,
	}
}

func (s *storageAdapter) Matchers() []*labels.Matcher { return nil }

func (s *storageAdapter) GetSeries(ctx context.Context, _, _ int) ([]engstore.SignedSeries, error) {
	s.once.Do(func() { s.executeQuery(ctx) })
	if s.err != nil {
		return nil, s.err
	}

	return s.series, nil
}

func (s *storageAdapter) executeQuery(ctx context.Context) {
	result := s.query.Exec(ctx)
	if result.Err != nil {
		s.err = result.Err
		return
	}

	switch val := result.Value.(type) {
	case promql.Matrix:
		s.series = make([]engstore.SignedSeries, len(val))
		for i, series := range val {
			s.series[i] = engstore.SignedSeries{
				Signature: uint64(i),
				Series:    promql.NewStorageSeries(series),
			}
		}
	case promql.Vector:
		s.series = make([]engstore.SignedSeries, len(val))
		for i, point := range val {
			s.series[i] = engstore.SignedSeries{
				Signature: uint64(i),
				Series: promql.NewStorageSeries(promql.Series{
					Metric: point.Metric,
					Points: []promql.Point{{T: point.T, V: point.V}},
				}),
			}
		}
	}
}

func (s *storageAdapter) Close() {
	s.query.Close()
}
