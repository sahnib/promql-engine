// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package unary

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"gonum.org/v1/gonum/floats"

	"github.com/thanos-community/promql-engine/execution/model"
)

type unaryNegation struct {
	next model.VectorOperator
	once sync.Once

	series []labels.Labels
}

func (u *unaryNegation) Explain() (me string, next []model.VectorOperator) {
	return "[*unaryNegation]", []model.VectorOperator{u.next}
}

func NewUnaryNegation(
	next model.VectorOperator,
	stepsBatch int,
) (model.VectorOperator, error) {
	u := &unaryNegation{
		next: next,
	}
	return u, nil
}

func (u *unaryNegation) Series(ctx context.Context, tracer *model.OperatorTracer) ([]labels.Labels, error) {
	if err := u.loadSeries(ctx, tracer); err != nil {
		return nil, err
	}
	return u.series, nil
}

func (u *unaryNegation) loadSeries(ctx context.Context, tracer *model.OperatorTracer) error {
	var err error
	u.once.Do(func() {
		var series []labels.Labels
		series, err = u.next.Series(ctx, tracer)
		if err != nil {
			return
		}
		u.series = make([]labels.Labels, len(series))
		for i := range series {
			lbls := labels.NewBuilder(series[i]).Del(labels.MetricName).Labels(nil)
			u.series[i] = lbls
		}
	})
	return err
}

func (u *unaryNegation) GetPool() *model.VectorPool {
	return u.next.GetPool()
}

func (u *unaryNegation) Next(ctx context.Context, tracer *model.OperatorTracer) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	in, err := u.next.Next(ctx, tracer)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}
	for i := range in {
		floats.Scale(-1, in[i].Samples)
	}
	return in, nil
}
