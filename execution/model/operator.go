// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"

	"github.com/prometheus/prometheus/util/stats"

	"github.com/prometheus/prometheus/model/labels"
)

// OperatorTracer aggregates the observability information from all VectorOperator operators
// during query execution.
type OperatorTracer struct {
	QuerySamples *stats.QuerySamples
}

// VectorOperator performs operations on series in step by step fashion.
type VectorOperator interface {
	// Next yields vectors of samples from all series for one or more execution steps.
	Next(ctx context.Context, tracer *OperatorTracer) ([]StepVector, error)

	// Series returns all series that the operator will process during Next results.
	// The result can be used by upstream operators to allocate output tables and buffers
	// before starting to process samples.
	Series(ctx context.Context, tracer *OperatorTracer) ([]labels.Labels, error)

	// GetPool returns pool of vectors that can be shared across operators.
	GetPool() *VectorPool

	// Explain returns human-readable explanation of the current operator and optional nested operators.
	Explain() (me string, next []VectorOperator)
}
