// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/execution/parse"
	"github.com/thanos-community/promql-engine/query"
)

// functionOperator returns []model.StepVector after processing input with desired function.
type functionOperator struct {
	funcExpr *parser.Call
	series   []labels.Labels
	once     sync.Once

	vectorIndex int
	nextOps     []model.VectorOperator

	call         FunctionCall
	scalarPoints [][]float64
	pointBuf     []promql.Point
}

type noArgFunctionOperator struct {
	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	stepsBatch  int
	funcExpr    *parser.Call
	call        FunctionCall
	vectorPool  *model.VectorPool
	series      []labels.Labels
	sampleIDs   []uint64
}

func (o *noArgFunctionOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*noArgFunctionOperator] %v()", o.funcExpr.Func.Name), []model.VectorOperator{}
}

func (o *noArgFunctionOperator) Series(_ context.Context) ([]labels.Labels, error) {
	return o.series, nil
}

func (o *noArgFunctionOperator) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *noArgFunctionOperator) Next(_ context.Context) ([]model.StepVector, int64, error) {
	if o.currentStep > o.maxt {
		return nil, 0, nil
	}
	ret := o.vectorPool.GetVectorBatch()
	for i := 0; i < o.stepsBatch && o.currentStep <= o.maxt; i++ {
		sv := o.vectorPool.GetStepVector(o.currentStep)
		result := o.call(FunctionArgs{
			StepTime: o.currentStep,
		})
		sv.Samples = []float64{result.V}
		sv.SampleIDs = o.sampleIDs

		ret = append(ret, sv)
		o.currentStep += o.step
	}

	return ret, 0, nil
}

func NewFunctionOperator(funcExpr *parser.Call, call FunctionCall, nextOps []model.VectorOperator, stepsBatch int, opts *query.Options) (model.VectorOperator, error) {
	// Short-circuit functions that take no args. Their only input is the step's timestamp.
	if len(nextOps) == 0 {
		interval := opts.Step.Milliseconds()
		// We set interval to be at least 1.
		if interval == 0 {
			interval = 1
		}

		op := &noArgFunctionOperator{
			currentStep: opts.Start.UnixMilli(),
			mint:        opts.Start.UnixMilli(),
			maxt:        opts.End.UnixMilli(),
			step:        interval,
			stepsBatch:  stepsBatch,
			funcExpr:    funcExpr,
			call:        call,
			vectorPool:  model.NewVectorPool(stepsBatch),
		}

		switch funcExpr.Func.Name {
		case "pi", "time", "scalar":
		default:
			// Other functions require non-nil labels.
			op.series = []labels.Labels{{}}
			op.sampleIDs = []uint64{0}
		}

		return op, nil
	}
	scalarPoints := make([][]float64, stepsBatch)
	for i := 0; i < stepsBatch; i++ {
		scalarPoints[i] = make([]float64, len(nextOps)-1)
	}
	f := &functionOperator{
		nextOps:      nextOps,
		call:         call,
		funcExpr:     funcExpr,
		vectorIndex:  0,
		scalarPoints: scalarPoints,
		pointBuf:     make([]promql.Point, 1),
	}

	for i := range funcExpr.Args {
		if funcExpr.Args[i].Type() == parser.ValueTypeVector {
			f.vectorIndex = i
			break
		}
	}

	// Check selector type.
	// TODO(saswatamcode): Add support for string and matrix.
	switch funcExpr.Args[f.vectorIndex].Type() {
	case parser.ValueTypeVector, parser.ValueTypeScalar:
		return f, nil
	default:
		return nil, errors.Wrapf(parse.ErrNotImplemented, "got %s:", funcExpr.String())
	}
}

func (o *functionOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*functionOperator] %v(%v)", o.funcExpr.Func.Name, o.funcExpr.Args), o.nextOps
}

func (o *functionOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *functionOperator) GetPool() *model.VectorPool {
	return o.nextOps[o.vectorIndex].GetPool()
}

func (o *functionOperator) Next(ctx context.Context) ([]model.StepVector, int64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	default:
	}

	if err := o.loadSeries(ctx); err != nil {
		return nil, 0, err
	}

	var currSamples int64 = 0
	// Process non-variadic single/multi-arg instant vector and scalar input functions.
	// Call next on vector input.
	vectors, samples, err := o.nextOps[o.vectorIndex].Next(ctx)
	currSamples += samples

	if err != nil {
		return nil, currSamples, err
	}

	if len(vectors) == 0 {
		return nil, currSamples, nil
	}

	scalarIndex := 0
	for i := range o.nextOps {
		if i == o.vectorIndex {
			continue
		}

		scalarVectors, scalarSamples, err := o.nextOps[i].Next(ctx)
		currSamples += scalarSamples

		if err != nil {
			return nil, currSamples, err
		}

		for batchIndex := range vectors {
			val := math.NaN()
			if len(scalarVectors) > 0 && len(scalarVectors[batchIndex].Samples) > 0 {
				val = scalarVectors[batchIndex].Samples[0]
				o.nextOps[i].GetPool().PutStepVector(scalarVectors[batchIndex])
			}
			o.scalarPoints[batchIndex][scalarIndex] = val
		}
		o.nextOps[i].GetPool().PutVectors(scalarVectors)
		scalarIndex++
	}

	for batchIndex, vector := range vectors {
		// scalar() depends on number of samples per vector and returns NaN if len(samples) != 1.
		// So need to handle this separately here, instead of going via call which is per point.
		if o.funcExpr.Func.Name == "scalar" {
			if len(vector.Samples) <= 1 {
				continue
			}

			vectors[batchIndex].Samples = vector.Samples[:1]
			vectors[batchIndex].SampleIDs = vector.SampleIDs[:1]
			vector.Samples[0] = math.NaN()
			continue
		}

		for i := range vector.Samples {
			o.pointBuf[0].V = vector.Samples[i]
			// Call function by separately passing major input and scalars.
			result := o.call(FunctionArgs{
				Labels:       o.series[0],
				Points:       o.pointBuf,
				StepTime:     vector.T,
				ScalarPoints: o.scalarPoints[batchIndex],
			})

			vector.Samples[i] = result.V
		}
	}

	return vectors, currSamples, nil
}

func (o *functionOperator) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		if o.funcExpr.Func.Name == "vector" {
			o.series = []labels.Labels{labels.New()}
			return
		}

		if o.funcExpr.Func.Name == "scalar" {
			o.series = []labels.Labels{}
			return
		}

		series, loadErr := o.nextOps[o.vectorIndex].Series(ctx)
		if loadErr != nil {
			err = loadErr
			return
		}

		o.series = make([]labels.Labels, len(series))
		for i, s := range series {
			lbls := s
			if o.funcExpr.Func.Name != "last_over_time" {
				lbls, _ = DropMetricName(s.Copy())
			}

			o.series[i] = lbls
		}
	})

	return err
}

func DropMetricName(l labels.Labels) (labels.Labels, labels.Label) {
	return dropLabel(l, labels.MetricName)
}

// dropLabel removes the label with name from l and returns the dropped label.
func dropLabel(l labels.Labels, name string) (labels.Labels, labels.Label) {
	if len(l) == 0 {
		return l, labels.Label{}
	}

	if len(l) == 1 {
		if l[0].Name == name {
			return l[:0], l[0]

		}
		return l, labels.Label{}
	}

	for i := range l {
		if l[i].Name == name {
			lbl := l[i]
			return append(l[:i], l[i+1:]...), lbl
		}
	}

	return l, labels.Label{}
}
