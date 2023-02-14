// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/util/stats"
	"sync"
	"sync/atomic"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-community/promql-engine/execution/model"
	engstore "github.com/thanos-community/promql-engine/execution/storage"
	"github.com/thanos-community/promql-engine/query"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"

	"github.com/prometheus/prometheus/storage"
)

var ErrNativeHistogramsUnsupported = errors.Newf("querying native histograms is not supported")

type vectorScanner struct {
	labels    labels.Labels
	signature uint64
	samples   *storage.MemoizedSeriesIterator
}

type vectorSelector struct {
	storage  engstore.SeriesSelector
	scanners []vectorScanner
	series   []labels.Labels

	once       sync.Once
	vectorPool *model.VectorPool

	numSteps      int
	mint          int64
	maxt          int64
	lookbackDelta int64
	step          int64
	currentStep   int64
	offset        int64

	shard     int
	numShards int

	querySamples *stats.QuerySamples
}

// NewVectorSelector creates operator which selects vector of series.
func NewVectorSelector(
	pool *model.VectorPool,
	selector engstore.SeriesSelector,
	queryOpts *query.Options,
	offset time.Duration,
	shard, numShards int,
	querySamples *stats.QuerySamples,
) model.VectorOperator {
	return &vectorSelector{
		storage:    selector,
		vectorPool: pool,

		mint:          queryOpts.Start.UnixMilli(),
		maxt:          queryOpts.End.UnixMilli(),
		step:          queryOpts.Step.Milliseconds(),
		currentStep:   queryOpts.Start.UnixMilli(),
		lookbackDelta: queryOpts.LookbackDelta.Milliseconds(),
		offset:        offset.Milliseconds(),
		numSteps:      queryOpts.NumSteps(),

		shard:     shard,
		numShards: numShards,

		querySamples: querySamples,
	}
}

func (o *vectorSelector) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*vectorSelector %p] {%v} %v mod %v", o, o.storage.Matchers(), o.shard, o.numShards), nil
}

func (o *vectorSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *vectorSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *vectorSelector) Next(ctx context.Context) ([]model.StepVector, int64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return nil, 0, nil
	}

	if err := o.loadSeries(ctx); err != nil {
		return nil, 0, err
	}

	vectors := o.vectorPool.GetVectorBatch()
	ts := o.currentStep
	var currentSamples int64 = 0
	for i := 0; i < len(o.scanners); i++ {
		var (
			series   = o.scanners[i]
			seriesTs = ts
		)

		for currStep := 0; currStep < o.numSteps && seriesTs <= o.maxt; currStep++ {
			if len(vectors) <= currStep {
				vectors = append(vectors, o.vectorPool.GetStepVector(seriesTs))
			}
			_, v, h, ok, err := selectPoint(series.samples, seriesTs, o.lookbackDelta, o.offset)
			if err != nil {
				return nil, currentSamples, err
			}
			if ok {
				if h != nil {
					currentSamples += int64(len(h.PositiveBuckets) + len(h.NegativeBuckets))
					vectors[currStep].AppendHistogram(o.vectorPool, series.signature, h)
				} else {
					currentSamples += 1
					vectors[currStep].AppendSample(o.vectorPool, series.signature, v)
				}
			}
			seriesTs += o.step
		}
	}
	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if o.step == 0 {
		o.step = 1
	}
	o.currentStep += o.step * int64(o.numSteps)

	atomic.AddInt64(&o.querySamples.TotalSamples, currentSamples)

	return vectors, currentSamples, nil
}

func (o *vectorSelector) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		series, loadErr := o.storage.GetSeries(ctx, o.shard, o.numShards)
		if loadErr != nil {
			err = loadErr
			return
		}

		o.scanners = make([]vectorScanner, len(series))
		o.series = make([]labels.Labels, len(series))
		for i, s := range series {
			o.scanners[i] = vectorScanner{
				labels:    s.Labels(),
				signature: s.Signature,
				samples:   storage.NewMemoizedIterator(s.Iterator(nil), o.lookbackDelta),
			}
			o.series[i] = s.Labels()
		}
		o.vectorPool.SetStepSize(len(series))
	})
	return err
}

// TODO(fpetkovski): Add max samples limit.
func selectPoint(it *storage.MemoizedSeriesIterator, ts, lookbackDelta, offset int64) (int64, float64, *histogram.FloatHistogram, bool, error) {
	refTime := ts - offset
	var t int64
	var v float64
	var h *histogram.FloatHistogram

	valueType := it.Seek(refTime)
	switch valueType {
	case chunkenc.ValNone:
		if it.Err() != nil {
			return 0, 0, nil, false, it.Err()
		}
	case chunkenc.ValFloatHistogram:
		return 0, 0, nil, false, ErrNativeHistogramsUnsupported
	case chunkenc.ValHistogram:
		t, h := it.AtHistogram()
		return t, 0, h.ToFloat(), true, nil
	case chunkenc.ValFloat:
		t, v = it.At()
	default:
		panic(errors.Newf("unknown value type %v", valueType))
	}
	if valueType == chunkenc.ValNone || t > refTime {
		var ok bool
		t, v, _, h, ok = it.PeekPrev()
		if !ok || t < refTime-lookbackDelta {
			return 0, 0, nil, false, nil
		}
	}
	if value.IsStaleNaN(v) {
		return 0, 0, nil, false, nil
	}
	return t, v, h, true, nil
}
