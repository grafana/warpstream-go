package main

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestCounterSampler_RecordsInitialAndBucketBoundaries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()
	prim := prometheus.NewCounter(prometheus.CounterOpts{Name: "p_total"})
	hedg := prometheus.NewCounter(prometheus.CounterOpts{Name: "h_total"})
	reg.MustRegister(prim, hedg)

	read := func() (int64, int64) {
		return int64(testutil.ToFloat64(prim)), int64(testutil.ToFloat64(hedg))
	}

	start := time.Now()
	s := startCounterSampler(start, 50*time.Millisecond, read)

	// Bump counters between boundaries so deltas are testable.
	prim.Add(3)
	time.Sleep(60 * time.Millisecond)
	prim.Add(5)
	hedg.Add(2)
	time.Sleep(60 * time.Millisecond)

	samples := s.stop()
	// At least: initial + 2 boundaries + 1 final = 4 samples.
	assert.GreaterOrEqual(t, len(samples), 4)

	assert.Equal(t, int64(0), samples[0].primary)
	assert.Equal(t, int64(0), samples[0].hedge)

	final := samples[len(samples)-1]
	assert.Equal(t, int64(8), final.primary)
	assert.Equal(t, int64(2), final.hedge)

	deltas := bucketDeltas(samples)
	assert.Equal(t, len(samples)-1, len(deltas))
	var sumP, sumH int64
	for _, d := range deltas {
		sumP += d.primary
		sumH += d.hedge
	}
	assert.Equal(t, int64(8), sumP)
	assert.Equal(t, int64(2), sumH)
}

func TestCounterSampler_BoundariesAreAbsolute(t *testing.T) {
	t.Parallel()

	prim := prometheus.NewCounter(prometheus.CounterOpts{Name: "p"})
	hedg := prometheus.NewCounter(prometheus.CounterOpts{Name: "h"})
	read := func() (int64, int64) {
		return int64(testutil.ToFloat64(prim)), int64(testutil.ToFloat64(hedg))
	}

	start := time.Now()
	s := startCounterSampler(start, 30*time.Millisecond, read)
	time.Sleep(110 * time.Millisecond) // span 3+ boundaries
	samples := s.stop()

	assert.Equal(t, start, samples[0].at)
	// Boundary samples land near start + k*30ms (allow 20ms slop) up to the final.
	for k := 1; k < len(samples)-1; k++ {
		target := start.Add(time.Duration(k) * 30 * time.Millisecond)
		diff := samples[k].at.Sub(target)
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqualf(t, diff, 20*time.Millisecond, "sample %d at %v target %v diff %v", k, samples[k].at, target, diff)
	}
}

func TestBucketDeltas_EmptyAndSingle(t *testing.T) {
	t.Parallel()
	assert.Nil(t, bucketDeltas(nil))
	assert.Nil(t, bucketDeltas([]counterSample{{}}))
	deltas := bucketDeltas([]counterSample{
		{primary: 5, hedge: 1},
		{primary: 9, hedge: 3},
	})
	assert.Equal(t, []counterBucketDelta{{primary: 4, hedge: 2}}, deltas)
}

func TestGatherCounter(t *testing.T) {
	t.Parallel()

	t.Run("reads by name", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: "thing_total"})
		reg.MustRegister(c)
		c.Add(7)
		assert.Equal(t, int64(7), gatherCounter(reg, "thing_total"))
	})

	t.Run("panics on missing metric", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		assert.PanicsWithError(t, `gatherCounter: metric "missing_total" not registered`, func() {
			gatherCounter(reg, "missing_total")
		})
	})
}
