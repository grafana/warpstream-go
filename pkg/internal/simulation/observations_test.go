package main

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservations_RecordIsThreadSafe(t *testing.T) {
	t.Parallel()
	obs := &observations{}
	const goroutines = 16
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				obs.record(time.Now(), 10*time.Millisecond, nil)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, goroutines*perGoroutine, len(obs.snapshot().list))
}

func TestObservations_Summary(t *testing.T) {
	t.Parallel()

	t.Run("empty returns zero", func(t *testing.T) {
		assert.Equal(t, observationsSummary{}, (&observations{}).snapshot().summary())
	})

	t.Run("counts and percentiles", func(t *testing.T) {
		boom := errors.New("boom")
		obs := &observations{}
		obs.add(observation{latency: 100 * time.Millisecond})
		obs.add(observation{latency: 200 * time.Millisecond})
		obs.add(observation{latency: 300 * time.Millisecond, err: boom})
		obs.add(observation{latency: 400 * time.Millisecond})

		s := obs.snapshot().summary()
		assert.Equal(t, 4, s.total)
		assert.Equal(t, 3, s.successes)
		assert.Equal(t, 1, s.failures)
		assert.Equal(t, 250*time.Millisecond, s.meanLatency)
		assert.Equal(t, 300*time.Millisecond, s.p50Latency) // index 4*50/100 = 2
		assert.Equal(t, 400*time.Millisecond, s.p99Latency) // index 4*99/100 = 3
	})
}

func TestObservations_BucketedSummary(t *testing.T) {
	t.Parallel()

	t.Run("groups by issue time", func(t *testing.T) {
		start := time.Unix(1_700_000_000, 0)
		obs := &observations{}
		obs.add(observation{issued: start, latency: 100 * time.Millisecond})
		obs.add(observation{issued: start.Add(2 * time.Second), latency: 100 * time.Millisecond})
		obs.add(observation{issued: start.Add(11 * time.Second), latency: 200 * time.Millisecond})
		obs.add(observation{issued: start.Add(31 * time.Second), latency: 300 * time.Millisecond})

		buckets := obs.snapshot().bucketedSummary(10 * time.Second)
		require.Len(t, buckets, 4) // 0-10, 10-20, 20-30, 30-40
		assert.Equal(t, 2, buckets[0].total)
		assert.Equal(t, 1, buckets[1].total)
		assert.Equal(t, 0, buckets[2].total) // gap
		assert.Equal(t, 1, buckets[3].total)
	})

	t.Run("empty returns nil", func(t *testing.T) {
		assert.Nil(t, (&observations{}).snapshot().bucketedSummary(10*time.Second))
	})
}

func TestObservations_LatencyQuantile(t *testing.T) {
	t.Parallel()

	t.Run("zero when empty", func(t *testing.T) {
		assert.Equal(t, time.Duration(0), (&observations{}).snapshot().latencyQuantile(0.5))
	})

	t.Run("picks the quantile by floor index", func(t *testing.T) {
		obs := &observations{}
		for i := 1; i <= 10; i++ { // sorted latencies 100ms..1000ms
			obs.add(observation{latency: time.Duration(i) * 100 * time.Millisecond})
		}
		assert.Equal(t, 100*time.Millisecond, obs.snapshot().latencyQuantile(0.0))   // idx 0
		assert.Equal(t, 600*time.Millisecond, obs.snapshot().latencyQuantile(0.5))   // idx 5
		assert.Equal(t, 1000*time.Millisecond, obs.snapshot().latencyQuantile(0.99)) // idx 9
		assert.Equal(t, 1000*time.Millisecond, obs.snapshot().latencyQuantile(1.0))  // clamped to last
	})
}

func TestObservations_ErrorCounts(t *testing.T) {
	t.Parallel()
	obs := &observations{}
	obs.add(observation{err: errors.New("a")})
	obs.add(observation{err: errors.New("b")})
	obs.add(observation{err: errors.New("a")})
	obs.add(observation{}) // success: ignored
	obs.add(observation{err: errors.New("a")})
	assert.Equal(t, map[string]int{"a": 3, "b": 1}, obs.snapshot().errorCounts())
}
