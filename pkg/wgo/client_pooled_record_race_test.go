package wgo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/wgo/internal/testkafka"
)

// These tests exercise the produce API's record-ownership contract under
// pooled-record reuse: once a record's promise fires (or ProduceSync returns
// for it), the caller may reset the record and return it to a pool. They
// mirror the real callers (the distributor forwarder pools records via a
// pooled request object; the cardinality-summary writer does *record =
// kgo.Record{} in its promise).
//
// The bug they guard against is a data race between a still-in-flight hedge
// leg re-reading the caller's *kgo.Record and the caller's pool reset. It only
// manifests when hedging is active, so the setup forces hedge legs to fire and
// linger past the primary win. The race detector is the reproduction signal:
// on the buggy code `go test -race` reports the hedge goroutine reading the
// record concurrently with the reset; both tests are race-free once the client
// stops touching records after their promise resolves. CI runs `go test -race`.

// newHedgingClientForRaceTest builds a client whose hedge path reliably fires:
// a 3-broker cluster with one deliberately-slow agent and an aggressive hedge
// delay. The slow agent makes the partitions it leads hedge to a fast secondary
// while the primary still wins, leaving a hedge leg in flight.
//
// The agent-stats window is seeded directly (white-box) rather than warmed up:
// the tracker only qualifies stats once activity spans minFilledBuckets across
// 10s buckets, so a warmup burst would qualify only when the run happens to
// straddle a 10s wall-clock boundary. Seeding a full window of healthy,
// low-latency stats makes shouldHedge fire deterministically from the first
// produce; the slow agent's real 10ms latency then exceeds the hedge delay, so
// its partitions dispatch a hedge that lingers past the primary win.
func newHedgingClientForRaceTest(t *testing.T, topic string, numPartitions int32) *WarpstreamClient {
	t.Helper()

	_, addr := testkafka.CreateCluster(t, numPartitions, topic, testkafka.WithNumBrokers(3))

	c, err := NewWarpstreamClient(log.NewNopLogger(), prometheus.NewPedanticRegistry(), []Opt{
		WithAddress(addr),
		WithTopic(topic),
		WithClientID("pooled-record-race-test"),
		WithDialTimeout(2 * time.Second),
		WithWriteTimeout(5 * time.Second),
		WithLinger(20 * time.Millisecond),
		WithBatchMaxBytes(1 << 20),
		WithHealthCheckSlowMultiplier(1.5),
		WithHealthCheckMaxSlowFraction(0.9),
		WithHealthCheckFaultyThreshold(0.5),
		WithHealthCheckMaxFaultyFraction(0.9),
		WithHedgerMinHedgeDelay(time.Millisecond),
		WithHedgerMaxHedgeAgents(3),
		WithDemoterProbeInterval(time.Second),
		WithClusterStatsTTL(50 * time.Millisecond),
		WithMetadataRefreshInterval(10 * time.Second),
		WithProduceRequestTimeout(2 * time.Second),
		WithProduceRequestTimeoutOverhead(time.Second),
	}...)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	// Node 0 is slow: partitions it leads dispatch a hedge to a fast secondary
	// while the primary still wins, so a hedge leg lingers past the primary win.
	c.SetTestProduceResponseHook(func(ctx context.Context, nodeID int32, _ *kmsg.ProduceResponse, _ error) {
		d := time.Millisecond
		if nodeID == 0 {
			d = 10 * time.Millisecond
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	})

	// Seed a full window of healthy, low-latency stats for every agent so
	// shouldHedge is not suppressed (no cold-start, no dependence on crossing a
	// 10s bucket boundary). 1ms baseline keeps the hedge delay well below the
	// slow agent's injected 10ms latency.
	inner := c.tracker.inner.(*AverageAgentStatsTracker)
	nowNs := time.Now().UnixNano()
	for _, nodeID := range c.pool.Agents() {
		seedFullWindow(inner, nodeID, nowNs, 50, 1, 0)
	}

	return c
}

func TestWarpstreamClient_ProduceSync_ReusedPooledRecordsAreRaceFree(t *testing.T) {
	const topic = "test-topic"
	const numPartitions = 6
	c := newHedgingClientForRaceTest(t, topic, numPartitions)

	pool := sync.Pool{New: func() any { return new(kgo.Record) }}
	var nextPartition atomic.Int32
	var failures atomic.Int64

	const goroutines = 16
	const perGoroutine = 40
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				rec := pool.Get().(*kgo.Record)
				rec.Topic = topic
				rec.Partition = nextPartition.Add(1) % numPartitions
				rec.Value = append(rec.Value[:0], 'v')

				if res := c.ProduceSync(context.Background(), []*kgo.Record{rec}); res.FirstErr() != nil {
					failures.Add(1)
				}

				// ProduceSync has returned for rec, so the caller owns it again:
				// reset and return it to the pool. The client must not read rec
				// after this point (an in-flight hedge leg doing so is the bug).
				*rec = kgo.Record{}
				pool.Put(rec)
			}
		}()
	}
	wg.Wait()

	// The primary (partition leader) always succeeds, so every produce must too;
	// this keeps the test on the success path, where a hedge leg outlives the win.
	require.Zero(t, failures.Load(), "produces must succeed")
	// Fail loudly if the setup did not actually exercise the hedge path.
	require.Positive(t, testutil.ToFloat64(c.metrics.produceRequestsHedgeTotal),
		"expected the hedge path to fire; the test would not cover the regression otherwise")
}

func TestWarpstreamClient_Produce_ReusedPooledRecordsAreRaceFree(t *testing.T) {
	const topic = "test-topic"
	const numPartitions = 6
	c := newHedgingClientForRaceTest(t, topic, numPartitions)

	pool := sync.Pool{New: func() any { return new(kgo.Record) }}
	var nextPartition atomic.Int32
	var failures atomic.Int64

	const total = 800
	var wg sync.WaitGroup
	wg.Add(total)
	// Bound concurrency so the async producer doesn't buffer without limit.
	sem := make(chan struct{}, 64)
	for i := 0; i < total; i++ {
		sem <- struct{}{}
		rec := pool.Get().(*kgo.Record)
		rec.Topic = topic
		rec.Partition = nextPartition.Add(1) % numPartitions
		rec.Value = append(rec.Value[:0], 'v')

		c.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
			if err != nil {
				failures.Add(1)
			}
			// The promise has fired for r: reset and return it to the pool,
			// mirroring the cardinality-summary writer. The client must not
			// read r after this (an in-flight hedge leg doing so is the bug).
			*r = kgo.Record{}
			pool.Put(r)
			<-sem
			wg.Done()
		})
	}
	wg.Wait()

	// The primary (partition leader) always succeeds, so every produce must too;
	// this keeps the test on the success path, where a hedge leg outlives the win.
	require.Zero(t, failures.Load(), "produces must succeed")
	require.Positive(t, testutil.ToFloat64(c.metrics.produceRequestsHedgeTotal),
		"expected the hedge path to fire; the test would not cover the regression otherwise")
}
