package wgo

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/wgo/internal/testkafka"
)

// testWarpstreamOpts returns the options used to build a WarpstreamClient
// against a test cluster. Shared so a test can supply its own registry.
func testWarpstreamOpts(clusterAddr, topic string) []Opt {
	return []Opt{
		WithAddress(clusterAddr),
		WithTopic(topic),
		WithClientID("warpstream-test"),
		WithDialTimeout(2 * time.Second),
		WithWriteTimeout(5 * time.Second),
		WithLinger(10 * time.Millisecond),
		WithBatchMaxBytes(1 << 20),
		WithHealthCheckSlowMultiplier(2.0),
		WithHealthCheckMaxSlowFraction(0.3),
		WithHealthCheckFaultyThreshold(0.05),
		WithHealthCheckMaxFaultyFraction(0.3),
		WithHedgerMinHedgeDelay(10 * time.Millisecond),
		WithHedgerMaxHedgeAgents(3),
		WithDemoterProbeInterval(time.Second),
		WithClusterStatsTTL(time.Second),
		WithMetadataRefreshInterval(10 * time.Second),
		WithProduceRequestTimeout(2 * time.Second),
		WithProduceRequestTimeoutOverhead(time.Second),
	}
}

// newTestWarpstreamClient brings up a kfake cluster on an in-memory network and
// wires a WarpstreamClient against it so the whole thing runs inside a
// testing/synctest bubble. It must be called from within synctest.Test. The
// returned VirtualNetwork's DialContext must be passed to any other client
// (e.g. a consumer) so it shares the same in-memory transport.
//
// The background metadata refresh goroutine runs but its ticker
// (MetadataRefreshInterval) is well above any individual test's virtual runtime,
// so it never fires; t.Cleanup runs inside the bubble and Close cancels the
// refresh ctx and joins the goroutine before the bubble ends.
func newTestWarpstreamClient(t *testing.T, topic string, numPartitions int32) (*WarpstreamClient, *kfake.Cluster, string, *kfake.VirtualNetwork) {
	t.Helper()

	vnet := &kfake.VirtualNetwork{}
	cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic, testkafka.WithVirtualNetwork(vnet))

	c, err := NewWarpstreamClient(
		nil,
		prometheus.NewPedanticRegistry(),
		append(testWarpstreamOpts(clusterAddr, topic), WithDialer(vnet.DialContext))...,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
	})
	return c, cluster, clusterAddr, vnet
}

func TestWarpstreamClient_ProduceSync(t *testing.T) {
	const topic = "test-topic"

	t.Run("single record produces and is consumable", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, clusterAddr, vnet := newTestWarpstreamClient(t, topic, 1)

			results := c.ProduceSync(t.Context(), []*kgo.Record{
				{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()},
			})
			require.Len(t, results, 1)
			require.NoError(t, results[0].Err)

			// Verify the record landed by consuming it back.
			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(clusterAddr),
				kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {0: kgo.NewOffset().AtStart()},
				}),
			)
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetches := consumer.PollFetches(t.Context())
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)
			assert.Equal(t, []byte("v"), fetches.Records()[0].Value)

			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("record with unset timestamp ships produce time on the wire", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, clusterAddr, vnet := newTestWarpstreamClient(t, topic, 1)

			// Produce with a zero Timestamp: the client must stamp the produce
			// time like franz-go.
			before := time.Now()
			results := c.ProduceSync(t.Context(), []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("v")},
			})
			require.Len(t, results, 1)
			require.NoError(t, results[0].Err)

			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(clusterAddr),
				kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {0: kgo.NewOffset().AtStart()},
				}),
			)
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetches := consumer.PollFetches(t.Context())
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)

			got := fetches.Records()[0].Timestamp
			assert.False(t, got.IsZero())
			// Under the fake clock the stamp is captured at the same instant as
			// `before`, so the tolerance is tight.
			assert.WithinDuration(t, before, got, time.Millisecond)
		})
	})

	t.Run("record with set timestamp keeps it on the wire", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, clusterAddr, vnet := newTestWarpstreamClient(t, topic, 1)

			// A caller-set Timestamp must reach the wire unchanged, save for the
			// truncation to the millisecond resolution Kafka stores.
			ts := time.Date(2024, 1, 2, 3, 4, 5, 123_456_789, time.UTC)
			results := c.ProduceSync(t.Context(), []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: ts},
			})
			require.Len(t, results, 1)
			require.NoError(t, results[0].Err)

			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(clusterAddr),
				kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {0: kgo.NewOffset().AtStart()},
				}),
			)
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetches := consumer.PollFetches(t.Context())
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)

			assert.WithinDuration(t, ts.Truncate(time.Millisecond), fetches.Records()[0].Timestamp, 0)
		})
	})

	t.Run("unset timestamps in one call share a single produce time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			// ProduceSync stamps every unset record with one shared now, so records
			// buffered together carry an identical produce timestamp.
			rec1 := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("a")}
			rec2 := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("b")}
			results := c.ProduceSync(t.Context(), []*kgo.Record{rec1, rec2})
			require.Len(t, results, 2)
			require.NoError(t, results[0].Err)
			require.NoError(t, results[1].Err)

			assert.False(t, rec1.Timestamp.IsZero())
			assert.Equal(t, rec1.Timestamp, rec2.Timestamp)
		})
	})

	t.Run("records across two partitions both succeed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 2)

			results := c.ProduceSync(t.Context(), []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()},
				{Topic: topic, Partition: 1, Value: []byte("b"), Timestamp: time.Now()},
			})
			require.Len(t, results, 2)
			assert.NoError(t, results[0].Err)
			assert.NoError(t, results[1].Err)

			assert.Equal(t, float64(2), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("results preserve input record order", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			records := []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("0"), Timestamp: time.Now()},
				{Topic: topic, Partition: 0, Value: []byte("1"), Timestamp: time.Now()},
				{Topic: topic, Partition: 0, Value: []byte("2"), Timestamp: time.Now()},
			}
			results := c.ProduceSync(t.Context(), records)
			require.Len(t, results, 3)
			for i, r := range results {
				assert.Same(t, records[i], r.Record, "result[%d] must reference input record %d", i, i)
			}
		})
	})

	t.Run("empty input returns nil", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)
			results := c.ProduceSync(t.Context(), nil)
			assert.Nil(t, results)

			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("record for unknown topic-partition fails fast at the resolver", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			results := c.ProduceSync(t.Context(), []*kgo.Record{
				{Topic: "does-not-exist", Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
			})
			require.Len(t, results, 1)
			require.Error(t, results[0].Err)
			assert.ErrorContains(t, results[0].Err, "no agent assigned")
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned)))
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			// A rejection is not a failure: produceRecordsFailedTotal stays 0.
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("canceled ctx ends the wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			results := c.ProduceSync(ctx, []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
			})
			require.Len(t, results, 1)
			require.ErrorIs(t, results[0].Err, context.Canceled)

			// A canceled context is a post-dispatch failure, not a rejection.
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned)))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedRecordTooLarge)))
		})
	})

	t.Run("oversized record fails per-record while ok records succeed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			// BatchMaxBytes is set to 1<<20 (see newTestWarpstreamClient); a 2 MB
			// value exceeds the cap by itself.
			oversize := make([]byte, 2<<20)
			small := []byte("v")

			records := []*kgo.Record{
				{Topic: topic, Partition: 0, Value: oversize, Timestamp: time.Now()},
				{Topic: topic, Partition: 0, Value: small, Timestamp: time.Now()},
			}
			results := c.ProduceSync(t.Context(), records)
			require.Len(t, results, 2)

			// Per-record outcomes preserve input order.
			require.Error(t, results[0].Err)
			assert.ErrorIs(t, results[0].Err, kerr.MessageTooLarge)
			assert.Same(t, records[0], results[0].Record)

			assert.NoError(t, results[1].Err)
			assert.Same(t, records[1], results[1].Record)

			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedRecordTooLarge)))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned)))
			assert.Equal(t, float64(2), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			// The oversized record is a rejection, not a failure; the ok record succeeds.
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("multi-record batch with an unroutable partition counts every ok record rejected", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			// One record targets an unknown topic: routeRecords fails the whole
			// batch uniformly, so every ok record is counted under no_agent_assigned.
			records := []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()},
				{Topic: "does-not-exist", Partition: 0, Value: []byte("b"), Timestamp: time.Now()},
				{Topic: topic, Partition: 0, Value: []byte("c"), Timestamp: time.Now()},
			}
			results := c.ProduceSync(t.Context(), records)
			require.Len(t, results, 3)
			for i := range results {
				assert.ErrorContains(t, results[i].Err, "no agent assigned")
			}

			assert.Equal(t, float64(len(records)), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned)))
			assert.Equal(t, float64(len(records)), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			// All records are rejections, not failures.
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})
}

func TestWarpstreamClient_RoutingFailureLeavesTimestampUnstamped(t *testing.T) {
	const topic = "test-topic"

	// A record with an unset timestamp routed to an unknown topic-partition fails
	// before dispatch. The record must be left unstamped so a later retry stamps a
	// fresh produce time instead of reusing the failed attempt's.
	t.Run("ProduceSync", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)
			rec := &kgo.Record{Topic: "does-not-exist", Partition: 0, Value: []byte("v")}

			results := c.ProduceSync(t.Context(), []*kgo.Record{rec})
			require.Len(t, results, 1)
			require.Error(t, results[0].Err)
			assert.True(t, rec.Timestamp.IsZero(), "routing failure must not stamp the caller's record")
		})
	})

	t.Run("Produce", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)
			rec := &kgo.Record{Topic: "does-not-exist", Partition: 0, Value: []byte("v")}

			var gotErr error
			done := make(chan struct{})
			c.Produce(t.Context(), rec, func(_ *kgo.Record, err error) {
				gotErr = err
				close(done)
			})
			<-done
			require.Error(t, gotErr)
			assert.True(t, rec.Timestamp.IsZero(), "routing failure must not stamp the caller's record")
		})
	})
}

func TestWarpstreamClient_Produce(t *testing.T) {
	const topic = "test-topic"

	t.Run("invokes promise once with the same record on success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			input := &kgo.Record{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan struct {
				r   *kgo.Record
				err error
			}, 1)
			c.Produce(t.Context(), input, func(r *kgo.Record, err error) {
				done <- struct {
					r   *kgo.Record
					err error
				}{r, err}
			})

			got := <-done
			require.NoError(t, got.err)
			assert.Same(t, input, got.r)
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("invokes promise with error when topic is unknown to the agent pool", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			input := &kgo.Record{Topic: "does-not-exist", Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan error, 1)
			c.Produce(t.Context(), input, func(_ *kgo.Record, err error) {
				done <- err
			})

			err := <-done
			require.Error(t, err)
			assert.ErrorContains(t, err, "no agent assigned")
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned)))
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			// A rejection is not a failure: produceRecordsFailedTotal stays 0.
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("rejects oversized record synchronously with kerr.MessageTooLarge", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			// BatchMaxBytes is 1<<20 in the test client; a 2 MB value exceeds it.
			input := &kgo.Record{Topic: topic, Partition: 0, Value: make([]byte, 2<<20), Timestamp: time.Now()}
			done := make(chan error, 1)
			c.Produce(t.Context(), input, func(r *kgo.Record, err error) {
				assert.Same(t, input, r)
				done <- err
			})

			// The rejection must be synchronous, not buffered: once every goroutine
			// is durably blocked (no virtual time has passed for a linger to fire),
			// the promise must already have resolved.
			synctest.Wait()
			select {
			case err := <-done:
				require.Error(t, err)
				assert.ErrorIs(t, err, kerr.MessageTooLarge)
			default:
				t.Fatal("promise did not fire — rejection must be synchronous, not buffered")
			}
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedRecordTooLarge)))
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			// A rejection is not a failure: produceRecordsFailedTotal stays 0.
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
		})
	})

	t.Run("canceled ctx counts a post-dispatch failure, not a rejection", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			input := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan error, 1)
			c.Produce(ctx, input, func(_ *kgo.Record, err error) {
				done <- err
			})

			err := <-done
			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
			assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsFailedTotal))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned)))
			assert.Equal(t, float64(0), testutil.ToFloat64(c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedRecordTooLarge)))
		})
	})
}

func TestWarpstreamClient_BufferedProduceBytes(t *testing.T) {
	const topic = "test-topic"

	t.Run("zero before any record is buffered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)
			assert.Equal(t, int64(0), c.BufferedProduceBytes())
		})
	})

	t.Run("reflects in-flight bytes while produce is held, drops to zero once it completes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, cluster, _, _ := newTestWarpstreamClient(t, topic, 1)

			// Hold the broker's Produce handler so the bytes are observed while a
			// Produce is genuinely outstanding at the broker (flushed, awaiting ack).
			release := make(chan struct{})
			cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				<-release
				return nil, nil, false
			})

			const value = "in-flight value"
			records := []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte(value), Timestamp: time.Now()},
			}
			produced := make(chan struct{})
			go func() {
				defer close(produced)
				c.ProduceSync(t.Context(), records)
			}()

			// Fire the linger timer so the batch flushes; the Produce then lands at
			// the broker and blocks on <-release. synctest.Wait then settles the
			// flush goroutine + broker, so the counter is exact — no polling.
			time.Sleep(c.cfg.Linger)
			synctest.Wait()
			require.Equal(t, int64(len(value)), c.BufferedProduceBytes())

			// Release the broker; the counter drops back to zero once the produce
			// completes.
			close(release)
			<-produced
			synctest.Wait()
			require.Equal(t, int64(0), c.BufferedProduceBytes())
		})
	})
}

func TestWarpstreamClient_BufferedProduceRecords(t *testing.T) {
	const topic = "test-topic"

	t.Run("zero before any record is buffered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1)
			assert.Equal(t, int64(0), c.BufferedProduceRecords())
		})
	})

	t.Run("reflects in-flight records while produce is held, drops to zero once it completes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, cluster, _, _ := newTestWarpstreamClient(t, topic, 1)

			release := make(chan struct{})
			cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				<-release
				return nil, nil, false
			})

			records := []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()},
				{Topic: topic, Partition: 0, Value: []byte("b"), Timestamp: time.Now()},
				{Topic: topic, Partition: 0, Value: []byte("c"), Timestamp: time.Now()},
			}
			produced := make(chan struct{})
			go func() {
				defer close(produced)
				c.ProduceSync(t.Context(), records)
			}()

			// Fire the linger so the batch flushes and lands at the (held) broker,
			// then settle: the records are now genuinely in-flight.
			time.Sleep(c.cfg.Linger)
			synctest.Wait()
			require.Equal(t, int64(len(records)), c.BufferedProduceRecords())

			close(release)
			<-produced
			synctest.Wait()
			require.Equal(t, int64(0), c.BufferedProduceRecords())
		})
	})
}

func TestWarpstreamClient_Close(t *testing.T) {
	t.Run("close is idempotent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _, _, _ := newTestWarpstreamClient(t, "test-topic", 1)
			c.Close()
			c.Close()
		})
	})

	t.Run("close flushes pending records", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			const topic = "test-topic"
			c, _, clusterAddr, vnet := newTestWarpstreamClient(t, topic, 1)

			// Produce without waiting for the linger.
			doneCh := make(chan error, 1)
			c.buffer.Add(t.Context(), routedToSharedDone(0, []*kgo.Record{
				{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
			}, func(err error) { doneCh <- err }))

			c.Close()
			require.NoError(t, <-doneCh)

			// Verify the record landed.
			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(clusterAddr),
				kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topic: {0: kgo.NewOffset().AtStart()},
				}),
			)
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetches := consumer.PollFetches(t.Context())
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)
		})
	})
}

// TestWarpstreamClient_Metrics verifies the metric ownership split: transport
// metrics come from kprom, producer-state metrics are tracked by this client,
// and warpstream-specific metrics carry the warpstream_ prefix — all on one
// registry without a duplicate registration.
func TestWarpstreamClient_Metrics(t *testing.T) {
	const topic = "test-topic"

	synctest.Test(t, func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, 1, topic, testkafka.WithVirtualNetwork(&vnet))

		c, err := NewWarpstreamClient(nil, reg, append(testWarpstreamOpts(clusterAddr, topic), WithDialer(vnet.DialContext))...)
		require.NoError(t, err)
		t.Cleanup(c.Close)

		results := c.ProduceSync(t.Context(), []*kgo.Record{
			{Topic: topic, Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)

		// Producer-state metrics, tracked by this client with real values.
		assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceWireRecordsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceWireBatchesTotal))
		assert.Greater(t, testutil.ToFloat64(c.metrics.produceWireBytesTotal), float64(0))
		assert.Greater(t, testutil.ToFloat64(c.metrics.produceWireCompressedBytesTotal), float64(0))

		// Registered exactly once (kprom's own versions are filtered out), so
		// there's no collision.
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "produce_records_total"))
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "produce_batches_total"))
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "buffered_produce_bytes"))
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "buffered_produce_records_total"))

		// Transport metrics come from kprom and populate on the produce path
		// (the embedded kgo.Client connects to issue the Produce request).
		assert.GreaterOrEqual(t, testutil.CollectAndCount(reg, "connects_total"), 1)

		// Warpstream-specific metrics carry the warpstream_ prefix; the
		// client-boundary record counter is distinct from the wire-level one above.
		assert.Equal(t, float64(1), testutil.ToFloat64(c.metrics.produceRecordsTotal))
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "warpstream_produce_records_total"))
		assert.GreaterOrEqual(t, testutil.CollectAndCount(reg, "warpstream_produce_direct_requests_total"), 1)

		// The whole set gathers cleanly: no duplicate registration across kprom,
		// the producer-state metrics, and the warpstream_ metrics.
		_, err = reg.Gather()
		require.NoError(t, err)
	})
}

// TestNewWarpstreamClient_NilRegisterer verifies the client builds and produces
// with a nil registerer (metrics disabled), without panicking.
func TestNewWarpstreamClient_NilRegisterer(t *testing.T) {
	const topic = "test-topic"

	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, 1, topic, testkafka.WithVirtualNetwork(&vnet))

		var c *WarpstreamClient
		require.NotPanics(t, func() {
			var err error
			c, err = NewWarpstreamClient(nil, nil, append(testWarpstreamOpts(clusterAddr, topic), WithDialer(vnet.DialContext))...)
			require.NoError(t, err)
		})
		t.Cleanup(c.Close)

		// Producing still works; the metric updates are no-ops on unregistered
		// collectors.
		results := c.ProduceSync(t.Context(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)
	})
}

// TestNewWarpstreamClient_UsesLoggerForEmbeddedClient verifies the logger passed
// to NewWarpstreamClient is installed on the embedded franz-go client. A clean
// produce logs nothing from wgo itself, so any captured output comes from
// franz-go. The logger is at debug because franz-go emits nothing at info+ here.
func TestNewWarpstreamClient_UsesLoggerForEmbeddedClient(t *testing.T) {
	const topic = "test-topic"

	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, 1, topic, testkafka.WithVirtualNetwork(&vnet))

		var buf lockedBuffer
		logger := kgo.BasicLogger(&buf, kgo.LogLevelDebug, nil)

		c, err := NewWarpstreamClient(logger, prometheus.NewPedanticRegistry(), append(testWarpstreamOpts(clusterAddr, topic), WithDialer(vnet.DialContext))...)
		require.NoError(t, err)
		t.Cleanup(c.Close)

		results := c.ProduceSync(t.Context(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)

		// Let franz-go's goroutines finish logging before inspecting the sink.
		synctest.Wait()
		require.NotEmpty(t, buf.String(), "embedded franz-go client should log through the provided logger")
	})
}

func TestLog(t *testing.T) {
	t.Run("emits when the level is enabled", func(t *testing.T) {
		var buf bytes.Buffer
		log(kgo.BasicLogger(&buf, kgo.LogLevelInfo, nil), kgo.LogLevelInfo, "hello")
		require.Contains(t, buf.String(), "hello")
	})

	t.Run("suppresses when the level is disabled", func(t *testing.T) {
		var buf bytes.Buffer
		log(kgo.BasicLogger(&buf, kgo.LogLevelWarn, nil), kgo.LogLevelInfo, "hello")
		require.Empty(t, buf.String())
	})
}

// produceClient is the minimum surface BenchmarkClient_Produce exercises against
// both kgo.Client and *WarpstreamClient.
type produceClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Close()
}

// BenchmarkClient_Produce stresses Produce() throughput for both backends against
// the same kfake cluster (multi-broker, multi-partition), with many concurrent
// goroutines fanning small records across partitions. To keep the comparison
// apples-to-apples, the franz-go side uses the *kgo.Client embedded in the
// WarpstreamClient — both legs run with identical kgo configuration. The
// custom records/sec metric reports post-completion throughput; combined
// with -benchmem it gives a per-record CPU and allocation profile.
func BenchmarkClient_Produce(b *testing.B) {
	const (
		topic         = "bench-topic"
		numPartitions = int32(500)
		numBrokers    = 100
		valueLen      = 1024
		recordsPerOp  = 100
	)

	cluster, addr := testkafka.CreateCluster(b, numPartitions, topic, testkafka.WithNumBrokers(numBrokers))
	b.Cleanup(cluster.Close)

	wsc, err := NewWarpstreamClient(
		nil,
		prometheus.NewPedanticRegistry(),
		WithAddress(addr),
		WithTopic(topic),
		WithClientID("warpstream-bench"),
		WithDialTimeout(2*time.Second),
		WithWriteTimeout(30*time.Second),
		WithLinger(50*time.Millisecond),
		WithBatchMaxBytes(16<<20),
		WithHealthCheckSlowMultiplier(2.0),
		WithHealthCheckMaxSlowFraction(0.3),
		WithHealthCheckFaultyThreshold(0.05),
		WithHealthCheckMaxFaultyFraction(0.3),
		WithHedgerMinHedgeDelay(time.Hour), // Disable hedging: never fires within the benchmark.
		WithHedgerMaxHedgeAgents(3),
		WithDemoterProbeInterval(time.Second),
		WithClusterStatsTTL(time.Second),
		WithMetadataRefreshInterval(time.Hour),
		WithProduceRequestTimeout(10*time.Second),
		WithProduceRequestTimeoutOverhead(time.Second),
	)
	require.NoError(b, err)
	b.Cleanup(wsc.Close)

	// The franz-go leg reuses the kgo.Client embedded in the WarpstreamClient
	// (its produce path is unaffected by the wrapping logic), so both legs
	// share the exact same kgo configuration, broker set, and metadata view.
	cases := []struct {
		name   string
		client produceClient
	}{
		{name: "kgo", client: wsc.Client},
		{name: "warpstream", client: wsc},
	}

	value := make([]byte, valueLen)
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			p := tc.client
			ctx := context.Background()
			var (
				wg        sync.WaitGroup
				partition atomic.Int32
			)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					wg.Add(recordsPerOp)
					for i := 0; i < recordsPerOp; i++ {
						pid := partition.Add(1) % numPartitions
						rec := &kgo.Record{
							Topic:     topic,
							Partition: pid,
							Value:     value,
						}
						p.Produce(ctx, rec, func(*kgo.Record, error) { wg.Done() })
					}
				}
			})
			wg.Wait()
			b.StopTimer()

			b.ReportMetric(float64(b.N*recordsPerOp)/b.Elapsed().Seconds(), "records/sec")
		})
	}
}

// TestWarpstreamClient_ReusedPooledRecordsAreRaceFree exercises the produce
// API's record-ownership contract under pooled-record reuse: once a record's
// promise fires (or ProduceSync returns for it), the caller may reset the record
// and return it to a pool.
//
// The bug it guards against is a data race between a still-in-flight hedge leg
// re-reading the caller's *kgo.Record and the caller resetting or reusing that
// record after its promise resolved. It only manifests when hedging is active,
// so the setup forces hedge legs to fire and linger past the primary win. The
// race detector is the reproduction signal: on the buggy code `go test -race`
// reports the hedge goroutine reading the record concurrently with the reset;
// both subtests are race-free once the client stops touching records after their
// promise resolves. CI runs `go test -race`.
func TestWarpstreamClient_ReusedPooledRecordsAreRaceFree(t *testing.T) {
	const topic = "test-topic"
	const numPartitions = 6

	// newHedgingClient builds a client whose hedge path reliably fires: a
	// 3-broker cluster with one deliberately-slow agent and an aggressive hedge
	// delay. The slow agent makes the partitions it leads hedge to a fast
	// secondary while the primary still wins, leaving a hedge leg in flight.
	//
	// The agent-stats window is seeded directly (white-box) rather than warmed
	// up: the tracker only qualifies stats once activity spans minFilledBuckets
	// across 10s buckets, so a warmup burst would qualify only when the run
	// happens to straddle a 10s wall-clock boundary. Seeding a full window of
	// healthy, low-latency stats makes shouldHedge fire deterministically from
	// the first produce; the slow agent's real 10ms latency then exceeds the
	// hedge delay, so its partitions dispatch a hedge that lingers past the win.
	newHedgingClient := func(t *testing.T, vnet *kfake.VirtualNetwork) *WarpstreamClient {
		t.Helper()

		_, addr := testkafka.CreateCluster(t, numPartitions, topic, testkafka.WithNumBrokers(3), testkafka.WithVirtualNetwork(vnet))

		c, err := NewWarpstreamClient(nil, prometheus.NewPedanticRegistry(), []Opt{
			WithAddress(addr),
			WithTopic(topic),
			WithClientID("pooled-record-race-test"),
			WithDialer(vnet.DialContext),
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
		// Under the fake clock these latencies are exact, so the lingering hedge is
		// deterministic rather than a timing race.
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

	t.Run("ProduceSync", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vnet kfake.VirtualNetwork
			c := newHedgingClient(t, &vnet)

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

						if res := c.ProduceSync(t.Context(), []*kgo.Record{rec}); res.FirstErr() != nil {
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

			// The primary (partition leader) always succeeds, so every produce must
			// too; this keeps the test on the success path, where a hedge leg outlives
			// the win.
			require.Zero(t, failures.Load(), "produces must succeed")
			// Fail loudly if the setup did not actually exercise the hedge path.
			require.Positive(t, testutil.ToFloat64(c.metrics.produceRequestsHedgeTotal),
				"expected the hedge path to fire; the test would not cover the regression otherwise")
		})
	})

	t.Run("Produce", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vnet kfake.VirtualNetwork
			c := newHedgingClient(t, &vnet)

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

				c.Produce(t.Context(), rec, func(r *kgo.Record, err error) {
					if err != nil {
						failures.Add(1)
					}
					// The promise has fired for r, so the caller owns it again: reset
					// and return it to the pool. The client must not read r after this
					// point (an in-flight hedge leg doing so is the bug).
					*r = kgo.Record{}
					pool.Put(r)
					<-sem
					wg.Done()
				})
			}
			wg.Wait()

			require.Zero(t, failures.Load(), "produces must succeed")
			require.Positive(t, testutil.ToFloat64(c.metrics.produceRequestsHedgeTotal),
				"expected the hedge path to fire; the test would not cover the regression otherwise")
		})
	})
}

// lockedBuffer is a concurrency-safe sink for logger output, which franz-go
// writes from its background broker goroutines concurrently with the test.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
