package wgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// routedToMany maps each record to its destination nodeID via routeBy and
// installs a shared done that fires exactly once after every record has
// completed. routeBy(topic, partition) returns the destination; the helper
// is the test equivalent of WarpstreamClient.routeRecords paired with the
// caller's promise/wait coordination.
func routedToMany(records []*kgo.Record, routeBy func(string, int32) int32, sharedDone func(error)) []promised[routedTopicPartitionRecords] {
	if len(records) == 0 {
		sharedDone(nil)
		return nil
	}
	groups := make(map[topicPartition]*promised[routedTopicPartitionRecords])
	var order []topicPartition
	for _, r := range records {
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		g, ok := groups[key]
		if !ok {
			g = &promised[routedTopicPartitionRecords]{
				item: routedTopicPartitionRecords{
					topicPartitionRecords: topicPartitionRecords{topic: r.Topic, partition: r.Partition},
					nodeID:                routeBy(r.Topic, r.Partition),
				},
			}
			groups[key] = g
			order = append(order, key)
		}
		g.item.records = append(g.item.records, r)
	}
	var (
		mu       sync.Mutex
		pending  = len(order)
		firstErr error
		fired    bool
	)
	fan := func(res ProduceResult) {
		mu.Lock()
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
		pending--
		last := pending == 0 && !fired
		if last {
			fired = true
		}
		final := firstErr
		mu.Unlock()
		if last {
			sharedDone(final)
		}
	}
	out := make([]promised[routedTopicPartitionRecords], 0, len(order))
	for _, key := range order {
		g := groups[key]
		g.done = fan
		out = append(out, *g)
	}
	return out
}

// TestClusterBuffer_Add runs every single-item enqueue case against both Add and
// MultiAdd (via a one-element slice): each case enqueues one item, so both entry
// points must behave identically.
func TestClusterBuffer_Add(t *testing.T) {
	adders := []struct {
		name string
		add  func(*ClusterBuffer[routedTopicPartitionRecords], context.Context, promised[routedTopicPartitionRecords])
	}{
		{
			"Add",
			func(c *ClusterBuffer[routedTopicPartitionRecords], ctx context.Context, p promised[routedTopicPartitionRecords]) {
				c.Add(ctx, p)
			},
		}, {
			"MultiAdd",
			func(c *ClusterBuffer[routedTopicPartitionRecords], ctx context.Context, p promised[routedTopicPartitionRecords]) {
				c.MultiAdd(ctx, []promised[routedTopicPartitionRecords]{p})
			},
		},
	}

	for _, adder := range adders {
		t.Run(adder.name, func(t *testing.T) {
			t.Run("routes through and flushes via linger", func(t *testing.T) {
				flush := newRecordingFlush()
				m := newMetrics(prometheus.NewPedanticRegistry())
				c := NewClusterBuffer[routedTopicPartitionRecords](20*time.Millisecond, 1<<20, flush.Func(), m, nil)
				t.Cleanup(c.Close)

				p, done := singleRouted(1, []*kgo.Record{makeRecord("t", 0, "v")})
				adder.add(c, context.Background(), p)

				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("done did not fire")
				}
				require.Equal(t, 1, flush.callCount())
				assert.Equal(t, int32(1), flush.snapshot()[0].nodeID)
				assert.Equal(t, int64(0), c.BufferedBytes())
				assert.Equal(t, int64(0), c.BufferedRecords())
			})

			t.Run("non-cancelable ctx: cancelling the parent does not detach", func(t *testing.T) {
				flush := newRecordingFlush()
				m := newMetrics(prometheus.NewPedanticRegistry())
				c := NewClusterBuffer[routedTopicPartitionRecords](20*time.Millisecond, 1<<20, flush.Func(), m, nil)
				t.Cleanup(c.Close)

				// A non-cancelable ctx (Done() == nil) whose cancelable parent is then
				// canceled: the buffer must not detach the caller — done resolves with
				// the flush outcome, never ctx.Err().
				parent, cancel := context.WithCancel(context.Background())
				ctx := context.WithoutCancel(parent)

				p, done := singleRouted(1, []*kgo.Record{makeRecord("t", 0, "v")})
				adder.add(c, ctx, p)
				cancel()

				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("done did not fire")
				}
				require.Equal(t, 1, flush.callCount())
				assert.Equal(t, int64(0), c.BufferedBytes())
				assert.Equal(t, int64(0), c.BufferedRecords())
			})

			t.Run("add after close fails fast", func(t *testing.T) {
				flush := newRecordingFlush()
				m := newMetrics(prometheus.NewPedanticRegistry())
				c := NewClusterBuffer[routedTopicPartitionRecords](time.Hour, 1<<20, flush.Func(), m, nil)
				c.Close()

				p, done := singleRouted(1, []*kgo.Record{makeRecord("t", 0, "v")})
				adder.add(c, context.Background(), p)

				select {
				case err := <-done:
					require.ErrorIs(t, err, errBufferClosed)
				case <-time.After(time.Second):
					t.Fatal("done did not fire after add on closed cluster")
				}
				assert.Equal(t, int64(0), c.BufferedBytes())
				assert.Equal(t, int64(0), c.BufferedRecords())
			})

			t.Run("pre-canceled ctx fails without dispatching a flush", func(t *testing.T) {
				flush := newRecordingFlush()
				m := newMetrics(prometheus.NewPedanticRegistry())
				c := NewClusterBuffer[routedTopicPartitionRecords](time.Hour, 1<<20, flush.Func(), m, nil)
				t.Cleanup(c.Close)

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				// The pre-cancel fast path resolves done with ctx.Err() and never
				// dispatches to the flush (distinct from a mid-flight cancel).
				p, done := singleRouted(1, []*kgo.Record{makeRecord("t", 0, "v")})
				adder.add(c, ctx, p)

				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("done did not fire for pre-canceled ctx")
				}
				assert.Equal(t, 0, flush.callCount())
				assert.Equal(t, int64(0), c.BufferedBytes())
				assert.Equal(t, int64(0), c.BufferedRecords())
			})

			t.Run("ctx canceled mid-flight: done fires with ctx error but records still flush", func(t *testing.T) {
				flush := newRecordingFlush()
				release := make(chan struct{})
				flush.onFlush = func(int32, []*kgo.Record) error {
					<-release
					return nil
				}
				m := newMetrics(prometheus.NewPedanticRegistry())
				reg := prometheus.NewPedanticRegistry()
				c := NewClusterBuffer[routedTopicPartitionRecords](10*time.Millisecond, 1<<20, flush.Func(), m, reg)
				t.Cleanup(c.Close)

				ctx, cancel := context.WithCancel(context.Background())
				const value = "v"
				p, done := singleRouted(1, []*kgo.Record{makeRecord("t", 0, value)})
				adder.add(c, ctx, p)

				// Wait for the linger to fire and the flush goroutine to enter onFlush.
				require.Eventually(t, func() bool { return flush.callCount() == 1 },
					time.Second, 10*time.Millisecond)

				// While the flush is held, the bytes are accounted as in-flight, and
				// the buffered-producer gauges mirror the counters.
				assert.Equal(t, int64(len(value)), c.BufferedBytes())
				assert.Equal(t, int64(1), c.BufferedRecords())
				assert.Equal(t, float64(len(value)), gaugeValue(t, reg, "buffered_produce_bytes"))
				assert.Equal(t, float64(1), gaugeValue(t, reg, "buffered_produce_records_total"))

				// Cancel while the flush is still blocked.
				cancel()
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("done did not fire after ctx cancel")
				}

				// ctx-cancel detaches the caller, but the records are still being
				// produced — bufferedBytes / bufferedRecords stay non-zero until the
				// actual flush completes.
				assert.Equal(t, int64(len(value)), c.BufferedBytes())
				assert.Equal(t, int64(1), c.BufferedRecords())

				// Release the flush; the records still get produced even though the
				// caller already gave up. Bookkeeping drains once the flush reports.
				close(release)
				require.Eventually(t, func() bool { return c.BufferedBytes() == 0 },
					time.Second, 10*time.Millisecond)
				require.Eventually(t, func() bool { return c.BufferedRecords() == 0 },
					time.Second, 10*time.Millisecond)
				assert.Equal(t, float64(0), gaugeValue(t, reg, "buffered_produce_bytes"))
				assert.Equal(t, float64(0), gaugeValue(t, reg, "buffered_produce_records_total"))
			})

			t.Run("ctx canceled after success is a no-op", func(t *testing.T) {
				flush := newRecordingFlush()
				m := newMetrics(prometheus.NewPedanticRegistry())
				c := NewClusterBuffer[routedTopicPartitionRecords](10*time.Millisecond, 1<<20, flush.Func(), m, nil)
				t.Cleanup(c.Close)

				ctx, cancel := context.WithCancel(context.Background())
				var fires atomic.Int32
				var observedErr atomic.Pointer[error]
				p := routedToSharedDone(1, []*kgo.Record{makeRecord("t", 0, "v")}, func(err error) {
					fires.Add(1)
					observedErr.Store(&err)
				})[0]
				adder.add(c, ctx, p)

				require.Eventually(t, func() bool { return fires.Load() == 1 },
					time.Second, 10*time.Millisecond, "done did not fire on flush success")
				// Success drained bufferedBytes; the late ctx-cancel must not touch it.
				assert.Equal(t, int64(0), c.BufferedBytes())

				// Cancel after the success has already fired done; the watcher should
				// have been detached, so no second firing occurs.
				cancel()
				require.Never(t, func() bool { return fires.Load() > 1 },
					100*time.Millisecond, 10*time.Millisecond)
				require.NotNil(t, observedErr.Load())
				assert.NoError(t, *observedErr.Load())
				assert.Equal(t, int64(0), c.BufferedBytes())
				assert.Equal(t, int64(0), c.BufferedRecords())
			})
		})
	}
}

// TestClusterBuffer_MultiAdd covers behavior specific to the multi-item entry
// point (multiple partitions / agents in one call); the single-item cases it
// shares with Add live in TestClusterBuffer_Add.
func TestClusterBuffer_MultiAdd(t *testing.T) {
	t.Run("two agents flush independently into separate batches", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{"t", 0}: healthyAgents(1),
				{"t", 1}: healthyAgents(2),
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](20*time.Millisecond, 1<<20, flush.Func(), m, nil)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.MultiAdd(context.Background(), routedToMany([]*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, primaryOf(strategy), func(err error) { done <- err }))

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}

		calls := flush.snapshot()
		require.Len(t, calls, 2)
		nodeIDs := []int32{calls[0].nodeID, calls[1].nodeID}
		assert.ElementsMatch(t, []int32{1, 2}, nodeIDs)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("multi-partition records to same agent batch into one flush", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{"t", 0}: healthyAgents(1),
				{"t", 1}: healthyAgents(1), // both partitions go to the same agent
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](20*time.Millisecond, 1<<20, flush.Func(), m, nil)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.MultiAdd(context.Background(), routedToMany([]*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, primaryOf(strategy), func(err error) { done <- err }))

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}

		calls := flush.snapshot()
		require.Len(t, calls, 1)
		assert.Equal(t, int32(1), calls[0].nodeID)
		assert.Len(t, calls[0].records, 2)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("done called once even when records span two agents", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{"t", 0}: healthyAgents(1),
				{"t", 1}: healthyAgents(2),
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](10*time.Millisecond, 1<<20, flush.Func(), m, nil)
		t.Cleanup(c.Close)

		var fires atomic.Int32
		ch := make(chan struct{}, 1)
		c.MultiAdd(context.Background(), routedToMany([]*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, primaryOf(strategy), func(error) {
			fires.Add(1)
			select {
			case ch <- struct{}{}:
			default:
			}
		}))

		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		// done must not fire a second time.
		require.Never(t, func() bool { return fires.Load() > 1 },
			100*time.Millisecond, 10*time.Millisecond)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("done returns first error when one of two agents fails", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{"t", 0}: healthyAgents(1),
				{"t", 1}: healthyAgents(2),
			},
		}
		flush := newRecordingFlush()
		boom := errors.New("agent 2 failed")
		flush.onFlush = func(nodeID int32, _ []*kgo.Record) error {
			if nodeID == 2 {
				return boom
			}
			return nil
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](10*time.Millisecond, 1<<20, flush.Func(), m, nil)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.MultiAdd(context.Background(), routedToMany([]*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, primaryOf(strategy), func(err error) { done <- err }))

		select {
		case err := <-done:
			require.ErrorIs(t, err, boom)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("empty records: done fires synchronously with nil", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](time.Hour, 1<<20, flush.Func(), m, nil)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.MultiAdd(context.Background(), routedToSharedDone(1, nil, func(err error) { done <- err }))

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("done did not fire for empty Add")
		}
		assert.Equal(t, 0, flush.callCount())
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("multi-agent: ctx canceled mid-flight fires done exactly once with ctx error", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{"t", 0}: healthyAgents(1),
				{"t", 1}: healthyAgents(2),
			},
		}
		flush := newRecordingFlush()
		// Hold both agents' flushes so we can cancel before either completes.
		release := make(chan struct{})
		flush.onFlush = func(int32, []*kgo.Record) error {
			<-release
			return nil
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](10*time.Millisecond, 1<<20, flush.Func(), m, nil)
		t.Cleanup(c.Close)

		ctx, cancel := context.WithCancel(context.Background())
		var fires atomic.Int32
		done := make(chan error, 1)
		const valueA, valueB = "aa", "bb"
		c.MultiAdd(ctx, routedToMany([]*kgo.Record{
			makeRecord("t", 0, valueA),
			makeRecord("t", 1, valueB),
		}, primaryOf(strategy), func(err error) {
			fires.Add(1)
			done <- err
		}))

		// Wait for both agent flushes to enter onFlush (i.e., both linger
		// timers fired and dispatched).
		require.Eventually(t, func() bool { return flush.callCount() == 2 },
			time.Second, 10*time.Millisecond)

		// Both agents' bytes are accounted as in-flight.
		assert.Equal(t, int64(len(valueA)+len(valueB)), c.BufferedBytes())
		assert.Equal(t, int64(2), c.BufferedRecords())

		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after ctx cancel")
		}

		// ctx-cancel detaches the caller; both agents' records remain in the
		// producer until their flushes actually complete.
		assert.Equal(t, int64(len(valueA)+len(valueB)), c.BufferedBytes())
		assert.Equal(t, int64(2), c.BufferedRecords())

		// Releasing both flushes invokes reportResult on both agents; neither
		// must double-fire done. Also exercises the firstErr fan-in path:
		// once.Do guarantees the late reports are no-ops.
		close(release)
		require.Never(t, func() bool { return fires.Load() > 1 },
			100*time.Millisecond, 10*time.Millisecond)
		require.Eventually(t, func() bool { return c.BufferedBytes() == 0 },
			time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool { return c.BufferedRecords() == 0 },
			time.Second, 10*time.Millisecond)
	})
}

func TestClusterBuffer_Close(t *testing.T) {
	t.Run("idempotent", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](time.Hour, 1<<20, flush.Func(), m, nil)

		c.Close()
		c.Close() // must not panic or hang
	})

	t.Run("flushes pending in every per-agent buffer", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{"t", 0}: healthyAgents(1),
				{"t", 1}: healthyAgents(2),
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterBuffer[routedTopicPartitionRecords](time.Hour, 1<<20, flush.Func(), m, nil)

		done := make(chan error, 1)
		const valueA, valueB = "a", "b"
		c.MultiAdd(context.Background(), routedToMany([]*kgo.Record{
			makeRecord("t", 0, valueA),
			makeRecord("t", 1, valueB),
		}, primaryOf(strategy), func(err error) { done <- err }))

		assert.Equal(t, 0, flush.callCount())
		assert.Equal(t, int64(len(valueA)+len(valueB)), c.BufferedBytes())
		assert.Equal(t, int64(2), c.BufferedRecords())

		c.Close()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Close")
		}
		assert.Equal(t, 2, flush.callCount())
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})
}
