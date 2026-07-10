package wgo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWarpstreamClient_ProduceRecordHooks(t *testing.T) {
	const topic = "test-topic"

	t.Run("Produce fires buffered then unbuffered then promise on success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan struct{})
			c.Produce(t.Context(), rec, func(r *kgo.Record, err error) {
				hook.record("promise", r, err)
				close(done)
			})
			<-done

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "unbuffered", "promise"}, kindsOf(events))
			assert.NoError(t, events[1].err)
			assert.Same(t, rec, events[1].rec)
		})
	})

	t.Run("ProduceSync fires buffered then unbuffered before the result", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			results := c.ProduceSync(t.Context(), []*kgo.Record{rec})
			require.NoError(t, results[0].Err)
			// Marking the result after ProduceSync returns proves unbuffered already fired.
			hook.record("promise", results[0].Record, results[0].Err)

			require.Equal(t, []string{"buffered", "unbuffered", "promise"}, hook.kinds())
		})
	})

	t.Run("Produce records too_large as buffered then unbuffered with MessageTooLarge", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			rec := &kgo.Record{Topic: topic, Partition: 0, Value: make([]byte, 2<<20), Timestamp: time.Now()}
			done := make(chan struct{})
			c.Produce(t.Context(), rec, func(r *kgo.Record, err error) {
				hook.record("promise", r, err)
				close(done)
			})
			<-done

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "unbuffered", "promise"}, kindsOf(events))
			assert.ErrorIs(t, events[1].err, kerr.MessageTooLarge)
		})
	})

	t.Run("ProduceSync records too_large as buffered then unbuffered with MessageTooLarge", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			rec := &kgo.Record{Topic: topic, Partition: 0, Value: make([]byte, 2<<20), Timestamp: time.Now()}
			results := c.ProduceSync(t.Context(), []*kgo.Record{rec})
			require.ErrorIs(t, results[0].Err, kerr.MessageTooLarge)

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "unbuffered"}, kindsOf(events))
			assert.ErrorIs(t, events[1].err, kerr.MessageTooLarge)
		})
	})

	t.Run("no_agent_assigned still fires buffered then unbuffered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			rec := &kgo.Record{Topic: "does-not-exist", Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan struct{})
			c.Produce(t.Context(), rec, func(r *kgo.Record, err error) {
				hook.record("promise", r, err)
				close(done)
			})
			<-done

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "unbuffered", "promise"}, kindsOf(events))
			assert.ErrorContains(t, events[1].err, "no agent assigned")
		})
	})

	t.Run("canceled ctx fires unbuffered with context.Canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan struct{})
			c.Produce(ctx, rec, func(r *kgo.Record, err error) {
				hook.record("promise", r, err)
				close(done)
			})
			<-done

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "unbuffered", "promise"}, kindsOf(events))
			assert.ErrorIs(t, events[1].err, context.Canceled)
		})
	})

	t.Run("ProduceSync fires each hook once per record, in input order, with per-record errors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			// A mixed batch: one oversized record (rejected pre-dispatch) and two
			// that succeed. Exercises the deferred per-record unbuffered loop.
			oversized := &kgo.Record{Topic: topic, Partition: 0, Value: make([]byte, 2<<20), Timestamp: time.Now()}
			ok1 := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()}
			ok2 := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("b"), Timestamp: time.Now()}
			records := []*kgo.Record{oversized, ok1, ok2}

			results := c.ProduceSync(t.Context(), records)
			require.ErrorIs(t, results[0].Err, kerr.MessageTooLarge)
			require.NoError(t, results[1].Err)
			require.NoError(t, results[2].Err)

			events := hook.snapshot()
			// Buffered fires for every record up front, then unbuffered for every
			// record from the deferred loop — each exactly once, in input order.
			require.Equal(t, []string{"buffered", "buffered", "buffered", "unbuffered", "unbuffered", "unbuffered"}, kindsOf(events))
			assert.Equal(t, []*kgo.Record{oversized, ok1, ok2}, []*kgo.Record{events[0].rec, events[1].rec, events[2].rec})
			assert.Equal(t, []*kgo.Record{oversized, ok1, ok2}, []*kgo.Record{events[3].rec, events[4].rec, events[5].rec})
			assert.ErrorIs(t, events[3].err, kerr.MessageTooLarge)
			assert.NoError(t, events[4].err)
			assert.NoError(t, events[5].err)
		})
	})

	t.Run("ProduceSync no_agent_assigned fires buffered then unbuffered per record", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			// An unknown-topic record fails routing for the whole batch uniformly.
			r1 := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("a"), Timestamp: time.Now()}
			r2 := &kgo.Record{Topic: "does-not-exist", Partition: 0, Value: []byte("b"), Timestamp: time.Now()}
			results := c.ProduceSync(t.Context(), []*kgo.Record{r1, r2})
			require.ErrorContains(t, results[0].Err, "no agent assigned")
			require.ErrorContains(t, results[1].Err, "no agent assigned")

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "buffered", "unbuffered", "unbuffered"}, kindsOf(events))
			assert.ErrorContains(t, events[2].err, "no agent assigned")
			assert.ErrorContains(t, events[3].err, "no agent assigned")
		})
	})

	t.Run("ProduceSync canceled ctx fires unbuffered with context.Canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			results := c.ProduceSync(ctx, []*kgo.Record{rec})
			require.ErrorIs(t, results[0].Err, context.Canceled)

			events := hook.snapshot()
			require.Equal(t, []string{"buffered", "unbuffered"}, kindsOf(events))
			assert.ErrorIs(t, events[1].err, context.Canceled)
		})
	})
}

func TestWarpstreamClient_ProduceRecordHooksContext(t *testing.T) {
	const topic = "test-topic"
	type ctxKey struct{}

	t.Run("seeds record.Context from the produce ctx when nil", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			ctx := context.WithValue(t.Context(), ctxKey{}, "parent")
			rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
			done := make(chan struct{})
			c.Produce(ctx, rec, func(*kgo.Record, error) { close(done) })
			<-done

			buffered := hook.snapshot()[0]
			require.Equal(t, "buffered", buffered.kind)
			require.NotNil(t, buffered.ctx)
			assert.Equal(t, "parent", buffered.ctx.Value(ctxKey{}))
		})
	})

	t.Run("preserves a caller-set record.Context as the parent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hook := &recordingHook{}
			c, _, _, _ := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

			preset := context.WithValue(t.Context(), ctxKey{}, "preset")
			rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now(), Context: preset}
			done := make(chan struct{})
			// A different produce ctx must NOT override the record's own context.
			c.Produce(context.WithValue(t.Context(), ctxKey{}, "other"), rec, func(*kgo.Record, error) { close(done) })
			<-done

			buffered := hook.snapshot()[0]
			require.Equal(t, "buffered", buffered.kind)
			assert.Equal(t, "preset", buffered.ctx.Value(ctxKey{}))
		})
	})
}

func TestWarpstreamClient_ProduceRecordHooksHeaderInjection(t *testing.T) {
	const topic = "test-topic"

	synctest.Test(t, func(t *testing.T) {
		// A header injected by the buffered hook must be encoded into the produced
		// record — proving the hook fires before encoding.
		hook := &recordingHook{inject: injectTraceparent("00-trace-span-01")}
		c, _, clusterAddr, vnet := newTestWarpstreamClient(t, topic, 1, WithHooks(hook))

		results := c.ProduceSync(t.Context(), []*kgo.Record{
			{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()},
		})
		require.NoError(t, results[0].Err)

		got := consumeOne(t, clusterAddr, topic, vnet)
		assert.Equal(t, "00-trace-span-01", headerValue(got, "traceparent"))
	})
}

func TestWarpstreamClient_ProduceRecordHooksDisabled(t *testing.T) {
	const topic = "test-topic"
	type ctxKey struct{}

	synctest.Test(t, func(t *testing.T) {
		// With no produce-record hooks the client fires no hooks and injects no
		// headers, but it still seeds the record's parent context like franz-go —
		// context seeding is unconditional, not gated on a hook being registered.
		c, _, _, _ := newTestWarpstreamClient(t, topic, 1)

		ctx := context.WithValue(t.Context(), ctxKey{}, "parent")
		rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v"), Timestamp: time.Now()}
		results := c.ProduceSync(ctx, []*kgo.Record{rec})
		require.NoError(t, results[0].Err)

		require.NotNil(t, rec.Context)
		assert.Equal(t, "parent", rec.Context.Value(ctxKey{}))
		assert.Empty(t, rec.Headers)
	})
}

// TestWarpstreamClient_ProduceRecordHooksFranzGoParity asserts wgo drives the
// produce-record hooks with the same buffered/unbuffered sequence a real franz-go
// producer does for the shared scenarios (success and too_large). no_agent_assigned
// has no franz-go analog, so it is covered by the wgo-only cases above.
func TestWarpstreamClient_ProduceRecordHooksFranzGoParity(t *testing.T) {
	const topic = "test-topic"

	t.Run("success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wgoHook := &recordingHook{}
			wc, _, addr, vnet := newTestWarpstreamClient(t, topic, 1, WithHooks(wgoHook))

			fgHook := &recordingHook{}
			fc, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.Dialer(vnet.DialContext), kgo.WithHooks(fgHook))
			require.NoError(t, err)
			t.Cleanup(fc.Close)

			wres := wc.ProduceSync(t.Context(), []*kgo.Record{{Topic: topic, Partition: 0, Value: []byte("v")}})
			require.NoError(t, wres[0].Err)
			fres := fc.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v")})
			require.NoError(t, fres.FirstErr())

			assert.Equal(t, []string{"buffered", "unbuffered"}, fgHook.kinds())
			assert.Equal(t, fgHook.kinds(), wgoHook.kinds())
		})
	})

	t.Run("too_large", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wgoHook := &recordingHook{}
			// testWarpstreamOpts sets BatchMaxBytes to 1<<20.
			wc, _, addr, vnet := newTestWarpstreamClient(t, topic, 1, WithHooks(wgoHook))

			fgHook := &recordingHook{}
			// MaxBufferedBytes is franz-go's synchronous too-large gate (producer.go),
			// so a record larger than it is rejected with MessageTooLarge before dispatch.
			fc, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.Dialer(vnet.DialContext), kgo.WithHooks(fgHook), kgo.MaxBufferedBytes(1<<20))
			require.NoError(t, err)
			t.Cleanup(fc.Close)

			big := make([]byte, 2<<20)
			wres := wc.ProduceSync(t.Context(), []*kgo.Record{{Topic: topic, Partition: 0, Value: big}})
			require.ErrorIs(t, wres[0].Err, kerr.MessageTooLarge)
			fres := fc.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Partition: 0, Value: big})
			require.ErrorIs(t, fres.FirstErr(), kerr.MessageTooLarge)

			assert.Equal(t, []string{"buffered", "unbuffered"}, fgHook.kinds())
			assert.Equal(t, fgHook.kinds(), wgoHook.kinds())

			// Both deliver MessageTooLarge to the unbuffered hook.
			assert.ErrorIs(t, unbufferedErr(fgHook), kerr.MessageTooLarge)
			assert.ErrorIs(t, unbufferedErr(wgoHook), kerr.MessageTooLarge)
		})
	})
}

// hookEvent is one observed produce-record lifecycle event. ctx is captured only
// for buffered events (the record's parent context at span-start time).
type hookEvent struct {
	kind string
	rec  *kgo.Record
	err  error
	ctx  context.Context
}

// recordingHook implements the two produce-record hook interfaces and records
// every call in order. It optionally injects into the record on buffered to
// simulate a tracer writing a trace-context header. Safe for concurrent use:
// the hooks may fire on the caller goroutine or a background flush goroutine.
type recordingHook struct {
	mu     sync.Mutex
	events []hookEvent
	inject func(*kgo.Record)
}

var (
	_ kgo.HookProduceRecordBuffered   = (*recordingHook)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*recordingHook)(nil)
)

func (h *recordingHook) OnProduceRecordBuffered(r *kgo.Record) {
	h.mu.Lock()
	h.events = append(h.events, hookEvent{kind: "buffered", rec: r, ctx: r.Context})
	h.mu.Unlock()
	if h.inject != nil {
		h.inject(r)
	}
}

func (h *recordingHook) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	h.mu.Lock()
	h.events = append(h.events, hookEvent{kind: "unbuffered", rec: r, err: err})
	h.mu.Unlock()
}

// record appends a synthetic event (used by tests to mark when the caller's
// promise or result fires, so ordering against the hooks can be asserted).
func (h *recordingHook) record(kind string, r *kgo.Record, err error) {
	h.mu.Lock()
	h.events = append(h.events, hookEvent{kind: kind, rec: r, err: err})
	h.mu.Unlock()
}

func (h *recordingHook) kinds() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.events))
	for i, e := range h.events {
		out[i] = e.kind
	}
	return out
}

func (h *recordingHook) snapshot() []hookEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hookEvent(nil), h.events...)
}

// injectTraceparent upserts a traceparent header, matching kotel's carrier
// semantics (update in place if present, else append).
func injectTraceparent(value string) func(*kgo.Record) {
	return func(r *kgo.Record) {
		for i, hd := range r.Headers {
			if hd.Key == "traceparent" {
				r.Headers[i].Value = []byte(value)
				return
			}
		}
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(value)})
	}
}

func kindsOf(events []hookEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.kind
	}
	return out
}

func unbufferedErr(h *recordingHook) error {
	for _, e := range h.snapshot() {
		if e.kind == "unbuffered" {
			return e.err
		}
	}
	return errors.New("no unbuffered event recorded")
}

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func consumeOne(t *testing.T, clusterAddr, topic string, vnet *kfake.VirtualNetwork) *kgo.Record {
	t.Helper()
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
	recs := fetches.Records()
	require.Len(t, recs, 1)
	return recs[0]
}
