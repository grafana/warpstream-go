package wgo

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNewMultiRoutedTopicPartitionRecords(t *testing.T) {
	t.Run("stamps every entry with the same nodeID and done", func(t *testing.T) {
		parts := []topicPartitionRecords{
			{topic: "t", partition: 0, records: []*kgo.Record{{Topic: "t", Partition: 0}}},
			{topic: "t", partition: 1, records: []*kgo.Record{{Topic: "t", Partition: 1}}},
			{topic: "u", partition: 5, records: []*kgo.Record{{Topic: "u", Partition: 5}}},
		}
		var fired int
		done := func(ProduceResult) { fired++ }

		out := newMultiRoutedTopicPartitionRecords(parts, 42, done)
		require.Len(t, out, len(parts))
		for i, r := range out {
			assert.Equal(t, parts[i].topic, r.topic)
			assert.Equal(t, parts[i].partition, r.partition)
			assert.Equal(t, parts[i].records, r.records)
			assert.Equal(t, int32(42), r.nodeID)
			require.NotNil(t, r.done)
			r.done(ProduceResult{})
		}
		assert.Equal(t, len(parts), fired)
	})

	t.Run("empty input returns empty slice", func(t *testing.T) {
		out := newMultiRoutedTopicPartitionRecords(nil, 1, func(ProduceResult) {})
		assert.Empty(t, out)
	})

	t.Run("nil done is preserved", func(t *testing.T) {
		parts := []topicPartitionRecords{{topic: "t", partition: 0}}
		out := newMultiRoutedTopicPartitionRecords(parts, 1, nil)
		require.Len(t, out, 1)
		assert.Nil(t, out[0].done)
	})

	t.Run("done propagates the ProduceResult to every entry", func(t *testing.T) {
		parts := []topicPartitionRecords{
			{topic: "t", partition: 0},
			{topic: "t", partition: 1},
		}
		want := errors.New("boom")
		resp := &kmsg.ProduceResponse{}
		var calls []ProduceResult
		done := func(res ProduceResult) {
			calls = append(calls, res)
		}

		out := newMultiRoutedTopicPartitionRecords(parts, 7, done)
		for _, r := range out {
			r.done(ProduceResult{resp: resp, err: want})
		}
		require.Len(t, calls, len(parts))
		for _, c := range calls {
			assert.Same(t, resp, c.resp)
			assert.Same(t, want, c.err)
		}
	})
}

func TestMergePromisedRoutedTopicPartitionRecordsByTopicPartition(t *testing.T) {
	rec := func(v string) *kgo.Record { return &kgo.Record{Value: []byte(v)} }
	prom := func(topic string, partition int32, recs ...*kgo.Record) promisedRoutedTopicPartitionRecords {
		return promisedRoutedTopicPartitionRecords{
			routedTopicPartitionRecords: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{topic: topic, partition: partition, records: recs},
				nodeID:                7,
				nodeState:             AgentStateDemoted,
			},
		}
	}

	t.Run("distinct partitions are kept separate, in first-seen order", func(t *testing.T) {
		a, b, c := rec("a"), rec("b"), rec("c")
		out := mergePromisedRoutedTopicPartitionRecordsByTopicPartition([]promisedRoutedTopicPartitionRecords{
			prom("t", 0, a),
			prom("t", 1, b),
			prom("u", 0, c),
		})
		require.Len(t, out, 3)
		assert.Equal(t, topicPartitionRecords{topic: "t", partition: 0, records: []*kgo.Record{a}}, out[0].topicPartitionRecords)
		assert.Equal(t, topicPartitionRecords{topic: "t", partition: 1, records: []*kgo.Record{b}}, out[1].topicPartitionRecords)
		assert.Equal(t, topicPartitionRecords{topic: "u", partition: 0, records: []*kgo.Record{c}}, out[2].topicPartitionRecords)
		// Routing decision is carried onto the merged wire view.
		assert.Equal(t, int32(7), out[0].nodeID)
		assert.Equal(t, AgentStateDemoted, out[0].nodeState)
	})

	t.Run("same topic-partition entries merge with records concatenated in arrival order", func(t *testing.T) {
		a, b, c, d := rec("a"), rec("b"), rec("c"), rec("d")
		out := mergePromisedRoutedTopicPartitionRecordsByTopicPartition([]promisedRoutedTopicPartitionRecords{
			prom("t", 0, a),
			prom("u", 0, c),
			prom("t", 0, b, d), // merges into the first t/0 entry
		})
		require.Len(t, out, 2)
		assert.Equal(t, []*kgo.Record{a, b, d}, out[0].records)
		assert.Equal(t, []*kgo.Record{c}, out[1].records)
	})

	t.Run("merge does not append into an input's spare capacity", func(t *testing.T) {
		a, b := rec("a"), rec("b")
		// The first entry's records slice has spare capacity, so an in-place
		// append during merge would clobber index 1 of this backing array.
		first := make([]*kgo.Record, 1, 4)
		first[0] = a
		firstEntry := promisedRoutedTopicPartitionRecords{
			routedTopicPartitionRecords: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 0, records: first},
			},
		}
		out := mergePromisedRoutedTopicPartitionRecordsByTopicPartition([]promisedRoutedTopicPartitionRecords{
			firstEntry,
			prom("t", 0, b),
		})
		require.Len(t, out, 1)
		assert.Equal(t, []*kgo.Record{a, b}, out[0].records)
		// The input's backing array must be untouched beyond its length.
		assert.Equal(t, []*kgo.Record{a}, first[:1])
		assert.Nil(t, first[:cap(first)][1], "merge must not write into the input's spare capacity")
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		assert.Empty(t, mergePromisedRoutedTopicPartitionRecordsByTopicPartition(nil))
	})
}

func TestSplitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(t *testing.T) {
	const maxBatchBytes = 512
	mk := func(recs []*kgo.Record, done func(ProduceResult)) promisedRoutedTopicPartitionRecords {
		return promisedRoutedTopicPartitionRecords{
			routedTopicPartitionRecords: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 3, records: recs},
				nodeID:                9,
				nodeState:             AgentStateDemoted,
			},
			done: done,
		}
	}
	// oversized builds records that together exceed maxBatchBytes while each
	// stays well within it (200-byte values).
	oversized := func() []*kgo.Record {
		recs := make([]*kgo.Record, 5)
		for i := range recs {
			recs[i] = &kgo.Record{Value: make([]byte, 200)}
		}
		return recs
	}

	t.Run("group within cap is returned unchanged", func(t *testing.T) {
		recs := []*kgo.Record{{Value: []byte("v")}}
		out := splitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(mk(recs, func(ProduceResult) {}), maxBatchBytes)
		require.Len(t, out, 1)
		assert.Equal(t, recs, out[0].records)
	})

	t.Run("oversized group splits into <=cap chunks, records preserved in order, routing kept", func(t *testing.T) {
		recs := oversized()
		out := splitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(mk(recs, func(ProduceResult) {}), maxBatchBytes)
		require.Greater(t, len(out), 1)

		var got []*kgo.Record
		for _, c := range out {
			require.NotEmpty(t, c.records)
			assert.LessOrEqual(t, multiRecordBatchEstimateBytes(c.records), int64(maxBatchBytes))
			assert.Equal(t, "t", c.topic)
			assert.Equal(t, int32(3), c.partition)
			assert.Equal(t, int32(9), c.nodeID)
			assert.Equal(t, AgentStateDemoted, c.nodeState)
			got = append(got, c.records...)
		}
		assert.Equal(t, recs, got)
	})

	t.Run("many small records with wide timestamp gaps stay within cap per chunk", func(t *testing.T) {
		// Tiny records pack many per chunk, and the hour-spaced timestamps make
		// each record's tsDelta varint grow within a chunk (1 byte up to ~5).
		// That makes the per-record size estimate load-bearing: a sizer that
		// under-counted tsDelta would pack a chunk the real encoder then makes
		// exceed the cap, failing the assertion below. A handful of large
		// records can't catch this — the few-byte varint error per record never
		// accumulates enough to tip a chunk boundary.
		//
		// Use maxBatchBytes records: every record costs at least one wire byte,
		// so this many records cannot fit in a single <=maxBatchBytes chunk —
		// guaranteeing multiple, densely-packed chunks regardless of the cap.
		base := time.UnixMilli(1_000_000)
		recs := make([]*kgo.Record, maxBatchBytes)
		for i := range recs {
			recs[i] = &kgo.Record{Timestamp: base.Add(time.Duration(i) * time.Hour)}
		}
		out := splitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(mk(recs, func(ProduceResult) {}), maxBatchBytes)
		require.Greater(t, len(out), 1)

		var got []*kgo.Record
		for _, c := range out {
			require.NotEmpty(t, c.records)
			assert.LessOrEqual(t, multiRecordBatchEstimateBytes(c.records), int64(maxBatchBytes),
				"each chunk's real wire size must stay within the cap")
			got = append(got, c.records...)
		}
		assert.Equal(t, recs, got)
	})

	// success is a non-empty response, which ProduceResult.error() reports as
	// success; a bare ProduceResult{} would be treated as a failure.
	success := ProduceResult{resp: &kmsg.ProduceResponse{}}

	t.Run("fan-in done fires the original exactly once, after the last chunk", func(t *testing.T) {
		var (
			calls int
			got   ProduceResult
		)
		out := splitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(mk(oversized(), func(res ProduceResult) {
			calls++
			got = res
		}), maxBatchBytes)
		require.Greater(t, len(out), 1)
		for i, c := range out {
			c.done(success)
			if i < len(out)-1 {
				assert.Equal(t, 0, calls, "original done must not fire before the last chunk resolves")
			}
		}
		require.Equal(t, 1, calls)
		assert.NoError(t, got.error(), "all chunks succeeded, so the partition succeeds")
	})

	t.Run("fan-in reports the first failure, else success", func(t *testing.T) {
		boom1 := errors.New("boom1")
		boom2 := errors.New("boom2")
		// run fires one result per chunk (built from len(out)) and returns the
		// single result delivered to the original done.
		run := func(t *testing.T, results func(n int) []ProduceResult) ProduceResult {
			var got ProduceResult
			out := splitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(mk(oversized(), func(res ProduceResult) { got = res }), maxBatchBytes)
			require.GreaterOrEqual(t, len(out), 3, "oversized() must split into >=3 chunks to exercise these orderings")
			rs := results(len(out))
			for i, c := range out {
				c.done(rs[i])
			}
			return got
		}
		allSuccess := func(n int) []ProduceResult {
			rs := make([]ProduceResult, n)
			for i := range rs {
				rs[i] = success
			}
			return rs
		}

		t.Run("all chunks succeed", func(t *testing.T) {
			assert.NoError(t, run(t, allSuccess).error())
		})
		t.Run("failure in the first chunk wins", func(t *testing.T) {
			got := run(t, func(n int) []ProduceResult { rs := allSuccess(n); rs[0] = ProduceResult{err: boom1}; return rs })
			assert.ErrorIs(t, got.err, boom1)
		})
		t.Run("failure after a success still wins (override branch)", func(t *testing.T) {
			got := run(t, func(n int) []ProduceResult { rs := allSuccess(n); rs[1] = ProduceResult{err: boom1}; return rs })
			assert.ErrorIs(t, got.err, boom1)
		})
		t.Run("first of two failures wins", func(t *testing.T) {
			got := run(t, func(n int) []ProduceResult {
				rs := allSuccess(n)
				rs[0] = ProduceResult{err: boom1}
				rs[1] = ProduceResult{err: boom2}
				return rs
			})
			assert.ErrorIs(t, got.err, boom1)
		})
	})

	t.Run("concurrent chunk dones fire the original exactly once", func(t *testing.T) {
		var calls int32
		out := splitPromisedRoutedTopicPartitionRecordsByMaxBatchBytes(mk(oversized(), func(ProduceResult) {
			atomic.AddInt32(&calls, 1)
		}), maxBatchBytes)
		require.Greater(t, len(out), 1)

		var wg sync.WaitGroup
		for _, c := range out {
			wg.Add(1)
			go func(c promisedRoutedTopicPartitionRecords) {
				defer wg.Done()
				c.done(success)
			}(c)
		}
		wg.Wait()
		assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	})
}
