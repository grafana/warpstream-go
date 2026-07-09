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

func TestTopicPartitionRecords_PayloadBytes(t *testing.T) {
	tests := map[string]struct {
		records       []*kgo.Record
		expectedBytes int64
	}{
		"empty group": {
			records:       nil,
			expectedBytes: 0,
		},
		"value only": {
			records:       []*kgo.Record{{Value: []byte("hello")}},
			expectedBytes: 5,
		},
		"key and value": {
			records:       []*kgo.Record{{Key: []byte("ab"), Value: []byte("hello")}},
			expectedBytes: 7,
		},
		"headers counted": {
			records: []*kgo.Record{{
				Key:   []byte("ab"),
				Value: []byte("hello"),
				Headers: []kgo.RecordHeader{
					{Key: "h1", Value: []byte("xyz")},
					{Key: "h22", Value: []byte("z")},
				},
			}},
			expectedBytes: 2 + 5 + (2 + 3) + (3 + 1),
		},
		"multiple records summed": {
			records: []*kgo.Record{
				{Key: []byte("a"), Value: []byte("bb")},
				{Value: []byte("ccc")},
			},
			expectedBytes: (1 + 2) + 3,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := topicPartitionRecords{records: tc.records}
			assert.Equal(t, tc.expectedBytes, p.payloadBytes())
		})
	}
}

func TestMergePromisedRoutedBatchByTopicPartition_Records(t *testing.T) {
	rec := func(v string) *kgo.Record { return &kgo.Record{Value: []byte(v)} }
	prom := func(topic string, partition int32, recs ...*kgo.Record) promised[routedTopicPartitionRecords] {
		return promised[routedTopicPartitionRecords]{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{topic: topic, partition: partition, records: recs},
				nodeID:                7,
				nodeState:             AgentStateDemoted,
			},
		}
	}

	t.Run("distinct partitions are kept separate, in first-seen order", func(t *testing.T) {
		a, b, c := rec("a"), rec("b"), rec("c")
		out := mergePromisedRoutedBatchByTopicPartition([]promised[routedTopicPartitionRecords]{
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
		out := mergePromisedRoutedBatchByTopicPartition([]promised[routedTopicPartitionRecords]{
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
		firstEntry := promised[routedTopicPartitionRecords]{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 0, records: first},
				nodeID:                7,
			},
		}
		out := mergePromisedRoutedBatchByTopicPartition([]promised[routedTopicPartitionRecords]{
			firstEntry,
			prom("t", 0, b),
		})
		require.Len(t, out, 1)
		assert.Equal(t, []*kgo.Record{a, b}, out[0].records)
		// The input's backing array must be untouched beyond its length.
		assert.Equal(t, []*kgo.Record{a}, first[:1])
		assert.Nil(t, first[:cap(first)][1], "merge must not write into the input's spare capacity")
	})

	t.Run("merged entry takes the last contributor's nodeState", func(t *testing.T) {
		promState := func(state AgentState, r *kgo.Record) promised[routedTopicPartitionRecords] {
			return promised[routedTopicPartitionRecords]{
				item: routedTopicPartitionRecords{
					topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 0, records: []*kgo.Record{r}},
					nodeID:                7,
					nodeState:             state,
				},
			}
		}
		// The last Add's routing-time state is the freshest, so it wins — whether
		// that means an agent just got demoted or just recovered.
		demotedLast := mergePromisedRoutedBatchByTopicPartition([]promised[routedTopicPartitionRecords]{
			promState(AgentStateHealthy, rec("a")),
			promState(AgentStateDemoted, rec("b")),
		})
		require.Len(t, demotedLast, 1)
		assert.Equal(t, AgentStateDemoted, demotedLast[0].nodeState)

		healthyLast := mergePromisedRoutedBatchByTopicPartition([]promised[routedTopicPartitionRecords]{
			promState(AgentStateDemoted, rec("a")),
			promState(AgentStateHealthy, rec("b")),
		})
		require.Len(t, healthyLast, 1)
		assert.Equal(t, AgentStateHealthy, healthyLast[0].nodeState)
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		assert.Empty(t, mergePromisedRoutedBatchByTopicPartition[routedTopicPartitionRecords](nil))
	})
}

func TestMergePromisedRoutedBatchByTopicPartition_Encoded(t *testing.T) {
	promEnc := func(topic string, partition int32, values ...string) promised[routedEncodedTopicPartitionRecords] {
		recs := make([]*kgo.Record, len(values))
		for i, v := range values {
			recs[i] = &kgo.Record{Value: []byte(v), Timestamp: time.UnixMilli(1_700_000_000_000 + int64(i))}
		}
		return promised[routedEncodedTopicPartitionRecords]{
			item: routedEncodedTopicPartitionRecords{
				encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, recs),
				nodeID:                       7,
				nodeState:                    AgentStateDemoted,
			},
		}
	}
	values := func(records []*kgo.Record) []string {
		out := make([]string, len(records))
		for i, r := range records {
			out[i] = string(r.Value)
		}
		return out
	}

	t.Run("same topic-partition entries merge into one batch decoding to all records in order", func(t *testing.T) {
		out := mergePromisedRoutedBatchByTopicPartition([]promised[routedEncodedTopicPartitionRecords]{
			promEnc("t", 0, "a1", "a2"),
			promEnc("u", 0, "z1"),
			promEnc("t", 0, "b1"), // merges into the first t/0 entry
		})
		require.Len(t, out, 2)

		// t/0: a single batch carrying all three records in arrival order.
		assert.Equal(t, int64(1), out[0].encodedStats.batches)
		assert.Equal(t, int64(3), out[0].encodedStats.records)
		assert.Equal(t, []string{"a1", "a2", "b1"}, values(decodeBatch(out[0].encoded)))
		// Routing decision is carried onto the merged wire view.
		assert.Equal(t, int32(7), out[0].nodeID)

		// u/0 is a single entry, returned unchanged (still one batch).
		assert.Equal(t, int64(1), out[1].encodedStats.records)
		assert.Equal(t, []string{"z1"}, values(decodeBatch(out[1].encoded)))
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		assert.Empty(t, mergePromisedRoutedBatchByTopicPartition[routedEncodedTopicPartitionRecords](nil))
	})
}

func TestTopicPartitionRecords_RecordCount(t *testing.T) {
	t.Run("records state returns the live count", func(t *testing.T) {
		p := topicPartitionRecords{records: []*kgo.Record{{}, {}}}
		assert.Equal(t, 2, p.recordCount())
	})
	t.Run("empty group returns zero", func(t *testing.T) {
		p := topicPartitionRecords{}
		assert.Equal(t, 0, p.recordCount())
	})
}

func TestRoutedTopicPartitionRecords_UncompressedWireBytes(t *testing.T) {
	recs := makeRecords(0, "v1", "v2")
	p := routedTopicPartitionRecords{topicPartitionRecords: topicPartitionRecords{records: recs}}
	assert.Equal(t, multiRecordBatchEstimateBytes(recs), p.uncompressedWireBytes())
}

func TestRoutedTopicPartitionRecords_MergeWith(t *testing.T) {
	base := routedTopicPartitionRecords{
		topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 0, records: []*kgo.Record{{Value: []byte("a")}}},
		nodeID:                1,
		nodeState:             AgentStateHealthy,
	}

	t.Run("concatenates records and takes the later contributor's nodeState", func(t *testing.T) {
		other := base
		other.records = []*kgo.Record{{Value: []byte("b")}}
		other.nodeState = AgentStateDemoted

		merged := base.mergeWith([]routedTopicPartitionRecords{other})
		require.Len(t, merged.records, 2)
		assert.Equal(t, []byte("a"), merged.records[0].Value)
		assert.Equal(t, []byte("b"), merged.records[1].Value)
		assert.Equal(t, AgentStateDemoted, merged.nodeState)
	})

	t.Run("panics when the two items target different routing", func(t *testing.T) {
		diffTopic := base
		diffTopic.topic = "u"
		diffPartition := base
		diffPartition.partition = 1
		diffNodeID := base
		diffNodeID.nodeID = 2

		assert.Panics(t, func() { base.mergeWith([]routedTopicPartitionRecords{diffTopic}) })
		assert.Panics(t, func() { base.mergeWith([]routedTopicPartitionRecords{diffPartition}) })
		assert.Panics(t, func() { base.mergeWith([]routedTopicPartitionRecords{diffNodeID}) })
	})

	t.Run("returns a freshly allocated batch without mutating either input", func(t *testing.T) {
		ra, rb := &kgo.Record{Value: []byte("a")}, &kgo.Record{Value: []byte("b")}
		// The receiver slice has spare capacity, so an in-place append would
		// clobber index 1 of its backing array.
		first := make([]*kgo.Record, 1, 4)
		first[0] = ra
		a := routedTopicPartitionRecords{
			topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 0, records: first},
			nodeID:                1,
			nodeState:             AgentStateHealthy,
		}
		b := routedTopicPartitionRecords{
			topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 0, records: []*kgo.Record{rb}},
			nodeID:                1,
			nodeState:             AgentStateDemoted,
		}

		merged := a.mergeWith([]routedTopicPartitionRecords{b})
		assert.Equal(t, []*kgo.Record{ra, rb}, merged.records)

		// Neither input is mutated in place.
		assert.Equal(t, []*kgo.Record{ra}, a.records)
		assert.Equal(t, AgentStateHealthy, a.nodeState)
		assert.Equal(t, []*kgo.Record{rb}, b.records)
		assert.Equal(t, AgentStateDemoted, b.nodeState)
		assert.Nil(t, first[:cap(first)][1], "merge must not write into the receiver's spare capacity")

		// The returned batch is a fresh allocation, aliasing neither input's array.
		assert.NotSame(t, &a.records[0], &merged.records[0])
		assert.NotSame(t, &b.records[0], &merged.records[0])
	})
}

func TestSplitPromisedRoutedBatchByBatchMaxBytes(t *testing.T) {
	const batchMaxBytes = 512
	mk := func(recs []*kgo.Record, done func(ProduceResult)) promised[routedTopicPartitionRecords] {
		return promised[routedTopicPartitionRecords]{
			item: routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{topic: "t", partition: 3, records: recs},
				nodeID:                9,
				nodeState:             AgentStateDemoted,
			},
			done: done,
		}
	}
	// oversized builds records that together exceed batchMaxBytes while each
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
		out := splitPromisedRoutedBatchByBatchMaxBytes(mk(recs, func(ProduceResult) {}), batchMaxBytes)
		require.Len(t, out, 1)
		assert.Equal(t, recs, out[0].item.records)
	})

	t.Run("oversized group splits into <=cap chunks, records preserved in order, routing kept", func(t *testing.T) {
		recs := oversized()
		out := splitPromisedRoutedBatchByBatchMaxBytes(mk(recs, func(ProduceResult) {}), batchMaxBytes)
		require.Greater(t, len(out), 1)

		var got []*kgo.Record
		for _, c := range out {
			require.NotEmpty(t, c.item.records)
			assert.LessOrEqual(t, multiRecordBatchEstimateBytes(c.item.records), int64(batchMaxBytes))
			assert.Equal(t, "t", c.item.topic)
			assert.Equal(t, int32(3), c.item.partition)
			assert.Equal(t, int32(9), c.item.nodeID)
			assert.Equal(t, AgentStateDemoted, c.item.nodeState)
			got = append(got, c.item.records...)
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
		// Use batchMaxBytes records: every record costs at least one wire byte,
		// so this many records cannot fit in a single <=batchMaxBytes chunk —
		// guaranteeing multiple, densely-packed chunks regardless of the cap.
		base := time.UnixMilli(1_000_000)
		recs := make([]*kgo.Record, batchMaxBytes)
		for i := range recs {
			recs[i] = &kgo.Record{Timestamp: base.Add(time.Duration(i) * time.Hour)}
		}
		out := splitPromisedRoutedBatchByBatchMaxBytes(mk(recs, func(ProduceResult) {}), batchMaxBytes)
		require.Greater(t, len(out), 1)

		var got []*kgo.Record
		for _, c := range out {
			require.NotEmpty(t, c.item.records)
			assert.LessOrEqual(t, multiRecordBatchEstimateBytes(c.item.records), int64(batchMaxBytes),
				"each chunk's real wire size must stay within the cap")
			got = append(got, c.item.records...)
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
		out := splitPromisedRoutedBatchByBatchMaxBytes(mk(oversized(), func(res ProduceResult) {
			calls++
			got = res
		}), batchMaxBytes)
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
			out := splitPromisedRoutedBatchByBatchMaxBytes(mk(oversized(), func(res ProduceResult) { got = res }), batchMaxBytes)
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
		out := splitPromisedRoutedBatchByBatchMaxBytes(mk(oversized(), func(ProduceResult) {
			atomic.AddInt32(&calls, 1)
		}), batchMaxBytes)
		require.Greater(t, len(out), 1)

		var wg sync.WaitGroup
		for _, c := range out {
			wg.Add(1)
			go func(c promised[routedTopicPartitionRecords]) {
				defer wg.Done()
				c.done(success)
			}(c)
		}
		wg.Wait()
		assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	})
}
