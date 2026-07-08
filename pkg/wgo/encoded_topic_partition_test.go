package wgo

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestEncodedTopicPartitionRecords_RecordCount(t *testing.T) {
	t.Run("returns the encoded batch record count", func(t *testing.T) {
		p := encodedTopicPartitionRecords{encoded: []byte("batch"), encodedStats: produceRequestStats{records: 5}}
		assert.Equal(t, 5, p.recordCount())
	})
	t.Run("empty group returns zero", func(t *testing.T) {
		assert.Equal(t, 0, encodedTopicPartitionRecords{}.recordCount())
	})
}

func TestEncodedTopicPartitionRecords_PayloadBytes(t *testing.T) {
	// payloadBytes is always zero for encoded items regardless of contents.
	assert.Equal(t, int64(0), encodedTopicPartitionRecords{}.payloadBytes())
	assert.Equal(t, int64(0), encodedTopicPartitionRecords{
		encoded:      []byte("some-bytes"),
		encodedStats: produceRequestStats{records: 3, uncompressedBytes: 100},
	}.payloadBytes())
}

func TestRoutedEncodedTopicPartitionRecords_Getters(t *testing.T) {
	p := routedEncodedTopicPartitionRecords{
		encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "t", partition: 3},
		nodeID:                       7,
		nodeState:                    AgentStateDemoted,
	}
	assert.Equal(t, topicPartition{topic: "t", partition: 3}, p.getTopicPartition())
	assert.Equal(t, int32(7), p.getNodeID())
}

func TestRoutedEncodedTopicPartitionRecords_WireBytes(t *testing.T) {
	t.Run("is the encoded byte length", func(t *testing.T) {
		p := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: encodedTopicPartitionRecords{encoded: []byte("batch-bytes")},
		}
		assert.Equal(t, int64(len("batch-bytes")), p.wireBytes())
	})
	t.Run("empty batch is zero", func(t *testing.T) {
		assert.Equal(t, int64(0), routedEncodedTopicPartitionRecords{}.wireBytes())
	})
}

func TestRoutedEncodedTopicPartitionRecords_SplitByMaxBytes(t *testing.T) {
	p := routedEncodedTopicPartitionRecords{
		encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "t", partition: 3, encoded: []byte("batch-bytes"), encodedStats: produceRequestStats{records: 4}},
		nodeID:                       7,
		nodeState:                    AgentStateDemoted,
	}

	// Encoded batches are pre-sized upstream, so split is always a no-op that
	// returns the item unchanged — even when max is smaller than the batch.
	for _, max := range []int32{1, int32(len(p.encoded)), 1 << 20} {
		out := p.splitByMaxBytes(max)
		require.Len(t, out, 1)
		assert.Equal(t, p, out[0])
	}
}

func TestRoutedEncodedTopicPartitionRecords_MergeWith(t *testing.T) {
	mk := func(topic string, partition, nodeID int32, state AgentState, values ...string) routedEncodedTopicPartitionRecords {
		recs := make([]*kgo.Record, len(values))
		for i, v := range values {
			recs[i] = &kgo.Record{Value: []byte(v), Timestamp: time.UnixMilli(1_700_000_000_000 + int64(i))}
		}
		return routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, recs),
			nodeID:                       nodeID,
			nodeState:                    state,
		}
	}
	values := func(records []*kgo.Record) []string {
		out := make([]string, len(records))
		for i, r := range records {
			out[i] = string(r.Value)
		}
		return out
	}

	t.Run("merges into one batch decoding to all records in order, latest nodeState wins", func(t *testing.T) {
		a := mk("t", 0, 5, AgentStateHealthy, "a1", "a2")
		b := mk("t", 0, 5, AgentStateDemoted, "b1")

		merged := a.mergeWith([]routedEncodedTopicPartitionRecords{b})

		assert.Equal(t, "t", merged.topic)
		assert.Equal(t, int32(0), merged.partition)
		assert.Equal(t, int32(5), merged.nodeID)
		// The freshest routing-time classification (the last contributor's) wins.
		assert.Equal(t, AgentStateDemoted, merged.nodeState)
		// One standard batch (not two concatenated), with recomputed stats.
		assert.Equal(t, int64(1), merged.encodedStats.batches)
		assert.Equal(t, int64(3), merged.encodedStats.records)
		// The single batch decodes back to every record in arrival order.
		assert.Equal(t, []string{"a1", "a2", "b1"}, values(decodeBatch(merged.encoded)))
	})

	t.Run("merges every item in the group in one shot", func(t *testing.T) {
		a := mk("t", 0, 5, AgentStateHealthy, "a")
		b := mk("t", 0, 5, AgentStateHealthy, "b")
		c := mk("t", 0, 5, AgentStateHealthy, "c")

		merged := a.mergeWith([]routedEncodedTopicPartitionRecords{b, c})
		assert.Equal(t, int64(1), merged.encodedStats.batches)
		assert.Equal(t, []string{"a", "b", "c"}, values(decodeBatch(merged.encoded)))
	})

	t.Run("returns the receiver unchanged when there is nothing to merge", func(t *testing.T) {
		a := mk("t", 0, 5, AgentStateHealthy, "a")
		assert.Equal(t, a, a.mergeWith(nil))
	})

	t.Run("panics when an item targets different routing", func(t *testing.T) {
		base := mk("t", 0, 1, AgentStateHealthy, "x")
		diffTopic := mk("u", 0, 1, AgentStateHealthy, "x")
		diffPartition := mk("t", 1, 1, AgentStateHealthy, "x")
		diffNodeID := mk("t", 0, 2, AgentStateHealthy, "x")

		assert.Panics(t, func() { base.mergeWith([]routedEncodedTopicPartitionRecords{diffTopic}) })
		assert.Panics(t, func() { base.mergeWith([]routedEncodedTopicPartitionRecords{diffPartition}) })
		assert.Panics(t, func() { base.mergeWith([]routedEncodedTopicPartitionRecords{diffNodeID}) })
	})

	t.Run("does not mutate the inputs", func(t *testing.T) {
		a := mk("t", 0, 5, AgentStateHealthy, "a")
		b := mk("t", 0, 5, AgentStateHealthy, "b")
		aEncoded := append([]byte(nil), a.encoded...)
		bEncoded := append([]byte(nil), b.encoded...)
		aStats, bStats := a.encodedStats, b.encodedStats

		_ = a.mergeWith([]routedEncodedTopicPartitionRecords{b})

		assert.Equal(t, aEncoded, a.encoded)
		assert.Equal(t, aStats, a.encodedStats)
		assert.Equal(t, bEncoded, b.encoded)
		assert.Equal(t, bStats, b.encodedStats)
	})
}

func TestUnrouteEncodedTopicPartitionRecords(t *testing.T) {
	t.Run("strips routing while preserving payload and order", func(t *testing.T) {
		parts := []routedEncodedTopicPartitionRecords{
			{encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "t", partition: 0, encoded: []byte("a"), encodedStats: produceRequestStats{records: 1}}, nodeID: 1, nodeState: AgentStateHealthy},
			{encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "u", partition: 9, encoded: []byte("bb"), encodedStats: produceRequestStats{records: 2}}, nodeID: 2, nodeState: AgentStateDemoted},
		}
		out := unrouteEncodedTopicPartitionRecords(parts)
		require.Len(t, out, 2)
		assert.Equal(t, encodedTopicPartitionRecords{topic: "t", partition: 0, encoded: []byte("a"), encodedStats: produceRequestStats{records: 1}}, out[0])
		assert.Equal(t, encodedTopicPartitionRecords{topic: "u", partition: 9, encoded: []byte("bb"), encodedStats: produceRequestStats{records: 2}}, out[1])
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		assert.Empty(t, unrouteEncodedTopicPartitionRecords(nil))
	})
}

func TestNewMultiRoutedEncodedTopicPartitionRecords(t *testing.T) {
	enc := func(topic string, partition int32) encodedTopicPartitionRecords {
		return encodedTopicPartitionRecords{topic: topic, partition: partition, encoded: []byte("b")}
	}

	t.Run("stamps every entry with the same nodeID and done", func(t *testing.T) {
		parts := []encodedTopicPartitionRecords{enc("t", 0), enc("t", 1), enc("u", 5)}
		var fired int
		done := func(ProduceResult) { fired++ }

		out := newMultiRoutedEncodedTopicPartitionRecords(parts, 42, done)
		require.Len(t, out, len(parts))
		for i, r := range out {
			assert.Equal(t, parts[i], r.item.encodedTopicPartitionRecords)
			assert.Equal(t, int32(42), r.item.nodeID)
			require.NotNil(t, r.done)
			r.done(ProduceResult{})
		}
		assert.Equal(t, len(parts), fired)
	})

	t.Run("empty input returns empty slice", func(t *testing.T) {
		assert.Empty(t, newMultiRoutedEncodedTopicPartitionRecords(nil, 1, func(ProduceResult) {}))
	})

	t.Run("nil done is preserved", func(t *testing.T) {
		out := newMultiRoutedEncodedTopicPartitionRecords([]encodedTopicPartitionRecords{enc("t", 0)}, 1, nil)
		require.Len(t, out, 1)
		assert.Nil(t, out[0].done)
	})

	t.Run("done propagates the same ProduceResult to every entry", func(t *testing.T) {
		parts := []encodedTopicPartitionRecords{enc("t", 0), enc("t", 1)}
		want := errors.New("boom")
		resp := &kmsg.ProduceResponse{}
		var calls []ProduceResult
		done := func(res ProduceResult) { calls = append(calls, res) }

		out := newMultiRoutedEncodedTopicPartitionRecords(parts, 7, done)
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
