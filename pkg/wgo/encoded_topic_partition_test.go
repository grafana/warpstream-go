package wgo

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	t.Run("concatenates bytes, sums stats, and keeps identity/routing of the receiver", func(t *testing.T) {
		a := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: encodedTopicPartitionRecords{
				topic: "t", partition: 0,
				encoded:      []byte("AAA"),
				encodedStats: produceRequestStats{records: 2, batches: 1, uncompressedBytes: 30, compressedBytes: 10},
			},
			nodeID:    5,
			nodeState: AgentStateHealthy,
		}
		b := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: encodedTopicPartitionRecords{
				topic: "t", partition: 0,
				encoded:      []byte("BB"),
				encodedStats: produceRequestStats{records: 3, batches: 1, uncompressedBytes: 20, compressedBytes: 8},
			},
			nodeID:    5,
			nodeState: AgentStateDemoted,
		}

		merged := a.mergeWith(b)

		assert.Equal(t, []byte("AAABB"), merged.encoded)
		assert.Equal(t, produceRequestStats{records: 5, batches: 2, uncompressedBytes: 50, compressedBytes: 18}, merged.encodedStats)
		assert.Equal(t, "t", merged.topic)
		assert.Equal(t, int32(0), merged.partition)
		assert.Equal(t, int32(5), merged.nodeID)
		// The freshest routing-time classification (the other's) wins.
		assert.Equal(t, AgentStateDemoted, merged.nodeState)
	})

	t.Run("panics when the two items target different routing", func(t *testing.T) {
		base := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "t", partition: 0, encoded: []byte("A")},
			nodeID:                       1,
		}
		diffTopic := base
		diffTopic.topic = "u"
		diffPartition := base
		diffPartition.partition = 1
		diffNodeID := base
		diffNodeID.nodeID = 2

		assert.Panics(t, func() { base.mergeWith(diffTopic) })
		assert.Panics(t, func() { base.mergeWith(diffPartition) })
		assert.Panics(t, func() { base.mergeWith(diffNodeID) })
	})

	t.Run("returns a freshly allocated batch without mutating either input", func(t *testing.T) {
		// The receiver slice has spare capacity, so an in-place append would
		// clobber the byte past its length.
		first := make([]byte, 3, 8)
		copy(first, "AAA")
		a := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "t", encoded: first, encodedStats: produceRequestStats{records: 1}},
		}
		b := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: "t", encoded: []byte("BB"), encodedStats: produceRequestStats{records: 1}},
		}

		merged := a.mergeWith(b)
		assert.Equal(t, []byte("AAABB"), merged.encoded)

		// Neither input is mutated in place.
		assert.Equal(t, []byte("AAA"), a.encoded)
		assert.Equal(t, produceRequestStats{records: 1}, a.encodedStats)
		assert.Equal(t, []byte("BB"), b.encoded)
		assert.Equal(t, produceRequestStats{records: 1}, b.encodedStats)
		assert.Equal(t, byte(0), first[:cap(first)][3], "merge must not write into the receiver's spare capacity")

		// The returned batch is a fresh allocation, aliasing neither input's array.
		assert.NotSame(t, &a.encoded[0], &merged.encoded[0])
		assert.NotSame(t, &b.encoded[0], &merged.encoded[0])
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
