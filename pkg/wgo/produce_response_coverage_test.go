package wgo

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProduceResponseCoversAll(t *testing.T) {
	const topic = "t"
	resp := func(partitions ...int32) *kmsg.ProduceResponse {
		entries := make([]kmsg.ProduceResponseTopicPartition, len(partitions))
		for i, p := range partitions {
			entries[i] = kmsg.ProduceResponseTopicPartition{Partition: p}
		}
		return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{Topic: topic, Partitions: entries}}}
	}

	tests := map[string]struct {
		resp      *kmsg.ProduceResponse
		requested []encodedTopicPartitionRecords
		want      bool
	}{
		"covers every requested partition": {
			resp:      resp(0, 1),
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a"), makeTopicPartitionRecords(topic, 1, "b")},
			want:      true,
		},
		"omits a requested partition": {
			resp:      resp(0),
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a"), makeTopicPartitionRecords(topic, 1, "b")},
			want:      false,
		},
		"extra partitions in response are fine": {
			resp:      resp(0, 1, 2),
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a")},
			want:      true,
		},
		"different topic does not count as coverage": {
			resp:      resp(0),
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords("other", 0, "a")},
			want:      false,
		},
		"nil response covers nothing": {
			resp:      nil,
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a")},
			want:      false,
		},
		"nil response covers an empty request": {
			resp:      nil,
			requested: nil,
			want:      true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, produceResponseCoversAll(tc.resp, tc.requested))
		})
	}
}

// A Produce response that omits a requested partition must not resolve that
// partition's records as produced. Without the coverage check the omission is
// indistinguishable from an ack: succeeded() sees no non-zero ErrorCode,
// recordErrFromResult finds no entry and returns nil, and the records are
// dropped instead of retried.
func TestHedger_ProduceSync_OmittedPartitionIsNotReportedAsProduced(t *testing.T) {
	const (
		topic     = "ingest"
		primaryID = int32(1)
		acked     = int32(0)
		omitted   = int32(1)
	)

	strategy := &mockPartitionAssignmentStrategy{
		candidates: map[partitionKey][]Agent{
			{topic, acked}:   healthyAgents(primaryID, 2),
			{topic, omitted}: healthyAgents(primaryID, 2),
		},
	}
	tracker := NewAverageAgentStatsTracker()
	nowNs := time.Now().UnixNano()
	for _, id := range []int32{primaryID, 2} {
		seedFullWindow(tracker, id, nowNs, 20, 1, 0)
	}

	producer := newMockDirectProducer()
	producer.respFn = func(_ int32, _ []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
		// The agent acks one partition and says nothing about the other.
		return &kmsg.ProduceResponse{
			Topics: []kmsg.ProduceResponseTopic{{
				Topic:      topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: acked, ErrorCode: 0, BaseOffset: 42}},
			}},
		}, nil
	}

	h := NewHedger(producer, tracker, strategy,
		HealthCheckConfig{SlowMultiplier: 2.0, MaxSlowFraction: 0.3, FaultyThreshold: 0.05, MaxFaultyFraction: 0.3},
		HedgerConfig{MinHedgeDelay: 10 * time.Millisecond, MaxHedgeAgents: 3},
		0, 1<<20, newMetrics(prometheus.NewPedanticRegistry()))
	defer h.Close()

	routed := func(p int32) routedEncodedTopicPartitionRecords {
		return routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, p,
				[]*kgo.Record{{Topic: topic, Partition: p, Value: []byte("payload")}}),
			nodeID:    primaryID,
			nodeState: AgentStateHealthy,
		}
	}

	res := h.ProduceSync(context.Background(), primaryID,
		[]routedEncodedTopicPartitionRecords{routed(acked), routed(omitted)})

	assert.False(t, res.succeeded(), "a response missing a requested partition is not a full success")
	require.NoError(t, recordErrFromResult(res, topic, acked), "the acked partition still succeeds")
	require.Error(t, recordErrFromResult(res, topic, omitted),
		"records for an unacknowledged partition must not report success")
	assert.Greater(t, len(producer.recordedCalls()), 1, "the omitted partition must be retried")
}
