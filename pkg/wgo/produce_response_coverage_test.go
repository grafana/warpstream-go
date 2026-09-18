package wgo

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProduceResponse_FirstMissingPartition(t *testing.T) {
	const topic = "t"
	resp := func(partitions ...int32) *kmsg.ProduceResponse {
		entries := make([]kmsg.ProduceResponseTopicPartition, len(partitions))
		for i, p := range partitions {
			entries[i] = kmsg.ProduceResponseTopicPartition{Partition: p}
		}
		return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{Topic: topic, Partitions: entries}}}
	}

	tests := map[string]struct {
		resp        *kmsg.ProduceResponse
		requested   []encodedTopicPartitionRecords
		missing     topicPartition
		wantMissing bool
	}{
		"covers every requested partition": {
			resp:      resp(0, 1),
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a"), makeTopicPartitionRecords(topic, 1, "b")},
		},
		"omits a requested partition": {
			resp:        resp(0),
			requested:   []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a"), makeTopicPartitionRecords(topic, 1, "b")},
			missing:     topicPartition{topic: topic, partition: 1},
			wantMissing: true,
		},
		"reports the first missing partition in request order": {
			resp:        resp(0),
			requested:   []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 2, "a"), makeTopicPartitionRecords("other", 1, "b")},
			missing:     topicPartition{topic: topic, partition: 2},
			wantMissing: true,
		},
		"extra partitions in response are fine": {
			resp:      resp(0, 1, 2),
			requested: []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a")},
		},
		"different topic does not count as coverage": {
			resp:        resp(0),
			requested:   []encodedTopicPartitionRecords{makeTopicPartitionRecords("other", 0, "a")},
			missing:     topicPartition{topic: "other", partition: 0},
			wantMissing: true,
		},
		"nil response covers nothing": {
			requested:   []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a")},
			missing:     topicPartition{topic: topic, partition: 0},
			wantMissing: true,
		},
		"empty response covers nothing": {
			resp:        resp(),
			requested:   []encodedTopicPartitionRecords{makeTopicPartitionRecords(topic, 0, "a")},
			missing:     topicPartition{topic: topic, partition: 0},
			wantMissing: true,
		},
		"nil response covers an empty request": {},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			missing, ok := firstMissingProducePartition(tc.resp, tc.requested)
			assert.Equal(t, tc.wantMissing, ok)
			assert.Equal(t, tc.missing, missing)
		})
	}
}

func TestHedger_ProduceSync_ResponseCoverage(t *testing.T) {
	const (
		topic       = "ingest"
		primaryID   = int32(1)
		secondaryID = int32(2)
		acked       = int32(0)
		omitted     = int32(1)
	)

	for _, tc := range []struct {
		name            string
		primaryComplete bool
		primaryErr      error
		retryComplete   bool
		nilLogger       bool
	}{
		{name: "omitted partition fails after retries are exhausted"},
		{name: "warning is emitted before a successful retry", retryComplete: true},
		{name: "complete primary response needs no retry or warning", primaryComplete: true},
		{name: "transport failure is retried without a coverage warning", primaryErr: context.DeadlineExceeded, retryComplete: true},
		{name: "nil logger still retries incomplete responses", nilLogger: true, retryComplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				strategy := &mockPartitionAssignmentStrategy{
					candidates: map[partitionKey][]Agent{
						{topic, acked}:   healthyAgents(primaryID, secondaryID),
						{topic, omitted}: healthyAgents(primaryID, secondaryID),
					},
				}
				var logs lockedBuffer
				var logger kgo.Logger
				if !tc.nilLogger {
					logger = kgo.BasicLogger(&logs, kgo.LogLevelWarn, nil)
				}
				wantWarning := !tc.primaryComplete && tc.primaryErr == nil && !tc.nilLogger
				assertWarning := func() {
					assert.Contains(t, logs.String(), "[WARN] warpstream produce response omits requested partitions")
					assert.Contains(t, logs.String(), "node_id: 1")
					assert.Contains(t, logs.String(), "first_missing_topic: ingest")
					assert.Contains(t, logs.String(), "first_missing_partition: 1")
				}
				producer := newMockDirectProducer()
				producer.respFn = func(nodeID int32, _ []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
					complete := tc.primaryComplete
					if nodeID == primaryID && tc.primaryErr != nil {
						return nil, tc.primaryErr
					}
					if nodeID != primaryID {
						if wantWarning {
							assertWarning()
						}
						complete = tc.retryComplete
					}
					entries := []kmsg.ProduceResponseTopicPartition{{Partition: acked, BaseOffset: 42}}
					if complete {
						entries = append(entries, kmsg.ProduceResponseTopicPartition{Partition: omitted, BaseOffset: 43})
					}
					return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{Topic: topic, Partitions: entries}}}, nil
				}

				// No stats suppresses the hedge timer, so the retry observes the primary's warning.
				h := NewHedger(producer, NewAverageAgentStatsTracker(), strategy,
					HealthCheckConfig{SlowMultiplier: 2.0, MaxSlowFraction: 0.3, FaultyThreshold: 0.05, MaxFaultyFraction: 0.3},
					HedgerConfig{MinHedgeDelay: time.Millisecond, MaxHedgeAgents: 2},
					0, 1<<20, newMetrics(prometheus.NewPedanticRegistry()), logger)
				defer h.Close()

				routed := func(p int32) routedEncodedTopicPartitionRecords {
					return routedEncodedTopicPartitionRecords{
						encodedTopicPartitionRecords: makeTopicPartitionRecords(topic, p, "payload"),
						nodeID:                       primaryID,
						nodeState:                    AgentStateHealthy,
					}
				}
				res := h.ProduceSync(t.Context(), primaryID, []routedEncodedTopicPartitionRecords{routed(acked), routed(omitted)})
				require.NoError(t, recordErrFromResult(res, topic, acked))
				if tc.primaryComplete || tc.retryComplete {
					require.NoError(t, res.error())
					require.NoError(t, recordErrFromResult(res, topic, omitted))
				} else {
					require.ErrorIs(t, res.error(), errIncompleteProduceResponse)
					require.ErrorIs(t, recordErrFromResult(res, topic, omitted), kgo.ErrRecordTimeout)
					require.ErrorIs(t, recordErrFromResult(res, topic, omitted), kerr.RequestTimedOut)
				}
				if tc.primaryComplete {
					assert.Equal(t, []int32{primaryID}, producer.recordedCallNodeIDs())
				} else {
					require.Equal(t, []int32{primaryID, secondaryID}, producer.recordedCallNodeIDs())
					assert.Len(t, producer.recordedCalls()[1].partitions, 2)
				}
				if wantWarning {
					assertWarning()
				} else {
					assert.Empty(t, logs.String())
				}
			})
		})
	}
}
