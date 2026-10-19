package wgo

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHedger_StalledFallbackRacesNextCandidate(t *testing.T) {
	for name, delays := range map[string]map[int32]time.Duration{
		"third wins":                  {1: time.Hour, 2: time.Hour},
		"second remains eligible":     {1: time.Hour, 2: 1500 * time.Millisecond, 3: time.Hour},
		"wait after budget exhausted": {1: time.Hour, 2: 2500 * time.Millisecond, 3: time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				producer := newMockDirectProducer()
				producer.delays = delays
				producer.respFn = func(_ int32, parts []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
					response := kmsg.NewPtrProduceResponse()
					for _, p := range parts {
						response.Topics = append(response.Topics, kmsg.ProduceResponseTopic{Topic: p.topic, Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: p.partition}}})
					}
					return response, nil
				}
				strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{"t", 0}: healthyAgents(1, 2, 3)}}
				cfg := DefaultConfig()
				metrics := newMetrics(prometheus.NewPedanticRegistry())
				h := NewHedger(producer, NewAverageAgentStatsTracker(), strategy, cfg.HealthCheck, cfg.Hedger, 0, 1<<20, metrics, nil)
				defer h.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				parts := []encodedTopicPartitionRecords{newEncodedTopicPartitionRecords("t", 0, []*kgo.Record{{Topic: "t", Partition: 0}})}
				begin := time.Now()
				result := h.runHedgingAttempts(ctx, 1, parts, newHedgerCandidates(strategy, 3), hedgeTriggerLatency, time.Second)
				require.NoError(t, result.result.error())
				require.Equal(t, 3, result.attempts)
				want := time.Second
				if name != "third wins" {
					want = delays[2]
				}
				require.Equal(t, want, time.Since(begin))
			})
		})
	}
}

func TestHedger_FailedPrimaryStillRacesStalledFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.errs[1] = kerr.NetworkException
		producer.delays[2] = time.Hour
		producer.respFn = func(_ int32, _ []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{Topic: "t", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0}}}}}, nil
		}
		strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{"t", 0}: healthyAgents(1, 2, 3)}}
		tracker := NewAverageAgentStatsTracker()
		for _, id := range []int32{1, 2, 3} {
			seedFullWindow(tracker, id, time.Now().UnixNano(), 20, 1, 0)
		}
		cfg := DefaultConfig()
		metrics := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, tracker, strategy, cfg.HealthCheck, cfg.Hedger, 0, 1<<20, metrics, nil)
		defer h.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		part := newEncodedTopicPartitionRecords("t", 0, []*kgo.Record{{Topic: "t", Partition: 0}})
		begin := time.Now()
		result := h.ProduceSync(ctx, 1, []routedEncodedTopicPartitionRecords{{encodedTopicPartitionRecords: part, nodeID: 1, nodeState: AgentStateHealthy}})
		require.NoError(t, result.error())
		require.Equal(t, cfg.Hedger.MinHedgeDelay, time.Since(begin))
		require.Equal(t, float64(2), testutil.ToFloat64(metrics.produceRequestsHedgeTotal))
	})
}

func TestHedger_LateFailureDoesNotAbortResolvedPartitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.delays = map[int32]time.Duration{2: 1500 * time.Millisecond, 4: time.Hour, 5: 1500 * time.Millisecond}
		producer.errs[2] = kerr.TopicAuthorizationFailed
		producer.respFn = func(_ int32, parts []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			response := kmsg.NewPtrProduceResponse()
			for _, p := range parts {
				response.Topics = append(response.Topics, kmsg.ProduceResponseTopic{Topic: p.topic, Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: p.partition}}})
			}
			return response, nil
		}
		strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{"t", 0}: healthyAgents(1, 2, 3), {"t", 1}: healthyAgents(1, 4, 5)}}
		cfg := DefaultConfig()
		h := NewHedger(producer, NewAverageAgentStatsTracker(), strategy, cfg.HealthCheck, cfg.Hedger, 0, 1<<20, newMetrics(prometheus.NewPedanticRegistry()), nil)
		defer h.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var parts []encodedTopicPartitionRecords
		for _, partition := range []int32{0, 1} {
			parts = append(parts, newEncodedTopicPartitionRecords("t", partition, []*kgo.Record{{Topic: "t", Partition: partition}}))
		}
		begin := time.Now()
		result := h.runHedgingAttempts(ctx, 1, parts, newHedgerCandidates(strategy, 3), hedgeTriggerLatency, time.Second)
		require.NoError(t, result.result.error())
		require.Equal(t, 2500*time.Millisecond, time.Since(begin))
	})
}
