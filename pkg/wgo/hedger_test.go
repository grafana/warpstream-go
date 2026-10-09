package wgo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// histogramCountSum returns the observation count and sum of a histogram.
// Works for native histograms, where SampleCount/SampleSum are always set.
func histogramCountSum(t *testing.T, h prometheus.Histogram) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	require.NoError(t, h.Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// assertProduceFailures checks every failure-reason series. Reasons omitted from
// want are asserted at zero, so a nil want means the invocation emitted none.
func assertProduceFailures(t *testing.T, m *metrics, want map[produceFailureReason]float64) {
	t.Helper()
	reasons := []produceFailureReason{
		produceFailureReasonCandidatesExhausted,
		produceFailureReasonTerminalError,
		produceFailureReasonWriteTimeout,
		produceFailureReasonInternalError,
	}
	for _, reason := range reasons {
		assert.Equal(t, want[reason], testutil.ToFloat64(m.produceRequestsFailed[reason]), "failure reason %d", reason)
	}
}

// assertTriggerWins checks every hedge-trigger win series. Triggers omitted
// from want are asserted at zero. It also checks the two invariants of the
// family: the series sum to hedgeWinsTotal, and a trigger never wins more
// cascades than it started.
func assertTriggerWins(t *testing.T, m *metrics, want map[hedgeTrigger]float64) {
	t.Helper()
	var sum float64
	for trigger := hedgeTrigger(0); trigger < hedgeTriggerCount; trigger++ {
		got := testutil.ToFloat64(m.hedgeTriggerWins[trigger])
		assert.Equal(t, want[trigger], got, "trigger %d wins", trigger)
		assert.LessOrEqual(t, got, testutil.ToFloat64(m.hedgeTriggers[trigger]), "trigger %d wins exceed its triggers", trigger)
		sum += got
	}
	assert.Equal(t, testutil.ToFloat64(m.hedgeWinsTotal), sum, "trigger wins must sum to hedgeWinsTotal")
}

// assertAttemptPayload checks the attempted-payload counters of both roles
// against the producer-state counts the test expects each role to have sent.
func assertAttemptPayload(t *testing.T, m *metrics, primary, hedge produceRequestStats) {
	t.Helper()
	for role, want := range map[attemptRole]produceRequestStats{attemptPrimary: primary, attemptHedge: hedge} {
		assert.Equal(t, float64(want.records), testutil.ToFloat64(m.produceAttemptRecords[role]), "role %d records", role)
		assert.Equal(t, float64(want.compressedBytes), testutil.ToFloat64(m.produceAttemptBytes[role]), "role %d bytes", role)
	}
}

// hedgerResult captures one partition's terminal outcome as fired by the
// Hedger's per-partition done callback.
type hedgerResult struct {
	resp *kmsg.ProduceResponse
	err  error
}

// resultCapture lets a Hedger test install a done callback per
// (topic, partition) and inspect the captured outcomes after
// ProduceSync returns.
type resultCapture struct {
	mu      sync.Mutex
	results map[topicPartition]hedgerResult
}

func newResultCapture() *resultCapture {
	return &resultCapture{results: make(map[topicPartition]hedgerResult)}
}

func (c *resultCapture) doneFor(topic string, partition int32) func(ProduceResult) {
	return func(res ProduceResult) {
		c.mu.Lock()
		c.results[topicPartition{topic: topic, partition: partition}] = hedgerResult(res)
		c.mu.Unlock()
	}
}

// runHedger bridges old test-call sites — which build routed entries with
// per-partition `done` callbacks — onto the new Hedger.ProduceSync
// signature. It extracts bare partitions, calls the Hedger, then fires
// each routed entry's done with the per-partition outcome from the merged
// response (wrapping kerr.RequestTimedOut with kgo.ErrRecordTimeout to
// match perPartitionDone's behaviour in the real WarpstreamClient).
func runHedger(h *Hedger, ctx context.Context, partitions []promised[routedEncodedTopicPartitionRecords]) {
	if len(partitions) == 0 {
		return
	}
	primaryID := partitions[0].item.nodeID
	res := h.ProduceSync(ctx, primaryID, unpromise(partitions))
	for _, p := range partitions {
		if p.done == nil {
			continue
		}
		// Mirror perPartitionDone: prefer the per-partition entry from
		// the merged response; fall back to the top-level err only when
		// no response is available.
		if res.resp == nil {
			p.done(res)
			continue
		}
		e := partitionErrorsFromResp(res.resp)[topicPartition{topic: p.item.topic, partition: p.item.partition}]
		if kerr.IsRetriable(e) {
			e = fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, e)
		}
		p.done(ProduceResult{resp: res.resp, err: e})
	}
}

func (c *resultCapture) get(topic string, partition int32) hedgerResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.results[topicPartition{topic: topic, partition: partition}]
}

func routedHedgeRecord(topic string, partition, primary int32, value string, done func(ProduceResult)) promised[routedEncodedTopicPartitionRecords] {
	return promised[routedEncodedTopicPartitionRecords]{
		item: routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, []*kgo.Record{{Topic: topic, Partition: partition, Value: []byte(value)}}),
			nodeID:                       primary,
		},
		done: done,
	}
}

func runHedgerAsync(h *Hedger, parts ...promised[routedEncodedTopicPartitionRecords]) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		runHedger(h, context.Background(), parts)
	}()
	return done
}

func encodedRecordValues(encoded []byte) []string {
	var out []string
	for _, r := range decodeBatch(encoded) {
		out = append(out, string(r.Value))
	}
	return out
}

func bufferedRecordValues(b *AgentBuffer[routedEncodedTopicPartitionRecords]) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, p := range b.nextProduceItems {
		out = append(out, encodedRecordValues(p.item.encoded)...)
	}
	return out
}

func TestHedger_ProduceSync(t *testing.T) {
	const (
		primaryID   = int32(1)
		secondaryID = int32(2)
		topic       = "t"
		partition   = int32(0)
	)

	stratPrimaryAndSecondary := &mockPartitionAssignmentStrategy{
		candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primaryID, secondaryID)},
	}

	healthyTracker := func() *AverageAgentStatsTracker {
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		for _, id := range []int32{primaryID, secondaryID, 3, 4} {
			seedFullWindow(tr, id, nowNs, 20, 1, 0)
		}
		return tr
	}
	slowPrimaryTracker := func() *AverageAgentStatsTracker {
		tr := healthyTracker()
		ps := tr.stats[primaryID]
		ps.bucketsMu.Lock()
		for i := range ps.buckets {
			if ps.buckets[i].successfulLatencyCount > 0 {
				ps.buckets[i].successfulLatencySumMs = ps.buckets[i].successfulLatencyCount * 100
			}
		}
		ps.bucketsMu.Unlock()
		return tr
	}

	health := HealthCheckConfig{
		SlowMultiplier:    2.0,
		MaxSlowFraction:   0.3,
		FaultyThreshold:   0.05,
		MaxFaultyFraction: 0.3,
	}
	cfg := HedgerConfig{
		MinHedgeDelay:  10 * time.Millisecond,
		MaxHedgeAgents: 3,
	}

	// makePromisedEncoded builds a routed encoded partition paired with its done callback.
	makePromisedEncoded := func(topic string, partition int32, nodeID int32, nodeState AgentState, done func(ProduceResult)) promised[routedEncodedTopicPartitionRecords] {
		return promised[routedEncodedTopicPartitionRecords]{
			item: routedEncodedTopicPartitionRecords{
				encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, []*kgo.Record{{Topic: topic, Partition: partition}}),
				nodeID:                       nodeID,
				nodeState:                    nodeState,
			},
			done: done,
		}
	}

	// makeReq builds a single-partition request whose done feeds capture.
	makeReq := func(capture *resultCapture) []promised[routedEncodedTopicPartitionRecords] {
		return []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topic, partition, primaryID, AgentStateHealthy, capture.doneFor(topic, partition)),
		}
	}
	successResp := func(_ int32, _ []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
		return &kmsg.ProduceResponse{
			Topics: []kmsg.ProduceResponseTopic{{
				Topic: topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{
					{Partition: partition, ErrorCode: 0, BaseOffset: 42},
				},
			}},
		}, nil
	}

	// reqStats is the payload makeReq sends; makeReq encodes deterministically.
	reqStats := sumEncodedStats(unpromise(makeReq(newResultCapture())))

	t.Run("no hedge decision: only primary is called", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		assert.Len(t, producer.recordedCalls(), 1)
		assert.Equal(t, primaryID, producer.recordedCalls()[0].nodeID)
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assertProduceFailures(t, m, nil)
		// Primary won → one success observation of attempt depth 1.
		count, sum := histogramCountSum(t, m.produceRequestsAttemptsSuccess.(prometheus.Histogram))
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(1), sum)
		assertAttemptPayload(t, m, reqStats, produceRequestStats{})
	})

	t.Run("empty input is trivially successful", func(t *testing.T) {
		producer := newMockDirectProducer()
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		// Nothing to produce must classify as success, not the empty/error
		// sentinel (guards the non-nil empty resp returned for this case).
		res := h.ProduceSync(context.Background(), primaryID, nil)
		assert.True(t, res.succeeded())
		assert.NoError(t, res.error())
		assert.Empty(t, producer.recordedCalls())
		assertProduceFailures(t, m, nil)
		successCount, _ := histogramCountSum(t, m.produceRequestsAttemptsSuccess.(prometheus.Histogram))
		failureCount, _ := histogramCountSum(t, m.produceRequestsAttemptsFailure.(prometheus.Histogram))
		assert.Equal(t, uint64(0), successCount)
		assert.Equal(t, uint64(0), failureCount)
		// Nothing was dispatched, so nothing was attempted.
		assertAttemptPayload(t, m, produceRequestStats{}, produceRequestStats{})
	})

	t.Run("hedging suppressed when primary has no agent stats yet", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		m := newMetrics(prometheus.NewPedanticRegistry())
		// Empty tracker: AgentStats returns !ok for the primary, so
		// shouldHedge bails at the no-agent-stats gate.
		h := NewHedger(producer, NewAverageAgentStatsTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		assert.Len(t, producer.recordedCalls(), 1)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoAgentStats)))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
	})

	t.Run("primary slow: hedge fires and secondary wins", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		syncDone := make(chan struct{})
		go func() {
			defer close(syncDone)
			runHedger(h, context.Background(), makeReq(capture))
		}()

		select {
		case <-syncDone:
			require.NoError(t, capture.get(topic, partition).err)
		case <-time.After(time.Second):
			t.Fatal("ProduceSync did not return; secondary failed to win the race")
		}
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
		// The fallback won the race; the canceled primary leg emits nothing.
		assertProduceFailures(t, m, nil)
		assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerLatency: 1})
		// The blocked primary was dispatched and lost, and still counts.
		assertAttemptPayload(t, m, reqStats, reqStats)
	})

	t.Run("primary fails with retriable error: cascade to secondary", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		callIDs := producer.recordedCallNodeIDs()
		assert.Contains(t, callIDs, primaryID)
		assert.Contains(t, callIDs, secondaryID)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
		// Primary failed, one hedge wave resolved it → success attempt depth 2.
		count, sum := histogramCountSum(t, m.produceRequestsAttemptsSuccess.(prometheus.Histogram))
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(2), sum)
		// A failed primary that the cascade recovered is not a final failure.
		assertProduceFailures(t, m, nil)
		assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerPrimaryFailure: 1})
		// The failed primary and the retry both count.
		assertAttemptPayload(t, m, reqStats, reqStats)
	})

	t.Run("hedge timer fires and secondary wins: hedgeWinsTotal incremented", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		fastCfg := cfg
		fastCfg.MinHedgeDelay = time.Millisecond
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, fastCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)

		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(m.hedgeAttemptsTotal) == 1 && testutil.ToFloat64(m.hedgeWinsTotal) == 1
		}, time.Second, 10*time.Millisecond)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
		assertProduceFailures(t, m, nil)
		assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerLatency: 1})
		assertAttemptPayload(t, m, reqStats, reqStats)
	})

	t.Run("hedge timer fires but primary wins the race: hedgeAttemptsTotal++ but no hedgeWinsTotal", func(t *testing.T) {
		// Hedge timer fires before primary returns (so the racing variant
		// is entered and runHedgingAttempts is invoked once), but primary
		// then completes ahead of the fallback. Racing variant cancels
		// workCtx; the defer must NOT count a win.
		producer := newMockDirectProducer()
		producer.respFn = successResp

		// Primary unblocks after a delay long enough for the hedge timer
		// (≥ BaselineLatency) to fire — then completes ahead of any fallback.
		primaryGate := make(chan struct{})
		producer.blockCh[primaryID] = primaryGate
		go func() {
			time.Sleep(150 * time.Millisecond)
			close(primaryGate)
		}()

		// Secondary blocks forever so the fallback can never finish before
		// the primary's success cancels it.
		secondaryGate := make(chan struct{})
		producer.blockCh[secondaryID] = secondaryGate
		t.Cleanup(func() { close(secondaryGate) })

		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)

		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assertProduceFailures(t, m, nil)
		// The cascade started but lost: a trigger without a win.
		assertTriggerWins(t, m, nil)
		// The losing hedge leg was dispatched before the primary won.
		assertAttemptPayload(t, m, reqStats, reqStats)
	})

	t.Run("hedge decision but primary wins before timer: no hedgeAttemptsTotal increment", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assertAttemptPayload(t, m, reqStats, produceRequestStats{})
	})

	t.Run("per-partition fanout: each partition resolves independently", func(t *testing.T) {
		// Two partitions, both routed to the same primary. Primary errors
		// out. Each partition has a distinct per-partition secondary —
		// the per-partition retry loop sends each to its own next agent
		// and both succeed.
		const topicMulti = "tm"
		const secondaryB = int32(3)
		multiStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topicMulti, 0}: healthyAgents(primaryID, secondaryID),
				{topicMulti, 1}: healthyAgents(primaryID, secondaryB),
			},
		}
		producer := newMockDirectProducer()
		producer.respFn = func(nodeID int32, partitions []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			parts := make([]kmsg.ProduceResponseTopicPartition, 0, len(partitions))
			for _, p := range partitions {
				parts = append(parts, kmsg.ProduceResponseTopicPartition{Partition: p.partition, ErrorCode: 0, BaseOffset: 200 + int64(p.partition)})
			}
			return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
				Topic:      topicMulti,
				Partitions: parts,
			}}}, nil
		}
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), multiStrat, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		req := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topicMulti, 0, primaryID, AgentStateHealthy, capture.doneFor(topicMulti, 0)),
			makePromisedEncoded(topicMulti, 1, primaryID, AgentStateHealthy, capture.doneFor(topicMulti, 1)),
		}
		runHedger(h, context.Background(), req)

		assert.NoError(t, capture.get(topicMulti, 0).err)
		assert.NoError(t, capture.get(topicMulti, 1).err)
		// Each partition routed to its distinct secondary.
		callIDs := producer.recordedCallNodeIDs()
		assert.Contains(t, callIDs, secondaryID)
		assert.Contains(t, callIDs, secondaryB)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
		assertProduceFailures(t, m, nil)
		assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerPrimaryFailure: 1})
		// Two partitions go out together on the primary, then to two different
		// fallback agents: the hedge total is the same payload split across flushes.
		both := sumEncodedStats(unpromise(req))
		assertAttemptPayload(t, m, both, both)
	})

	t.Run("single-agent cluster (no secondaries): primary error propagates", func(t *testing.T) {
		emptyStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primaryID)},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), emptyStrat, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.RequestTimedOut)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		// Primary fires once; hedge never fires because there is no
		// fallback candidate.
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
		// No hedge candidate → only the primary attempt was made before
		// giving up, recorded under the failure outcome.
		count, sum := histogramCountSum(t, m.produceRequestsAttemptsFailure.(prometheus.Histogram))
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(1), sum)
		// The cascade dispatched nothing, so it adds no hedge payload.
		assertAttemptPayload(t, m, reqStats, produceRequestStats{})
	})

	t.Run("hedging suppressed and primary fails: cascade is classified as primary failure", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, NewAverageAgentStatsTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]))
		assertProduceFailures(t, m, nil)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoAgentStats)))
	})

	t.Run("hedging suppressed and primary fails with no secondary: candidates exhausted", func(t *testing.T) {
		emptyStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primaryID)},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, NewAverageAgentStatsTracker(), emptyStrat, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.Error(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoAgentStats)))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
	})

	t.Run("partial fallback coverage: any partition exhausting candidates stops the whole attempt", func(t *testing.T) {
		// Two partitions; partition 0 has a fallback (secondaryID),
		// partition 1 has none. Primary fails. runHedgingAttempt bails
		// out the first wave because partition 1 has nothing to try —
		// partition 0's fallback never runs, since the batch can never
		// fully succeed anyway.
		const topicMulti = "tm"
		partialStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topicMulti, 0}: healthyAgents(primaryID, secondaryID),
				{topicMulti, 1}: healthyAgents(primaryID), // no fallback configured
			},
		}
		producer := newMockDirectProducer()
		producer.respFn = func(nodeID int32, partitions []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			parts := make([]kmsg.ProduceResponseTopicPartition, 0, len(partitions))
			for _, p := range partitions {
				parts = append(parts, kmsg.ProduceResponseTopicPartition{Partition: p.partition, ErrorCode: 0, BaseOffset: 100})
			}
			return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
				Topic:      topicMulti,
				Partitions: parts,
			}}}, nil
		}
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), partialStrat, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		req := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topicMulti, 0, primaryID, AgentStateHealthy, capture.doneFor(topicMulti, 0)),
			makePromisedEncoded(topicMulti, 1, primaryID, AgentStateHealthy, capture.doneFor(topicMulti, 1)),
		}
		runHedger(h, context.Background(), req)

		require.Error(t, capture.get(topicMulti, 0).err)
		assert.ErrorIs(t, capture.get(topicMulti, 0).err, kerr.RequestTimedOut)
		require.Error(t, capture.get(topicMulti, 1).err)
		assert.ErrorIs(t, capture.get(topicMulti, 1).err, kerr.RequestTimedOut)
		// Only the primary leg ran on the inner producer; the fallback
		// wave bailed out the moment partition 1 had no candidate left.
		callIDs := producer.recordedCallNodeIDs()
		assert.NotContains(t, callIDs, secondaryID)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
	})

	t.Run("primary marked as probe (demoted): hedge fires immediately, ignoring hedge delay", func(t *testing.T) {
		// healthyTracker means the stats-based shouldHedge would return
		// false. A long MinHedgeDelay further proves the point: if the
		// hedge were gated on the timer, the test would deadlock on the
		// blocked primary. The Demoter-driven nodeState=Demoted must
		// short-circuit both gates.
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		probeCfg := cfg
		probeCfg.MinHedgeDelay = time.Hour

		probeStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
				{NodeID: secondaryID, State: AgentStateHealthy},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), probeStrat, health, probeCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		// primary is a probe (nodeState demoted).
		probeReq := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topic, partition, primaryID, AgentStateDemoted, capture.doneFor(topic, partition)),
		}

		start := time.Now()
		runHedger(h, context.Background(), probeReq)
		elapsed := time.Since(start)

		require.NoError(t, capture.get(topic, partition).err)
		// Probe primary must fast-hedge: the fallback wins immediately
		// instead of waiting for MinHedgeDelay (1h here).
		assert.Less(t, elapsed, 250*time.Millisecond)

		var sawFallback bool
		for _, c := range producer.recordedCalls() {
			if c.nodeID == secondaryID {
				sawFallback = true
			}
		}
		assert.True(t, sawFallback)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]))
		assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerDemotedProbe: 1})
	})

	t.Run("demoted primary is the only candidate: hedge fires but finds no fallback; primary's success wins", func(t *testing.T) {
		// shouldHedge triggers from nodeState=Demoted alone, but
		// runHedgingAttempt exhausts immediately (primary is the only
		// candidate). The primary still runs and decides the outcome; the
		// producer is only ever called once.
		producer := newMockDirectProducer()
		producer.respFn = successResp

		probeStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), probeStrat, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		req := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topic, partition, primaryID, AgentStateDemoted, capture.doneFor(topic, partition)),
		}

		runHedger(h, context.Background(), req)

		require.NoError(t, capture.get(topic, partition).err)
		// hedgeAttemptsTotal is incremented on the fallback goroutine, which
		// may not have run yet when the primary already won the race.
		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(m.hedgeAttemptsTotal) == 1 &&
				testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]) == 1
		}, time.Second, 10*time.Millisecond)
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assertProduceFailures(t, m, nil)
		// Primary fires once; the hedge cascade has no candidates to try
		// so no hedge wire request is issued.
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		callIDs := producer.recordedCallNodeIDs()
		assert.Equal(t, []int32{primaryID}, callIDs)
	})

	t.Run("racing variant safety net: fallback returns empty before primary; primary's success is honoured", func(t *testing.T) {
		// Forces shouldHedge true (fallback exists at shouldHedge time)
		// but makes runHedgingAttempt exhaust instantly (fallback nodeID
		// equals primary, already in tried). The racing variant must
		// wait for primary instead of preempting on the empty fallback.
		producer := newMockDirectProducer()
		producer.respFn = successResp
		producer.delays[primaryID] = 50 * time.Millisecond

		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
				{NodeID: primaryID, State: AgentStateHealthy},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		req := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topic, partition, primaryID, AgentStateDemoted, capture.doneFor(topic, partition)),
		}

		runHedger(h, context.Background(), req)

		require.NoError(t, capture.get(topic, partition).err,
			"primary's slow but successful probe must be honoured even when the racing-variant fallback returned an empty result")
		// The fallback's exhausted stop is dropped because the primary then succeeded.
		assertProduceFailures(t, m, nil)
		// The probe cascade found nothing to win with; the primary did.
		assertTriggerWins(t, m, nil)
	})

	t.Run("both legs fail with retriable errors: surfaces the last error", func(t *testing.T) {
		// Primary and secondary both error out with retriable errors
		// (request timed out). After both have been tried and there are
		// no more candidates, the partition's done fires with the last
		// retriable error seen.
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		producer.errs[secondaryID] = kerr.LeaderNotAvailable

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, kerr.RequestTimedOut) || errors.Is(err, kerr.LeaderNotAvailable))
		assert.Len(t, producer.recordedCalls(), 2)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
		assertTriggerWins(t, m, nil)
	})

	t.Run("non-retriable fallback err aborts: no further waves even with candidates left", func(t *testing.T) {
		// Primary fails retriably; the first fallback wave hits a
		// non-retriable err (kerr.MessageTooLarge), which aborts the
		// accumulator. Even though MaxHedgeAgents=4 still leaves
		// candidates untried, runHedgingAttempt must stop iterating —
		// no extra agents are contacted.
		const (
			agentA = int32(1) // primary
			agentB = int32(2) // first fallback (the aborting one)
			agentC = int32(3)
			agentD = int32(4)
		)
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(agentA, agentB, agentC, agentD)},
		}
		producer := newMockDirectProducer()
		producer.errs[agentA] = kerr.RequestTimedOut // retriable
		producer.errs[agentB] = kerr.MessageTooLarge // non-retriable

		failingCfg := cfg
		failingCfg.MaxHedgeAgents = 4

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, failingCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.MessageTooLarge)
		// Only primary (A) and the first fallback (B) should have been
		// contacted — C and D are still in the candidate list but the
		// accumulator aborted, so iteration must stop.
		seen := map[int32]int{}
		for _, c := range producer.recordedCalls() {
			seen[c.nodeID]++
		}
		assert.Equal(t, 1, seen[agentA])
		assert.Equal(t, 1, seen[agentB])
		assert.Zero(t, seen[agentC])
		assert.Zero(t, seen[agentD])
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonTerminalError: 1,
		})
	})

	t.Run("primary and secondary legs share the tried set: no agent is attempted twice", func(t *testing.T) {
		// Both legs running concurrently must coordinate so no single
		// agent receives two attempts for the same partition. We force
		// the race by blocking every agent's response on a shared gate:
		// each agent's attempt parks until the test releases the gate,
		// guaranteeing both legs are simultaneously inside the claim
		// loop. Then we release the gate, let everything fail with a
		// retriable error, and verify the recorded nodeIDs are unique.
		const (
			agentA = int32(1)
			agentB = int32(2)
			agentC = int32(3)
			agentD = int32(4)
		)
		strategy := &mockPartitionAssignmentStrategy{
			// Pad Candidates beyond just primary+secondary so the second
			// worker can keep claiming.
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(agentA, agentB, agentC, agentD)},
		}

		producer := newMockDirectProducer()
		gate := make(chan struct{})
		for _, id := range []int32{agentA, agentB, agentC, agentD} {
			producer.blockCh[id] = gate
		}
		producer.respFn = func(int32, []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			// After gate releases, every leg fails with the same
			// retriable error so the loop keeps cycling.
			return nil, kerr.RequestTimedOut
		}

		fastCfg := cfg
		fastCfg.MinHedgeDelay = time.Millisecond
		fastCfg.MaxHedgeAgents = 4

		tr := healthyTracker()
		// Mark primary slow so shouldHedge fires.
		ps := tr.stats[agentA]
		ps.bucketsMu.Lock()
		for i := range ps.buckets {
			if ps.buckets[i].successfulLatencyCount > 0 {
				ps.buckets[i].successfulLatencySumMs = ps.buckets[i].successfulLatencyCount * 100
			}
		}
		ps.bucketsMu.Unlock()

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, tr, strategy, health, fastCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		req := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topic, partition, agentA, AgentStateHealthy, capture.doneFor(topic, partition)),
		}

		syncDone := make(chan struct{})
		go func() {
			defer close(syncDone)
			runHedger(h, context.Background(), req)
		}()

		// Release the gate; both legs unwind in parallel.
		close(gate)
		select {
		case <-syncDone:
		case <-time.After(time.Second):
			t.Fatal("ProduceSync did not return")
		}

		seen := map[int32]int{}
		for _, c := range producer.recordedCalls() {
			seen[c.nodeID]++
		}
		// The two legs must share the tried set so no agent is contacted
		// twice for the same partition.
		for agent, count := range seen {
			assert.Equal(t, 1, count, "agent %d was attempted %d times", agent, count)
		}
		// All four agents should appear (cap was 4); the order is
		// non-deterministic across legs but the set is fixed.
		require.Len(t, seen, 4)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("racing path: primary fails before fallback exhausts → underlying kerr preserved", func(t *testing.T) {
		// slowPrimaryTracker + a hedge delay forces ProduceSync into
		// the racing wrapper. Primary takes long enough for the timer
		// to fire and short enough to return before the slow fallback
		// agent. The fallback agent also fails. Before the fix the
		// wrapper returned (synthetic_resp, nil); the caller's
		// perPartitionDone fell back to extracting REQUEST_TIMED_OUT
		// from the resp, masking the actual leg kerrs. After the fix
		// the wrapper propagates a wrapped error chain that retains
		// primary's and/or fallback's specific kerrs.
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 20 * time.Millisecond
		producer.errs[primaryID] = kerr.LeaderNotAvailable
		// Make the fallback slow enough that primary returns first
		// inside the wrapper's select.
		producer.delays[secondaryID] = 200 * time.Millisecond
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		failingCfg := cfg
		failingCfg.MinHedgeDelay = time.Millisecond
		failingCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, failingCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kgo.ErrRecordTimeout)
		assert.True(t,
			errors.Is(err, kerr.LeaderNotAvailable) || errors.Is(err, kerr.NotLeaderForPartition),
			"got %v", err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		// Both legs failed: exactly one failure reason, taken from the fallback's stop.
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
		assertTriggerWins(t, m, nil)
	})

	t.Run("racing path: fallback exhausts before primary returns → fallback kerr preserved", func(t *testing.T) {
		// Fallback exhausts fast; racing variant waits for the
		// (slower) primary and merges both failures into the chain.
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 50 * time.Millisecond
		producer.errs[primaryID] = kerr.RequestTimedOut
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		failingCfg := cfg
		failingCfg.MinHedgeDelay = time.Millisecond
		failingCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, failingCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, err, kerr.NotLeaderForPartition, "got %v", err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
		assertTriggerWins(t, m, nil)
	})

	t.Run("primary fails non-retriably and the candidate budget is one: terminal error", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.MessageTooLarge
		one := cfg
		one.MaxHedgeAgents = 1
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, one, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.Error(t, capture.get(topic, partition).err)
		assert.Equal(t, []int32{primaryID}, producer.recordedCallNodeIDs())
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonTerminalError: 1,
		})
	})

	t.Run("primary fails non-retriably and it is the only candidate: terminal error", func(t *testing.T) {
		onlyPrimary := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primaryID)},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.MessageTooLarge
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), onlyPrimary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.Error(t, capture.get(topic, partition).err)
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonTerminalError: 1,
		})
	})

	t.Run("racing path: fallback exhausts then the primary fails non-retriably: terminal error", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 50 * time.Millisecond
		producer.errs[primaryID] = kerr.MessageTooLarge
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		raceCfg := cfg
		raceCfg.MinHedgeDelay = time.Millisecond
		raceCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, raceCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.Error(t, capture.get(topic, partition).err)
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonTerminalError: 1,
		})
		assertTriggerWins(t, m, nil)
	})

	t.Run("racing path: fallback exhausts then the primary hits the work deadline is a write timeout", func(t *testing.T) {
		// The fallback tries the one secondary and exhausts quickly, leaving
		// the primary to run out the clock. The deadline is why this failed.
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 10 * time.Second
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		raceCfg := cfg
		raceCfg.MinHedgeDelay = time.Millisecond
		raceCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, raceCfg, 0, 1<<20, m, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		capture := newResultCapture()
		runHedger(h, ctx, makeReq(capture))

		require.Error(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonWriteTimeout: 1,
		})
		assertTriggerWins(t, m, nil)
	})

	t.Run("racing path: fallback exhausts then the caller cancels while the primary runs: no failure counted", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 10 * time.Second
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		raceCfg := cfg
		raceCfg.MinHedgeDelay = time.Millisecond
		raceCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, raceCfg, 0, 1<<20, m, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// The secondary fails within milliseconds, so the cascade has
		// exhausted its candidates well before the cancel.
		time.AfterFunc(150*time.Millisecond, cancel)
		capture := newResultCapture()
		runHedger(h, ctx, makeReq(capture))

		// The caller still sees the fallback's last error from the merged response.
		require.Error(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assertProduceFailures(t, m, nil)
		assertTriggerWins(t, m, nil)
	})

	t.Run("ctx canceled mid-fallback: result surfaces ctx err, not synthesized timeout", func(t *testing.T) {
		// Primary fails immediately so we cascade into runHedgingAttempts.
		// The fallback agent blocks forever; cancelling the caller's ctx
		// must unblock the loop, and runHedgingAttempts must surface the
		// ctx err directly (not acc.result()'s ErrRecordTimeout envelope).
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		gate := make(chan struct{})
		producer.blockCh[secondaryID] = gate
		t.Cleanup(func() { close(gate) })

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := h.ProduceSync(ctx, primaryID, []routedEncodedTopicPartitionRecords{{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, []*kgo.Record{{Topic: topic, Partition: partition}}),
			nodeID:                       primaryID,
		}}).error()
		require.Error(t, err)
		// The ctx err is chained inside the primary+fallback envelope by
		// selectProduceResult.
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assertProduceFailures(t, m, nil)
	})

	t.Run("ctx deadline mid-fallback: failure reason is write timeout", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		gate := make(chan struct{})
		producer.blockCh[secondaryID] = gate
		t.Cleanup(func() { close(gate) })

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := h.ProduceSync(ctx, primaryID, []routedEncodedTopicPartitionRecords{{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, []*kgo.Record{{Topic: topic, Partition: partition}}),
			nodeID:                       primaryID,
		}}).error()
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonWriteTimeout: 1,
		})
	})

	t.Run("fallback wins: primary leg is canceled via workCtx instead of running until its per-attempt deadline", func(t *testing.T) {
		producer := newMockDirectProducer()
		// Primary blocks until workCtx is cancelled (or the gate closes,
		// which the test never does — the cancellation path is what we
		// want to exercise).
		producer.blockCh[primaryID] = make(chan struct{})
		producer.respFn = successResp

		// Demoted primary forces shouldHedge=true with delay=0 so the
		// fallback fires immediately and wins the race.
		probeStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
				{NodeID: secondaryID, State: AgentStateHealthy},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), probeStrat, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		req := []promised[routedEncodedTopicPartitionRecords]{
			makePromisedEncoded(topic, partition, primaryID, AgentStateDemoted, capture.doneFor(topic, partition)),
		}
		runHedger(h, context.Background(), req)
		require.NoError(t, capture.get(topic, partition).err)

		// Primary's blocked attempt must observe workCtx cancellation and
		// record a ctx-err call, not keep blocking.
		require.Eventually(t, func() bool {
			for _, c := range producer.recordedCalls() {
				if c.nodeID == primaryID && errors.Is(c.err, context.Canceled) {
					return true
				}
			}
			return false
		}, time.Second, 10*time.Millisecond, "primary leg was not canceled after fallback won")
		// The canceled primary leg is a loser, not a failed produce.
		assertProduceFailures(t, m, nil)
		assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerDemotedProbe: 1})
	})

	t.Run("returns error when a routed partition's nodeID disagrees with primaryID", func(t *testing.T) {
		producer := newMockDirectProducer()
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		req := []routedEncodedTopicPartitionRecords{
			{
				encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: topic, partition: 0},
				nodeID:                       primaryID,
			},
			{
				encodedTopicPartitionRecords: encodedTopicPartitionRecords{topic: topic, partition: 1},
				nodeID:                       secondaryID,
			},
		}
		err := h.ProduceSync(context.Background(), primaryID, req).error()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "primaryID=1")
		assert.Empty(t, producer.recordedCalls())
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonInternalError: 1,
		})
		successCount, _ := histogramCountSum(t, m.produceRequestsAttemptsSuccess.(prometheus.Histogram))
		failureCount, _ := histogramCountSum(t, m.produceRequestsAttemptsFailure.(prometheus.Histogram))
		assert.Equal(t, uint64(0), successCount)
		assert.Equal(t, uint64(0), failureCount)
		// Nothing was dispatched, so nothing was attempted.
		assertAttemptPayload(t, m, produceRequestStats{}, produceRequestStats{})
	})

	t.Run("healthy primary with zero computed delay is a latency trigger", func(t *testing.T) {
		// MinHedgeDelay and the baseline are both zero, so the fast path
		// races immediately. That delay is not a demoted probe.
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		for _, id := range []int32{primaryID, secondaryID} {
			seedFullWindow(tr, id, nowNs, 20, 0, 0)
		}
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		zeroCfg := cfg
		zeroCfg.MinHedgeDelay = 0
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, tr, stratPrimaryAndSecondary, health, zeroCfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerDemotedProbe]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
		assertProduceFailures(t, m, nil)
	})

	t.Run("max hedge agents of one exhausts before any fallback dispatch", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		one := cfg
		one.MaxHedgeAgents = 1
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, one, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.Error(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assert.Equal(t, []int32{primaryID}, producer.recordedCallNodeIDs())
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonCandidatesExhausted: 1,
		})
		assertAttemptPayload(t, m, reqStats, produceRequestStats{})
	})

	t.Run("canceled before fallback dispatch counts the cascade and emits no failure", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.blockCh[primaryID] = make(chan struct{})
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := h.ProduceSync(ctx, primaryID, []routedEncodedTopicPartitionRecords{{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, []*kgo.Record{{Topic: topic, Partition: partition}}),
			nodeID:                       primaryID,
		}}).error()
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerPrimaryFailure]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeTriggers[hedgeTriggerLatency]))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assertProduceFailures(t, m, nil)
	})

	t.Run("unknown fallback error is a terminal failure reason", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		producer.errs[secondaryID] = errors.New("boom")
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.Error(t, capture.get(topic, partition).err)
		assert.Equal(t, []int32{primaryID, secondaryID}, producer.recordedCallNodeIDs())
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonTerminalError: 1,
		})
	})

	t.Run("duplicate partition is an internal error and dispatches no fallback", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m, nil)

		part := routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(topic, partition, []*kgo.Record{{Topic: topic, Partition: partition}}),
			nodeID:                       primaryID,
		}
		err := h.ProduceSync(context.Background(), primaryID, []routedEncodedTopicPartitionRecords{part, part}).error()
		require.Error(t, err)
		assert.ErrorContains(t, err, "duplicate")
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		assert.Equal(t, []int32{primaryID}, producer.recordedCallNodeIDs())
		assertProduceFailures(t, m, map[produceFailureReason]float64{
			produceFailureReasonInternalError: 1,
		})
	})

	t.Run("shared hedge flush does not resolve a partition the leg did not send", func(t *testing.T) {
		// Every caller's primary fails. caller-1 retries partition-0 on
		// fallback-agent-B; caller-2 sends partition-0 to fallback-agent-A
		// and partition-1 to fallback-agent-B. The two fallback-agent-B
		// items share one flush — caller-2 must not mistake that flush's
		// partition-0 entry (caller-1's) for its own.
		const (
			primaryAgent   = int32(100)
			fallbackAgentA = int32(101)
			fallbackAgentB = int32(102)
			partition0     = int32(0)
			partition1     = int32(1)
		)
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, partition0}: healthyAgents(primaryAgent, fallbackAgentA, fallbackAgentB),
				{topic, partition1}: healthyAgents(primaryAgent, fallbackAgentB),
			},
		}
		carries := func(partitions []encodedTopicPartitionRecords, value string) bool {
			for _, p := range partitions {
				if slices.Contains(encodedRecordValues(p.encoded), value) {
					return true
				}
			}
			return false
		}

		producer := newMockDirectProducer()
		producer.errs[primaryAgent] = kerr.RequestTimedOut
		producer.errs[fallbackAgentA] = kerr.KafkaStorageError
		producer.respFn = func(_ int32, partitions []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			if carries(partitions, "caller-2-partition-0") {
				return nil, kerr.KafkaStorageError
			}
			resp := &kmsg.ProduceResponse{}
			for _, p := range partitions {
				resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
					Topic:      p.topic,
					Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: p.partition}},
				})
			}
			return resp, nil
		}

		// A primary failure returns long before this delay, so each caller
		// cascades into runHedgingAttempts without racing the timer.
		noRaceCfg := cfg
		noRaceCfg.MinHedgeDelay = time.Hour

		// The linger never fires: the test flushes each agent buffer explicitly.
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, noRaceCfg, time.Hour, 1<<20, m, nil)
		t.Cleanup(h.Close)

		agentBuffers := map[int32]*AgentBuffer[routedEncodedTopicPartitionRecords]{}
		for _, agent := range []int32{fallbackAgentA, fallbackAgentB} {
			b, err := h.hedgeBuffer.agentBufferFor(agent)
			require.NoError(t, err)
			agentBuffers[agent] = b
		}
		flush := func(agent int32) {
			agentBuffers[agent].timerFlush()
		}
		waitBuffered := func(agent int32, want ...string) {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.ElementsMatch(c, want, bufferedRecordValues(agentBuffers[agent]))
			}, time.Second, time.Millisecond)
		}

		// caller-1: wave 1 sends partition-0 to fallback-agent-A, which fails,
		// so wave 2 buffers partition-0 on fallback-agent-B.
		caller1 := newResultCapture()
		caller1Done := runHedgerAsync(h,
			routedHedgeRecord(topic, partition0, primaryAgent, "caller-1-partition-0", caller1.doneFor(topic, partition0)),
		)
		waitBuffered(fallbackAgentA, "caller-1-partition-0")
		flush(fallbackAgentA)
		waitBuffered(fallbackAgentB, "caller-1-partition-0")

		// caller-2: wave 1 buffers partition-0 on fallback-agent-A and
		// partition-1 on fallback-agent-B, next to caller-1's partition-0.
		caller2 := newResultCapture()
		caller2Done := runHedgerAsync(h,
			routedHedgeRecord(topic, partition0, primaryAgent, "caller-2-partition-0", caller2.doneFor(topic, partition0)),
			routedHedgeRecord(topic, partition1, primaryAgent, "caller-2-partition-1", caller2.doneFor(topic, partition1)),
		)
		waitBuffered(fallbackAgentA, "caller-2-partition-0")
		waitBuffered(fallbackAgentB, "caller-1-partition-0", "caller-2-partition-1")

		// fallback-agent-B succeeds; caller-2's fallback-agent-A attempt
		// fails, so it retries on fallback-agent-B — pump that buffer
		// until both callers finish, like a real linger flush would.
		flush(fallbackAgentB)
		flush(fallbackAgentA)
		// Each waiter gets its own deadline: a shared one would let a slow
		// but still-progressing second caller be falsely failed by however
		// much of the budget the first caller's wait consumed.
		pumpUntil := func(done <-chan struct{}) {
			deadline := time.After(5 * time.Second)
			for {
				select {
				case <-done:
					return
				case <-deadline:
					t.Fatal("timed out waiting for caller to finish")
				case <-time.After(time.Millisecond):
					if len(bufferedRecordValues(agentBuffers[fallbackAgentB])) > 0 {
						flush(fallbackAgentB)
					}
				}
			}
		}
		pumpUntil(caller1Done)
		pumpUntil(caller2Done)

		require.NoError(t, caller1.get(topic, partition0).err)
		require.NoError(t, caller2.get(topic, partition1).err)
		// caller-2's own partition-0 fails on every agent, so this is
		// still an error — a true one now, not a false success borrowed
		// from caller-1's entry in the shared flush.
		err := caller2.get(topic, partition0).err
		require.ErrorIs(t, err, kgo.ErrRecordTimeout)
		require.ErrorIs(t, err, kerr.KafkaStorageError)
	})

	t.Run("shared hedge flush still credits a succeeding partition when a sibling's fails", func(t *testing.T) {
		// The mirror of the case above: two callers share one flush whose
		// response is a genuine mix — one partition ok, the other coded
		// as failed. The failing partition must not drag the succeeding
		// one down with it.
		const (
			primaryAgent  = int32(200)
			fallbackAgent = int32(201)
			partitionOK   = int32(0)
			partitionBad  = int32(1)
		)
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, partitionOK}:  healthyAgents(primaryAgent, fallbackAgent),
				{topic, partitionBad}: healthyAgents(primaryAgent, fallbackAgent),
			},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryAgent] = kerr.RequestTimedOut
		producer.respFn = func(_ int32, partitions []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			resp := &kmsg.ProduceResponse{}
			for _, p := range partitions {
				code := kerrNoError
				if p.partition == partitionBad {
					code = kerr.NotLeaderForPartition.Code
				}
				resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
					Topic:      p.topic,
					Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: p.partition, ErrorCode: code}},
				})
			}
			return resp, nil
		}

		noRaceCfg := cfg
		noRaceCfg.MinHedgeDelay = time.Hour
		noRaceCfg.MaxHedgeAgents = 2 // primary + the one fallback candidate

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, noRaceCfg, time.Hour, 1<<20, m, nil)
		t.Cleanup(h.Close)

		agentBuffer, err := h.hedgeBuffer.agentBufferFor(fallbackAgent)
		require.NoError(t, err)

		callerOK := newResultCapture()
		callerBad := newResultCapture()
		doneOK := runHedgerAsync(h, routedHedgeRecord(topic, partitionOK, primaryAgent, "ok", callerOK.doneFor(topic, partitionOK)))
		doneBad := runHedgerAsync(h, routedHedgeRecord(topic, partitionBad, primaryAgent, "bad", callerBad.doneFor(topic, partitionBad)))

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c, []string{"ok", "bad"}, bufferedRecordValues(agentBuffer))
		}, time.Second, time.Millisecond)
		agentBuffer.timerFlush()

		for _, done := range []<-chan struct{}{doneOK, doneBad} {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for caller to finish")
			}
		}

		require.NoError(t, callerOK.get(topic, partitionOK).err)
		err2 := callerBad.get(topic, partitionBad).err
		require.ErrorIs(t, err2, kgo.ErrRecordTimeout)
		require.ErrorIs(t, err2, kerr.NotLeaderForPartition)
	})

	t.Run("one caller's own two-partition group stays all-or-nothing on a mixed response", func(t *testing.T) {
		// Unlike the two subtests above, this is a single caller (one
		// runHedger call) with both partitions landing in the same
		// fallback-agent group in the same wave. A response that scopes
		// down to just this caller's own partitions can still be a mix —
		// this proves that mix keeps failing both together instead of
		// crediting the one with a clean code.
		const (
			primaryAgent  = int32(300)
			fallbackAgent = int32(301)
			partitionA    = int32(0)
			partitionB    = int32(1)
		)
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, partitionA}: healthyAgents(primaryAgent, fallbackAgent),
				{topic, partitionB}: healthyAgents(primaryAgent, fallbackAgent),
			},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryAgent] = kerr.RequestTimedOut
		producer.respFn = func(_ int32, partitions []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
			resp := &kmsg.ProduceResponse{}
			for _, p := range partitions {
				code := kerrNoError
				if p.partition == partitionB {
					code = kerr.NotLeaderForPartition.Code
				}
				resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
					Topic:      p.topic,
					Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: p.partition, ErrorCode: code}},
				})
			}
			return resp, nil
		}

		noRaceCfg := cfg
		noRaceCfg.MinHedgeDelay = time.Hour
		noRaceCfg.MaxHedgeAgents = 2 // primary + the one fallback candidate, so a mixed wave can't retry its way around the assertion

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, noRaceCfg, time.Hour, 1<<20, m, nil)
		t.Cleanup(h.Close)

		agentBuffer, err := h.hedgeBuffer.agentBufferFor(fallbackAgent)
		require.NoError(t, err)

		caller := newResultCapture()
		done := runHedgerAsync(h,
			routedHedgeRecord(topic, partitionA, primaryAgent, "a", caller.doneFor(topic, partitionA)),
			routedHedgeRecord(topic, partitionB, primaryAgent, "b", caller.doneFor(topic, partitionB)),
		)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c, []string{"a", "b"}, bufferedRecordValues(agentBuffer))
		}, time.Second, time.Millisecond)
		agentBuffer.timerFlush()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for caller to finish")
		}

		// partitionA's own coded entry was a clean success, but it shared
		// the flush with partitionB's failing entry in the same caller's
		// group — all-or-nothing means it must not be credited on its own.
		errA := caller.get(topic, partitionA).err
		require.ErrorIs(t, errA, kgo.ErrRecordTimeout)
		require.ErrorIs(t, errA, kerr.NotLeaderForPartition)
		errB := caller.get(topic, partitionB).err
		require.ErrorIs(t, errB, kgo.ErrRecordTimeout)
		require.ErrorIs(t, errB, kerr.NotLeaderForPartition)
	})
}

// TestHedger_AttemptPayloadCountsMergedHedgeFlush checks the hedge payload is
// measured after buffer merging: two callers' same-partition retries share one
// flush, which is one attempt carrying the merged batch.
func TestHedger_AttemptPayloadCountsMergedHedgeFlush(t *testing.T) {
	const (
		topic     = "t"
		partition = int32(0)
		primary   = int32(100)
		fallback  = int32(101)
	)
	tracker := NewAverageAgentStatsTracker()
	nowNs := time.Now().UnixNano()
	for _, id := range []int32{primary, fallback} {
		seedFullWindow(tracker, id, nowNs, 20, 1, 0)
	}
	strategy := &mockPartitionAssignmentStrategy{
		candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primary, fallback)},
	}
	producer := newMockDirectProducer()
	producer.errs[primary] = kerr.RequestTimedOut
	producer.respFn = func(_ int32, partitions []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
		resp := &kmsg.ProduceResponse{}
		for _, p := range partitions {
			resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
				Topic:      p.topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: p.partition}},
			})
		}
		return resp, nil
	}
	health := HealthCheckConfig{SlowMultiplier: 2.0, MaxSlowFraction: 0.3, FaultyThreshold: 0.05, MaxFaultyFraction: 0.3}
	// A failed primary returns long before the hedge delay, so each caller
	// cascades without racing the timer.
	cfg := HedgerConfig{MinHedgeDelay: time.Hour, MaxHedgeAgents: 2}

	m := newMetrics(prometheus.NewPedanticRegistry())
	// The linger never fires: the test flushes the fallback buffer itself.
	h := NewHedger(producer, tracker, strategy, health, cfg, time.Hour, 1<<20, m, nil)
	t.Cleanup(h.Close)

	buffer, err := h.hedgeBuffer.agentBufferFor(fallback)
	require.NoError(t, err)

	done1 := runHedgerAsync(h, routedHedgeRecord(topic, partition, primary, "caller-1", nil))
	done2 := runHedgerAsync(h, routedHedgeRecord(topic, partition, primary, "caller-2", nil))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, []string{"caller-1", "caller-2"}, bufferedRecordValues(buffer))
	}, time.Second, time.Millisecond)
	buffer.timerFlush()
	for _, done := range []<-chan struct{}{done1, done2} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("ProduceSync did not return after the hedge flush")
		}
	}

	// What the producer actually received, per role.
	sent := map[attemptRole]produceRequestStats{}
	var hedgeCalls int
	for _, call := range producer.recordedCalls() {
		role := attemptHedge
		if call.nodeID == primary {
			role = attemptPrimary
		} else {
			hedgeCalls++
		}
		for _, p := range call.partitions {
			sent[role] = sent[role].add(p.encodedStats)
		}
	}
	require.Equal(t, 1, hedgeCalls, "both retries share one hedge flush")
	assert.Equal(t, int64(2), sent[attemptHedge].records, "the one flush carries both callers' records")
	assert.Equal(t, int64(2), sent[attemptPrimary].records, "each caller's primary carries one record")

	assertAttemptPayload(t, m, sent[attemptPrimary], sent[attemptHedge])
	assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsHedgeTotal))
	assert.Equal(t, float64(2), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
}

// deadlineCtx reports a fixed deadline without ever arming its timer, like a
// context whose deadline has been reached but whose timer has not fired yet.
type deadlineCtx struct {
	context.Context
	deadline time.Time
}

func (c deadlineCtx) Deadline() (time.Time, bool) { return c.deadline, true }

func TestHedger_CtxStopErr(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NoError(t, ctxStopErr(context.Background()), "no deadline, not done")
	assert.ErrorIs(t, ctxStopErr(canceled), context.Canceled)
	assert.NoError(t, ctxStopErr(deadlineCtx{context.Background(), time.Now().Add(time.Hour)}), "deadline not reached")
	assert.ErrorIs(t,
		ctxStopErr(deadlineCtx{context.Background(), time.Now().Add(-time.Millisecond)}),
		context.DeadlineExceeded, "deadline reached before its timer fired")
	assert.ErrorIs(t, ctxStopErr(deadlineCtx{canceled, time.Now().Add(-time.Millisecond)}), context.Canceled,
		"an explicit cancel is reported as a cancel, not a deadline")
}

// TestHedger_WinCountMatchesSelectedResult releases a primary and a fallback
// that both succeed, one after the other in either order, and checks a win is
// counted exactly when the returned result is the fallback's. The loser is held
// inside the producer, ignoring the work context, so it finishes successfully
// after the owner has returned and must not count.
func TestHedger_WinCountMatchesSelectedResult(t *testing.T) {
	const (
		topic         = "t"
		partition     = int32(0)
		primary       = int32(100)
		fallback      = int32(101)
		primaryOffset = int64(1000)
		fallbackOff   = int64(2000)
	)
	strategy := &mockPartitionAssignmentStrategy{
		candidates: map[partitionKey][]Agent{{topic, partition}: {
			{NodeID: primary, State: AgentStateDemoted},
			{NodeID: fallback, State: AgentStateHealthy},
		}},
	}
	tracker := NewAverageAgentStatsTracker()
	nowNs := time.Now().UnixNano()
	for _, id := range []int32{primary, fallback} {
		seedFullWindow(tracker, id, nowNs, 20, 1, 0)
	}
	health := HealthCheckConfig{SlowMultiplier: 2.0, MaxSlowFraction: 0.3, FaultyThreshold: 0.05, MaxFaultyFraction: 0.3}
	cfg := HedgerConfig{MinHedgeDelay: time.Hour, MaxHedgeAgents: 2}

	for _, fallbackFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("fallbackFirst=%v", fallbackFirst), func(t *testing.T) {
			gates := map[int32]chan struct{}{primary: make(chan struct{}), fallback: make(chan struct{})}
			entered := map[int32]chan struct{}{primary: make(chan struct{}, 1), fallback: make(chan struct{}, 1)}

			producer := newMockDirectProducer()
			// respFn takes no context, so a held call completes successfully
			// even after the work context is canceled.
			producer.respFn = func(nodeID int32, _ []encodedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
				entered[nodeID] <- struct{}{}
				<-gates[nodeID]
				offset := primaryOffset
				if nodeID == fallback {
					offset = fallbackOff
				}
				return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
					Topic:      topic,
					Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: partition, BaseOffset: offset}},
				}}}, nil
			}

			m := newMetrics(prometheus.NewPedanticRegistry())
			h := NewHedger(producer, tracker, strategy, health, cfg, 0, 1<<20, m, nil)
			t.Cleanup(h.Close)

			// A demoted primary starts the fallback immediately.
			part := routedHedgeRecord(topic, partition, primary, "v", nil)
			part.item.nodeState = AgentStateDemoted
			resCh := make(chan ProduceResult, 1)
			go func() {
				resCh <- h.ProduceSync(context.Background(), primary, unpromise([]promised[routedEncodedTopicPartitionRecords]{part}))
			}()

			// Both legs are in flight before either is released.
			for _, id := range []int32{primary, fallback} {
				select {
				case <-entered[id]:
				case <-time.After(time.Second):
					t.Fatalf("agent %d was never called", id)
				}
			}

			winner, loser := primary, fallback
			wantOffset, wantWins := primaryOffset, float64(0)
			if fallbackFirst {
				winner, loser = fallback, primary
				wantOffset, wantWins = fallbackOff, 1
			}
			close(gates[winner])
			res := <-resCh
			require.NoError(t, res.error())
			assert.Equal(t, wantOffset, res.resp.Topics[0].Partitions[0].BaseOffset)

			// The loser now completes successfully, after the owner returned.
			close(gates[loser])
			require.Eventually(t, func() bool {
				for _, call := range producer.recordedCalls() {
					if call.nodeID == loser && call.err == nil {
						return true
					}
				}
				return false
			}, time.Second, time.Millisecond, "the losing leg must finish successfully")
			h.Close()

			assert.Equal(t, wantWins, testutil.ToFloat64(m.hedgeWinsTotal))
			assertTriggerWins(t, m, map[hedgeTrigger]float64{hedgeTriggerDemotedProbe: wantWins})
		})
	}
}

func TestMaxFractionFloor(t *testing.T) {
	tests := map[string]struct {
		configured   float64
		contributors int64
		want         float64
	}{
		"single contributor: floor at 1.0":      {configured: 0.10, contributors: 1, want: 1.0},
		"two contributors: floor at 0.5 raises": {configured: 0.10, contributors: 2, want: 0.5},
		"large cluster: configured dominates":   {configured: 0.10, contributors: 100, want: 0.10},
		"configured wins when above 1/N":        {configured: 0.50, contributors: 5, want: 0.50},
		"zero contributors: returns configured": {configured: 0.10, contributors: 0, want: 0.10},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.InDelta(t, tc.want, maxFractionFloor(tc.configured, tc.contributors), 1e-9)
		})
	}
}

func TestHedgerCandidates_fetch(t *testing.T) {
	const topic = "t"

	t.Run("first call queries strategy and caches the result", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, 0}: healthyAgents(1, 2)},
		}
		hc := newHedgerCandidates(strategy, 3)
		tp := topicPartition{topic: topic, partition: 0}

		first := hc.fetch(tp)
		require.Len(t, first, 2)
		assert.Equal(t, int32(1), first[0].NodeID)
		assert.Equal(t, int32(2), first[1].NodeID)
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))

		// Repeated calls reuse the cached slice and never hit the strategy.
		for i := 0; i < 5; i++ {
			again := hc.fetch(tp)
			assert.Equal(t, first, again)
		}
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))
	})

	t.Run("different (topic, partition) keys cache independently", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, 0}: healthyAgents(1),
				{topic, 1}: healthyAgents(2),
			},
		}
		hc := newHedgerCandidates(strategy, 3)

		_ = hc.fetch(topicPartition{topic: topic, partition: 0})
		_ = hc.fetch(topicPartition{topic: topic, partition: 1})
		_ = hc.fetch(topicPartition{topic: topic, partition: 0})
		_ = hc.fetch(topicPartition{topic: topic, partition: 1})

		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 1))
	})

	t.Run("forwards maxHedgeAgents to the strategy", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, 0}: healthyAgents(1, 2, 3, 4, 5)},
		}
		hc := newHedgerCandidates(strategy, 2)

		got := hc.fetch(topicPartition{topic: topic, partition: 0})
		assert.Len(t, got, 2)
		assert.Equal(t, 2, strategy.lastMaxCandidates(topic, 0))
	})
}

func TestHedger_FinalStop(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired := deadlineCtx{context.Background(), time.Now().Add(-time.Millisecond)}

	failed := ProduceResult{err: kerr.RequestTimedOut}
	succeeded := ProduceResult{resp: &kmsg.ProduceResponse{}}

	tests := []struct {
		name    string
		ctx     context.Context
		primary ProduceResult
		result  ProduceResult
		stop    produceStop
		want    produceStop
	}{
		{"a successful result is left alone", context.Background(), failed, succeeded, produceStopCandidatesExhausted, produceStopCandidatesExhausted},
		{"a settled terminal stop is left alone", expired, failed, failed, produceStopTerminalError, produceStopTerminalError},
		{"a settled write timeout is left alone", context.Background(), failed, failed, produceStopWriteTimeout, produceStopWriteTimeout},
		{"a write timeout after a non-retriable primary error is terminal", expired, ProduceResult{err: kerr.MessageTooLarge}, failed, produceStopWriteTimeout, produceStopTerminalError},
		{"a write timeout after a canceled primary stays a write timeout", expired, ProduceResult{err: context.Canceled}, failed, produceStopWriteTimeout, produceStopWriteTimeout},
		{"exhausted stays exhausted while the context is live", context.Background(), failed, failed, produceStopCandidatesExhausted, produceStopCandidatesExhausted},
		{"exhausted after a non-retriable primary error is terminal", context.Background(), ProduceResult{err: kerr.MessageTooLarge}, failed, produceStopCandidatesExhausted, produceStopTerminalError},
		{"a non-retriable primary error outranks an expired deadline", expired, ProduceResult{err: kerr.MessageTooLarge}, failed, produceStopCandidatesExhausted, produceStopTerminalError},
		{"exhausted with an expired deadline is a write timeout", expired, failed, failed, produceStopCandidatesExhausted, produceStopWriteTimeout},
		{"exhausted with a canceled context emits no failure", canceled, failed, failed, produceStopCandidatesExhausted, produceStopCanceled},
		{"a canceled primary is a cancel, not a terminal error", canceled, ProduceResult{err: context.Canceled}, failed, produceStopCandidatesExhausted, produceStopCanceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, finalStop(tt.ctx, tt.primary, tt.result, tt.stop))
		})
	}
}

func TestHedger_ClassifyProduceStop(t *testing.T) {
	brokerErr := kerr.MessageTooLarge
	unknownErr := errors.New("boom")

	tests := []struct {
		name        string
		terminal    bool
		terminalErr error
		ctxErr      error
		exhausted   bool
		want        produceStop
	}{
		{name: "success", want: produceStopNone},
		{name: "candidates exhausted", exhausted: true, want: produceStopCandidatesExhausted},
		{name: "terminal broker error", terminal: true, terminalErr: brokerErr, want: produceStopTerminalError},
		{name: "terminal unknown error", terminal: true, terminalErr: unknownErr, want: produceStopTerminalError},
		{name: "deadline is the returned failure", ctxErr: context.DeadlineExceeded, want: produceStopWriteTimeout},
		{name: "deadline after exhaustion is the returned failure", ctxErr: context.DeadlineExceeded, exhausted: true, want: produceStopWriteTimeout},
		{name: "deadline does not relabel a terminal broker error", terminal: true, terminalErr: brokerErr, ctxErr: context.DeadlineExceeded, want: produceStopTerminalError},
		{name: "explicit cancel", ctxErr: context.Canceled, want: produceStopCanceled},
		{name: "explicit cancel after exhaustion", ctxErr: context.Canceled, exhausted: true, want: produceStopCanceled},
		{name: "cancel that aborted the accumulator", terminal: true, terminalErr: context.Canceled, ctxErr: context.Canceled, want: produceStopCanceled},
		{name: "cancel does not relabel a terminal broker error", terminal: true, terminalErr: brokerErr, ctxErr: context.Canceled, want: produceStopTerminalError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifyProduceStop(tt.terminal, tt.terminalErr, tt.ctxErr, tt.exhausted))
		})
	}
}
