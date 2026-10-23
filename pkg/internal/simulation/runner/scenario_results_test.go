package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScenarioResult_SuccessRate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0.0, scenarioResult{}.successRate())
	r := scenarioResult{wgoSummary: observationsSummary{total: 4, successes: 3}}
	assert.Equal(t, 0.75, r.successRate())
}

func TestScenarioResults_Check(t *testing.T) {
	t.Parallel()
	sr := scenarioResults{entries: []scenarioResult{
		{sc: scenario{name: "ok", expect: scenarioExpectations{minWgoSuccessRate: 0.9}}, wgoSummary: observationsSummary{total: 10, successes: 10}},
		{sc: scenario{name: "bad", expect: scenarioExpectations{minWgoSuccessRate: 0.9}}, wgoSummary: observationsSummary{total: 10, successes: 5}},
	}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "bad", failures[0].scenario)
	assert.Contains(t, failures[0].detail, "50.0%")
	assert.Contains(t, failures[0].detail, "90.0%")
}

func TestScenarioResults_Check_SlowFraction(t *testing.T) {
	t.Parallel()
	sr := scenarioResults{entries: []scenarioResult{{
		sc: scenario{name: "too slow", expect: scenarioExpectations{
			minWgoSuccessRate:  1.0,
			slowBudget:         ptr(2 * time.Second),
			maxWgoSlowFraction: ptr(0.1),
		}},
		wgoSummary:      observationsSummary{total: 10, successes: 10},
		wgoSlowFraction: ptr(0.5),
	}}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "too slow", failures[0].scenario)
	assert.Contains(t, failures[0].detail, "50.0%")
	assert.Contains(t, failures[0].detail, "10.0%")
}

func TestScenarioResults_Check_DeltaVsKgo(t *testing.T) {
	t.Parallel()
	sr := scenarioResults{entries: []scenarioResult{{
		sc:         scenario{name: "no better than kgo", expect: scenarioExpectations{minWgoSuccessRate: 0.5, minSuccessDeltaVsKgo: ptr(0.5)}},
		wgoSummary: observationsSummary{total: 10, successes: 6},
		kgoSummary: observationsSummary{total: 10, successes: 5},
	}}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "no better than kgo", failures[0].scenario)
}

func TestScenarioResults_Check_CascadeContinuation(t *testing.T) {
	t.Parallel()
	evidence := func() *staleFallbackEvidence {
		return &staleFallbackEvidence{
			BrokerOnlyWithdrawals: 1, StaleRoutingResponses: 1, FallbackErrors: 1, ThirdAttemptSuccesses: 1,
			FallbackErrorReasons: map[string]int64{"unknown_broker": 1},
		}
	}
	result := scenarioResult{sc: scenario{requireCascadeContinuation: true}, staleFallback: evidence()}
	require.Empty(t, checkResult(result))

	for name, mutate := range map[string]func(*staleFallbackEvidence){
		"no fallback error":       func(e *staleFallbackEvidence) { e.FallbackErrors = 0 },
		"no third attempt":        func(e *staleFallbackEvidence) { e.ThirdAttemptSuccesses = 0 },
		"no broker-only withdraw": func(e *staleFallbackEvidence) { e.BrokerOnlyWithdrawals = 0 },
		"no stale routing":        func(e *staleFallbackEvidence) { e.StaleRoutingResponses = 0 },
		"only known-agent errors": func(e *staleFallbackEvidence) { e.FallbackErrorReasons = map[string]int64{"transport": 1} },
		"terminal error":          func(e *staleFallbackEvidence) { e.TerminalErrors = 1 },
	} {
		result.staleFallback = evidence()
		mutate(result.staleFallback)
		require.NotEmpty(t, checkResult(result), name)
	}

	// Builds before the unknown_broker label report the same failure as "unknown".
	result.staleFallback = evidence()
	result.staleFallback.FallbackErrorReasons = map[string]int64{"unknown": 1}
	require.Empty(t, checkResult(result))

	result.staleFallback = nil
	require.Len(t, checkResult(result), 4)
}

func TestScenarioResults_RecoveryAndSurge(t *testing.T) {
	t.Parallel()
	result := scenarioResult{
		sc:           scenario{name: "recovery", expect: scenarioExpectations{recoveryFrom: ptr(50 * time.Second), slowBudget: ptr(2 * time.Second), maxHedgeSurge: ptr(0.25)}},
		wgoRecovery:  observationsSummary{total: 20, successes: 20},
		totalPrimary: 100, totalHedge: 25,
	}
	assert.Empty(t, checkResult(result))
	result.wgoRecovery.successes--
	require.Len(t, checkResult(result), 1)
	result.wgoRecovery.successes++
	result.wgoRecovery.total--
	require.Len(t, checkResult(result), 1)
	result.wgoRecovery.total++
	result.wgoRecoverySlowFraction = 0.05
	require.Len(t, checkResult(result), 1)
	result.wgoRecoverySlowFraction = 0
	result.totalHedge++
	require.Len(t, checkResult(result), 1)
}

func TestScenarioResults_TransportFaultEvidence(t *testing.T) {
	t.Parallel()
	result := scenarioResult{
		sc: scenario{name: "blackhole", transportPhases: []transportPhase{{0, 1, blackholePod}, {time.Second, 1, restorePod}}},
	}
	require.Len(t, checkResult(result), 3)
	result.wgoTransport.byBroker = map[int32]brokerTransportStats{1: {droppedBytes: 100}}
	result.kgoTransport.byBroker = map[int32]brokerTransportStats{1: {droppedBytes: 100}}
	require.Len(t, checkResult(result), 1)
	result.wgoTransport.recoveredResponses = map[int32]int64{1: 1}
	assert.Empty(t, checkResult(result))
	result.sc.transportPhases[0].action = killPod
	require.Len(t, checkResult(result), 2)
	result.wgoTransport.byBroker[1] = brokerTransportStats{closedConnections: 1}
	result.kgoTransport.byBroker[1] = brokerTransportStats{closedConnections: 1}
	assert.Empty(t, checkResult(result))
}

func TestScenarioResults_MissingLeaderEvidence(t *testing.T) {
	t.Parallel()
	result := scenarioResult{
		sc:           scenario{name: "restart", transportPhases: []transportPhase{{0, 1, withdrawPod}, {time.Second, 1, replacePod}}},
		wgoTransport: transportStats{byBroker: map[int32]brokerTransportStats{1: {closedConnections: 1}}, recoveredResponses: map[int32]int64{1: 1}},
		kgoTransport: transportStats{byBroker: map[int32]brokerTransportStats{1: {closedConnections: 1}}},
	}
	require.Len(t, checkResult(result), 2)
	result.wgoMissingLeaders = map[int32]int64{1: 1}
	require.Len(t, checkResult(result), 1)
	result.kgoMissingLeaders = map[int32]int64{1: 1}
	assert.Empty(t, checkResult(result))
	result.sc.transportPhases = append(result.sc.transportPhases, transportPhase{2 * time.Second, 2, withdrawPod})
	require.Len(t, checkResult(result), 4)
}

func TestScenarioResults_RecoveryUsesScenarioDuration(t *testing.T) {
	t.Parallel()
	result := scenarioResult{
		sc:          scenario{observedFor: 90 * time.Second, expect: scenarioExpectations{recoveryFrom: ptr(80 * time.Second)}},
		wgoRecovery: observationsSummary{total: 20, successes: 20},
	}
	assert.Empty(t, checkResult(result))
	result.wgoRecovery.total--
	assert.Len(t, checkResult(result), 1)
}

func TestScenarioResults_FaultEvidenceIsBrokerSpecific(t *testing.T) {
	t.Parallel()
	for _, action := range []transportAction{killPod, withdrawPod, removePod, blackholePod} {
		stats := transportStats{
			closedConnections: 3, droppedBytes: 100,
			byBroker: map[int32]brokerTransportStats{1: {closedConnections: 3, droppedBytes: 100}},
		}
		result := scenarioResult{
			sc:           scenario{transportPhases: []transportPhase{{0, 2, action}}},
			wgoTransport: stats, kgoTransport: stats,
			wgoMissingLeaders: map[int32]int64{2: 1}, kgoMissingLeaders: map[int32]int64{2: 1},
		}
		failures := checkResult(result)
		require.Len(t, failures, 2)
		for _, failure := range failures {
			assert.Contains(t, failure.detail, "broker 2 did not affect live traffic")
		}
	}
}

func TestScenarioResults_RecoveryBudgetIsReported(t *testing.T) {
	t.Parallel()
	result := scenarioResult{
		sc:          scenario{expect: scenarioExpectations{recoveryFrom: ptr(50 * time.Second), slowBudget: ptr(3 * time.Second)}},
		wgoRecovery: observationsSummary{total: 20, successes: 20}, wgoRecoverySlowFraction: 0.1,
	}
	failures := checkResult(result)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[0].detail, "10.0% slower than 3s")
	report := newResultsReport(scenarioResults{entries: []scenarioResult{result}}).generateMarkdown()
	assert.Contains(t, report, "every recovery event must finish within 3s")
	result.sc.expect.slowBudget = nil
	assert.Empty(t, checkResult(result))
}
