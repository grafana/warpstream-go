package runner

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvestigation_ProfileSelection(t *testing.T) {
	_, err := profileScenarios("mimir-investigation", false, defaultRunID)
	require.ErrorContains(t, err, "external workload")
	_, err = profileScenarios("unknown", true, defaultRunID)
	require.Error(t, err)
	standard, err := profileScenarios("standard", false, defaultRunID)
	require.NoError(t, err)
	require.Len(t, standard, len(scenarios(defaultRunID)))
	investigation, err := profileScenarios("mimir-investigation", true, defaultRunID)
	require.NoError(t, err)
	require.Len(t, investigation, 7+len(researchScenarios()))
	all, err := profileScenarios("all", true, defaultRunID)
	require.NoError(t, err)
	require.Len(t, all, len(standard)+len(investigation)+len(storagePressureScenarios()))
	found := false
	for _, candidate := range all {
		if candidate.requireStaleFallback {
			found = true
		}
	}
	require.True(t, found)
	sc := investigation[1]
	brokers, partitions := sc.topology()
	require.EqualValues(t, 3, brokers)
	require.EqualValues(t, 1001, partitions)
	require.Len(t, buildScenarioEvents("", 0, sc.duration(), sc.spacing()), 360)
	result := scenarioResult{sc: sc, wgoLatencySelections: map[int32]int64{0: 1, 1: 1, 2: 1, 3: 1, 4: 1}, kgoLatencySelections: map[int32]int64{0: 1, 1: 1, 2: 1, 3: 1, 4: 1}, wgoSummary: observationsSummary{total: 360, successes: 360}, wgoRecovery: observationsSummary{total: 70, successes: 70}}
	require.Empty(t, checkResult(result))
	result.wgoRecovery.successes--
	require.NotEmpty(t, checkResult(result))
}

func TestInvestigation_PhaseTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sc := investigationScenarios()[1]
		env := &environment{behaviours: newBrokersBehaviourProvider(sc.behaviours)}
		done := make(chan error, 1)
		go func() { done <- runPhases(t.Context(), env, time.Now(), sc) }()
		time.Sleep(9 * time.Second)
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
		time.Sleep(2 * time.Second)
		require.Equal(t, 3*time.Second, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
		require.Equal(t, 9*time.Second, env.behaviours.nextLatencyFor(clientTypeKgo, 1))
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 2))
		time.Sleep(120 * time.Second)
		require.NoError(t, <-done)
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
	})
}

func TestInvestigation_PhaseCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sc := investigationScenarios()[1]
		env := &environment{behaviours: newBrokersBehaviourProvider(sc.behaviours)}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- runPhases(ctx, env, time.Now(), sc) }()
		time.Sleep(time.Second)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
	})
}

func TestInvestigation_ScheduledWindows(t *testing.T) {
	start := time.Now()
	snapshot := observationsSnapshot{list: []observation{
		{scheduled: start.Add(7 * time.Second), issued: start.Add(11 * time.Second)},
		{scheduled: start.Add(10 * time.Second), issued: start.Add(12 * time.Second)},
		{scheduled: start.Add(129 * time.Second), err: errors.New("failed")},
		{scheduled: start.Add(130 * time.Second)},
		{scheduled: start.Add(145 * time.Second)},
		{scheduled: start.Add(180 * time.Second)},
	}}
	sc := investigationScenarios()[1]
	windows := summarizeWindows(sc.windows, start, snapshot, snapshot)
	require.Equal(t, 1, windows[0].wgo.total)
	require.Equal(t, 2, windows[1].wgo.total)
	require.Equal(t, 1, windows[1].wgo.successes)
	require.Equal(t, 1, windows[2].kgo.total)
	report := newResultsReport(scenarioResults{entries: []scenarioResult{{sc: sc, windows: windows}}})
	data, err := report.generateJSON()
	require.NoError(t, err)
	require.Contains(t, string(data), `"from_seconds": 145`)
	require.Contains(t, string(data), `"attempts": 2`)
	require.Contains(t, report.generateMarkdown(), "3 brokers / 1001 partitions")
	require.Contains(t, report.generateMarkdown(), "event spacing 500ms")
}

func TestInvestigation_LatencyEvidence(t *testing.T) {
	provider := newBrokersBehaviourProvider(brokersBehaviour{byBroker: map[int32]brokerBehaviour{0: {latencyFn: constantLatency(50 * time.Millisecond), injectedLatency: true, minInjectedLatency: 50 * time.Millisecond}}})
	provider.nextLatencyFor(clientTypeWgo, 0)
	require.Empty(t, provider.selectedLatencies(clientTypeWgo))
	provider.set(brokersBehaviour{byBroker: map[int32]brokerBehaviour{0: {latencyFn: constantLatency(9 * time.Second), injectedLatency: true, minInjectedLatency: 50 * time.Millisecond}}})
	provider.nextLatencyFor(clientTypeWgo, 0)
	selected := provider.selectedLatencies(clientTypeWgo)
	require.EqualValues(t, 1, selected[0])
	selected[0] = 100
	require.EqualValues(t, 1, provider.selectedLatencies(clientTypeWgo)[0])
}

func TestInvestigation_RequiredFallbackFault(t *testing.T) {
	sc := scenario{name: "fallback fault", requiredWgoDelayBrokers: []int32{2}}
	result := scenarioResult{sc: sc, wgoLatencySelections: map[int32]int64{0: 100}}
	require.Len(t, checkResult(result), 1)
	result.wgoLatencySelections[2] = 1
	require.Empty(t, checkResult(result))
}

func TestInvestigation_RotatingLeaderOverStaleDiscovery(t *testing.T) {
	t.Parallel()
	var sc scenario
	for _, candidate := range investigationScenarios() {
		if candidate.name == "investigation: rotating leader over stale discovery" {
			sc = candidate
		}
	}
	require.NotEmpty(t, sc.name)
	require.True(t, sc.requireCascadeContinuation)
	brokers, _ := sc.topology()
	require.EqualValues(t, rotatingAgents, brokers)
	require.Equal(t, rotatingAgents, sc.profile.advertisedAgents)
	assert.True(t, slices.IsSortedFunc(sc.transportPhases, func(a, b transportPhase) int { return cmp.Compare(a.at, b.at) }))

	dead := map[int32]bool{}
	var mostDead int
	for _, phase := range sc.transportPhases {
		dead[phase.nodeID] = phase.action == killPod
		var count int
		for _, isDead := range dead {
			if isDead {
				count++
			}
		}
		mostDead = max(mostDead, count)
	}
	assert.Equal(t, rotatingAgents/5, mostDead)
	for _, isDead := range dead {
		assert.False(t, isDead, "every agent that left must return")
	}

	var named int
	for _, phase := range sc.behaviourPhases {
		require.NotEmpty(t, phase.behaviours.leaderPool)
		if len(phase.behaviours.leaderPool) == 1 {
			named++
		}
		for _, nodeID := range phase.behaviours.brokerOnlyMissing {
			assert.Less(t, nodeID, int32(rotatingAgents))
		}
	}
	assert.Equal(t, rotatingWaves, named)
}
