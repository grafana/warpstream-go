package runner

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvestigation_RollingStallsOverStaleDiscovery(t *testing.T) {
	t.Parallel()
	var sc scenario
	for _, candidate := range investigationScenarios() {
		if candidate.name == "investigation: rolling stalls over stale discovery" {
			sc = candidate
		}
	}
	require.NotEmpty(t, sc.name)
	brokers, _ := sc.topology()
	require.EqualValues(t, rollingStallAgents, brokers)
	require.Equal(t, rollingStallAgents, sc.profile.advertisedAgents)
	require.True(t, sc.hasInjectedDelays())
	assert.Empty(t, sc.transportPhases)
	assert.True(t, slices.IsSortedFunc(sc.behaviourPhases, func(a, b behaviourPhase) int { return int(a.at - b.at) }))

	var mostStalled, single int
	for _, phase := range sc.behaviourPhases {
		stalled := map[int32]bool{}
		for id, behaviour := range phase.behaviours.byBroker {
			if behaviour.injectedLatency {
				stalled[id] = true
				assert.Equal(t, rollingStallLatency, behaviour.latencyFn(nil))
				assert.Zero(t, behaviour.failRate, "a stalled agent does not return an error")
			}
		}
		mostStalled = max(mostStalled, len(stalled))
		require.NotEmpty(t, phase.behaviours.leaderPool)
		if len(phase.behaviours.leaderPool) == 1 {
			single++
			// The cached listing still names the newly stalled agent.
			assert.True(t, stalled[phase.behaviours.leaderPool[0]])
			continue
		}
		for _, id := range phase.behaviours.leaderPool {
			assert.False(t, stalled[id], "agent %d is stalled but could be named leader", id)
		}
	}
	assert.Equal(t, rollingStallAgents/5, mostStalled)
	assert.Equal(t, rollingStallWaves, single)

	last := sc.behaviourPhases[len(sc.behaviourPhases)-1]
	for _, behaviour := range last.behaviours.byBroker {
		assert.False(t, behaviour.injectedLatency, "every agent must recover")
	}
	assert.Equal(t, sc.duration(), 90*time.Second)
}
