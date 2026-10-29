package runner

import (
	"fmt"
	"slices"
	"time"
)

const (
	rollingStallAgents        = 20
	rollingStallFirstWave     = 5 * time.Second
	rollingStallWaveSpacing   = 10 * time.Second
	rollingStallWaveLength    = 20 * time.Second
	rollingStallWaves         = 6
	rollingStallAgentsPerWave = 2
	rollingStallLatency       = 9 * time.Second
)

// rollingStallScenario stalls agents in waves, as when a fifth of the pods are slow
// at a time in an incident. Each wave stalls agents the client has not seen fail,
// so unlike the fixed slow agents of the other investigation scenarios it has to
// keep learning. Metadata keeps listing a stalled agent, and for the first
// rollingStallWaveSpacing of a wave its cached listing still names the wave's first
// agent as leader of every partition.
func rollingStallScenario(base *workloadProfile) scenario {
	profile := *base
	profile.brokers = rollingStallAgents
	profile.advertisedAgents = rollingStallAgents
	sc := scenario{profile: &profile}

	type wave struct {
		start, end time.Duration
		agents     []int32
	}
	waves := make([]wave, rollingStallWaves)
	var boundaries []time.Duration
	for i := range waves {
		start := rollingStallFirstWave + time.Duration(i)*rollingStallWaveSpacing
		waves[i] = wave{start: start, end: start + rollingStallWaveLength}
		for j := range int32(rollingStallAgentsPerWave) {
			waves[i].agents = append(waves[i].agents, int32(i*rollingStallAgentsPerWave)+j)
		}
		boundaries = append(boundaries, start, start+rollingStallWaveSpacing, waves[i].end)
	}
	slices.Sort(boundaries)
	boundaries = slices.Compact(boundaries)

	var behaviourPhases []behaviourPhase
	for _, at := range boundaries {
		behaviours := sc.healthyBehaviours()
		stalled := map[int32]bool{}
		var newest *wave
		for i := range waves {
			if at < waves[i].start || at >= waves[i].end {
				continue
			}
			for _, id := range waves[i].agents {
				stalled[id] = true
				behaviours.byBroker[id] = brokerBehaviour{latencyFn: constantLatency(rollingStallLatency), injectedLatency: true}
			}
			if at < waves[i].start+rollingStallWaveSpacing {
				newest = &waves[i]
			}
		}
		if newest != nil {
			behaviours.leaderPool = []int32{newest.agents[0]}
		} else {
			for id := range int32(rollingStallAgents) {
				if !stalled[id] {
					behaviours.leaderPool = append(behaviours.leaderPool, id)
				}
			}
		}
		behaviourPhases = append(behaviourPhases, behaviourPhase{at, behaviours})
	}

	stalledAtOnce := int(rollingStallWaveLength/rollingStallWaveSpacing) * rollingStallAgentsPerWave
	return scenario{
		name:        "investigation: rolling stalls over stale discovery",
		profile:     &profile,
		observedFor: 90 * time.Second,
		description: fmt.Sprintf("%d agents are all advertised and every partition shares one leader per Metadata response. %d waves each make %d agents delay every Produce response by %s, for %s and %s apart, so %d of %d (%.0f%%) are stalled at once. "+
			"The delay applies to every connection of an agent and no stalled agent returns an error. For %s after a wave starts, Metadata still names a newly stalled agent as the leader of every partition, so the first writes stall together and must be hedged to another agent, which may be stalled too. "+
			"Each wave stalls agents not seen before, so the client has to learn them again.",
			rollingStallAgents, rollingStallWaves, rollingStallAgentsPerWave, rollingStallLatency, rollingStallWaveLength, rollingStallWaveSpacing, stalledAtOnce, rollingStallAgents, 100*float64(stalledAtOnce)/rollingStallAgents, rollingStallWaveSpacing),
		behaviours:      sc.healthyBehaviours(),
		behaviourPhases: behaviourPhases,
		expect:          scenarioExpectations{minWgoSuccessRate: 1},

		collectClientEvidence: true,
	}
}
