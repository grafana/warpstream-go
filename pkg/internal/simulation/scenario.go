package main

import (
	"fmt"
	"time"
)

// scenario is one comparison case: a set of per-broker behaviours swapped in for
// the observed phase, plus the minimum wgo success rate we expect under it.
type scenario struct {
	name        string
	description string
	behaviours  brokersBehaviour

	// expectedMinSuccessRate is the floor the wgo app-level success rate must
	// clear for the scenario to pass (drives the binary's exit code).
	expectedMinSuccessRate float64
}

// scenarios returns the comparison cases. Behaviours and expectations mirror the
// original scenario tests verbatim.
func scenarios() []scenario {
	// agentCount returns numerator/denominator of the cluster as an agent count,
	// panicking if the split is degenerate (no affected agents, or none left healthy).
	agentCount := func(label string, numerator, denominator int32) int32 {
		n := clusterSize * numerator / denominator
		if n <= 0 || n >= clusterSize {
			panic(fmt.Sprintf("simulation: %s split computed %d agents, need 1..%d (clusterSize=%d)",
				label, n, clusterSize-1, clusterSize))
		}
		return n
	}

	return []scenario{
		{
			name:                   "all healthy",
			description:            "Every agent at healthy latency. Every record should succeed via the primary; no hedge fires.",
			behaviours:             healthyBehaviours(),
			expectedMinSuccessRate: 1.0,
		},
		{
			name:                   "1 fast-failing agent",
			description:            "Broker 1 returns NotLeaderForPartition immediately; the cascade path retries on another agent.",
			behaviours:             failingBehavioursFor(1),
			expectedMinSuccessRate: 1.0,
		},
		{
			name:                   "1 slow agent",
			description:            "Broker 1's latency jumps to avg 1.5s (max 3s). The Hedger flags the primary as slow, fires a hedge, and the fallback wins the race.",
			behaviours:             highLatencyBehavioursFor(1),
			expectedMinSuccessRate: 1.0,
		},
		{
			name:                   "2 fast-failing agents",
			description:            "Brokers 1 and 2 fail; cascade retries on healthy agents.",
			behaviours:             failingBehavioursFor(1, 2),
			expectedMinSuccessRate: 1.0,
		},
		{
			name:                   "2 slow agents",
			description:            "Brokers 1 and 2 are slow; the hedge fires for both and the fallbacks win.",
			behaviours:             highLatencyBehavioursFor(1, 2),
			expectedMinSuccessRate: 1.0,
		},
		func() scenario {
			// Every agent: 1% random hard-failure on top of healthy latency.
			bh := healthyBehaviours()
			for i := int32(0); i < clusterSize; i++ {
				b := bh.byBroker[i]
				b.failRate = 0.01
				bh.byBroker[i] = b
			}
			return scenario{
				name:                   "1% failure rate across all agents",
				description:            "Every agent has a 1% random hard-failure probability on top of healthy latency.",
				behaviours:             bh,
				expectedMinSuccessRate: 0.95,
			}
		}(),
		{
			name: "1% timeouts across all agents",
			description: "Every agent has a 1% per-request chance of an extra ~10s delay (= WriteTimeout). " +
				"The burst matches the flush deadline, so a hit on the primary can't be recovered within budget. " +
				"With 50 partitions per request, P(no partition trips a burst) = 0.99^50 ≈ 0.605, so app success sits ~60%.",
			behaviours:             burstyLatencyBehaviours(0.01, 10*time.Second),
			expectedMinSuccessRate: 0.50,
		},
		{
			name:                   "1% slow bursts across all agents",
			description:            "Every agent has a 1% per-request chance of an extra ~3s slow burst (within the per-attempt deadline but slow enough that the hedge timer fires).",
			behaviours:             burstyLatencyBehaviours(0.01, 3*time.Second),
			expectedMinSuccessRate: 0.95,
		},
		func() scenario {
			// 25% of agents permanently slow.
			bh := healthyBehaviours()
			slowCount := agentCount("25% slow", 1, 4)
			for i := int32(0); i < slowCount; i++ {
				bh.byBroker[i] = brokerBehaviour{latencyFn: highLatency}
			}
			return scenario{
				name:                   "25% slow agents",
				description:            "A quarter of agents are permanently slow (avg 1.5s, max 3s); the hedge fallback should steer most traffic onto the healthy majority.",
				behaviours:             bh,
				expectedMinSuccessRate: 1.0,
			}
		}(),
		func() scenario {
			// GCS outage: first 10% very bad, next 40% moderately slow, rest healthy.
			// Both model object-storage degradation as healthy base + occasional burst,
			// so the tail stays heavy rather than smearing across all requests.
			gcsBad := burstyLatency(normalLatency, 0.25, 9*time.Second)      // avg ≈ 2.65s, max ≈ 10s
			gcsModerate := burstyLatency(normalLatency, 0.10, 3*time.Second) // avg ≈ 700ms, max ≈ 4s
			bh := healthyBehaviours()
			badCount := agentCount("GCS bad", 1, 10)
			modCount := agentCount("GCS moderate", 4, 10)
			if badCount+modCount >= clusterSize {
				panic(fmt.Sprintf("simulation: GCS bad+moderate (%d) leaves no healthy agents (clusterSize=%d)", badCount+modCount, clusterSize))
			}
			for i := int32(0); i < badCount; i++ {
				bh.byBroker[i] = brokerBehaviour{latencyFn: gcsBad}
			}
			for i := badCount; i < badCount+modCount; i++ {
				bh.byBroker[i] = brokerBehaviour{latencyFn: gcsModerate}
			}
			return scenario{
				name: "GCS slow outage (10% bad, 40% moderate)",
				description: "Reproduces an incident where object storage degraded asymmetrically: 50% healthy, 40% avg ≈ 700ms/max ≈ 4s, 10% avg ≈ 2.65s/max ≈ 10s. " +
					"We expect a large initial failure spike; the Demoter should kick in within ~30s and reroute away from the worst offenders. The threshold is intentionally loose.",
				behaviours:             bh,
				expectedMinSuccessRate: 0.20,
			}
		}(),
	}
}
