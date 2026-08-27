package main

import (
	"fmt"
	"time"
)

// scenarioExpectations declares what a scenario's results must show to pass.
// minWgoSuccessRate is always enforced; the other two are opt-in per scenario
// (nil disables them) because a single success-rate floor can't prove wgo
// actually did something a plain client wouldn't have: on the latency
// scenarios both clients finish every request inside appRequestTimeout, and
// on some of the failure scenarios a client with no rerouting at all still
// clears a loosely-set floor by chance. See the per-scenario comments in
// scenarios() for the reasoning behind each one's chosen values.
type scenarioExpectations struct {
	// minWgoSuccessRate is the floor the wgo app-level success rate must
	// clear. Always enforced; the zero value is a no-op floor every run
	// passes.
	minWgoSuccessRate float64

	// slowBudget and maxWgoSlowFraction together assert wgo's sub-timeout
	// tail: at most maxWgoSlowFraction of wgo requests may take longer than
	// slowBudget. Both must be set for the assertion to run; either alone is
	// ignored. Set on scenarios where hedging should mask a slow agent's
	// latency rather than merely finish before the deadline — a success-rate
	// floor alone can't distinguish "hedged around it" from "just waited it
	// out", since both finish within appRequestTimeout.
	slowBudget         *time.Duration
	maxWgoSlowFraction *float64

	// minSuccessDeltaVsKgo asserts wgo's success rate beats kgo's, measured
	// in the same run, by at least this much (wgoSuccessRate - kgoSuccessRate
	// >= it). Nil disables the check. Set on scenarios whose whole point is
	// that wgo reroutes around a broken or degraded agent and kgo's
	// ManualPartitioner cannot — the assertion is the head-to-head margin
	// itself, which can't be cleared by accident the way a hand-tuned
	// absolute floor can (see the review discussion on PR #3 for a case
	// where a kgo-only run cleared several scenarios' floors outright).
	minSuccessDeltaVsKgo *float64
}

// scenario is one comparison case: a set of per-broker behaviours swapped in
// for the observed phase, plus what we expect the results to show.
type scenario struct {
	name        string
	description string
	behaviours  brokersBehaviour
	expect      scenarioExpectations
}

// ptr returns a pointer to a copy of v, for building scenarioExpectations'
// optional fields inline.
func ptr[T any](v T) *T { return &v }

// scenarios returns the comparison cases. Behaviours mirror the original
// scenario tests verbatim; expectations are tightened from the original
// single-floor design per the PR #3 review (a floor alone can't tell a
// rerouting client from one that just waits — see scenarioExpectations).
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
			name:        "all healthy",
			description: "Every agent at healthy latency. Every record should succeed via the primary; no hedge fires.",
			behaviours:  healthyBehaviours(),
			// Both clients are legitimately tied here — this is the control
			// case, not a rerouting test — so no delta/slow-fraction check.
			expect: scenarioExpectations{minWgoSuccessRate: 1.0},
		},
		{
			name:        "1 fast-failing agent",
			description: "Broker 1 returns NotLeaderForPartition immediately; the cascade path retries on another agent.",
			behaviours:  failingBehavioursFor(1),
			// kgo's ManualPartitioner keeps sending to the same dead leader
			// forever and gets 0%; a generous 0.5 margin proves the gap
			// without being sensitive to run-to-run noise.
			expect: scenarioExpectations{minWgoSuccessRate: 1.0, minSuccessDeltaVsKgo: ptr(0.5)},
		},
		{
			name:        "1 slow agent",
			description: "Broker 1's latency jumps to avg 1.5s (max 3s). The Hedger flags the primary as slow, fires a hedge, and the fallback wins the race.",
			behaviours:  highLatencyBehavioursFor(1),
			// Every event fans out to all 50 partitions, so every event hits
			// the one slow broker on one of its 50 legs; the event's
			// recorded latency is the slowest leg. Without rerouting, that
			// leg alone (triangular(500ms, 1s, 3s)) exceeds 2s with
			// probability 1-(3-2)^2/((3-0.5)(3-1)) = 0.8, so ~80% of a
			// non-hedging client's events would land above a 2s budget.
			// wgo's hedge should mask nearly all of that by winning the
			// race with the healthy fallback (~400ms avg) instead.
			expect: scenarioExpectations{
				minWgoSuccessRate:  1.0,
				slowBudget:         ptr(2 * time.Second),
				maxWgoSlowFraction: ptr(0.10),
			},
		},
		{
			name:        "2 fast-failing agents",
			description: "Brokers 1 and 2 fail; cascade retries on healthy agents.",
			behaviours:  failingBehavioursFor(1, 2),
			expect:      scenarioExpectations{minWgoSuccessRate: 1.0, minSuccessDeltaVsKgo: ptr(0.5)},
		},
		{
			name:        "2 slow agents",
			description: "Brokers 1 and 2 are slow; the hedge fires for both and the fallbacks win.",
			behaviours:  highLatencyBehavioursFor(1, 2),
			// Same reasoning as "1 slow agent", with two chances per event to
			// hit a slow leg (~96% for a non-hedging client at the 2s
			// budget). Allow a slightly higher wgo ceiling: two concurrent
			// hedges per event is more probe/demotion traffic for the same
			// 60s observed window.
			expect: scenarioExpectations{
				minWgoSuccessRate:  1.0,
				slowBudget:         ptr(2 * time.Second),
				maxWgoSlowFraction: ptr(0.15),
			},
		},
		func() scenario {
			// Every agent: 1% random hard-failure on top of healthy latency.
			bh := healthyBehaviours()
			for i := range clusterSize {
				b := bh.byBroker[i]
				b.failRate = 0.01
				bh.byBroker[i] = b
			}
			return scenario{
				name:        "1% failure rate across all agents",
				description: "Every agent has a 1% random hard-failure probability on top of healthy latency.",
				behaviours:  bh,
				// kgo has no fallback for a hard failure on any of the 50
				// legs: P(no leg fails) = 0.99^50 ≈ 0.605, so ~40% of kgo's
				// events fail outright. wgo should recover essentially all
				// of them via cascade retry.
				expect: scenarioExpectations{minWgoSuccessRate: 0.95, minSuccessDeltaVsKgo: ptr(0.5)},
			}
		}(),
		{
			name: "1% timeouts across all agents",
			description: "Every agent has a 1% per-request chance of an extra ~10s delay (= WriteTimeout). " +
				"The burst matches the flush deadline, so a hit on the primary can't be recovered within budget. " +
				"With 50 partitions per request, P(no partition trips a burst) = 0.99^50 ≈ 0.605, so app success sits ~60%.",
			behaviours: burstyLatencyBehaviours(0.01, 10*time.Second),
			// The 50% floor alone is blind: a non-hedging client already
			// clears it at ~60% by construction (see the description). The
			// delta is the actual assertion — wgo should hedge the stalled
			// leg away well inside appRequestTimeout, so it should beat a
			// non-rerouting client by a wide margin even though both clear
			// the floor.
			expect: scenarioExpectations{minWgoSuccessRate: 0.50, minSuccessDeltaVsKgo: ptr(0.20)},
		},
		{
			name:        "1% slow bursts across all agents",
			description: "Every agent has a 1% per-request chance of an extra ~3s slow burst (within the per-attempt deadline but slow enough that the hedge timer fires).",
			behaviours:  burstyLatencyBehaviours(0.01, 3*time.Second),
			// P(at least one of 50 legs bursts) = 1-0.99^50 ≈ 0.395, and a
			// bursting leg (normal + 3s) comfortably clears a 2s budget. A
			// non-hedging client should show a slow fraction near that 40%;
			// wgo's hedge should mask nearly all of it.
			expect: scenarioExpectations{
				minWgoSuccessRate:  0.95,
				slowBudget:         ptr(2 * time.Second),
				maxWgoSlowFraction: ptr(0.10),
			},
		},
		func() scenario {
			// 25% of agents permanently slow.
			bh := healthyBehaviours()
			slowCount := agentCount("25% slow", 1, 4)
			for i := range slowCount {
				bh.byBroker[i] = brokerBehaviour{latencyFn: highLatency}
			}
			return scenario{
				name:        "25% slow agents",
				description: "A quarter of agents are permanently slow (avg 1.5s, max 3s); the hedge fallback should steer most traffic onto the healthy majority.",
				behaviours:  bh,
				// Every event fans out to all 50 partitions, so it
				// deterministically touches all 12 of the slow quarter every
				// time; the event's own latency is the max across all 50
				// legs. With 12 independent hedge races per event, the
				// fixed ~1s hedge delay (MinHedgeDelay) before the fallback
				// is raced already puts wgo's own p50 above 2s, so a 2s
				// budget can't discriminate here the way it does for the
				// 1/2-slow-agent scenarios (measured: wgo mean 2.1s, p50
				// 2.1s, p99 2.8s vs kgo mean 2.5s, p99 3.0s — a real but
				// modest gap, not the order-of-magnitude difference seen
				// with only 1-2 slow agents). Budget above wgo's own p50 so
				// the assertion is "the fallback race still keeps most
				// events well clear of the ~3s unhedged worst case", not an
				// unreachable bar every run would fail on the median alone.
				expect: scenarioExpectations{
					minWgoSuccessRate:  1.0,
					slowBudget:         ptr(2500 * time.Millisecond),
					maxWgoSlowFraction: ptr(0.35),
				},
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
			for i := range badCount {
				bh.byBroker[i] = brokerBehaviour{latencyFn: gcsBad}
			}
			for i := badCount; i < badCount+modCount; i++ {
				bh.byBroker[i] = brokerBehaviour{latencyFn: gcsModerate}
			}
			return scenario{
				name: "GCS slow outage (10% bad, 40% moderate)",
				description: "Reproduces an incident where object storage degraded asymmetrically: 50% healthy, 40% avg ≈ 700ms/max ≈ 4s, 10% avg ≈ 2.65s/max ≈ 10s. " +
					"We expect a large initial failure spike; the Demoter should kick in within ~30s and reroute away from the worst offenders. The threshold is intentionally loose.",
				behaviours: bh,
				// The absolute floor is deliberately loose (this is an
				// incident scenario, not a clean pass/fail), and a
				// non-rerouting client can still clear it by chance — the
				// delta vs. kgo (measured in the same run) is what proves
				// the Demoter/Hedger actually helped here.
				expect: scenarioExpectations{minWgoSuccessRate: 0.20, minSuccessDeltaVsKgo: ptr(0.15)},
			}
		}(),
	}
}
