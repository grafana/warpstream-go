package runner

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

// fractionMargin is how far, as a share of agents, a scenario sits from its limit.
const fractionMargin = 0.05

// The Mimir harness hedges after the library default and the direct harness after
// its own setting. A latency that must stay below hedging derives from the shorter
// delay, and one that must be hedged from the longer.
const (
	shortestHedgeDelay = min(wgo.DefaultHedgerMinHedgeDelay, wgoHedgerMinHedgeDelay)
	longestHedgeDelay  = max(wgo.DefaultHedgerMinHedgeDelay, wgoHedgerMinHedgeDelay)

	moderateHealthyLatency = shortestHedgeDelay / 10
	moderateSlowLatency    = shortestHedgeDelay * 4 / 5

	// A barely hedged slow agent answers just after the hedge delay but before a
	// hedge to a healthy agent could, so the slow primary wins and is never
	// cancelled. Its connection and its latency samples survive.
	barelyHedgedHealthyLatency = shortestHedgeDelay / 8
	barelyHedgedSlowLatency    = shortestHedgeDelay + barelyHedgedHealthyLatency/2

	// A stalled agent answers after the application deadline but before the
	// produce wire timeout: kgo misses the deadline, and a hedge can still rescue
	// the write.
	stalledSlowLatency = (appRequestTimeout + clientProduceRequestTimeout) / 2
)

const (
	suppressedSlowFraction   = "slow_fraction_exceeded"
	suppressedFaultyFraction = "faulty_fraction_exceeded"
)

func fixedSettingsDescription() string {
	return fmt.Sprintf("Fixed settings, identical for the below and above scenarios of a pair. Mimir harness wgo (library defaults): slow multiplier %.1fx, max slow fraction %.0f%%, faulty threshold %.0f%%, max faulty fraction %.0f%%, hedge min delay %s, max hedge agents %d, demoter probe interval %s. "+
		"Direct harness wgo: slow multiplier %.1fx, max slow fraction %.0f%%, faulty threshold %.0f%%, max faulty fraction %.0f%%, hedge min delay %s, max hedge agents %d, demoter probe interval %s. "+
		"Both harnesses: %d agents, write timeout %s, request timeout overhead %s, dial timeout %s.",
		wgo.DefaultHealthCheckSlowMultiplier, 100*wgo.DefaultHealthCheckMaxSlowFraction, 100*wgo.DefaultHealthCheckFaultyThreshold, 100*wgo.DefaultHealthCheckMaxFaultyFraction, wgo.DefaultHedgerMinHedgeDelay, wgo.DefaultHedgerMaxHedgeAgents, wgo.DefaultDemoterProbeInterval,
		wgoHealthCheckSlowMultiplier, 100*wgoHealthCheckMaxSlowFraction, 100*wgoHealthCheckFaultyThreshold, 100*wgoHealthCheckMaxFaultyFraction, wgoHedgerMinHedgeDelay, wgoHedgerMaxHedgeAgents, wgoDemoterProbeInterval,
		clusterSize, clientWriteTimeout, clientRequestTimeoutOverhead, clientDialTimeout)
}

// fractionGate is a health-check limit on the share of agents. Its scenario pair
// injects the same fault just below the limit, where wgo still acts on it, and
// just above, where it stops. The below scenario is the control for the above one.
type fractionGate struct {
	// splitWrites makes a Mimir event one write per series, so the report counts
	// successful writes as well as events.
	splitWrites          bool
	fault                string  // names the scenario
	subject              string  // replaces "<fault> agents" in the name when set
	limitName            string  // the limit's parameter name in wgo's health-check config
	limit                float64 // the share of agents at which the gate trips
	does                 string  // what each affected agent does
	belowEffect          string
	aboveEffect          string
	tail                 func(share float64) string // extra description for this fault
	behaviours           func(count int32) brokersBehaviour
	expect               func(above bool) scenarioExpectations
	valid                func(share float64) error // rejects a share at which the fault would not behave as described
	suppression          string                    // the hedge-suppression reason of this gate; empty for a survival comparison
	forbiddenSuppression []string                  // reasons of other gates; one firing means the fault was not isolated
}

var (
	slowFractionGate = fractionGate{
		fault: "moderately slow", limitName: "MaxSlowFraction", limit: wgo.DefaultHealthCheckMaxSlowFraction,
		does:        "answer every Produce in " + moderateSlowLatency.String() + " while the rest answer in " + moderateHealthyLatency.String(),
		belowEffect: "slow-based hedging stays enabled",
		aboveEffect: "slow-based hedging is switched off (observed: slow_fraction_exceeded)",
		tail: func(float64) string {
			return "Healthy agents answer in " + moderateHealthyLatency.String() + ", so " + moderateSlowLatency.String() + " counts as slow yet stays below the " + shortestHedgeDelay.String() +
				" hedge delay. Nothing is hedged or cancelled, so no difference in outcome is expected: the pair shows that the gate trips, not a loss."
		},
		behaviours: func(count int32) brokersBehaviour {
			return slowBehaviours(count, moderateHealthyLatency, moderateSlowLatency)
		},
		expect: func(bool) scenarioExpectations { return scenarioExpectations{minWgoSuccessRate: 1.0} },
		valid: func(share float64) error {
			if moderateSlowLatency <= time.Duration(float64(moderateHealthyLatency)*requiredSlowRatio(share)) {
				return fmt.Errorf("%s is not slow enough against %s at %.0f%% slow agents to count as slow", moderateSlowLatency, moderateHealthyLatency, 100*share)
			}
			return nil
		},
		suppression:          suppressedSlowFraction,
		forbiddenSuppression: []string{suppressedFaultyFraction},
	}
	barelyHedgedSlowFractionGate = fractionGate{
		fault: "barely hedged slow", limitName: "MaxSlowFraction", limit: wgo.DefaultHealthCheckMaxSlowFraction,
		does:        "answer every Produce in " + barelyHedgedSlowLatency.String() + " while the rest answer in " + barelyHedgedHealthyLatency.String(),
		belowEffect: "wgo still hedges these writes after the " + shortestHedgeDelay.String() + " hedge delay",
		aboveEffect: "hedging is switched off (observed: slow_fraction_exceeded)",
		tail: func(share float64) string {
			return fmt.Sprintf("%s is just past the %s hedge delay, so wgo hedges, but a hedge answers after %s, later than the slow primary. The primary wins, so no slow request is cancelled and the slow agents are not charged as faulty. The hedge that loses is cancelled on a healthy agent, which before #127 can close that agent's shared connection; the report shows it if that raises the faulty-fraction gate. "+
				"The slow agents count as slow at this share (%s against %s is %.1fx, and %.1fx is needed). Success should be the same on both sides; the difference is hedge surge, some below the limit and little above it. "+
				"The direct harness hedges only after %s, so it never hedges this latency.",
				barelyHedgedSlowLatency, shortestHedgeDelay, shortestHedgeDelay+barelyHedgedHealthyLatency, barelyHedgedSlowLatency, barelyHedgedHealthyLatency,
				float64(barelyHedgedSlowLatency)/float64(barelyHedgedHealthyLatency), requiredSlowRatio(share), longestHedgeDelay)
		},
		behaviours: func(count int32) brokersBehaviour {
			return slowBehaviours(count, barelyHedgedHealthyLatency, barelyHedgedSlowLatency)
		},
		expect: func(bool) scenarioExpectations { return scenarioExpectations{minWgoSuccessRate: 1.0} },
		valid: func(share float64) error {
			baseline := time.Duration(share*float64(barelyHedgedSlowLatency) + (1-share)*float64(barelyHedgedHealthyLatency))
			switch {
			case float64(barelyHedgedSlowLatency) <= requiredSlowRatio(share)*float64(barelyHedgedHealthyLatency):
				return fmt.Errorf("slow agents at %.0f%% would not count as slow", 100*share)
			case barelyHedgedSlowLatency <= shortestHedgeDelay:
				return fmt.Errorf("%s is not past the hedge delay, so nothing would be hedged", barelyHedgedSlowLatency)
			case barelyHedgedSlowLatency >= shortestHedgeDelay+barelyHedgedHealthyLatency:
				return fmt.Errorf("%s would lose the race to a hedge, so the slow request would be cancelled", barelyHedgedSlowLatency)
			case baseline > shortestHedgeDelay:
				return fmt.Errorf("the mean latency %s would stretch the hedge delay past %s", baseline, shortestHedgeDelay)
			}
			return nil
		},
		suppression:          suppressedSlowFraction,
		forbiddenSuppression: []string{suppressedFaultyFraction},
	}
	faultyFractionGate = fractionGate{
		fault: "failing", limitName: "MaxFaultyFraction", limit: wgo.DefaultHealthCheckMaxFaultyFraction,
		does:        "return NotLeaderForPartition immediately",
		belowEffect: "failing agents are still demoted",
		aboveEffect: "failing agents are no longer demoted (a cascade only succeeds if it reaches a healthy agent within three candidates) and wgo is expected to collapse",
		behaviours: func(count int32) brokersBehaviour {
			ids := make([]int32, count)
			for i := range ids {
				ids[i] = int32(i)
			}
			return failingBehavioursFor(ids...)
		},
		expect: func(above bool) scenarioExpectations {
			if above {
				// A probe of the limit: wgo is expected to collapse here, so there is no floor.
				return scenarioExpectations{}
			}
			return scenarioExpectations{minWgoSuccessRate: 1.0, minSuccessDeltaVsKgo: ptr(0.5)}
		},
		suppression: suppressedFaultyFraction,
	}
)

// requiredSlowRatio is how many times slower than the healthy agents a share of
// slow agents must be to count as slow: the baseline is the mean of the agents'
// latencies, which the slow agents raise themselves.
func requiredSlowRatio(share float64) float64 {
	headroom := 1 - wgo.DefaultHealthCheckSlowMultiplier*share
	if headroom <= 0 {
		// The slow agents lift the mean so far that none can exceed a multiple of it.
		return math.Inf(1)
	}
	return wgo.DefaultHealthCheckSlowMultiplier * (1 - share) / headroom
}

func stalledFastFirstFallbackGate(runID int) fractionGate {
	return stalledAgentsGate("stalled agents with a fast first fallback", fastFirstFallback, runID)
}

func stalledFastSecondFallbackGate(runID int) fractionGate {
	return stalledAgentsGate("stalled agents with a fast second fallback", fastSecondFallback, runID)
}

// stalledAgentsGate builds a gate for agents that answer after the application
// deadline, with the stalled agents placed by rule so that every partition has a
// way out. The agents are fixed for a run id, so every build in a run meets the
// same chains; other run ids place them differently.
func stalledAgentsGate(subject string, rule stalledRule, runID int) fractionGate {
	hedgeWaves := 1
	belowEffect := "wgo races each stalled write against its fast first fallback, which every build does, so writes finish about one hedge delay after they start"
	tail := func(ids []int32, share float64) string {
		return fmt.Sprintf("%s is past the %s application deadline, so kgo misses it on every event that reaches a stalled agent, and under the %s produce wire timeout, so nothing times out. "+
			"Run id %d places the stalled agents at %v so that every stalled partition has a fast first fallback. Below the limit wgo should finish near 100%% while kgo fails the events that reach a stalled agent, nearly all of them; above it hedging stops and both fail.",
			stalledSlowLatency, appRequestTimeout, clientProduceRequestTimeout, runID, ids)
	}
	if rule == fastSecondFallback {
		hedgeWaves = 2
		belowEffect = "a client that races another candidate when a fallback stalls reaches the fast second fallback in time, and the others are expected to miss the deadline on the partitions whose first fallback is stalled"
		tail = func(ids []int32, share float64) string {
			return fmt.Sprintf("%s is past the %s application deadline, so kgo misses it on every event that reaches a stalled agent, and under the %s produce wire timeout, so nothing times out. "+
				"Run id %d places the stalled agents at %v so that no partition has its whole chain stalled and at least %d stalled partitions have a stalled first fallback and a fast second one. "+
				"Only a client that races another candidate on a timer when a fallback stalls (#128) reaches the second fallback in time, about two hedge delays after the start. Builds without it are expected to fail the events that reach those partitions, nearly all of them: a split event sends 50 writes hashed over the ring, so a rare event can miss them.",
				stalledSlowLatency, appRequestTimeout, clientProduceRequestTimeout, runID, ids, minNeedingSecondFallback)
		}
	}
	return fractionGate{
		splitWrites: true,
		subject:     subject, fault: "stalled", limitName: "MaxSlowFraction", limit: wgo.DefaultHealthCheckMaxSlowFraction,
		does:        "answer every Produce after " + stalledSlowLatency.String() + " while the rest answer in " + moderateHealthyLatency.String(),
		belowEffect: belowEffect,
		aboveEffect: "hedging is switched off (observed: slow_fraction_exceeded), so every stalled write waits out its stall and the events that reach a stalled agent miss the deadline, as for kgo",
		tail: func(share float64) string {
			ids, _ := findStalledAgents(int32(math.Round(share*float64(clusterSize))), rule, runID)
			return tail(ids, share)
		},
		behaviours: func(count int32) brokersBehaviour {
			ids := pickStalledAgents(count, rule, runID)
			bh := slowBehavioursAt(ids, moderateHealthyLatency, constantLatency(stalledSlowLatency))
			// Counting the delayed responses per agent puts the placement in the report.
			for _, id := range ids {
				behaviour := bh.byBroker[id]
				behaviour.injectedLatency = true
				bh.byBroker[id] = behaviour
			}
			return bh
		},
		expect: func(above bool) scenarioExpectations {
			if above {
				return scenarioExpectations{}
			}
			return scenarioExpectations{minWgoSuccessRate: 1.0, minSuccessDeltaVsKgo: ptr(0.5)}
		},
		valid: func(share float64) error {
			baseline := time.Duration(share*float64(stalledSlowLatency) + (1-share)*float64(moderateHealthyLatency))
			hedgeDelay := max(baseline, longestHedgeDelay)
			finishes := time.Duration(hedgeWaves)*hedgeDelay + moderateHealthyLatency
			switch {
			case stalledSlowLatency <= appRequestTimeout:
				return fmt.Errorf("%s does not exceed the %s application deadline, so kgo would not fail", stalledSlowLatency, appRequestTimeout)
			case stalledSlowLatency >= clientProduceRequestTimeout:
				return fmt.Errorf("%s reaches the %s produce wire timeout, so requests would time out", stalledSlowLatency, clientProduceRequestTimeout)
			case float64(stalledSlowLatency) <= requiredSlowRatio(share)*float64(moderateHealthyLatency):
				return fmt.Errorf("stalled agents at %.0f%% would not count as slow", 100*share)
			case finishes > appRequestTimeout-appRequestTimeout/20:
				return fmt.Errorf("a rescue after %d hedge delays of %s finishes at %s, too close to the %s deadline", hedgeWaves, hedgeDelay, finishes, appRequestTimeout)
			}
			_, err := findStalledAgents(int32(math.Round(share*float64(clusterSize))), rule, runID)
			return err
		},
		suppression:          suppressedSlowFraction,
		forbiddenSuppression: []string{suppressedFaultyFraction},
	}
}

// name carries no numbers, so it stays stable when a limit or a share changes
// and historical results stay comparable. The description holds the values.
func (g fractionGate) name(above bool) string {
	side := "below"
	if above {
		side = "above"
	}
	subject := g.subject
	if subject == "" {
		subject = g.fault + " agents"
	}
	return fmt.Sprintf("%s just %s %s", subject, side, g.limitName)
}

// agents returns how many agents carry the fault: the share fractionMargin
// below the limit, or above it. It panics if rounding to whole agents puts the
// count on the wrong side of the limit or leaves no healthy or no affected agent.
func (g fractionGate) agents(above bool) int32 {
	target := g.limit - fractionMargin
	if above {
		target = g.limit + fractionMargin
	}
	n := int32(target * float64(clusterSize))
	share := float64(n) / float64(clusterSize)
	if n <= 0 || n >= clusterSize || (share > g.limit) != above {
		panic(fmt.Sprintf("simulation: %s %.2f gives %d of %d agents for the %s scenario", g.limitName, g.limit, n, clusterSize, g.name(above)))
	}
	if g.valid != nil {
		if err := g.valid(share); err != nil {
			panic(fmt.Sprintf("simulation: %s: %v", g.name(above), err))
		}
	}
	return n
}

func (g fractionGate) description(above bool) string {
	side, effect, pair := "below", g.belowEffect, g.name(true)
	if above {
		side, effect, pair = "above", g.aboveEffect, g.name(false)
	}
	count := g.agents(above)
	d := fmt.Sprintf("Threshold probe for wgo's %s (%.0f%% of agents). %d of %d agents (%.0f%%, about %.0f points %s the limit) %s, so %s. Its pair is %q, the same fault on the other side of the limit. "+
		"The share is derived from the limit.",
		g.limitName, 100*g.limit, count, clusterSize, 100*float64(count)/float64(clusterSize), 100*fractionMargin, side, g.does, effect, pair)
	if g.tail != nil {
		d += " " + g.tail(float64(count)/float64(clusterSize))
	}
	return d + " " + fixedSettingsDescription()
}

// fractionScenario builds one half of a pair: the gate's fault on the share of
// agents just below its limit, or just above it when above is set.
func fractionScenario(g fractionGate, above bool) scenario {
	sc := scenario{name: g.name(above), collectClientEvidence: true, splitWrites: g.splitWrites, expect: g.expect(above), description: g.description(above), behaviours: g.behaviours(g.agents(above))}
	if g.suppression == "" {
		// A survival scenario: the report lists the gates that acted, none is required.
		return sc
	}
	sc.forbidSuppression = g.forbiddenSuppression
	if above {
		sc.expectedHedgeStop = g.suppression
		sc.requireSuppression = []string{g.suppression}
	} else {
		sc.forbidSuppression = append(slices.Clone(g.forbiddenSuppression), g.suppression)
	}
	return sc
}

// slowBehaviours makes the first count agents answer in latency and the rest in
// healthy; slowBehavioursFn draws the slow agents' latency instead.
func slowBehaviours(count int32, healthy, latency time.Duration) brokersBehaviour {
	return slowBehavioursFn(count, healthy, constantLatency(latency))
}

func slowBehavioursFn(count int32, healthy time.Duration, latency func(*rand.Rand) time.Duration) brokersBehaviour {
	ids := make([]int32, count)
	for i := range ids {
		ids[i] = int32(i)
	}
	return slowBehavioursAt(ids, healthy, latency)
}

// slowBehavioursAt makes the agents in ids answer with latency and the rest in healthy.
func slowBehavioursAt(ids []int32, healthy time.Duration, latency func(*rand.Rand) time.Duration) brokersBehaviour {
	bh := healthyBehaviours()
	for i := range int32(clusterSize) {
		bh.byBroker[i] = brokerBehaviour{latencyFn: constantLatency(healthy)}
	}
	for _, id := range ids {
		bh.byBroker[id] = brokerBehaviour{latencyFn: latency}
	}
	return bh
}
