package runner

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Research scenarios expose behaviour that work in progress (#127 and #128) is
// expected to change, so they are named by their setup, not by a mechanism a fix
// would make wrong. The numbers are fixed on purpose and not derived from the
// defaults; a test checks that they still sit where the descriptions say.
const (
	researchSlowAgents     = 17
	researchHealthyLatency = 50 * time.Millisecond

	researchSpreadFastest = 250 * time.Millisecond
	researchSpreadSlowest = 3 * time.Second

	// The distribution of highLatency, spelled out so the name can state it.
	researchTriangularFast = 500 * time.Millisecond
	researchTriangularMode = time.Second
	researchTriangularSlow = 3 * time.Second

	researchKnownEffect = "Known effect before #127 and #128: cancelling a hedged slow request closes the shared connection and fails the requests queued behind it. Those failures are charged to the slow agent, so the faulty-fraction gate can act too."
)

func researchScenarios() []scenario {
	return []scenario{researchTriangularScenario(), researchSpreadScenario()}
}

func researchName(distribution string) string {
	return fmt.Sprintf("investigation: %d of %d agents (%.0f%%) answer %s", researchSlowAgents, clusterSize, 100*float64(researchSlowAgents)/float64(clusterSize), distribution)
}

// researchTriangularScenario is main's "25% slow agents" at a larger share, past
// MaxFaultyFraction. Every agent answers, so no gate should act and wgo should do
// as well as kgo.
func researchTriangularScenario() scenario {
	ids := make([]int32, researchSlowAgents)
	for i := range ids {
		ids[i] = int32(i)
	}
	mean := (researchTriangularFast + researchTriangularMode + researchTriangularSlow) / 3
	return scenario{
		name:                  researchName(fmt.Sprintf("in %s to %s, mean %s", researchTriangularFast, researchTriangularSlow, mean)),
		collectClientEvidence: true,
		behaviours:            highLatencyBehavioursFor(ids...),
		forbidSuppression:     []string{suppressedFaultyFraction},
		expect:                scenarioExpectations{minWgoSuccessRate: 1.0},
		description: fmt.Sprintf("%d of %d agents answer every Produce after a latency drawn from a triangular distribution between %s and %s, with its mode at %s. "+
			"This is the latency of the \"25%% slow agents\" scenario, which has 12 of the 50 agents. "+
			"No agent returns an error, so no health gate should act and wgo should do as well as kgo. %s %s",
			researchSlowAgents, clusterSize, researchTriangularFast, researchTriangularSlow, researchTriangularMode, researchKnownEffect, fixedSettingsDescription()),
	}
}

// researchSpreadScenario puts a share of agents above MaxSlowFraction on answers
// that run from well under the hedge delay to well over it.
func researchSpreadScenario() scenario {
	return scenario{
		name:                  researchName(fmt.Sprintf("uniformly in %s to %s", researchSpreadFastest, researchSpreadSlowest)),
		collectClientEvidence: true,
		behaviours:            slowBehavioursFn(researchSlowAgents, researchHealthyLatency, uniformLatency(researchSpreadFastest, researchSpreadSlowest)),
		forbidSuppression:     []string{suppressedFaultyFraction},
		requireSuppression:    []string{suppressedSlowFraction},
		expectedHedgeStop:     suppressedSlowFraction,
		expect:                scenarioExpectations{minWgoSuccessRate: 1.0},
		description: fmt.Sprintf("%d of %d agents (%.0f%%, above MaxSlowFraction) answer every Produce after a latency drawn evenly between %s and %s while the rest answer in %s. "+
			"Expected: wgo stops hedging because of MaxSlowFraction and nothing else, so writes wait out their slow agents as kgo's do. "+
			"The slowest answers are past the hedge delay and under the application deadline and the produce wire timeout. %s %s",
			researchSlowAgents, clusterSize, 100*float64(researchSlowAgents)/float64(clusterSize), researchSpreadFastest, researchSpreadSlowest, researchHealthyLatency, researchKnownEffect, fixedSettingsDescription()),
	}
}

// uniformLatency draws evenly from [lo, hi).
func uniformLatency(lo, hi time.Duration) func(*rand.Rand) time.Duration {
	return func(rng *rand.Rand) time.Duration { return lo + time.Duration(rng.Int64N(int64(hi-lo))) }
}
