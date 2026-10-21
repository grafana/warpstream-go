package runner

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

func scenarioNamed(t *testing.T, name string) scenario {
	t.Helper()
	for _, sc := range scenarios() {
		if sc.name == name {
			return sc
		}
	}
	require.FailNow(t, "scenario not found", name)
	return scenario{}
}

func TestFractionScenarios_PairsShareSettings(t *testing.T) {
	t.Parallel()
	for _, pair := range []struct {
		gate     fractionGate
		affected func(brokerBehaviour) bool
	}{
		{slowFractionGate, func(b brokerBehaviour) bool {
			return b.latencyFn != nil && b.latencyFn(rand.New(rand.NewPCG(1, 1))) == moderateSlowLatency
		}},
		{barelyHedgedSlowFractionGate, func(b brokerBehaviour) bool {
			return b.latencyFn != nil && b.latencyFn(rand.New(rand.NewPCG(1, 1))) == barelyHedgedSlowLatency
		}},
		{stalledSlowFractionGate, func(b brokerBehaviour) bool {
			return b.latencyFn != nil && b.latencyFn(rand.New(rand.NewPCG(1, 1))) == stalledSlowLatency
		}},
		{faultyFractionGate, func(b brokerBehaviour) bool { return b.failRate == 1 }},
	} {
		low, high := scenarioNamed(t, pair.gate.name(false)), scenarioNamed(t, pair.gate.name(true))
		count := func(sc scenario) (n int) {
			for _, b := range sc.behaviours.byBroker {
				if pair.affected(b) {
					n++
				}
			}
			return n
		}
		for _, sc := range []scenario{low, high} {
			require.True(t, sc.collectClientEvidence, sc.name)
			require.True(t, strings.HasSuffix(sc.description, fixedSettingsDescription()), sc.name)
		}
		require.EqualValues(t, pair.gate.agents(false), count(low), pair.gate.fault)
		require.EqualValues(t, pair.gate.agents(true), count(high), pair.gate.fault)
		// The names promise one share below the limit and one above it.
		require.Less(t, float64(count(low))/float64(clusterSize), pair.gate.limit)
		require.Greater(t, float64(count(high))/float64(clusterSize), pair.gate.limit)
		require.Contains(t, low.description, fmt.Sprintf("%d of %d agents", count(low), clusterSize))
		require.Contains(t, high.description, fmt.Sprintf("%d of %d agents", count(high), clusterSize))
		require.Contains(t, low.description, fmt.Sprintf("(%.0f%% of agents)", 100*pair.gate.limit))
	}
}

func TestFractionScenarios_DirectHarnessProbesTheSameLimits(t *testing.T) {
	t.Parallel()
	// The scenarios derive their shares from the library defaults, which the
	// Mimir harness runs with, so the direct harness has to use the same limits.
	require.Equal(t, wgo.DefaultHealthCheckMaxSlowFraction, wgoHealthCheckMaxSlowFraction)
	require.Equal(t, wgo.DefaultHealthCheckMaxFaultyFraction, wgoHealthCheckMaxFaultyFraction)
}

func TestFractionGate_Agents(t *testing.T) {
	t.Parallel()
	for _, limit := range []float64{0.2, 0.3, 0.4, 0.5} {
		g := fractionGate{limitName: "test", limit: limit}
		below, above := g.agents(false), g.agents(true)
		require.Less(t, float64(below)/float64(clusterSize), limit)
		require.Greater(t, float64(above)/float64(clusterSize), limit)
		require.InDelta(t, limit-fractionMargin, float64(below)/float64(clusterSize), 1/float64(clusterSize))
		require.InDelta(t, limit+fractionMargin, float64(above)/float64(clusterSize), 1/float64(clusterSize))
	}
	// No agent would be left healthy, or none affected.
	require.Panics(t, func() { fractionGate{limitName: "test", limit: 0.98}.agents(true) })
	require.Panics(t, func() { fractionGate{limitName: "test", limit: 0.02}.agents(false) })
}

func TestFractionScenarios_SuppressionChecks(t *testing.T) {
	t.Parallel()
	below := scenarioNamed(t, barelyHedgedSlowFractionGate.name(false))
	above := scenarioNamed(t, barelyHedgedSlowFractionGate.name(true))
	evidence := func(suppression map[string]int64) *clientEvidence {
		return &clientEvidence{HedgeSuppression: suppression}
	}
	details := func(sc scenario, e *clientEvidence) (out []string) {
		for _, failure := range checkResult(scenarioResult{sc: sc, clientEvidence: e}) {
			out = append(out, failure.detail)
		}
		return out
	}
	contains := func(details []string, substring string) bool {
		return slices.ContainsFunc(details, func(d string) bool { return strings.Contains(d, substring) })
	}

	// Only the slow gate may act, and above the limit it has to.
	require.Contains(t, above.requireSuppression, suppressedSlowFraction)
	require.Contains(t, above.forbidSuppression, suppressedFaultyFraction)
	require.Contains(t, below.forbidSuppression, suppressedSlowFraction)
	require.Contains(t, below.forbidSuppression, suppressedFaultyFraction)
	require.False(t, contains(details(above, evidence(map[string]int64{suppressedSlowFraction: 10})), "suppress"))
	require.True(t, contains(details(above, evidence(map[string]int64{})), `never suppressed with reason "slow_fraction_exceeded"`))
	require.True(t, contains(details(above, evidence(map[string]int64{suppressedSlowFraction: 10, suppressedFaultyFraction: 3})), `"faulty_fraction_exceeded"`))
	require.True(t, contains(details(below, evidence(map[string]int64{suppressedSlowFraction: 1})), `"slow_fraction_exceeded"`))
	require.False(t, contains(details(below, evidence(map[string]int64{})), "suppress"))

	// The failing pair has to trip its own gate above the limit.
	require.Contains(t, scenarioNamed(t, faultyFractionGate.name(true)).requireSuppression, suppressedFaultyFraction)
}

func TestFractionGate_LatenciesStayDerivedFromHedgeDelays(t *testing.T) {
	t.Parallel()
	// The moderately slow agents never reach any hedge delay.
	require.Less(t, moderateSlowLatency, shortestHedgeDelay)
	require.Less(t, moderateHealthyLatency, moderateSlowLatency)
}

func TestFractionScenarios_StalledSlowAgentsDefeatOnlyAClientThatDoesNotHedge(t *testing.T) {
	t.Parallel()
	// kgo misses the application deadline, nothing times out on the wire.
	require.Greater(t, stalledSlowLatency, appRequestTimeout)
	require.Less(t, stalledSlowLatency, clientProduceRequestTimeout)
	below := scenarioNamed(t, stalledSlowFractionGate.name(false))
	above := scenarioNamed(t, stalledSlowFractionGate.name(true))
	// No gate is required: this pair compares survival, the report lists the gates.
	require.Empty(t, below.requireSuppression)
	require.Empty(t, above.requireSuppression)
	require.NotNil(t, below.expect.minSuccessDeltaVsKgo)
	require.NotNil(t, above.expect.minSuccessDeltaVsKgo)
	require.Greater(t, *below.expect.minSuccessDeltaVsKgo, *above.expect.minSuccessDeltaVsKgo)
}
