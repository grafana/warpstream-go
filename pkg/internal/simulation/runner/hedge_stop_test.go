package runner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHedgeStop_Verdict(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		expected   string
		suppressed map[string]int64
		verdict    string
		survival   bool
	}{
		"survival scenario lists gates only": {"", map[string]int64{suppressedFaultyFraction: 7}, "informational", true},
		"expected gate alone":                {expected: suppressedSlowFraction, suppressed: map[string]int64{suppressedSlowFraction: 10}, verdict: "as expected"},
		"expected gate and another":          {expected: suppressedSlowFraction, suppressed: map[string]int64{suppressedSlowFraction: 10, suppressedFaultyFraction: 4}, verdict: "mixed"},
		"only a different gate":              {expected: suppressedSlowFraction, suppressed: map[string]int64{suppressedFaultyFraction: 4}, verdict: "differs"},
		"expected gate never acted":          {expected: suppressedSlowFraction, suppressed: map[string]int64{"no_agent_stats": 9}, verdict: "differs"},
		"no gate expected, none acted":       {expected: "", suppressed: map[string]int64{"no_agent_stats": 9}, verdict: "as expected"},
		"no gate expected, one acted":        {expected: "", suppressed: map[string]int64{suppressedFaultyFraction: 1}, verdict: "differs"},
		"startup suppression is no gate":     {expected: suppressedFaultyFraction, suppressed: map[string]int64{suppressedFaultyFraction: 2, "no_cluster_stats": 50}, verdict: "as expected"},
	} {
		t.Run(name, func(t *testing.T) {
			sc := scenario{expectedHedgeStop: tc.expected}
			if !tc.survival {
				// A scenario that isolates a gate always names the gates it forbids.
				sc.forbidSuppression = []string{suppressedFaultyFraction}
			}
			res := scenarioResult{sc: sc, clientEvidence: &clientEvidence{HedgeSuppression: tc.suppressed}}
			stop := res.hedgeStop()
			require.NotNil(t, stop)
			assert.Equal(t, tc.verdict, stop.Verdict)
		})
	}
	assert.Nil(t, scenarioResult{}.hedgeStop())
	assert.False(t, (*hedgeStop)(nil).worthReporting())
}

func TestHedgeStop_Reporting(t *testing.T) {
	t.Parallel()
	res := scenarioResult{
		sc:             scenario{name: "probe", expectedHedgeStop: suppressedSlowFraction},
		clientEvidence: &clientEvidence{HedgeSuppression: map[string]int64{suppressedSlowFraction: 75, suppressedFaultyFraction: 25}},
	}
	stop := res.hedgeStop()
	require.True(t, stop.worthReporting())
	assert.Equal(t, "Hedging stopped by: slow fraction 75 (75%), faulty fraction 25 (25%); expected slow_fraction_exceeded: mixed", stop.line())

	var b strings.Builder
	writeHedgeStopTable(&b, []hedgeStopRow{{"probe", "114/120 (95.0%)", "120/120 (100.0%)", stop}})
	// Survival comes first; the gates are the secondary signal.
	assert.Contains(t, b.String(), "| probe | 114/120 (95.0%) | 120/120 (100.0%) | slow_fraction_exceeded | 75 (75%) | 25 (25%) | mixed |")
	assert.Contains(t, b.String(), "| Scenario | wgo success | kgo success |")

	// A scenario that expects no gate and saw none is left out of the report.
	quiet := scenarioResult{sc: scenario{}, clientEvidence: &clientEvidence{HedgeSuppression: map[string]int64{"no_agent_stats": 3}}}
	assert.False(t, quiet.hedgeStop().worthReporting())
}

func TestFractionScenarios_BarelyHedgedSlowAgentsWinTheirRace(t *testing.T) {
	t.Parallel()
	// Past the hedge delay so wgo hedges, yet ahead of the hedge's own answer.
	require.Greater(t, barelyHedgedSlowLatency, shortestHedgeDelay)
	require.Less(t, barelyHedgedSlowLatency, shortestHedgeDelay+barelyHedgedHealthyLatency)
	require.Less(t, barelyHedgedSlowLatency, longestHedgeDelay)
	require.NotPanics(t, func() { barelyHedgedSlowFractionGate.agents(false); barelyHedgedSlowFractionGate.agents(true) })
	g := barelyHedgedSlowFractionGate
	g.limit = 0.49
	require.Panics(t, func() { g.agents(true) })
}
