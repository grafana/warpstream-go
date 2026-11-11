package runner

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintSummary_PrintsMissedExpectationDetails(t *testing.T) {
	t.Parallel()
	r := scenarioResults{entries: []scenarioResult{
		{
			sc:         scenario{name: "all healthy", expect: scenarioExpectations{minWgoSuccessRate: 1.0}},
			wgoSummary: observationsSummary{total: 10, successes: 10},
		},
		{
			sc: scenario{name: "1 slow agent", expect: scenarioExpectations{
				minWgoSuccessRate:  1.0,
				slowBudget:         ptr(2 * time.Second),
				maxWgoSlowFraction: ptr(0.10),
			}},
			wgoSummary:      observationsSummary{total: 10, successes: 10},
			wgoSlowFraction: ptr(0.5),
		},
	}}

	var buf bytes.Buffer
	printSummary(&buf, r)
	out := buf.String()

	assert.Contains(t, out, "[PASS] all healthy")
	assert.Contains(t, out, "100.0% (want >= 100.0%)")
	assert.Contains(t, out, "[FAIL] 1 slow agent")
	assert.Contains(t, out, "wgo fraction of requests slower than 2s is 50.0%, above the 10.0% ceiling")
}

func TestSelectScenarios_Filter(t *testing.T) {
	selected, err := selectScenarios("standard", "^25% (dropped leaders|slow agents)$", 1, 1, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"25% dropped leaders", "25% slow agents"}, scenarioNames(selected))
}

func TestSelectScenarios_DefaultSelectsEveryScenario(t *testing.T) {
	for _, tc := range []struct {
		profile  string
		external bool
	}{{"standard", false}, {"all", true}} {
		t.Run(tc.profile, func(t *testing.T) {
			available, err := profileScenarios(tc.profile, tc.external)
			require.NoError(t, err)
			selected, err := selectScenarios(tc.profile, "", 1, 1, tc.external)
			require.NoError(t, err)
			assert.Equal(t, scenarioNames(available), scenarioNames(selected))
		})
	}
}

func TestSelectScenarios_InvalidPattern(t *testing.T) {
	_, err := selectScenarios("standard", "[", 1, 1, false)
	require.ErrorContains(t, err, "scenario filter")
}

func TestSelectScenarios_NoMatches(t *testing.T) {
	_, err := selectScenarios("standard", "not a scenario", 1, 1, false)
	require.ErrorContains(t, err, `no scenarios match "not a scenario"`)
}

func TestSelectScenarios_FiltersBeforeBatching(t *testing.T) {
	available, err := profileScenarios("mimir-investigation", true)
	require.NoError(t, err)
	selected, err := selectScenarios("all", "^investigation: ", 2, 2, true)
	require.NoError(t, err)
	assert.Equal(t, []string{available[1].name, available[3].name}, scenarioNames(selected))
}

func TestPrintScenarioNames_OneNamePerLine(t *testing.T) {
	var buf bytes.Buffer
	printScenarioNames(&buf, []scenario{{name: "all healthy"}, {name: "25% slow agents"}})
	assert.Equal(t, "all healthy\n25% slow agents\n", buf.String())
}

func scenarioNames(scs []scenario) []string {
	names := make([]string, len(scs))
	for i, sc := range scs {
		names[i] = sc.name
	}
	return names
}
