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
	selected, err := selectScenarios("^25% (dropped leaders|slow agents)$")
	require.NoError(t, err)
	require.Len(t, selected, 2)
	assert.Equal(t, "25% dropped leaders", selected[0].name)
	assert.Equal(t, "25% slow agents", selected[1].name)
	_, err = selectScenarios("not a scenario")
	require.ErrorContains(t, err, "no scenarios match")
	_, err = selectScenarios("[")
	require.ErrorContains(t, err, "scenario filter")
}
