package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
