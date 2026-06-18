package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScenarioResult_SuccessRate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0.0, scenarioResult{}.successRate())
	r := scenarioResult{wgoSummary: observationsSummary{total: 4, successes: 3}}
	assert.Equal(t, 0.75, r.successRate())
}

func TestScenarioResults_Check(t *testing.T) {
	t.Parallel()
	sr := scenarioResults{entries: []scenarioResult{
		{sc: scenario{name: "ok", expectedMinSuccessRate: 0.9}, wgoSummary: observationsSummary{total: 10, successes: 10}},
		{sc: scenario{name: "bad", expectedMinSuccessRate: 0.9}, wgoSummary: observationsSummary{total: 10, successes: 5}},
	}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "bad", failures[0].name)
	assert.Equal(t, 0.5, failures[0].got)
	assert.Equal(t, 0.9, failures[0].want)
}
