package main

import (
	"testing"
	"time"

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
		{sc: scenario{name: "ok", expect: scenarioExpectations{minWgoSuccessRate: 0.9}}, wgoSummary: observationsSummary{total: 10, successes: 10}},
		{sc: scenario{name: "bad", expect: scenarioExpectations{minWgoSuccessRate: 0.9}}, wgoSummary: observationsSummary{total: 10, successes: 5}},
	}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "bad", failures[0].scenario)
	assert.Contains(t, failures[0].detail, "50.0%")
	assert.Contains(t, failures[0].detail, "90.0%")
}

func TestScenarioResults_Check_SlowFraction(t *testing.T) {
	t.Parallel()
	sr := scenarioResults{entries: []scenarioResult{{
		sc: scenario{name: "too slow", expect: scenarioExpectations{
			minWgoSuccessRate:  1.0,
			slowBudget:         ptr(2 * time.Second),
			maxWgoSlowFraction: ptr(0.1),
		}},
		wgoSummary:      observationsSummary{total: 10, successes: 10},
		wgoSlowFraction: ptr(0.5),
	}}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "too slow", failures[0].scenario)
	assert.Contains(t, failures[0].detail, "50.0%")
	assert.Contains(t, failures[0].detail, "10.0%")
}

func TestScenarioResults_Check_DeltaVsKgo(t *testing.T) {
	t.Parallel()
	sr := scenarioResults{entries: []scenarioResult{{
		sc:         scenario{name: "no better than kgo", expect: scenarioExpectations{minWgoSuccessRate: 0.5, minSuccessDeltaVsKgo: ptr(0.5)}},
		wgoSummary: observationsSummary{total: 10, successes: 6},
		kgoSummary: observationsSummary{total: 10, successes: 5},
	}}}
	failures := sr.check()
	require.Len(t, failures, 1)
	assert.Equal(t, "no better than kgo", failures[0].scenario)
}
