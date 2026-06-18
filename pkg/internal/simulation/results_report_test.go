package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResultsReport_FormatDuration(t *testing.T) {
	t.Parallel()
	rr := &resultsReport{}
	cases := map[string]time.Duration{
		"0.0s": 0,
		"0.5s": 500 * time.Millisecond,
		"1.2s": 1234 * time.Millisecond,
		"4.6s": 4600 * time.Millisecond,
	}
	for want, in := range cases {
		assert.Equal(t, want, rr.formatDuration(in), "input %v", in)
	}
}

func TestResultsReport_SummaryLineNaWhenEmpty(t *testing.T) {
	t.Parallel()
	assert.Contains(t, (&resultsReport{}).summaryLine(observationsSummary{}), "success=n/a")
}

func TestResultsReport_HedgeSurgePct(t *testing.T) {
	t.Parallel()
	rr := &resultsReport{}
	assert.Equal(t, "n/a", rr.hedgeSurgePct(0, 5))
	assert.Equal(t, "+10.0%", rr.hedgeSurgePct(100, 10))
}

func TestResultsReport_WriteErrorTable(t *testing.T) {
	t.Parallel()
	rr := &resultsReport{}

	t.Run("empty when no errors", func(t *testing.T) {
		var b strings.Builder
		rr.writeErrorTable(&b, scenarioResult{wgoSummary: observationsSummary{total: 10}, kgoSummary: observationsSummary{total: 10}})
		assert.Empty(t, b.String())
	})

	t.Run("rate is per client over its own total", func(t *testing.T) {
		var b strings.Builder
		rr.writeErrorTable(&b, scenarioResult{
			wgoSummary:     observationsSummary{total: 120},
			kgoSummary:     observationsSummary{total: 120},
			wgoErrorCounts: map[string]int{"boom": 1},
			kgoErrorCounts: map[string]int{"context deadline exceeded": 120},
		})
		out := b.String()
		assert.Contains(t, out, "| error | wgo | kgo |")
		assert.Contains(t, out, "| boom | 0.8% | 0.0% |")
		assert.Contains(t, out, "| context deadline exceeded | 0.0% | 100.0% |")
	})
}

func TestResultsReport_GenerateMarkdown(t *testing.T) {
	t.Parallel()
	rr := newResultsReport(scenarioResults{entries: []scenarioResult{
		{
			sc:           scenario{name: "all healthy", description: "everything fine", expectedMinSuccessRate: 1.0},
			wgoSummary:   observationsSummary{total: 10, successes: 10, p99Latency: 900 * time.Millisecond},
			kgoSummary:   observationsSummary{total: 10, successes: 10, p99Latency: 900 * time.Millisecond},
			totalPrimary: 100,
			totalHedge:   0,
		},
	}})
	md := rr.generateMarkdown()
	assert.Contains(t, md, "# wgo vs kgo simulation report")
	assert.Contains(t, md, "## Summary")
	assert.Contains(t, md, "| Scenario | wgo success | kgo success |")
	assert.Contains(t, md, "## all healthy")
	assert.Contains(t, md, "everything fine")
}
