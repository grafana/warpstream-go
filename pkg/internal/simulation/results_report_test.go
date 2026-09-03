package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/warpstream-go/pkg/wgo"
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
			sc:           scenario{name: "all healthy", description: "everything fine", expect: scenarioExpectations{minWgoSuccessRate: 1.0}},
			wgoSummary:   observationsSummary{total: 10, successes: 10, p99Latency: 900 * time.Millisecond},
			kgoSummary:   observationsSummary{total: 10, successes: 10, p99Latency: 900 * time.Millisecond},
			totalPrimary: 100,
			totalHedge:   0,
		},
	}})
	md := rr.generateMarkdown()
	assert.Contains(t, md, "# wgo vs kgo simulation report")
	assert.Contains(t, md, "## Summary")
	assert.Contains(t, md, "| Scenario | wgo success | kgo success | success Δ | wgo slow |")
	assert.Contains(t, md, "| all healthy | 10/10 (100.0%) | 10/10 (100.0%) | +0.0 pts | n/a |")
	assert.Contains(t, md, "## all healthy")
	assert.Contains(t, md, "everything fine")
	assert.Contains(t, md, "- wgo-kgo success delta: +0.0 pts")
	assert.NotContains(t, md, "wgo slow fraction:")
}

func TestResultsReport_GenerateMarkdown_DocumentsEventAndConfig(t *testing.T) {
	t.Parallel()
	rr := newResultsReport(scenarioResults{entries: []scenarioResult{
		{
			sc:         scenario{name: "all healthy", expect: scenarioExpectations{minWgoSuccessRate: 1.0}},
			wgoSummary: observationsSummary{total: 10, successes: 10},
			kgoSummary: observationsSummary{total: 10, successes: 10},
		},
	}})
	md := rr.generateMarkdown()

	assert.Contains(t, md, "**Event**: one simulated application write")
	assert.Contains(t, md, fmt.Sprintf("%d records/event", clusterSize))

	assert.Contains(t, md, "## Client configuration")
	assert.Contains(t, md, "Shared by both clients")
	assert.Contains(t, md, fmt.Sprintf("| Dial timeout | %s |", clientDialTimeout))
	assert.Contains(t, md, fmt.Sprintf("| Linger | %s |", clientLinger))
	assert.Contains(t, md, "| Batch max bytes | 16,000,000 |")

	assert.Contains(t, md, "wgo-only — this simulation's own values")
	assert.Contains(t, md, "| Setting | Value | pkg/wgo default |")
	// This simulation deliberately overrides two wgo defaults: it hedges later
	// (1s vs. the library's 500ms) and flags an agent faulty sooner (5% vs.
	// 20%). Pin both sides so a formatting bug, or an accidental value change,
	// fails the test rather than silently misreporting either number.
	assert.Contains(t, md, fmt.Sprintf("| Hedger: min hedge delay | %s | %s |", wgoHedgerMinHedgeDelay, wgo.DefaultHedgerMinHedgeDelay))
	assert.Contains(t, md, fmt.Sprintf("| Hedger: max hedge agents | %d | %d |", wgoHedgerMaxHedgeAgents, wgo.DefaultHedgerMaxHedgeAgents))
	assert.Contains(t, md, "| Health check: max slow fraction | 30% | 30% |")
	assert.Contains(t, md, "| Health check: faulty threshold | 5% | 20% |")

	assert.Contains(t, md, "kgo-only:")
	assert.Contains(t, md, "| Partitioner | Manual")
	assert.Contains(t, md, fmt.Sprintf("| Max produce requests in-flight per broker | %d |", clientMaxInflight))

	// The config section must render before the Summary table, not after it.
	assert.Less(t, strings.Index(md, "## Client configuration"), strings.Index(md, "## Summary"))
}

func TestResultsReport_GenerateMarkdown_ShowsSlowFractionAndDeltaGates(t *testing.T) {
	t.Parallel()
	rr := newResultsReport(scenarioResults{entries: []scenarioResult{
		{
			sc: scenario{name: "1 slow agent", description: "hedge around a slow broker", expect: scenarioExpectations{
				minWgoSuccessRate:  1.0,
				slowBudget:         ptr(2 * time.Second),
				maxWgoSlowFraction: ptr(0.10),
			}},
			wgoSummary:      observationsSummary{total: 10, successes: 10, p99Latency: time.Second},
			kgoSummary:      observationsSummary{total: 10, successes: 10, p99Latency: 3 * time.Second},
			wgoSlowFraction: ptr(0.082),
		},
		{
			sc: scenario{name: "1 fast-failing agent", description: "reroute around a dead broker", expect: scenarioExpectations{
				minWgoSuccessRate:    1.0,
				minSuccessDeltaVsKgo: ptr(0.5),
			}},
			wgoSummary: observationsSummary{total: 10, successes: 10, p99Latency: time.Second},
			kgoSummary: observationsSummary{total: 10, successes: 0, p99Latency: time.Second},
		},
	}})
	md := rr.generateMarkdown()
	assert.Contains(t, md, "| 1 slow agent | 10/10 (100.0%) | 10/10 (100.0%) | +0.0 pts | 8.2% (≤10.0% >2s) |")
	assert.Contains(t, md, "- wgo slow fraction: 8.2% of requests slower than 2s (ceiling 10.0%)")
	assert.Contains(t, md, "| 1 fast-failing agent | 10/10 (100.0%) | 0/10 (0.0%) | +100.0 pts | n/a |")
	assert.Contains(t, md, "- wgo-kgo success delta: +100.0 pts (want ≥ 50.0 pts)")
}
