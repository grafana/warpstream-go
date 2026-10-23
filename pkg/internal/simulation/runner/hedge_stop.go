package runner

import (
	"fmt"
	"strings"
)

// healthGateReasons are the hedge-suppression reasons for a cluster-wide health
// gate. The other reasons only cover the start of a run, before statistics exist.
var healthGateReasons = []string{suppressedSlowFraction, suppressedFaultyFraction}

// hedgeStop says which health gate stopped hedging, next to the one the scenario
// was built to exercise.
type hedgeStop struct {
	// Expected is the gate the scenario isolates; empty when none should act.
	Expected string `json:"expected,omitempty"`
	// Observed counts suppressed hedges per health gate.
	Observed map[string]int64 `json:"observed"`
	// Verdict is "as expected", "mixed" (the expected gate acted and so did another),
	// "differs", or "informational" for a scenario that does not isolate a gate.
	Verdict string `json:"verdict"`
}

// hedgeStop returns nil when the run collected no client evidence.
func (res scenarioResult) hedgeStop() *hedgeStop {
	if res.clientEvidence == nil {
		return nil
	}
	stop := &hedgeStop{Expected: res.sc.expectedHedgeStop, Observed: map[string]int64{}}
	var other int64
	for _, reason := range healthGateReasons {
		stop.Observed[reason] = res.clientEvidence.HedgeSuppression[reason]
		if reason != stop.Expected {
			other += stop.Observed[reason]
		}
	}
	isolates := stop.Expected != "" || len(res.sc.requireSuppression) > 0 || len(res.sc.forbidSuppression) > 0
	switch {
	case !isolates:
		stop.Verdict = "informational"
	case stop.Expected == "" && stop.total() == 0, stop.Expected != "" && stop.Observed[stop.Expected] > 0 && other == 0:
		stop.Verdict = "as expected"
	case stop.Expected != "" && stop.Observed[stop.Expected] > 0:
		stop.Verdict = "mixed"
	default:
		stop.Verdict = "differs"
	}
	return stop
}

func (s *hedgeStop) total() (total int64) {
	for _, count := range s.Observed {
		total += count
	}
	return total
}

// worthReporting leaves out scenarios that neither expect a gate nor saw one.
func (s *hedgeStop) worthReporting() bool {
	return s != nil && (s.Expected != "" || s.total() > 0)
}

func (s *hedgeStop) observedCell(reason string) string {
	if s.Observed[reason] == 0 {
		return "0"
	}
	return fmt.Sprintf("%d (%.0f%%)", s.Observed[reason], 100*float64(s.Observed[reason])/float64(s.total()))
}

func (s *hedgeStop) expectedText() string {
	if s.Expected == "" {
		return "none"
	}
	return s.Expected
}

func (s *hedgeStop) line() string {
	return fmt.Sprintf("Hedging stopped by: slow fraction %s, faulty fraction %s; expected %s: %s", s.observedCell(suppressedSlowFraction), s.observedCell(suppressedFaultyFraction), s.expectedText(), s.Verdict)
}

// hedgeStopRow is one scenario of the table. Success comes first because whether
// wgo survives, and how it compares to kgo, is the main result.
type hedgeStopRow struct {
	name, wgoSuccess, kgoSuccess string
	stop                         *hedgeStop
}

func writeHedgeStopTable(b *strings.Builder, rows []hedgeStopRow) {
	if len(rows) == 0 {
		return
	}
	b.WriteString("### Why hedging stopped (secondary signal)\n\nThe success columns repeat the summary and stay the main result. The gate columns count the hedges each cluster-wide health gate suppressed in the observed phase. `mixed` or `differs` means a gate other than the expected one acted, so the fault was not isolated.\n\n")
	b.WriteString("| Scenario | wgo success | kgo success | Expected gate | Slow fraction | Faulty fraction | Verdict |\n| --- | --- | --- | --- | --- | --- | --- |\n")
	for _, row := range rows {
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s |\n", row.name, row.wgoSuccess, row.kgoSuccess, row.stop.expectedText(), row.stop.observedCell(suppressedSlowFraction), row.stop.observedCell(suppressedFaultyFraction), row.stop.Verdict)
	}
	b.WriteString("\n")
}
