package runner

import "fmt"

// scenarioResult holds everything the report and the pass/fail check need for one
// scenario.
type scenarioResult struct {
	sc                                         scenario
	staleFallback                              *staleFallbackEvidence
	windows                                    []windowResult
	wgoLatencySelections, kgoLatencySelections map[int32]int64
	workloadDescription, workloadConfiguration string
	recoveryResponseDescription                string

	wgoSummary, kgoSummary         observationsSummary
	wgoBuckets, kgoBuckets         []observationsSummary
	produceDeltas                  []counterBucketDelta
	totalPrimary                   int64
	totalHedge                     int64
	wgoErrorCounts, kgoErrorCounts map[string]int

	// wgoSlowFraction is the fraction of wgo observations whose latency
	// exceeded sc.expect.slowBudget. Set only when that expectation is
	// configured; nil otherwise.
	wgoSlowFraction                      *float64
	wgoRecovery                          observationsSummary
	wgoRecoverySlowFraction              float64
	wgoTransport, kgoTransport           transportStats
	wgoMissingLeaders, kgoMissingLeaders map[int32]int64
}

// successRate returns the wgo app-level success fraction, or 0 when nothing was
// produced.
func (r scenarioResult) successRate() float64 {
	return successFraction(r.wgoSummary)
}

// kgoSuccessRate returns the kgo app-level success fraction, or 0 when
// nothing was produced.
func (r scenarioResult) kgoSuccessRate() float64 {
	return successFraction(r.kgoSummary)
}

func successFraction(s observationsSummary) float64 {
	if s.total == 0 {
		return 0
	}
	return float64(s.successes) / float64(s.total)
}

// hasSlowFractionGate reports whether r has both the config and the
// measurement needed to evaluate the slow-fraction gate.
func (r scenarioResult) hasSlowFractionGate() bool {
	return r.sc.expect.slowBudget != nil && r.sc.expect.maxWgoSlowFraction != nil && r.wgoSlowFraction != nil
}

// hasSuccessDeltaGate reports whether r's scenario asserts a
// minSuccessDeltaVsKgo gate.
func (r scenarioResult) hasSuccessDeltaGate() bool {
	return r.sc.expect.minSuccessDeltaVsKgo != nil
}

// scenarioResults is the outcome of running every scenario.
type scenarioResults struct {
	entries []scenarioResult
}

// scenarioFailure is one expectation (see scenarioExpectations) that a
// scenario's results missed.
type scenarioFailure struct {
	scenario string
	detail   string
}

func (f scenarioFailure) String() string {
	return fmt.Sprintf("%s: %s", f.scenario, f.detail)
}

// checkResult returns every expectation res.sc.expect misses. A scenario can
// fail on more than one axis at once, so this can return more than one entry
// per scenario.
func checkResult(res scenarioResult) []scenarioFailure {
	var out []scenarioFailure
	exp := res.sc.expect

	if got := res.successRate(); got < exp.minWgoSuccessRate {
		out = append(out, scenarioFailure{
			scenario: res.sc.name,
			detail:   fmt.Sprintf("wgo success rate %.1f%% below required floor %.1f%%", 100*got, 100*exp.minWgoSuccessRate),
		})
	}

	if res.hasSlowFractionGate() {
		if got := *res.wgoSlowFraction; got > *exp.maxWgoSlowFraction {
			out = append(out, scenarioFailure{
				scenario: res.sc.name,
				detail: fmt.Sprintf("wgo fraction of requests slower than %s is %.1f%%, above the %.1f%% ceiling",
					*exp.slowBudget, 100*got, 100**exp.maxWgoSlowFraction),
			})
		}
	}

	if res.hasSuccessDeltaGate() {
		delta := res.successRate() - res.kgoSuccessRate()
		if delta < *exp.minSuccessDeltaVsKgo {
			out = append(out, scenarioFailure{
				scenario: res.sc.name,
				detail: fmt.Sprintf("wgo beat kgo's success rate by %.1f pts, below the required %.1f pts (wgo %.1f%%, kgo %.1f%%)",
					100*delta, 100**exp.minSuccessDeltaVsKgo, 100*res.successRate(), 100*res.kgoSuccessRate()),
			})
		}
	}

	if res.sc.requireStaleFallback {
		if res.staleFallback == nil || res.staleFallback.BrokerOnlyWithdrawals == 0 || res.staleFallback.StaleRoutingResponses == 0 || res.staleFallback.FallbackErrors == 0 {
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: "stale fallback was not exercised by both discovery paths and real client errors"})
		}
		if res.staleFallback == nil || res.staleFallback.ThirdAttemptSuccesses == 0 {
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: "no write completed after continuing to a third agent"})
		}
		if res.staleFallback != nil && res.staleFallback.TerminalErrors > 0 {
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("%d retry cascades stopped at a terminal error", res.staleFallback.TerminalErrors)})
		}
	}
	for _, phase := range res.sc.transportPhases {
		for _, client := range []struct {
			name  string
			stats transportStats
		}{{"wgo", res.wgoTransport}, {"kgo", res.kgoTransport}} {
			stats := client.stats.byBroker[phase.nodeID]
			if (phase.action == killPod || phase.action == removePod || phase.action == withdrawPod) && stats.closedConnections == 0 || phase.action == blackholePod && stats.droppedBytes == 0 {
				out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("%s transport fault on broker %d did not affect live traffic", client.name, phase.nodeID)})
			}
		}
		if phase.action == withdrawPod {
			for _, client := range []struct {
				name      string
				responses map[int32]int64
			}{{"wgo", res.wgoMissingLeaders}, {"kgo", res.kgoMissingLeaders}} {
				if client.responses[phase.nodeID] == 0 {
					out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("%s was served no Metadata response naming withdrawn broker %d as leader", client.name, phase.nodeID)})
				}
			}
		}
		if (phase.action == restorePod || phase.action == replacePod) && res.wgoTransport.recoveredResponses[phase.nodeID] == 0 {
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("no successful recovery Produce response (including probes) reached restored broker %d", phase.nodeID)})
		}
	}
	if res.sc.hasInjectedDelays() {
		for _, lane := range []struct {
			name     string
			selected map[int32]int64
		}{{"wgo", res.wgoLatencySelections}, {"kgo", res.kgoLatencySelections}} {
			var affected int64
			for _, count := range lane.selected {
				affected += count
			}
			if affected == 0 {
				out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("%s: no Produce response selected for injected delay", lane.name)})
			}
		}
	}
	for _, broker := range res.sc.requiredWgoDelayBrokers {
		if res.wgoLatencySelections[broker] == 0 {
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("wgo: no Produce response selected for injected delay on required broker %d", broker)})
		}
	}
	if exp.recoveryFrom != nil {
		want := len(buildScenarioEvents("", 0, res.sc.duration()-*exp.recoveryFrom, res.sc.spacing()))
		if res.wgoRecovery.total != want || res.wgoRecovery.successes != want || exp.slowBudget != nil && res.wgoRecoverySlowFraction > 0 {
			detail := fmt.Sprintf("recovery: %d/%d passes, want %d", res.wgoRecovery.successes, res.wgoRecovery.total, want)
			if exp.slowBudget != nil {
				detail += fmt.Sprintf("; %.1f%% slower than %s", 100*res.wgoRecoverySlowFraction, *exp.slowBudget)
			}
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: detail})
		}
	}
	if exp.maxHedgeSurge != nil {
		if res.totalPrimary == 0 || float64(res.totalHedge)/float64(res.totalPrimary) > *exp.maxHedgeSurge {
			out = append(out, scenarioFailure{scenario: res.sc.name, detail: fmt.Sprintf("hedge surge: %d hedges / %d primaries exceeds %.0f%% budget", res.totalHedge, res.totalPrimary, 100**exp.maxHedgeSurge)})
		}
	}
	return out
}

// check returns every scenario expectation missed across all results.
func (sr scenarioResults) check() []scenarioFailure {
	var out []scenarioFailure
	for _, res := range sr.entries {
		out = append(out, checkResult(res)...)
	}
	return out
}
