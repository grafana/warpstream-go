package main

import "fmt"

// scenarioResult holds everything the report and the pass/fail check need for one
// scenario.
type scenarioResult struct {
	sc scenario

	wgoSummary, kgoSummary         observationsSummary
	wgoBuckets, kgoBuckets         []observationsSummary
	produceDeltas                  []counterBucketDelta
	totalPrimary                   int64
	totalHedge                     int64
	wgoErrorCounts, kgoErrorCounts map[string]int

	// wgoSlowFraction is the fraction of wgo observations whose latency
	// exceeded sc.expect.slowBudget. Set only when that expectation is
	// configured; nil otherwise.
	wgoSlowFraction *float64
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

	if exp.slowBudget != nil && exp.maxWgoSlowFraction != nil && res.wgoSlowFraction != nil {
		if got := *res.wgoSlowFraction; got > *exp.maxWgoSlowFraction {
			out = append(out, scenarioFailure{
				scenario: res.sc.name,
				detail: fmt.Sprintf("wgo fraction of requests slower than %s is %.1f%%, above the %.1f%% ceiling",
					*exp.slowBudget, 100*got, 100* *exp.maxWgoSlowFraction),
			})
		}
	}

	if exp.minSuccessDeltaVsKgo != nil {
		delta := res.successRate() - res.kgoSuccessRate()
		if delta < *exp.minSuccessDeltaVsKgo {
			out = append(out, scenarioFailure{
				scenario: res.sc.name,
				detail: fmt.Sprintf("wgo beat kgo's success rate by %.1f pts, below the required %.1f pts (wgo %.1f%%, kgo %.1f%%)",
					100*delta, 100* *exp.minSuccessDeltaVsKgo, 100*res.successRate(), 100*res.kgoSuccessRate()),
			})
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
