package main

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
}

// successRate returns the wgo app-level success fraction, or 0 when nothing was
// produced.
func (r scenarioResult) successRate() float64 {
	if r.wgoSummary.total == 0 {
		return 0
	}
	return float64(r.wgoSummary.successes) / float64(r.wgoSummary.total)
}

// scenarioResults is the outcome of running every scenario.
type scenarioResults struct {
	entries []scenarioResult
}

// scenarioFailure is one scenario that missed its expected minimum success rate.
type scenarioFailure struct {
	name string
	got  float64
	want float64
}

// check returns the scenarios whose wgo success rate fell below their expectation.
func (sr scenarioResults) check() []scenarioFailure {
	var out []scenarioFailure
	for _, res := range sr.entries {
		if got := res.successRate(); got < res.sc.expectedMinSuccessRate {
			out = append(out, scenarioFailure{name: res.sc.name, got: got, want: res.sc.expectedMinSuccessRate})
		}
	}
	return out
}
