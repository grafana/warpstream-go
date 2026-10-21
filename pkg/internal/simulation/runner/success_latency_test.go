package runner

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservations_SuccessLatencyExcludesFailures(t *testing.T) {
	t.Parallel()
	obs := &observations{}
	for _, latency := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond} {
		obs.add(observation{latency: latency})
	}
	// A failure's latency is its caller's deadline.
	for range 2 {
		obs.add(observation{latency: 5 * time.Second, err: errors.New("deadline")})
	}
	s := obs.snapshot().summary()
	assert.Equal(t, 5*time.Second, s.p99Latency, "failures still count in the overall figures")
	assert.Equal(t, 300*time.Millisecond, s.successP99Latency)
	assert.Equal(t, 200*time.Millisecond, s.successP50Latency)
	assert.Equal(t, 200*time.Millisecond, s.successMeanLatency)

	failed := &observations{}
	failed.add(observation{latency: 5 * time.Second, err: errors.New("deadline")})
	s = failed.snapshot().summary()
	assert.Zero(t, s.successP99Latency)
	assert.Zero(t, s.successMeanLatency)
}

func TestResultsReport_SuccessLatencyTable(t *testing.T) {
	t.Parallel()
	obs := func(failures int, success time.Duration, successes int) observationsSummary {
		o := &observations{}
		for range successes {
			o.add(observation{latency: success})
		}
		for range failures {
			o.add(observation{latency: 5 * time.Second, err: errors.New("deadline")})
		}
		return o.snapshot().summary()
	}
	rr := newResultsReport(scenarioResults{entries: []scenarioResult{{
		sc:         scenario{name: "probe"},
		wgoSummary: obs(2, 400*time.Millisecond, 8),
		kgoSummary: obs(10, 0, 0),
	}}})
	var b strings.Builder
	rr.writeSuccessLatencyTable(&b)
	// Failures first, then latency of what succeeded; a client with no successes has none to time.
	require.Contains(t, b.String(), "| Scenario | wgo success | kgo success | wgo p50 | kgo p50 | wgo p99 | kgo p99 | wgo mean | kgo mean |")
	assert.Contains(t, b.String(), "| probe | 8/10 (80.0%) | 0/10 (0.0%) | 0.4s | n/a | 0.4s | n/a | 0.4s | n/a |")
}

func TestResultsReport_SummaryLineShowsSuccessfulOnlyWhenFailuresMixIn(t *testing.T) {
	t.Parallel()
	rr := &resultsReport{}
	mixed := observationsSummary{total: 10, successes: 8, failures: 2, meanLatency: time.Second, p99Latency: 5 * time.Second, successMeanLatency: 400 * time.Millisecond, successP99Latency: 500 * time.Millisecond}
	assert.Contains(t, rr.summaryLine(mixed), "(successful only: mean=0.4s p50=0.0s p99=0.5s)")
	clean := observationsSummary{total: 10, successes: 10, meanLatency: time.Second}
	assert.NotContains(t, rr.summaryLine(clean), "successful only")
}

func TestResultsReport_GenerateJSONCarriesSuccessLatency(t *testing.T) {
	t.Parallel()
	timed := observationsSummary{total: 10, successes: 8, failures: 2, p99Latency: 5 * time.Second, successMeanLatency: 400 * time.Millisecond, successP50Latency: 300 * time.Millisecond, successP99Latency: time.Second}
	data, err := newResultsReport(scenarioResults{entries: []scenarioResult{{sc: scenario{name: "probe"}, wgoSummary: timed, kgoSummary: observationsSummary{total: 10}}}}).generateJSON()
	require.NoError(t, err)
	assert.JSONEq(t, `[{"scenario":"probe","wgo":{"attempts":10,"passes":8},"kgo":{"attempts":10,"passes":0},
		"wgo_success_latency":{"mean_seconds":0.4,"p50_seconds":0.3,"p99_seconds":1}}]`, string(data))
}
