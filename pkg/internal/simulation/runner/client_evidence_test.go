package runner

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestClientEvidence_ObservedCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_warpstream_produce_requests_failed_total", Help: "test"}, []string{"reason", "writer"})
	suppressed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_warpstream_hedge_attempts_suppressed_total", Help: "test"}, []string{"reason"})
	triggers := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_warpstream_produce_hedge_triggers_total", Help: "test"}, []string{"trigger"})
	reg.MustRegister(failures, suppressed, triggers)
	failures.WithLabelValues("write_timeout", "a").Add(7)
	before, err := readClientEvidence(reg, "test_")
	require.NoError(t, err)
	failures.WithLabelValues("write_timeout", "a").Add(2)
	failures.WithLabelValues("write_timeout", "b").Add(3)
	suppressed.WithLabelValues("faulty_fraction_exceeded").Add(11)
	triggers.WithLabelValues("primary_failure").Add(5)
	after, err := readClientEvidence(reg, "test_")
	require.NoError(t, err)
	after.subtract(before)
	require.EqualValues(t, 5, after.WriteFailures["write_timeout"])
	require.EqualValues(t, 11, after.HedgeSuppression["faulty_fraction_exceeded"])
	require.EqualValues(t, 5, after.HedgeTriggers["primary_failure"])
	require.Zero(t, after.HedgeTriggers["latency"])
	report := newResultsReport(scenarioResults{entries: []scenarioResult{{clientEvidence: after}}})
	data, err := report.generateJSON()
	require.NoError(t, err)
	require.Contains(t, string(data), `"faulty_fraction_exceeded": 11`)
	require.Contains(t, report.generateMarkdown(), "write_timeout=5")
}
