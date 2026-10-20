package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestHedgeDiagnostics_WriterAggregationAndWarmup(t *testing.T) {
	const prefix = "cortex_ingest_storage_writer_"
	reg := prometheus.NewPedanticRegistry()
	primary := prometheus.NewCounter(prometheus.CounterOpts{Name: prefix + "warpstream_produce_requests_primary_total"})
	hedge := prometheus.NewCounter(prometheus.CounterOpts{Name: prefix + "warpstream_produce_requests_hedge_total"})
	fraction := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: prefix + "warpstream_cluster_faulty_fraction"}, []string{"simulation_writer"})
	suppressed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: prefix + "warpstream_hedge_attempts_suppressed_total"}, []string{"simulation_writer", "reason"})
	failed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: prefix + "warpstream_produce_direct_requests_failed_total"}, []string{"simulation_writer", "reason", "agent_state"})
	reg.MustRegister(primary, hedge, fraction, suppressed, failed)
	primary.Add(100)
	hedge.Add(5)
	suppressed.WithLabelValues("a", "no_agent_stats").Add(20)
	failed.WithLabelValues("a", "timeout", "healthy").Add(3)
	initial := readCounterSample(reg, prefix)
	initial.at = time.Unix(100, 0)

	fraction.WithLabelValues("a").Set(0.54)
	fraction.WithLabelValues("b").Set(0.2)
	suppressed.WithLabelValues("a", "faulty_fraction_exceeded").Add(10)
	suppressed.WithLabelValues("b", "faulty_fraction_exceeded").Add(7)
	failed.WithLabelValues("a", "timeout", "healthy").Add(2)
	failed.WithLabelValues("b", "transport", "healthy").Add(8)
	failed.WithLabelValues("a", "transport", "demoted").Add(4)
	final := readCounterSample(reg, prefix)
	final.at = initial.at.Add(10 * time.Second)
	intervals := diagnosticIntervals([]counterSample{initial, final})
	require.Len(t, intervals, 1)
	require.Equal(t, 0.54, *intervals[0].FaultyFraction)
	require.Equal(t, map[string]int64{"faulty_fraction_exceeded": 17}, intervals[0].Suppressed)
	require.Equal(t, map[string]int64{"timeout": 2, "transport": 12}, intervals[0].Failures)

	res := scenarioResult{sc: scenario{name: "timeouts"}, hedgeDiagnostics: intervals}
	data, err := newResultsReport(scenarioResults{entries: []scenarioResult{res}}).generateJSON()
	require.NoError(t, err)
	require.Contains(t, string(data), `"max_writer_faulty_fraction": 0.54`)
	require.Contains(t, string(data), `"faulty_fraction_exceeded": 17`)
	var b strings.Builder
	(&resultsReport{}).writeHedgeDiagnostics(&b, intervals)
	require.Contains(t, b.String(), "| 0.0s–10.0s | 54.0% | 17 | 2 | 12 |")
}

func TestHedgeDiagnostics_MissingGauge(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(
		prometheus.NewCounter(prometheus.CounterOpts{Name: "warpstream_produce_requests_primary_total"}),
		prometheus.NewCounter(prometheus.CounterOpts{Name: "warpstream_produce_requests_hedge_total"}),
	)
	sample := readCounterSample(reg, "")
	require.Nil(t, sample.diagnostics.faultyFraction)
	reg = prometheus.NewPedanticRegistry()
	require.Panics(t, func() { readCounterSample(reg, "") })
}
