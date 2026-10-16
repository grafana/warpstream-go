package runner

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStaleFallback_EvidenceGates(t *testing.T) {
	t.Parallel()
	result := scenarioResult{sc: scenario{requireStaleFallback: true}, staleFallback: &staleFallbackEvidence{BrokerOnlyWithdrawals: 1, StaleRoutingResponses: 1, FallbackErrors: 1, ThirdAttemptSuccesses: 1}}
	require.Empty(t, checkResult(result))
	for _, field := range []*int64{&result.staleFallback.BrokerOnlyWithdrawals, &result.staleFallback.StaleRoutingResponses, &result.staleFallback.FallbackErrors, &result.staleFallback.ThirdAttemptSuccesses} {
		*field = 0
		require.NotEmpty(t, checkResult(result))
		*field = 1
	}
	result.staleFallback.TerminalErrors = 1
	require.NotEmpty(t, checkResult(result))
	result.staleFallback = nil
	require.NotEmpty(t, checkResult(result))
}

func TestStaleFallback_ClientMetrics(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewPedanticRegistry()
	errors := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_warpstream_produce_direct_requests_failed_total", Help: "failures"}, []string{"reason"})
	terminal := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_warpstream_produce_requests_failed_total", Help: "failed cascades"}, []string{"reason"})
	attempts := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_warpstream_produce_requests_attempts", Help: "attempts", NativeHistogramBucketFactor: 1.1}, []string{"outcome"})
	registry.MustRegister(errors, terminal, attempts)
	errors.WithLabelValues("unknown_broker").Add(3)
	errors.WithLabelValues("unknown").Add(2)
	errors.WithLabelValues("transport").Inc()
	errors.WithLabelValues("kafka_retriable_error").Add(100)
	terminal.WithLabelValues("terminal_error").Add(2)
	attempts.WithLabelValues("success").Observe(2)
	attempts.WithLabelValues("success").Observe(3)
	attempts.WithLabelValues("failure").Observe(3)
	evidence, err := readStaleFallbackEvidence(registry, "test_", &metadataView{brokerOnlyWithdrawals: 4, staleRoutingResponses: 5})
	require.NoError(t, err)
	require.Equal(t, &staleFallbackEvidence{BrokerOnlyWithdrawals: 4, StaleRoutingResponses: 5, FallbackErrors: 6, FallbackErrorReasons: map[string]int64{"unknown_broker": 3, "unknown": 2, "transport": 1}, ThirdAttemptSuccesses: 1, TerminalErrors: 2}, evidence)
	report := newResultsReport(scenarioResults{entries: []scenarioResult{{sc: scenario{name: "restart", requireStaleFallback: true}, staleFallback: evidence}}})
	json, err := report.generateJSON()
	require.NoError(t, err)
	require.Contains(t, string(json), `"third_attempt_successes": 1`)
	require.Contains(t, report.generateMarkdown(), "successful cascades with at least three attempts")
}

func TestStaleFallback_ThirdAttempts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		buckets      []float64
		nativeFactor float64
	}{
		{name: "native", nativeFactor: 1.1},
		{name: "classic integer boundary", buckets: []float64{1, 2, 3, 4, 5, 8, 16}},
		{name: "classic fractional boundary", buckets: prometheus.DefBuckets},
		{name: "native and classic", nativeFactor: 1.1, buckets: []float64{1, 2, 3, 4, 5, 8, 16}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			histogram := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "attempts", Help: "attempts", Buckets: tc.buckets, NativeHistogramBucketFactor: tc.nativeFactor})
			counts := []struct{ attempts, count int }{{1, 7}, {2, 11}, {3, 5}, {4, 2}, {5, 8}, {8, 1}, {16, 3}}
			for _, entry := range counts {
				for range entry.count {
					histogram.Observe(float64(entry.attempts))
				}
			}
			metric := &dto.Metric{}
			require.NoError(t, histogram.Write(metric))
			encoded, err := proto.Marshal(metric)
			require.NoError(t, err)
			decoded := &dto.Metric{}
			require.NoError(t, proto.Unmarshal(encoded, decoded))
			require.EqualValues(t, 19, thirdAttempts(decoded.GetHistogram()))
		})
	}
}

func TestStaleFallback_ThirdAttemptsWithoutLaterAttempts(t *testing.T) {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "attempts", Help: "attempts", NativeHistogramBucketFactor: 1.1})
	histogram.Observe(1)
	histogram.Observe(2)
	metric := &dto.Metric{}
	require.NoError(t, histogram.Write(metric))
	require.Zero(t, thirdAttempts(metric.GetHistogram()))
	require.Zero(t, thirdAttempts(nil))
}
