package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type externalTestWorkload struct{ attempts []ApplicationObservation }

func (*externalTestWorkload) Produce(context.Context, *kgo.Record, func(*kgo.Record, error)) {
	panic("synthetic request sent")
}
func (*externalTestWorkload) Close() {}
func (w *externalTestWorkload) Observe(context.Context, time.Time, time.Duration) ([]ApplicationObservation, error) {
	return w.attempts, nil
}
func (*externalTestWorkload) ObservationDescription() string { return "actual attempts" }

func TestObservedWorkload_UsesActualAttempts(t *testing.T) {
	start := time.Now()
	client := &externalTestWorkload{attempts: []ApplicationObservation{{Started: start, Duration: time.Second}, {Started: start.Add(time.Second), Duration: 2 * time.Second, Error: errors.New("retryable")}}}
	env := &environment{wgoClient: client, kgoClient: client}
	wgo, kgo, err := observeWorkloads(t.Context(), env, start, time.Minute, buildScenarioEvents("", 1, time.Minute, time.Second), nil)
	require.NoError(t, err)
	for _, obs := range []*observations{wgo, kgo} {
		summary := obs.snapshot().summary()
		require.Equal(t, 2, summary.total)
		require.Equal(t, 1, summary.successes)
	}
}

func TestObservedWorkload_RecoveryUsesActualCount(t *testing.T) {
	result := scenarioResult{
		sc:                     scenario{expect: scenarioExpectations{recoveryFrom: ptr(30 * time.Second)}},
		observationDescription: "actual attempts",
		wgoRecovery:            observationsSummary{total: 17, successes: 17},
	}
	require.Empty(t, checkResult(result))
	result.wgoRecovery.successes--
	require.NotEmpty(t, checkResult(result))
	result.wgoRecovery = observationsSummary{}
	require.NotEmpty(t, checkResult(result))
}

func TestObservedWorkload_RejectsSenderLossAndLag(t *testing.T) {
	result := scenarioResult{sc: scenario{expect: scenarioExpectations{maxSenderLag: ptr(5 * time.Second)}}}
	require.NotEmpty(t, checkResult(result))
	result.wgoSender = map[string]float64{"samples_failed_total": 0, "samples_dropped_total": 0, "max_source_lag_seconds": 1}
	require.Empty(t, checkResult(result))
	result.wgoSender["samples_failed_total"] = 1
	require.NotEmpty(t, checkResult(result))
	result.wgoSender["samples_failed_total"] = 0
	result.wgoSender["samples_dropped_total"] = 1
	require.NotEmpty(t, checkResult(result))
	result.wgoSender["samples_dropped_total"] = 0
	result.wgoSender["max_source_lag_seconds"] = 129
	require.NotEmpty(t, checkResult(result))
}
