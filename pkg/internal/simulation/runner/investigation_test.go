package runner

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInvestigation_ProfileSelection(t *testing.T) {
	_, err := profileScenarios("mimir-investigation", false)
	require.ErrorContains(t, err, "external workload")
	_, err = profileScenarios("unknown", true)
	require.Error(t, err)
	standard, err := profileScenarios("standard", false)
	require.NoError(t, err)
	require.Len(t, standard, len(scenarios()))
	investigation, err := profileScenarios("mimir-investigation", true)
	require.NoError(t, err)
	require.Len(t, investigation, 2)
	sc := investigation[1]
	brokers, partitions := sc.topology()
	require.EqualValues(t, 10, brokers)
	require.EqualValues(t, 50, partitions)
	require.Len(t, buildScenarioEvents("", 0, sc.duration(), sc.spacing()), 3600)
	result := scenarioResult{sc: sc, wgoLatencySelections: map[int32]int64{0: 1, 1: 1, 2: 1, 3: 1, 4: 1}, kgoLatencySelections: map[int32]int64{0: 1, 1: 1, 2: 1, 3: 1, 4: 1}, wgoRecovery: observationsSummary{total: 700, successes: 700}}
	require.Empty(t, checkResult(result))
	result.wgoRecovery.successes--
	require.NotEmpty(t, checkResult(result))
}

func TestInvestigation_PhaseTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sc := investigationScenarios()[1]
		env := &environment{behaviours: newBrokersBehaviourProvider(sc.behaviours)}
		done := make(chan error, 1)
		go func() { done <- runPhases(t.Context(), env, time.Now(), sc) }()
		time.Sleep(9 * time.Second)
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
		time.Sleep(2 * time.Second)
		require.Equal(t, 9*time.Second, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
		require.Equal(t, 3*time.Second, env.behaviours.nextLatencyFor(clientTypeKgo, 4))
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 5))
		time.Sleep(120 * time.Second)
		require.NoError(t, <-done)
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
	})
}

func TestInvestigation_PhaseCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sc := investigationScenarios()[1]
		env := &environment{behaviours: newBrokersBehaviourProvider(sc.behaviours)}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- runPhases(ctx, env, time.Now(), sc) }()
		time.Sleep(time.Second)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Equal(t, 50*time.Millisecond, env.behaviours.nextLatencyFor(clientTypeWgo, 0))
	})
}

func TestInvestigation_ScheduledWindows(t *testing.T) {
	start := time.Now()
	snapshot := observationsSnapshot{list: []observation{
		{scheduled: start.Add(9 * time.Second), issued: start.Add(11 * time.Second)},
		{scheduled: start.Add(10 * time.Second), issued: start.Add(12 * time.Second)},
		{scheduled: start.Add(129 * time.Second), err: errors.New("failed")},
		{scheduled: start.Add(130 * time.Second)},
		{scheduled: start.Add(145 * time.Second)},
		{scheduled: start.Add(180 * time.Second)},
	}}
	sc := investigationScenarios()[1]
	windows := summarizeWindows(sc.windows, start, snapshot, snapshot)
	require.Equal(t, 1, windows[0].wgo.total)
	require.Equal(t, 2, windows[1].wgo.total)
	require.Equal(t, 1, windows[1].wgo.successes)
	require.Equal(t, 1, windows[2].kgo.total)
	report := newResultsReport(scenarioResults{entries: []scenarioResult{{sc: sc, windows: windows}}})
	data, err := report.generateJSON()
	require.NoError(t, err)
	require.Contains(t, string(data), `"from_seconds": 145`)
	require.Contains(t, string(data), `"attempts": 2`)
	require.Contains(t, report.generateMarkdown(), "10 brokers / 50 partitions")
	require.Contains(t, report.generateMarkdown(), "event spacing 50ms")
}
