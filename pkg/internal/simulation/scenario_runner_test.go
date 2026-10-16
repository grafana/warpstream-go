package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuildScenarioEvents_EventCountAndShape(t *testing.T) {
	t.Parallel()
	const topic = "t"
	const partitions = int32(3)
	events := buildScenarioEvents(topic, partitions, 5*time.Second, time.Second)
	assert.Len(t, events, 5)
	for i, ev := range events {
		assert.Equal(t, time.Duration(i)*time.Second, ev.at, "event %d", i)
		assert.Len(t, ev.records, int(partitions), "event %d records", i)
		for p := range partitions {
			assert.Equal(t, topic, ev.records[p].Topic)
			assert.Equal(t, p, ev.records[p].Partition)
			assert.Equal(t, []byte("event"), ev.records[p].Value)
		}
	}
}

func TestBuildScenarioEvents_EmptyWhenDurationLEZero(t *testing.T) {
	t.Parallel()
	assert.Empty(t, buildScenarioEvents("t", 3, 0, time.Second))
}

func TestRunScenarioEvents_RecordsObservations(t *testing.T) {
	t.Parallel()
	events := buildScenarioEvents("t", 2, 200*time.Millisecond, 100*time.Millisecond)

	app := runScenarioEvents(context.Background(), &fakeProduceClient{delay: 5 * time.Millisecond}, events, time.Now())

	snap := app.snapshot()
	require.Len(t, snap.list, 2) // 2 events
	for _, o := range snap.list {
		assert.NoError(t, o.err)
	}
}

func TestRunScenarioEvents_EventCtxTimeoutFailsEvent(t *testing.T) {
	t.Parallel()
	// One event at t=0 with one partition, with latency exceeding the per-event
	// app timeout.
	events := []scenarioEvent{{at: 0, records: []*kgo.Record{{Topic: "t", Partition: 0}}}}
	client := &fakeProduceClient{delay: appRequestTimeout + 200*time.Millisecond}

	app := runScenarioEvents(context.Background(), client, events, time.Now())

	snap := app.snapshot()
	require.Len(t, snap.list, 1)
	assert.Error(t, snap.list[0].err)
	assert.Less(t, snap.list[0].latency, appRequestTimeout+time.Second)
}

// fakeProduceClient fires the promise synchronously after an optional delay, so
// the scheduler can be exercised without real Kafka machinery.
type fakeProduceClient struct {
	delay time.Duration
	err   error
}

func (f *fakeProduceClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	go func() {
		if f.delay > 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				promise(r, ctx.Err())
				return
			}
		}
		promise(r, f.err)
	}()
}

func (f *fakeProduceClient) Close() {}

func TestRunScenarioEvents_LateCallbackCannotReportSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observations := runScenarioEvents(t.Context(), &lateProduceClient{}, buildScenarioEvents("t", 1, eventSpacing, eventSpacing), time.Now())
		snapshot := observations.snapshot()
		require.Len(t, snapshot.list, 1)
		require.ErrorIs(t, snapshot.list[0].err, context.DeadlineExceeded)
		assert.Equal(t, appRequestTimeout, snapshot.list[0].latency)
	})
}

type lateProduceClient struct{}

func (*lateProduceClient) Produce(_ context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	go func() { time.Sleep(2 * appRequestTimeout); promise(record, nil) }()
}
func (*lateProduceClient) Close() {}

func TestRunScenarioEvents_RecoveryWindowIgnoresSchedulerDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		events := buildScenarioEvents("t", 1, observedDuration, eventSpacing)
		time.Sleep(50*time.Second + eventSpacing)
		observations := runScenarioEvents(t.Context(), &fakeProduceClient{}, events, start)
		snapshot := observations.snapshot()
		require.Len(t, snapshot.list, 120)
		for _, observation := range snapshot.list {
			assert.False(t, observation.issued.Before(start.Add(50*time.Second)))
		}
		recovery := snapshot.scheduledSince(start.Add(50 * time.Second))
		require.Len(t, recovery.list, 20)
		for _, observation := range recovery.list {
			assert.False(t, observation.scheduled.Before(start.Add(50*time.Second)))
		}
		result := scenarioResult{
			sc:          scenario{name: "delayed recovery", expect: scenarioExpectations{recoveryFrom: ptr(50 * time.Second)}},
			wgoRecovery: recovery.summary(), wgoRecoverySlowFraction: recovery.slowFraction(2 * time.Second),
		}
		assert.Empty(t, checkResult(result))
	})
}
