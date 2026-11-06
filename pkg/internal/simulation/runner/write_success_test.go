package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// failingPartitions acknowledges every record except the ones for the listed
// partitions, which stay unanswered until the caller's deadline.
type failingPartitions struct{ stuck map[int32]bool }

func (f failingPartitions) Produce(ctx context.Context, rec *kgo.Record, promise func(*kgo.Record, error)) {
	if f.stuck[rec.Partition] {
		go func() { <-ctx.Done(); promise(rec, ctx.Err()) }()
		return
	}
	promise(rec, nil)
}

func (failingPartitions) Close() {}

func TestRunScenarioEvent_CountsAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	records := make([]*kgo.Record, 10)
	for i := range records {
		records[i] = &kgo.Record{Partition: int32(i)}
	}
	obs := &observations{}
	client := failingPartitions{stuck: map[int32]bool{3: true, 4: true}}
	runScenarioEvent(t.Context(), client, time.Now(), scenarioEvent{records: records}, obs)

	s := obs.snapshot().summary()
	assert.Equal(t, 1, s.total)
	assert.Equal(t, 0, s.successes, "one missing write fails the event")
	assert.Equal(t, 10, s.writes)
	assert.Equal(t, 8, s.writesOK, "the other writes still count")
}

func TestObservations_PlainObservationIsOneWrite(t *testing.T) {
	t.Parallel()
	obs := &observations{}
	obs.add(observation{})
	obs.add(observation{err: errors.New("deadline")})
	s := obs.snapshot().summary()
	assert.Equal(t, 2, s.writes)
	assert.Equal(t, 1, s.writesOK)
}

func TestResultsReport_WritesTableOnlyForFanOut(t *testing.T) {
	t.Parallel()
	fanOut := &observations{}
	fanOut.recordWrites(time.Time{}, time.Time{}, time.Second, errors.New("deadline"), 50, 45)
	fanOut.recordWrites(time.Time{}, time.Time{}, time.Second, nil, 50, 50)
	single := &observations{}
	single.add(observation{})

	var b strings.Builder
	newResultsReport(scenarioResults{entries: []scenarioResult{{sc: scenario{name: "plain"}, wgoSummary: single.snapshot().summary(), kgoSummary: single.snapshot().summary()}}}).writeWritesTable(&b)
	assert.Empty(t, b.String())

	rr := newResultsReport(scenarioResults{entries: []scenarioResult{{sc: scenario{name: "fan"}, wgoSummary: fanOut.snapshot().summary(), kgoSummary: single.snapshot().summary()}}})
	rr.writeWritesTable(&b)
	require.Contains(t, b.String(), "| fan | 95.0% (95/100) |")
}
