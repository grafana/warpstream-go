package main

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fixedLatency returns a latencyFn yielding draws in order, repeating the last.
func fixedLatency(draws ...time.Duration) func(*rand.Rand) time.Duration {
	i := 0
	return func(*rand.Rand) time.Duration {
		d := draws[min(i, len(draws)-1)]
		i++
		return d
	}
}

func newLatencyClient(t *testing.T, fn func(*rand.Rand) time.Duration) *latencyDelayedKgoClient {
	t.Helper()
	return &latencyDelayedKgoClient{
		behaviours: newBrokersBehaviourProvider(brokersBehaviour{
			byBroker: map[int32]brokerBehaviour{0: {latencyFn: fn}},
		}),
		client: clientTypeKgo,
	}
}

func TestAwaitSimulatedLatency_FastDrawSucceeds(t *testing.T) {
	t.Parallel()
	c := newLatencyClient(t, fixedLatency(50*time.Millisecond))

	start := time.Now()
	require.NoError(t, c.awaitSimulatedLatency(context.Background(), 0))
	assert.WithinDuration(t, start.Add(50*time.Millisecond), time.Now(), 200*time.Millisecond)
}

// A draw past the attempt deadline is abandoned and retried with a fresh draw,
// which is the recovery path the post-promise injection used to suppress.
// Uses a short delivery budget so the test doesn't wait out the real 10s one.
func TestAwaitSimulatedLatency_AbandonedAttemptRedraws(t *testing.T) {
	t.Parallel()
	const attempt = 100 * time.Millisecond
	c := newLatencyClient(t, fixedLatency(attempt+time.Second, 20*time.Millisecond))

	// Inline the loop with test-sized budgets: one overrunning draw, then a fast one.
	deliveryDeadline := time.Now().Add(2 * time.Second)
	var attempts int
	var err error
	for {
		attempts++
		draw := c.behaviours.nextLatencyFor(c.client, 0)
		wait := min(draw, attempt)
		if remaining := time.Until(deliveryDeadline); wait >= remaining {
			err = kgo.ErrRecordTimeout
			break
		}
		require.NoError(t, sleepCtx(context.Background(), wait))
		if draw <= attempt {
			break
		}
	}
	assert.NoError(t, err)
	assert.Equal(t, 2, attempts, "overrunning attempt should be retried once")
}

func TestAwaitSimulatedLatency_CtxCancelWins(t *testing.T) {
	t.Parallel()
	c := newLatencyClient(t, fixedLatency(10*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.awaitSimulatedLatency(ctx, 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.WithinDuration(t, start.Add(100*time.Millisecond), time.Now(), 500*time.Millisecond)
}

func TestAwaitSimulatedLatency_NoLatencyConfigured(t *testing.T) {
	t.Parallel()
	c := &latencyDelayedKgoClient{
		behaviours: newBrokersBehaviourProvider(brokersBehaviour{byBroker: map[int32]brokerBehaviour{}}),
		client:     clientTypeKgo,
	}
	require.NoError(t, c.awaitSimulatedLatency(context.Background(), 0))
}

// The harness gives kgo exactly one produce attempt: RecordDeliveryTimeout
// equals the per-attempt budget, so no draw can ever leave room for a retry.
// If this ever stops holding, awaitSimulatedLatency's redraw path goes live and
// the kgo baseline (which minSuccessDeltaVsKgo gates on) shifts.
func TestKgoRetryBudget_IsExhaustedByASingleAttempt(t *testing.T) {
	t.Parallel()
	assert.Equal(t, clientWriteTimeout, kgoAttemptDeadline,
		"delivery budget == attempt budget, so kgo gets one latency attempt")
}
