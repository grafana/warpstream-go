package main

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthyBehaviours_OneEntryPerBroker(t *testing.T) {
	t.Parallel()
	b := healthyBehaviours()
	require.Equal(t, int(clusterSize), len(b.byBroker))
	for i := int32(0); i < clusterSize; i++ {
		got, ok := b.forBroker(i)
		require.True(t, ok, "broker %d missing", i)
		require.NotNil(t, got.latencyFn, "broker %d has nil latencyFn", i)
	}
}

func TestBrokersBehaviourProvider_GetReturnsLastSet(t *testing.T) {
	t.Parallel()
	first := brokersBehaviour{byBroker: map[int32]brokerBehaviour{1: {failRate: 1.0}}}
	p := newBrokersBehaviourProvider(first)
	got := p.get()
	require.Contains(t, got.byBroker, int32(1))
	require.Equal(t, 1.0, got.byBroker[1].failRate)

	second := brokersBehaviour{byBroker: map[int32]brokerBehaviour{2: {failRate: 1.0}, 3: {failRate: 0.5}}}
	p.set(second)
	got = p.get()
	require.NotContains(t, got.byBroker, int32(1))
	require.Contains(t, got.byBroker, int32(2))
	require.Contains(t, got.byBroker, int32(3))
	require.Equal(t, 0.5, got.byBroker[3].failRate)
}

func TestBrokersBehaviourProvider_NextFailureFor(t *testing.T) {
	t.Parallel()

	t.Run("never fails with zero failRate", func(t *testing.T) {
		p := newBrokersBehaviourProvider(brokersBehaviour{byBroker: map[int32]brokerBehaviour{0: {}}})
		for i := 0; i < 100; i++ {
			assert.False(t, p.nextFailureFor(clientTypeWgo, 0))
		}
	})

	t.Run("always fails with failRate one", func(t *testing.T) {
		p := newBrokersBehaviourProvider(brokersBehaviour{byBroker: map[int32]brokerBehaviour{0: {failRate: 1.0}}})
		for i := 0; i < 100; i++ {
			assert.True(t, p.nextFailureFor(clientTypeWgo, 0))
		}
	})
}

func TestBrokersBehaviourProvider_NextLatencyFor(t *testing.T) {
	t.Parallel()

	t.Run("zero when broker has no latencyFn", func(t *testing.T) {
		p := newBrokersBehaviourProvider(brokersBehaviour{byBroker: map[int32]brokerBehaviour{0: {}}})
		assert.Equal(t, time.Duration(0), p.nextLatencyFor(clientTypeWgo, 0))
	})

	t.Run("draws from the broker's latencyFn", func(t *testing.T) {
		p := newBrokersBehaviourProvider(brokersBehaviour{byBroker: map[int32]brokerBehaviour{
			0: {latencyFn: func(*rand.Rand) time.Duration { return 42 * time.Millisecond }},
		}})
		assert.Equal(t, 42*time.Millisecond, p.nextLatencyFor(clientTypeWgo, 0))
	})

	t.Run("seeding is reproducible across providers", func(t *testing.T) {
		b := brokersBehaviour{byBroker: map[int32]brokerBehaviour{0: {latencyFn: normalLatency}}}
		p1 := newBrokersBehaviourProvider(b)
		p2 := newBrokersBehaviourProvider(b)
		assert.Equal(t, p1.nextLatencyFor(clientTypeWgo, 0), p2.nextLatencyFor(clientTypeWgo, 0))
	})
}

// TestTriangular verifies draws stay inside [min, max] with a mean close to the
// analytic (min+mode+max)/3. Bounds are loose enough to be stable across runs but
// tight enough to catch a regression where the distribution collapses or escapes
// its range.
func TestTriangular(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		min, mode, max time.Duration
	}{
		{"mid-mode", 250 * time.Millisecond, 500 * time.Millisecond, time.Second},
		{"slow", 500 * time.Millisecond, time.Second, 3 * time.Second},
		{"degenerate", 100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
		{"min-at-mode", 100 * time.Millisecond, 100 * time.Millisecond, time.Second},
		{"max-at-mode", 100 * time.Millisecond, time.Second, time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			const n = 100_000
			var sum time.Duration
			for i := 0; i < n; i++ {
				d := triangular(rng, tc.min, tc.mode, tc.max)
				require.GreaterOrEqualf(t, d, tc.min, "draw below min (i=%d)", i)
				require.LessOrEqualf(t, d, tc.max, "draw above max (i=%d)", i)
				sum += d
			}
			mean := sum / n
			expected := (tc.min + tc.mode + tc.max) / 3
			tolerance := time.Duration(0.05 * float64(tc.max-tc.min))
			if tolerance < time.Millisecond {
				tolerance = time.Millisecond
			}
			require.InDeltaf(t, float64(expected), float64(mean), float64(tolerance),
				"empirical mean %v outside %v±%v", mean, expected, tolerance)
		})
	}
}

func TestBurstyLatency_AddsExpectedExtraLatency(t *testing.T) {
	t.Parallel()
	base := func(*rand.Rand) time.Duration { return 100 * time.Millisecond }
	burst := burstyLatency(base, 0.5, 500*time.Millisecond)

	rng := rand.New(rand.NewPCG(7, 9))
	const n = 20_000
	var sum, bursts time.Duration
	for i := 0; i < n; i++ {
		d := burst(rng)
		sum += d
		if d > 100*time.Millisecond {
			bursts++
		}
	}
	mean := sum / n
	// Expected mean = 100ms + 0.5*500ms = 350ms; allow ±10ms.
	require.InDelta(t, float64(350*time.Millisecond), float64(mean), float64(10*time.Millisecond),
		"empirical mean %v not near 350ms", mean)
	// Expected burst rate ≈ 50%, allow ±2%.
	rate := float64(bursts) / float64(n)
	require.InDelta(t, 0.5, rate, 0.02, "empirical burst rate %.3f not near 0.5", rate)
}

func TestLatencyModels_AverageLatency(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		fn      func(*rand.Rand) time.Duration
		wantAvg time.Duration
	}{
		"normalLatency": {normalLatency, 400 * time.Millisecond}, // (100+100+1000)/3
		"highLatency":   {highLatency, 1500 * time.Millisecond},  // (500+1000+3000)/3
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			const n = 100_000
			var sum time.Duration
			for i := 0; i < n; i++ {
				sum += tc.fn(rng)
			}
			mean := sum / n
			require.InDeltaf(t, float64(tc.wantAvg), float64(mean), float64(20*time.Millisecond),
				"empirical mean %v not near %v", mean, tc.wantAvg)
		})
	}
}
