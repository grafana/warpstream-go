package wgo

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type countingAgentStatsTracker struct {
	clusterCalls atomic.Int32
	ok           bool
	stats        ClusterStats
}

func (c *countingAgentStatsTracker) TrackAgentRequest(time.Time, int32, time.Duration, error) {}

func (c *countingAgentStatsTracker) AgentStats(time.Time, int32) (AgentStats, bool) {
	return AgentStats{}, false
}

func (c *countingAgentStatsTracker) PurgeAgents([]int32) {}

func (c *countingAgentStatsTracker) ClusterStats(time.Time, float64, float64) (ClusterStats, bool) {
	c.clusterCalls.Add(1)
	return c.stats, c.ok
}

func TestObservedAgentStatsTracker_ClusterStats(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)
	inner := &countingAgentStatsTracker{
		ok: true,
		stats: ClusterStats{
			SlowFraction:            0.1,
			SlowContributorsCount:   8,
			FaultyFraction:          0.25,
			FaultyContributorsCount: 4,
		},
	}
	cached := NewCachedAgentStatsTracker(NewObservedAgentStatsTracker(inner, m), time.Second)
	now := time.Unix(1_700_000_000, 0)

	_, ok := cached.ClusterStats(now, 2.0, 0.05)
	require.True(t, ok)
	require.Equal(t, int32(1), inner.clusterCalls.Load())
	require.InDelta(t, 1.0, gaugeValue(t, reg, "warpstream_cluster_stats_available"), 0)
	require.InDelta(t, 0.1, gaugeValue(t, reg, "warpstream_cluster_slow_fraction"), 1e-9)
	require.InDelta(t, 8, gaugeValue(t, reg, "warpstream_cluster_slow_contributors"), 0)
	require.InDelta(t, 0.25, gaugeValue(t, reg, "warpstream_cluster_faulty_fraction"), 1e-9)
	require.InDelta(t, 4, gaugeValue(t, reg, "warpstream_cluster_faulty_contributors"), 0)

	// A cache hit must not reach the observer, so the gauges keep the cached
	// reading even though the inner tracker would now report something else.
	inner.stats = ClusterStats{SlowFraction: 0.8, SlowContributorsCount: 7, FaultyFraction: 0.9, FaultyContributorsCount: 9}
	_, ok = cached.ClusterStats(now.Add(100*time.Millisecond), 2.0, 0.05)
	require.True(t, ok)
	require.Equal(t, int32(1), inner.clusterCalls.Load())
	require.InDelta(t, 0.1, gaugeValue(t, reg, "warpstream_cluster_slow_fraction"), 1e-9)
	require.InDelta(t, 8, gaugeValue(t, reg, "warpstream_cluster_slow_contributors"), 0)
	require.InDelta(t, 0.25, gaugeValue(t, reg, "warpstream_cluster_faulty_fraction"), 1e-9)
	require.InDelta(t, 4, gaugeValue(t, reg, "warpstream_cluster_faulty_contributors"), 0)

	inner.ok = false
	inner.stats = ClusterStats{}
	_, ok = cached.ClusterStats(now.Add(time.Second), 2.0, 0.05)
	require.False(t, ok)
	require.Equal(t, int32(2), inner.clusterCalls.Load())
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_stats_available"), 0)
	require.InDelta(t, 0.1, gaugeValue(t, reg, "warpstream_cluster_slow_fraction"), 1e-9)
	require.InDelta(t, 8, gaugeValue(t, reg, "warpstream_cluster_slow_contributors"), 0)
	require.InDelta(t, 0.25, gaugeValue(t, reg, "warpstream_cluster_faulty_fraction"), 1e-9)
	require.InDelta(t, 4, gaugeValue(t, reg, "warpstream_cluster_faulty_contributors"), 0)
}
