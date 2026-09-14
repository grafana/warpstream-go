package wgo

import (
	"runtime/debug"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFilteringRegisterer_DropsBlockedNames(t *testing.T) {
	t.Run("drops blocked, registers the rest", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		fr := newFilteringRegisterer(reg, "blocked_total")

		promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "blocked_total", Help: "h"})
		promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "allowed_total", Help: "h"})

		assert.Equal(t, 0, testutil.CollectAndCount(reg, "blocked_total"))
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "allowed_total"))
	})

	t.Run("matches the bare name beneath an outer prefix", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// The filter sits between kprom and a prefixing registerer, so it sees
		// bare names; the prefix is applied only when it forwards.
		fr := newFilteringRegisterer(prometheus.WrapRegistererWithPrefix("outer_", reg), "blocked_total")

		promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "blocked_total", Help: "h"})
		promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "allowed_total", Help: "h"})

		assert.Equal(t, 0, testutil.CollectAndCount(reg, "outer_blocked_total"))
		assert.Equal(t, 1, testutil.CollectAndCount(reg, "outer_allowed_total"))
	})

	t.Run("re-registering a blocked name does not panic", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		fr := newFilteringRegisterer(reg, "blocked_total")

		require.NotPanics(t, func() {
			promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "blocked_total", Help: "h"})
			promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "blocked_total", Help: "h"})
		})
	})

	t.Run("nil wrapped registerer is a no-op", func(t *testing.T) {
		fr := newFilteringRegisterer(nil, "blocked_total")

		require.NotPanics(t, func() {
			c := promauto.With(fr).NewCounter(prometheus.CounterOpts{Name: "allowed_total", Help: "h"})
			c.Inc()
			fr.Unregister(c)
		})
	})
}

// TestProducerStateMetricsMatchKprom asserts that the producer-state metric
// names this client owns are exactly the names kprom emits for the same
// concepts. If kprom renames or adds one, this fails so the names this client
// registers (and the filter that drops kprom's versions) can be kept in sync.
func TestProducerStateMetricsMatchKprom(t *testing.T) {
	// Unfiltered kprom, built from the same config newKgoClient uses, so this
	// observes exactly the names a real client would (before the filter drops
	// the producer-state ones).
	reg := prometheus.NewPedanticRegistry()
	km := newKpromMetrics(reg)

	// OnNewClient (fired synchronously by NewClient) registers kprom's
	// collectors; no connection is made to the bogus seed broker.
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:0"), kgo.WithHooks(km))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	// The produce counters only emit a series once observed, so fire the hook;
	// the buffered gauges emit unconditionally.
	km.OnProduceBatchWritten(kgo.BrokerMetadata{}, "t", 0, kgo.ProduceBatchMetrics{NumRecords: 1, UncompressedBytes: 1, CompressedBytes: 1})

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var got []string
	for _, mf := range mfs {
		if name := mf.GetName(); strings.HasPrefix(name, "produce_") || strings.HasPrefix(name, "buffered_produce_") {
			got = append(got, name)
		}
	}
	assert.ElementsMatch(t, kpromProducerStateMetricNames, got,
		"kprom's producer-state metric names changed; update kpromProducerStateMetricNames and the names this client registers in newMetrics / NewClusterBuffer")
}

// Under `go test`, whether debug.ReadBuildInfo() fills in Deps depends on the
// Go version and local build setup, not on this code, so the exact version
// strings vary by machine. This checks the gauge carries whatever
// clientBuildInfo() itself returns; see TestBuildInfoVersions for coverage
// of the Deps-lookup logic on fixed, synthetic input.
func TestNewMetrics_BuildInfo(t *testing.T) {
	wantVersion, wantFranzGoVersion := clientBuildInfo()

	reg := prometheus.NewPedanticRegistry()
	newMetrics(reg)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != "warpstream_client_build_info" {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		m := mf.GetMetric()[0]
		assert.Equal(t, float64(1), m.GetGauge().GetValue())

		labels := map[string]string{}
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		assert.Equal(t, wantVersion, labels["version"])
		assert.Equal(t, wantFranzGoVersion, labels["franz_go_version"])
		return
	}
	t.Fatal("warpstream_client_build_info metric not found")
}

func TestBuildInfoVersions(t *testing.T) {
	t.Run("reads warpstream-go and franz-go versions from Deps", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/someapp", Version: "v1.0.0"},
			Deps: []*debug.Module{
				{Path: warpstreamGoModulePath, Version: "v1.2.3"},
				{Path: franzGoModulePath, Version: "v1.21.5"},
			},
		}
		version, franzGoVersion := buildInfoVersions(info)
		assert.Equal(t, "v1.2.3", version)
		assert.Equal(t, "v1.21.5", franzGoVersion)
	})

	t.Run("prefers dep.Replace over the pre-replace requirement", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/someapp", Version: "v1.0.0"},
			Deps: []*debug.Module{
				{
					Path:    franzGoModulePath,
					Version: "v0.0.0-00010101000000-000000000000",
					Replace: &debug.Module{Path: "../local-fork", Version: "v1.99.0"},
				},
			},
		}
		_, franzGoVersion := buildInfoVersions(info)
		assert.Equal(t, "v1.99.0", franzGoVersion)
	})

	t.Run("falls back to unknown for a malformed version", func(t *testing.T) {
		info := &debug.BuildInfo{
			Deps: []*debug.Module{
				{Path: franzGoModulePath, Version: ""},
			},
		}
		_, franzGoVersion := buildInfoVersions(info)
		assert.Equal(t, "unknown", franzGoVersion)
	})

	t.Run("uses Main.Version when warpstream-go is the main module", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Path: warpstreamGoModulePath, Version: "(devel)"},
		}
		version, _ := buildInfoVersions(info)
		assert.Equal(t, "(devel)", version)
	})
}

// gaugeValue returns the single-series value of the named gauge family.
func gaugeValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()

	mfs, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		return mf.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("gauge %q not found", name)
	return 0
}

func TestMetrics_ObserveMetadataRefresh(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	m.observeMetadataRefresh(metadataRefreshTriggerOnDemand, nil, []int32{1, 2}, nil)
	m.observeMetadataRefresh(metadataRefreshTriggerPeriodic, []int32{1, 2}, []int32{1, 2}, nil)
	m.observeMetadataRefresh(metadataRefreshTriggerOnDemand, []int32{1}, []int32{1}, assert.AnError)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_metadata_refresh_results_total Total number of live AgentPool Metadata refreshes, by trigger (periodic, on_demand) and result (membership_changed, unchanged, failed). membership_changed is the sorted Agent NodeID set only; leader-only or topic-only updates are unchanged. The constructor Refresh is not counted.
		# TYPE warpstream_metadata_refresh_results_total counter
		warpstream_metadata_refresh_results_total{result="failed",trigger="on_demand"} 1
		warpstream_metadata_refresh_results_total{result="membership_changed",trigger="on_demand"} 1
		warpstream_metadata_refresh_results_total{result="unchanged",trigger="periodic"} 1
	`), "warpstream_metadata_refresh_results_total"))
}

func TestMetrics_ObserveClusterStats(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_stats_available"), 0)
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_slow_fraction"), 0)
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_slow_contributors"), 0)
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_faulty_fraction"), 0)
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_faulty_contributors"), 0)

	m.observeClusterStats(ClusterStats{
		SlowFraction:            0.1,
		SlowContributorsCount:   10,
		FaultyFraction:          0.2,
		FaultyContributorsCount: 5,
	}, true)

	require.InDelta(t, 1.0, gaugeValue(t, reg, "warpstream_cluster_stats_available"), 0)
	require.InDelta(t, 0.1, gaugeValue(t, reg, "warpstream_cluster_slow_fraction"), 1e-9)
	require.InDelta(t, 10, gaugeValue(t, reg, "warpstream_cluster_slow_contributors"), 0)
	require.InDelta(t, 0.2, gaugeValue(t, reg, "warpstream_cluster_faulty_fraction"), 1e-9)
	require.InDelta(t, 5, gaugeValue(t, reg, "warpstream_cluster_faulty_contributors"), 0)

	m.observeClusterStats(ClusterStats{}, false)
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_cluster_stats_available"), 0)
	require.InDelta(t, 0.1, gaugeValue(t, reg, "warpstream_cluster_slow_fraction"), 1e-9)
	require.InDelta(t, 10, gaugeValue(t, reg, "warpstream_cluster_slow_contributors"), 0)
	require.InDelta(t, 0.2, gaugeValue(t, reg, "warpstream_cluster_faulty_fraction"), 1e-9)
	require.InDelta(t, 5, gaugeValue(t, reg, "warpstream_cluster_faulty_contributors"), 0)
}

func BenchmarkMetrics_DirectRequestAccounting(b *testing.B) {
	m := newMetrics(prometheus.NewRegistry())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := agentStateHealthy
		if i%4 == 0 {
			state = agentStateDemoted
		}
		m.produceDirectRequestsTotal.WithLabelValues(state).Inc()
		if i%8 == 0 {
			m.produceDirectRequestsFailedTotal.WithLabelValues("timeout", state).Inc()
		}
	}
}

func BenchmarkMetrics_ObserveClusterStats(b *testing.B) {
	m := newMetrics(prometheus.NewRegistry())
	stats := ClusterStats{
		SlowFraction:            0.1,
		SlowContributorsCount:   10,
		FaultyFraction:          0.05,
		FaultyContributorsCount: 2,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.observeClusterStats(stats, i%8 != 0)
	}
}

func BenchmarkMetrics_ClusterStatsCollect(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	m.observeClusterStats(ClusterStats{
		SlowFraction:            0.1,
		SlowContributorsCount:   10,
		FaultyFraction:          0.05,
		FaultyContributorsCount: 2,
	}, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := reg.Gather(); err != nil {
			b.Fatal(err)
		}
	}
}
