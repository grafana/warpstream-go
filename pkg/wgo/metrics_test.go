package wgo

import (
	"fmt"
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

func TestNewMetrics_HedgeTriggers(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	m.hedgeTriggers[hedgeTriggerLatency].Inc()
	m.hedgeTriggers[hedgeTriggerPrimaryFailure].Add(2)
	m.hedgeTriggers[hedgeTriggerDemotedProbe].Add(3)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_produce_hedge_triggers_total Why a logical fallback cascade started: latency (hedge timer, or a healthy primary whose computed delay is already zero), primary_failure (the primary failed before the race), or demoted_probe (the routing-time primary was demoted). One increment per cascade entry, including a cascade that dispatches no request. Not a wire request or a hedge wave.
		# TYPE warpstream_produce_hedge_triggers_total counter
		warpstream_produce_hedge_triggers_total{trigger="demoted_probe"} 3
		warpstream_produce_hedge_triggers_total{trigger="latency"} 1
		warpstream_produce_hedge_triggers_total{trigger="primary_failure"} 2
	`), "warpstream_produce_hedge_triggers_total"))
}

func TestNewMetrics_AgentPoolExcludedLeaders(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	// 0 before any refresh publishes a value.
	require.InDelta(t, 0.0, gaugeValue(t, reg, "warpstream_agentpool_excluded_leaders"), 0)

	m.agentPoolExcludedLeaders.Set(3)
	require.InDelta(t, 3.0, gaugeValue(t, reg, "warpstream_agentpool_excluded_leaders"), 0)

	// The old counter is gone: it was never deployed.
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		assert.NotEqual(t, "warpstream_agentpool_leader_dropped_total", f.GetName())
	}
}

func TestNewMetrics_ProduceAttemptPayload(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	m.observeAttempt(attemptPrimary, produceRequestStats{records: 3, batches: 2, uncompressedBytes: 900, compressedBytes: 300})
	m.observeAttempt(attemptHedge, produceRequestStats{records: 1, batches: 1, uncompressedBytes: 90, compressedBytes: 40})
	m.observeAttempt(attemptHedge, produceRequestStats{records: 2, batches: 1, uncompressedBytes: 60, compressedBytes: 20})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_produce_attempt_records_total Records handed to the direct producer, by attempt role (primary, hedge). Counted at dispatch whether or not the attempt succeeds, including a losing leg that is canceled afterwards. This is attempted work, not records confirmed on the wire; compare with produce_records_total, which counts only acked requests.
		# TYPE warpstream_produce_attempt_records_total counter
		warpstream_produce_attempt_records_total{attempt="hedge"} 3
		warpstream_produce_attempt_records_total{attempt="primary"} 3
		# HELP warpstream_produce_attempt_bytes_total Compressed record bytes handed to the direct producer, by attempt role (primary, hedge). Same boundary as warpstream_produce_attempt_records_total; compare with produce_compressed_bytes_total, which counts only acked requests.
		# TYPE warpstream_produce_attempt_bytes_total counter
		warpstream_produce_attempt_bytes_total{attempt="hedge"} 60
		warpstream_produce_attempt_bytes_total{attempt="primary"} 300
	`), "warpstream_produce_attempt_records_total", "warpstream_produce_attempt_bytes_total"))
}

func TestNewMetrics_HedgeTriggerWins(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	m.hedgeTriggerWins[hedgeTriggerLatency].Inc()
	m.hedgeTriggerWins[hedgeTriggerPrimaryFailure].Add(2)
	m.hedgeTriggerWins[hedgeTriggerDemotedProbe].Add(3)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_produce_hedge_trigger_wins_total Logical fallback cascades whose result won, by the trigger that started the cascade (latency, primary_failure, demoted_probe). Counted at the same point as warpstream_hedge_wins_total, so the series sum to it. Divide by warpstream_produce_hedge_triggers_total for the win rate of each trigger.
		# TYPE warpstream_produce_hedge_trigger_wins_total counter
		warpstream_produce_hedge_trigger_wins_total{trigger="demoted_probe"} 3
		warpstream_produce_hedge_trigger_wins_total{trigger="latency"} 1
		warpstream_produce_hedge_trigger_wins_total{trigger="primary_failure"} 2
	`), "warpstream_produce_hedge_trigger_wins_total"))
}

func TestNewMetrics_ProduceRequestsFailed(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	m.produceRequestsFailed[produceFailureReasonCandidatesExhausted].Inc()
	m.produceRequestsFailed[produceFailureReasonTerminalError].Add(2)
	m.produceRequestsFailed[produceFailureReasonWriteTimeout].Add(3)
	m.produceRequestsFailed[produceFailureReasonInternalError].Add(4)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_produce_requests_failed_total Why a Hedger produce failed: candidates_exhausted (a partition hit its candidate budget or had no unused candidate), terminal_error (a non-retriable or unknown error from the primary or a retry), write_timeout (the work deadline expired; it outranks candidate exhaustion but not a terminal error), or internal_error (routing mismatch, duplicate partition, or an unclassifiable result). The reason is why the retry cascade stopped. One increment per failed invocation, not per public call, record, partition, or wire attempt. Success and caller cancellation are omitted. The routing-mismatch guard is counted here but not in warpstream_produce_requests_attempts.
		# TYPE warpstream_produce_requests_failed_total counter
		warpstream_produce_requests_failed_total{reason="candidates_exhausted"} 1
		warpstream_produce_requests_failed_total{reason="internal_error"} 4
		warpstream_produce_requests_failed_total{reason="terminal_error"} 2
		warpstream_produce_requests_failed_total{reason="write_timeout"} 3
	`), "warpstream_produce_requests_failed_total"))
}

func TestMetrics_ObserveAgentPoolChurn(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	m.observeMetadataRefresh(metadataRefreshTriggerOnDemand, nil, []int32{1, 2}, nil)
	m.observeMetadataRefresh(metadataRefreshTriggerPeriodic, []int32{1, 2}, []int32{1, 2}, nil)
	// A failed refresh reports no churn even if the sets happen to differ.
	m.observeMetadataRefresh(metadataRefreshTriggerOnDemand, []int32{1}, []int32{1, 9}, assert.AnError)
	m.observeMetadataRefresh(metadataRefreshTriggerPeriodic, []int32{1, 2, 3}, []int32{1, 4}, nil)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_agentpool_agents_changed_total NodeIDs added to or removed from the AgentPool on a successful live Metadata refresh, by direction. Constructor initialization is excluded. An address-only or leader-only change is not membership churn.
		# TYPE warpstream_agentpool_agents_changed_total counter
		warpstream_agentpool_agents_changed_total{direction="added"} 3
		warpstream_agentpool_agents_changed_total{direction="removed"} 2
	`), "warpstream_agentpool_agents_changed_total"))
}

// BenchmarkMetrics_ObserveAttempt measures the per-dispatch cost of the
// attempted-payload counters for a many-partition payload.
func BenchmarkMetrics_ObserveAttempt(b *testing.B) {
	for _, partitions := range []int{1, 32, 256, 1024} {
		b.Run(fmt.Sprintf("partitions=%d", partitions), func(b *testing.B) {
			m := newMetrics(prometheus.NewRegistry())
			parts := make([]routedEncodedTopicPartitionRecords, partitions)
			for i := range parts {
				parts[i].encodedStats = produceRequestStats{records: 10, batches: 1, uncompressedBytes: 1000, compressedBytes: 400}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.observeAttempt(attemptPrimary, sumEncodedStats(parts))
			}
		})
	}
}

func BenchmarkMetrics_HedgeTriggerInc(b *testing.B) {
	m := newMetrics(prometheus.NewRegistry())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.hedgeAttemptsTotal.Inc()
		m.hedgeTriggers[hedgeTriggerLatency].Inc()
	}
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

func TestNewMetrics_RoutingCounters(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	assert.Equal(t, int(routeSourceCount), testutil.CollectAndCount(reg, "warpstream_partition_routes_total"))
	assert.Equal(t, int(routingMissCount), testutil.CollectAndCount(reg, "warpstream_routing_misses_total"))

	m.partitionRoutes[routeSourceStandIn].Add(3)
	m.routingMisses[routingMissPartitionOutOfRange].Inc()
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP warpstream_partition_routes_total Input records routed to an agent, by how the strategy chose it: leader (the partition's named leader is in the snapshot) or stand_in (the leader entry was missing for a known topic, so a live agent was picked). Counted once per record at the initial routing decision, not per hedge, retry or flush. A demoted leader replaced by the Demoter keeps the classification of the lookup. stand_in covers every missing leader entry, not only an excluded leader.
		# TYPE warpstream_partition_routes_total counter
		warpstream_partition_routes_total{source="leader"} 0
		warpstream_partition_routes_total{source="stand_in"} 3
		# HELP warpstream_routing_misses_total Input records rejected because the initial lookup found no agent, by reason: empty_pool (no agents), unknown_topic (the topic is not in the snapshot, including a topic Metadata returned with an error), no_leader (WarpStream named no leader for the partition), partition_out_of_range (the partition does not exist), or other (no agent was found and no reason was set; not expected with the default strategy). Counted once per record, matching warpstream_produce_records_rejected_total{reason="no_agent_assigned"}.
		# TYPE warpstream_routing_misses_total counter
		warpstream_routing_misses_total{reason="empty_pool"} 0
		warpstream_routing_misses_total{reason="no_leader"} 0
		warpstream_routing_misses_total{reason="other"} 0
		warpstream_routing_misses_total{reason="partition_out_of_range"} 1
		warpstream_routing_misses_total{reason="unknown_topic"} 0
	`), "warpstream_partition_routes_total", "warpstream_routing_misses_total"))
}
