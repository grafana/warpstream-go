package wgo

import (
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
