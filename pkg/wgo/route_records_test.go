package wgo

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWarpstreamClient_RouteRecords(t *testing.T) {
	const topic = "t"

	rec := func(topic string, partition int32, value string) *kgo.Record {
		return &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}
	}
	values := func(records []*kgo.Record) []string {
		out := make([]string, len(records))
		for i, r := range records {
			out[i] = string(r.Value)
		}
		return out
	}

	t.Run("all routable preserves destination, probe state, and grouping", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{
			{topic, 0}: {{NodeID: 7, State: AgentStateDemoted}},
			{topic, 1}: healthyAgents(8),
		}}
		nudge := make(chan struct{}, 4)
		c := newRouteRecordsClient(strategy, nudge)
		var accepted int
		records := []*kgo.Record{rec(topic, 1, "a"), rec(topic, 0, "b"), rec(topic, 1, "c")}

		routed, rejected := c.routeRecords(records, countAccepted(&accepted))

		require.Nil(t, rejected)
		require.Len(t, routed, 2)
		assert.Equal(t, int32(8), routed[0].item.nodeID)
		assert.Equal(t, AgentStateHealthy, routed[0].item.nodeState)
		assert.Equal(t, []string{"a", "c"}, values(routed[0].item.records))
		assert.Equal(t, int32(7), routed[1].item.nodeID)
		assert.Equal(t, AgentStateDemoted, routed[1].item.nodeState)
		assert.Equal(t, []string{"b"}, values(routed[1].item.records))
		assert.Equal(t, 2, accepted)
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 1))
		assert.Empty(t, nudge)
	})

	t.Run("interleaved topics keep input order and look up each partition once", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{
			{"a", 0}: healthyAgents(1),
			{"b", 1}: healthyAgents(2),
		}}
		c := newRouteRecordsClient(strategy, make(chan struct{}, 4))
		var accepted int
		records := []*kgo.Record{
			rec("a", 0, "a1"),
			rec("b", 0, "b1"),
			rec("a", 0, "a2"),
			rec("c", 0, "c1"),
			rec("b", 0, "b2"),
			rec("b", 1, "b3"),
		}

		routed, rejected := c.routeRecords(records, countAccepted(&accepted))

		require.Len(t, routed, 2)
		assert.Equal(t, []string{"a1", "a2"}, values(routed[0].item.records))
		assert.Equal(t, "a", routed[0].item.topic)
		assert.Equal(t, []string{"b3"}, values(routed[1].item.records))
		require.Len(t, rejected, 2)
		assert.Equal(t, []string{"b1", "b2"}, values(rejected[0].records))
		assert.Equal(t, "b", rejected[0].topic)
		assert.Equal(t, int32(0), rejected[0].partition)
		assert.ErrorContains(t, rejected[0].err, `no agent assigned for topic "b" partition 0`)
		assert.Equal(t, []string{"c1"}, values(rejected[1].records))
		assert.Equal(t, 2, accepted)
		assert.Equal(t, 1, strategy.candidatesCalls("a", 0))
		assert.Equal(t, 1, strategy.candidatesCalls("b", 0))
		assert.Equal(t, 1, strategy.candidatesCalls("b", 1))
		assert.Equal(t, 1, strategy.candidatesCalls("c", 0))
	})

	t.Run("all unroutable builds one group and no accepted callback", func(t *testing.T) {
		nudge := make(chan struct{}, 4)
		c := newRouteRecordsClient(&mockPartitionAssignmentStrategy{}, nudge)
		var accepted int
		records := []*kgo.Record{
			rec(topic, 3, "a"), rec(topic, 3, "b"), rec(topic, 3, "c"),
		}

		routed, rejected := c.routeRecords(records, countAccepted(&accepted))

		assert.Empty(t, routed)
		require.Len(t, rejected, 1)
		assert.Equal(t, []string{"a", "b", "c"}, values(rejected[0].records))
		assert.Equal(t, 0, accepted)
		assert.Len(t, nudge, 1)
	})

	for _, place := range []struct {
		name string
		at   int
	}{{"front", 0}, {"middle", 1}, {"back", 2}} {
		t.Run("one miss at the "+place.name, func(t *testing.T) {
			strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{
				{topic, 0}: healthyAgents(1),
				{topic, 1}: healthyAgents(1),
				{topic, 2}: healthyAgents(1),
			}}
			delete(strategy.candidates, partitionKey{topic, int32(place.at)})
			c := newRouteRecordsClient(strategy, make(chan struct{}, 4))
			records := []*kgo.Record{rec(topic, 0, "0"), rec(topic, 1, "1"), rec(topic, 2, "2")}

			routed, rejected := c.routeRecords(records, countAccepted(new(int)))

			require.Len(t, rejected, 1)
			assert.Equal(t, int32(place.at), rejected[0].partition)
			assert.Len(t, routed, 2)
			for _, g := range routed {
				assert.NotEqual(t, int32(place.at), g.item.partition)
			}
		})
	}

	t.Run("a repeated rejected partition is not looked up again", func(t *testing.T) {
		strategy := &seqStrategy{answer: func(partition int32, call int) []Agent {
			if partition == 1 && call == 1 {
				return nil
			}
			if partition == 1 {
				return healthyAgents(9)
			}
			if call == 1 {
				return []Agent{{NodeID: 1, State: AgentStateDemoted}}
			}
			return healthyAgents(2)
		}}
		nudge := make(chan struct{}, 4)
		c := newRouteRecordsClient(strategy, nudge)
		records := []*kgo.Record{
			rec(topic, 0, "a"),
			rec(topic, 1, "b"),
			rec(topic, 0, "c"),
			rec(topic, 1, "d"),
			rec(topic, 2, "e"),
		}

		routed, rejected := c.routeRecords(records, countAccepted(new(int)))

		require.Len(t, routed, 2)
		assert.Equal(t, int32(1), routed[0].item.nodeID)
		assert.Equal(t, AgentStateDemoted, routed[0].item.nodeState)
		assert.Equal(t, []string{"a", "c"}, values(routed[0].item.records))
		assert.Equal(t, int32(2), routed[1].item.partition)
		require.Len(t, rejected, 1)
		assert.Equal(t, []string{"b", "d"}, values(rejected[0].records))
		assert.Equal(t, 1, strategy.calls[topicPartition{topic, 0}])
		assert.Equal(t, 1, strategy.calls[topicPartition{topic, 1}])
		assert.Equal(t, 1, strategy.calls[topicPartition{topic, 2}])
		assert.Len(t, nudge, 1)
	})

	t.Run("several misses nudge once", func(t *testing.T) {
		nudge := make(chan struct{}, 8)
		c := newRouteRecordsClient(&mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{
			{topic, 0}: healthyAgents(1),
		}}, nudge)
		records := []*kgo.Record{rec(topic, 1, "a"), rec(topic, 2, "b"), rec(topic, 1, "c")}

		_, rejected := c.routeRecords(records, countAccepted(new(int)))

		require.Len(t, rejected, 2)
		assert.Len(t, nudge, 1)
	})

	t.Run("a full nudge channel does not block", func(t *testing.T) {
		nudge := make(chan struct{}, 1)
		nudge <- struct{}{}
		c := newRouteRecordsClient(&mockPartitionAssignmentStrategy{}, nudge)
		done := make(chan struct{})
		go func() {
			c.routeRecords([]*kgo.Record{rec(topic, 0, "a"), rec(topic, 1, "b")}, countAccepted(new(int)))
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("routeRecords blocked on a full nudge channel")
		}
		assert.Len(t, nudge, 1)
	})

	t.Run("no leader, unknown topic, and empty pool are rejected", func(t *testing.T) {
		agents := []int32{3}
		strategy := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
			{topic: topic, partition: 0}: 3,
		}, nil, map[topicPartition]struct{}{
			{topic: topic, partition: 1}: {},
		}, map[string]int32{topic: 10})
		c := newRouteRecordsClient(strategy, make(chan struct{}, 4))
		records := []*kgo.Record{
			rec(topic, 0, "leader"),
			rec(topic, 1, "unnamed"),
			rec("never-seen", 0, "unknown"),
			rec(topic, 9, "named-missing"),
		}

		routed, rejected := c.routeRecords(records, countAccepted(new(int)))

		require.Len(t, routed, 2)
		assert.Equal(t, int32(0), routed[0].item.partition)
		assert.Equal(t, int32(9), routed[1].item.partition)
		assert.Equal(t, agents[0], routed[1].item.nodeID)
		require.Len(t, rejected, 2)
		assert.Equal(t, int32(1), rejected[0].partition)
		assert.Equal(t, "never-seen", rejected[1].topic)

		empty := newRouteRecordsClient(newDefaultPartitionAssignmentStrategy(nil, nil, nil, nil, nil), make(chan struct{}, 1))
		routed, rejected = empty.routeRecords([]*kgo.Record{rec(topic, 0, "a")}, countAccepted(new(int)))
		assert.Empty(t, routed)
		require.Len(t, rejected, 1)
	})

	t.Run("out-of-range partition is rejected and still nudges a refresh", func(t *testing.T) {
		agents := []int32{3}
		nudge := make(chan struct{}, 1)
		strategy := newDefaultPartitionAssignmentStrategy(agents, map[topicPartition]int32{
			{topic: topic, partition: 0}: 3,
		}, nil, nil, map[string]int32{topic: 10})
		c := newRouteRecordsClient(strategy, nudge)

		routed, rejected := c.routeRecords([]*kgo.Record{
			rec(topic, 0, "leader"),
			rec(topic, 100, "out-of-range"),
		}, countAccepted(new(int)))

		require.Len(t, routed, 1)
		require.Len(t, rejected, 1)
		assert.Equal(t, int32(100), rejected[0].partition)
		assert.Len(t, nudge, 1)
	})
}

func newRouteRecordsClient(strategy PartitionAssignmentStrategy, nudge chan struct{}) *WarpstreamClient {
	return &WarpstreamClient{
		demoter:      NewDemoter(strategy, noopAgentStatsTracker{}, HealthCheckConfig{}, DemoterConfig{}, nopLogger{}, prometheus.NewRegistry()),
		metrics:      newMetrics(prometheus.NewRegistry()),
		refreshNowCh: nudge,
	}
}

func countAccepted(n *int) func([]*kgo.Record) func(ProduceResult) {
	return func([]*kgo.Record) func(ProduceResult) {
		*n++
		return func(ProduceResult) {}
	}
}

// seqStrategy changes its answer after the first call, so a repeat lookup shows up.
type seqStrategy struct {
	answer func(partition int32, call int) []Agent
	calls  map[topicPartition]int
}

func (s *seqStrategy) Candidates(topic string, partition int32, _ int) []Agent {
	if s.calls == nil {
		s.calls = map[topicPartition]int{}
	}
	key := topicPartition{topic: topic, partition: partition}
	s.calls[key]++
	return s.answer(partition, s.calls[key])
}

func BenchmarkWarpstreamClient_RouteRecords(b *testing.B) {
	const topic = "t"
	cases := []struct {
		name       string
		records    int
		partitions int
		missEvery  int
	}{
		{"all routable 32x1", 32, 1, 0},
		{"all routable 32x32", 32, 32, 0},
		{"all routable 256x32", 256, 32, 0},
		{"miss at front 256", 256, 32, -1},
		{"several misses 256", 256, 32, 2},
		{"all unroutable 256", 256, 32, 1},
		{"many records one rejected partition", 256, 1, 1},
		{"interleaved 256", 256, 16, 2},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			records, strategy := benchRouteInputs(topic, tc.records, tc.partitions, tc.missEvery)
			c := newRouteRecordsClient(strategy, make(chan struct{}, 1))
			doneFor := func([]*kgo.Record) func(ProduceResult) { return func(ProduceResult) {} }
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchRouted, benchRejected = c.routeRecords(records, doneFor)
			}
		})
	}
}

var (
	benchRouted   []promised[routedTopicPartitionRecords]
	benchRejected []rejectedTopicPartitionRecords
)

// missEvery: 0 routes all, 1 rejects all, -1 rejects the first record.
// Any other positive value rejects partitions divisible by it.
func benchRouteInputs(topic string, n, partitions, missEvery int) ([]*kgo.Record, *mockPartitionAssignmentStrategy) {
	records := make([]*kgo.Record, n)
	candidates := map[partitionKey][]Agent{}
	for i := range n {
		p := int32(i % partitions)
		if missEvery < 0 && i == 0 {
			p = int32(partitions + 1)
		}
		records[i] = &kgo.Record{Topic: topic, Partition: p, Value: []byte("v")}
		reject := missEvery == 1 || (missEvery > 1 && int(p)%missEvery == 0) || (missEvery < 0 && i == 0)
		if !reject {
			candidates[partitionKey{topic, p}] = healthyAgents(1)
		}
	}
	return records, &mockPartitionAssignmentStrategy{candidates: candidates}
}

func TestWarpstreamClient_RouteRecordsCountsRoutesAndMisses(t *testing.T) {
	const topic = "t"
	strategy := newDefaultPartitionAssignmentStrategy([]int32{1, 2},
		map[topicPartition]int32{{topic: topic, partition: 0}: 1},
		nil,
		map[topicPartition]struct{}{{topic: topic, partition: 2}: {}},
		map[string]int32{topic: 4})
	newClient := func() *WarpstreamClient {
		return newRouteRecordsClient(strategy, make(chan struct{}, 4))
	}
	rec := func(topic string, partition int32) *kgo.Record {
		return &kgo.Record{Topic: topic, Partition: partition, Value: []byte("v")}
	}
	routes := func(c *WarpstreamClient) [routeSourceCount]float64 {
		var out [routeSourceCount]float64
		for i := range out {
			out[i] = testutil.ToFloat64(c.metrics.partitionRoutes[i])
		}
		return out
	}
	misses := func(c *WarpstreamClient) [routingMissCount]float64 {
		var out [routingMissCount]float64
		for i := range out {
			out[i] = testutil.ToFloat64(c.metrics.routingMisses[i])
		}
		return out
	}

	t.Run("a batch counts input records, not groups", func(t *testing.T) {
		c := newClient()
		_, rejected := c.routeRecords([]*kgo.Record{
			rec(topic, 0), rec(topic, 0), rec(topic, 0),
			rec(topic, 1), rec(topic, 1),
			rec(topic, 2),
			rec(topic, 9), rec(topic, 9),
			rec("other", 0),
		}, countAccepted(new(int)))

		require.Len(t, rejected, 3)
		assert.Equal(t, [routeSourceCount]float64{routeSourceLeader: 3, routeSourceStandIn: 2}, routes(c))
		assert.Equal(t, [routingMissCount]float64{
			routingMissNoLeader:            1,
			routingMissPartitionOutOfRange: 2,
			routingMissUnknownTopic:        1,
		}, misses(c))
	})

	t.Run("a single record counts once", func(t *testing.T) {
		c := newClient()
		_, err := c.routeRecord(rec(topic, 1), func(ProduceResult) {})
		require.NoError(t, err)
		_, err = c.routeRecord(rec(topic, 9), func(ProduceResult) {})
		require.Error(t, err)

		assert.Equal(t, [routeSourceCount]float64{routeSourceStandIn: 1}, routes(c))
		assert.Equal(t, [routingMissCount]float64{routingMissPartitionOutOfRange: 1}, misses(c))
	})

	t.Run("a custom strategy is not counted as a route and misses are other", func(t *testing.T) {
		custom := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{topic, 0}: healthyAgents(5)}}
		c := newRouteRecordsClient(custom, make(chan struct{}, 4))
		routed, rejected := c.routeRecords([]*kgo.Record{rec(topic, 0), rec(topic, 1)}, countAccepted(new(int)))

		require.Len(t, routed, 1)
		require.Len(t, rejected, 1)
		assert.Equal(t, [routeSourceCount]float64{}, routes(c))
		assert.Equal(t, [routingMissCount]float64{routingMissOther: 1}, misses(c))
	})
}
