package wgo

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
)

func TestAgentPool_Refresh(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(3)
	)

	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))
		client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)

		pool := NewAgentPool(client)

		removed, dropped, err := pool.refresh(t.Context())
		require.NoError(t, err)
		assert.Empty(t, removed)
		assert.Zero(t, dropped.Count)
		assert.NotNil(t, pool.Strategy())

		id, ok := pool.TopicID(topicName)
		require.True(t, ok)
		assert.NotEqual(t, [16]byte{}, id)

		strategy := pool.Strategy()
		for part := int32(0); part < numPartitions; part++ {
			cands := strategy.Candidates(topicName, part, 1)
			require.NotEmpty(t, cands)
			assert.GreaterOrEqual(t, cands[0].NodeID, int32(0))
		}

		removed, err = pool.Refresh(t.Context())
		require.NoError(t, err)
		assert.Empty(t, removed)

		s1 := pool.Strategy()
		_, err = pool.Refresh(t.Context())
		require.NoError(t, err)
		s2 := pool.Strategy()
		assert.NotSame(t, s1, s2)

		_, ok = pool.TopicID("does-not-exist")
		assert.False(t, ok)
	})
}

func TestAgentPool_SummarizeAddresses(t *testing.T) {
	a := brokerAddress{host: "10.0.0.1", port: 9092}
	b := brokerAddress{host: "10.0.0.2", port: 9092}
	lb := brokerAddress{host: "agents.svc.cluster.local.", port: 9092}

	tests := map[string]struct {
		agents int
		counts map[brokerAddress]int
		want   addressStats
	}{
		"empty": {
			counts: map[brokerAddress]int{},
			want:   addressStats{},
		},
		"single agent": {
			agents: 1,
			counts: map[brokerAddress]int{a: 1},
			want:   addressStats{agents: 1, addresses: 1},
		},
		"all unique": {
			agents: 3,
			counts: map[brokerAddress]int{a: 1, b: 1, lb: 1},
			want:   addressStats{agents: 3, addresses: 3},
		},
		"one pair": {
			agents: 3,
			counts: map[brokerAddress]int{a: 2, b: 1},
			want:   addressStats{agents: 3, shared: 2, addresses: 2, top: a, topAgents: 2},
		},
		"whole pool on one address": {
			agents: 200,
			counts: map[brokerAddress]int{lb: 200},
			want:   addressStats{agents: 200, shared: 200, addresses: 1, top: lb, topAgents: 200},
		},
		"two groups, top is the larger": {
			agents: 7,
			counts: map[brokerAddress]int{a: 2, b: 3, lb: 1, {host: "10.0.0.9", port: 9092}: 1},
			want:   addressStats{agents: 7, shared: 5, addresses: 4, top: b, topAgents: 3},
		},
		"tie goes to the smaller address": {
			agents: 4,
			counts: map[brokerAddress]int{b: 2, a: 2},
			want:   addressStats{agents: 4, shared: 4, addresses: 2, top: a, topAgents: 2},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, summarizeAddresses(tt.agents, tt.counts))
		})
	}
}

func TestBrokerAddress_String(t *testing.T) {
	assert.Equal(t, "10.0.0.1:9092", brokerAddress{host: "10.0.0.1", port: 9092}.String())
	assert.Equal(t, "agents.svc.cluster.local.:9092", brokerAddress{host: "agents.svc.cluster.local.", port: 9092}.String())
	assert.Equal(t, "[fd00::1]:9092", brokerAddress{host: "fd00::1", port: 9092}.String())
}

func TestAgentPool_SummarizeBrokers(t *testing.T) {
	broker := func(node int32, host string, port int32) kmsg.MetadataResponseBroker {
		return kmsg.MetadataResponseBroker{NodeID: node, Host: host, Port: port}
	}

	tests := map[string]struct {
		brokers    []kmsg.MetadataResponseBroker
		wantAgents []int32
		want       addressStats
	}{
		"same host, different ports stays unshared": {
			brokers:    []kmsg.MetadataResponseBroker{broker(2, "localhost", 9092), broker(0, "localhost", 9093), broker(1, "localhost", 9094)},
			wantAgents: []int32{2, 0, 1},
			want:       addressStats{agents: 3, addresses: 3},
		},
		"same host and port across NodeIDs is shared": {
			brokers:    []kmsg.MetadataResponseBroker{broker(0, "lb", 9092), broker(1, "lb", 9092), broker(2, "10.0.0.3", 9092)},
			wantAgents: []int32{0, 1, 2},
			want:       addressStats{agents: 3, shared: 2, addresses: 2, top: brokerAddress{host: "lb", port: 9092}, topAgents: 2},
		},
		"same port, different hosts stays unshared": {
			brokers:    []kmsg.MetadataResponseBroker{broker(0, "10.0.0.1", 9092), broker(1, "10.0.0.2", 9092)},
			wantAgents: []int32{0, 1},
			want:       addressStats{agents: 2, addresses: 2},
		},
		"hosts differing only in case are different addresses": {
			brokers:    []kmsg.MetadataResponseBroker{broker(0, "Agents.local.", 9092), broker(1, "agents.local.", 9092)},
			wantAgents: []int32{0, 1},
			want:       addressStats{agents: 2, addresses: 2},
		},
		"empty hosts are compared as strings": {
			brokers:    []kmsg.MetadataResponseBroker{broker(0, "", 9092), broker(1, "", 9092)},
			wantAgents: []int32{0, 1},
			want:       addressStats{agents: 2, shared: 2, addresses: 1, top: brokerAddress{port: 9092}, topAgents: 2},
		},
		"no brokers": {
			wantAgents: []int32{},
			want:       addressStats{},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			agents, got := summarizeBrokers(tt.brokers)
			assert.Equal(t, tt.wantAgents, agents)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAgentPool_RefreshAddresses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, 3, "test-topic", testkafka.WithVirtualNetwork(&vnet), testkafka.WithNumBrokers(3))
		client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)

		pool := NewAgentPool(client)
		assert.Equal(t, addressStats{}, pool.addresses())

		// The fake brokers advertise one host on distinct ports. Check that
		// first, so the zero shared count below is the port doing its job.
		meta, err := client.RequestCachedMetadata(t.Context(), kmsg.NewPtrMetadataRequest(), time.Nanosecond)
		require.NoError(t, err)
		require.Len(t, meta.Brokers, 3)
		hosts := map[string]struct{}{}
		ports := map[int32]struct{}{}
		for _, b := range meta.Brokers {
			hosts[b.Host] = struct{}{}
			ports[b.Port] = struct{}{}
		}
		require.Len(t, hosts, 1)
		require.Len(t, ports, 3)

		_, _, err = pool.refresh(t.Context())
		require.NoError(t, err)
		assert.Equal(t, 3, pool.addresses().agents)
		assert.Equal(t, 3, pool.addresses().addresses)
		assert.Zero(t, pool.addresses().shared)
	})
}

func TestAgentPool_StrategyBeforeRefresh(t *testing.T) {
	t.Run("Strategy returns a non-nil empty strategy before first Refresh", func(t *testing.T) {
		pool := NewAgentPool(nil)
		s := pool.Strategy()
		require.NotNil(t, s)
		assert.Empty(t, s.Candidates("topic", 0, 2))
	})

	t.Run("TopicID returns ok=false before first Refresh", func(t *testing.T) {
		pool := NewAgentPool(nil)
		_, ok := pool.TopicID("topic")
		assert.False(t, ok)
	})
}

func TestAgentPool_RefreshMultiTopic(t *testing.T) {
	const (
		topicA        = "topic-a"
		topicB        = "topic-b"
		numPartitions = int32(2)
	)

	// Two stateful steps (create+discover, then delete+evict) share one client,
	// so they run sequentially in a single bubble.
	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicA, testkafka.WithVirtualNetwork(&vnet))

		client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)

		// Create the second topic via the standard CreateTopics protocol.
		createReq := kmsg.NewPtrCreateTopicsRequest()
		createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
			Topic:             topicB,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		}}
		_, err = client.Request(t.Context(), createReq)
		require.NoError(t, err)

		pool := NewAgentPool(client)

		// Refresh discovers all topics in the cluster.
		_, err = pool.Refresh(t.Context())
		require.NoError(t, err)

		idA, okA := pool.TopicID(topicA)
		idB, okB := pool.TopicID(topicB)
		require.True(t, okA)
		require.True(t, okB)
		assert.NotEqual(t, [16]byte{}, idA)
		assert.NotEqual(t, [16]byte{}, idB)
		assert.NotEqual(t, idA, idB)

		strategy := pool.Strategy()
		for part := int32(0); part < numPartitions; part++ {
			candsA := strategy.Candidates(topicA, part, 1)
			require.NotEmpty(t, candsA)
			assert.GreaterOrEqual(t, candsA[0].NodeID, int32(0))
			candsB := strategy.Candidates(topicB, part, 1)
			require.NotEmpty(t, candsB)
			assert.GreaterOrEqual(t, candsB[0].NodeID, int32(0))
		}

		// Topics deleted from the cluster are evicted from the snapshot.
		_, ok := pool.TopicID(topicB)
		require.True(t, ok)

		deleteReq := kmsg.NewPtrDeleteTopicsRequest()
		deleteReq.TopicNames = []string{topicB}
		deleteReq.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: kmsg.StringPtr(topicB)}}
		_, err = client.Request(t.Context(), deleteReq)
		require.NoError(t, err)

		// Advance the fake clock past Refresh's one-nanosecond cache-age limit.
		time.Sleep(time.Nanosecond)
		_, err = pool.Refresh(t.Context())
		require.NoError(t, err)
		_, okB = pool.TopicID(topicB)
		assert.False(t, okB)
		_, okA = pool.TopicID(topicA)
		assert.True(t, okA)
	})
}

func TestBuildLeadersAndTopicIDs(t *testing.T) {
	knownAgents := map[int32]struct{}{1: {}, 2: {}, 3: {}}
	idA := [16]byte{0x42}
	idB := [16]byte{0x43}

	tests := map[string]struct {
		respTopics          []kmsg.MetadataResponseTopic
		prevTopicIDs        map[string][16]byte
		wantLeaders         map[topicPartition]int32
		wantTopicIDs        map[string][16]byte
		wantNoLiveLeader    map[string]struct{}
		wantNoLeader        map[topicPartition]struct{}
		wantPartitionCounts map[string]int32
		wantDropped         leaderDrops
	}{
		"happy path: single topic, all leaders known": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: 1},
					{Partition: 1, Leader: 2},
				},
			}},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "a", partition: 1}: 2,
			},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantPartitionCounts: map[string]int32{"a": 2},
		},
		"multiple topics: each gets its own leaders and UUID": {
			respTopics: []kmsg.MetadataResponseTopic{
				{Topic: stringPtr("a"), TopicID: idA, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1}}},
				{Topic: stringPtr("b"), TopicID: idB, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 2}}},
			},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "b", partition: 0}: 2,
			},
			wantTopicIDs:        map[string][16]byte{"a": idA, "b": idB},
			wantPartitionCounts: map[string]int32{"a": 1, "b": 1},
		},
		"leader pointing to unknown agent is dropped": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: 1},
					{Partition: 1, Leader: 99}, // unknown
					{Partition: 2, Leader: 3},
				},
			}},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "a", partition: 2}: 3,
			},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantPartitionCounts: map[string]int32{"a": 3},
			wantDropped:         leaderDrops{Count: 1, Topic: "a", Partition: 1, NodeID: 99},
		},
		"topic absent from response is evicted (deletion is authoritative)": {
			respTopics:          []kmsg.MetadataResponseTopic{},
			prevTopicIDs:        map[string][16]byte{"a": idA},
			wantLeaders:         map[topicPartition]int32{},
			wantTopicIDs:        map[string][16]byte{},
			wantPartitionCounts: map[string]int32{},
		},
		"topic with non-zero error code: previous UUID preserved": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:      stringPtr("a"),
				ErrorCode:  5,          // LEADER_NOT_AVAILABLE
				TopicID:    [16]byte{}, // brokers often return zero UUID alongside the error
				Partitions: nil,
			}},
			prevTopicIDs:        map[string][16]byte{"a": idA},
			wantLeaders:         map[topicPartition]int32{},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantPartitionCounts: map[string]int32{},
		},
		"all leaders unknown: empty leader map but UUID still picked up": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:      stringPtr("a"),
				TopicID:    idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 99}},
			}},
			wantLeaders:         map[topicPartition]int32{},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantNoLiveLeader:    map[string]struct{}{"a": {}},
			wantPartitionCounts: map[string]int32{"a": 1},
			wantDropped:         leaderDrops{Count: 1, Topic: "a", Partition: 0, NodeID: 99},
		},
		"every partition leader unknown: first excluded leader is the sample": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: 99},
					{Partition: 1, Leader: 98},
				},
			}},
			wantLeaders:         map[topicPartition]int32{},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantNoLiveLeader:    map[string]struct{}{"a": {}},
			wantPartitionCounts: map[string]int32{"a": 2},
			wantDropped:         leaderDrops{Count: 2, Topic: "a", Partition: 0, NodeID: 99},
		},
		"one topic fully excluded, another kept": {
			respTopics: []kmsg.MetadataResponseTopic{
				{Topic: stringPtr("a"), TopicID: idA, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 99}}},
				{Topic: stringPtr("b"), TopicID: idB, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1}}},
			},
			wantLeaders:         map[topicPartition]int32{{topic: "b", partition: 0}: 1},
			wantTopicIDs:        map[string][16]byte{"a": idA, "b": idB},
			wantNoLiveLeader:    map[string]struct{}{"a": {}},
			wantPartitionCounts: map[string]int32{"a": 1, "b": 1},
			wantDropped:         leaderDrops{Count: 1, Topic: "a", Partition: 0, NodeID: 99},
		},
		"two topics both lose every leader: count sums and the sample stays on the first": {
			respTopics: []kmsg.MetadataResponseTopic{
				{Topic: stringPtr("a"), TopicID: idA, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 99}}},
				{Topic: stringPtr("b"), TopicID: idB, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 3, Leader: 98}}},
			},
			wantLeaders:      map[topicPartition]int32{},
			wantTopicIDs:     map[string][16]byte{"a": idA, "b": idB},
			wantNoLiveLeader: map[string]struct{}{"a": {}, "b": {}},
			// "b"'s only partition is index 3: count is highest index + 1, not list length.
			wantPartitionCounts: map[string]int32{"a": 1, "b": 4},
			wantDropped:         leaderDrops{Count: 2, Topic: "a", Partition: 0, NodeID: 99},
		},
		"partition leader below zero is not a drop and not a fallback topic": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{{
					Partition: 0,
					Leader:    -1,
					ErrorCode: 5, // LEADER_NOT_AVAILABLE
				}},
			}},
			wantLeaders:         map[topicPartition]int32{},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantNoLeader:        map[topicPartition]struct{}{{topic: "a", partition: 0}: {}},
			wantPartitionCounts: map[string]int32{"a": 1},
		},
		"leader below zero does not count when a sibling names a missing node": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: -1, ErrorCode: 5},
					{Partition: 1, Leader: 1},
					{Partition: 2, Leader: 99},
				},
			}},
			wantLeaders:         map[topicPartition]int32{{topic: "a", partition: 1}: 1},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantNoLeader:        map[topicPartition]struct{}{{topic: "a", partition: 0}: {}},
			wantPartitionCounts: map[string]int32{"a": 3},
			wantDropped:         leaderDrops{Count: 1, Topic: "a", Partition: 2, NodeID: 99},
		},
		"partition error that still names a live leader is kept": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{{
					Partition: 0,
					Leader:    1,
					ErrorCode: 9, // REPLICA_NOT_AVAILABLE
				}},
			}},
			wantLeaders:         map[topicPartition]int32{{topic: "a", partition: 0}: 1},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantPartitionCounts: map[string]int32{"a": 1},
		},
		"empty partition list with a zero error code stays out of the no-live-leader set": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:      stringPtr("a"),
				TopicID:    idA,
				Partitions: nil,
			}},
			wantLeaders:         map[topicPartition]int32{},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantPartitionCounts: map[string]int32{"a": 0},
		},
		"partition count is the highest reported index plus one, not the list length": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: 1},
					{Partition: 5, Leader: 2},
				},
			}},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "a", partition: 5}: 2,
			},
			wantTopicIDs:        map[string][16]byte{"a": idA},
			wantPartitionCounts: map[string]int32{"a": 6},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			leaders, topicIDs, noLiveLeader, noLeader, partitionCounts, dropped := buildLeadersAndTopicIDs(tc.respTopics, knownAgents, tc.prevTopicIDs)
			assert.Equal(t, tc.wantLeaders, leaders)
			assert.Equal(t, tc.wantTopicIDs, topicIDs)
			assert.Equal(t, tc.wantNoLiveLeader, noLiveLeader)
			assert.Equal(t, tc.wantNoLeader, noLeader)
			assert.Equal(t, tc.wantPartitionCounts, partitionCounts)
			assert.Equal(t, tc.wantDropped, dropped)
		})
	}
}

func stringPtr(s string) *string { return &s }

func TestAgentPool_DiffAgentMembership(t *testing.T) {
	tests := map[string]struct {
		old         []int32
		new         []int32
		wantAdded   int
		wantRemoved int
	}{
		"unchanged": {
			old: []int32{1, 2, 3}, new: []int32{1, 2, 3},
		},
		"one added": {
			old: []int32{1, 2}, new: []int32{1, 2, 3}, wantAdded: 1,
		},
		"one removed": {
			old: []int32{1, 2, 3}, new: []int32{1, 3}, wantRemoved: 1,
		},
		"replace one": {
			old: []int32{1, 2, 3}, new: []int32{1, 4}, wantAdded: 1, wantRemoved: 2,
		},
		"empty to some": {
			old: nil, new: []int32{1, 2}, wantAdded: 2,
		},
		"all removed": {
			old: []int32{1, 2}, new: nil, wantRemoved: 2,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			added, removed := diffAgentMembership(tc.old, tc.new)
			assert.Equal(t, tc.wantAdded, added)
			assert.Equal(t, tc.wantRemoved, removed)
		})
	}
}

func TestDiffRemovedAgents(t *testing.T) {
	tests := map[string]struct {
		old      []int32
		newSet   []int32
		expected []int32
	}{
		"all agents still present": {
			old: []int32{1, 2, 3}, newSet: []int32{1, 2, 3}, expected: nil,
		},
		"one agent removed": {
			old: []int32{1, 2, 3}, newSet: []int32{1, 3}, expected: []int32{2},
		},
		"multiple agents removed": {
			old: []int32{1, 2, 3, 4}, newSet: []int32{2, 4}, expected: []int32{1, 3},
		},
		"all agents removed": {
			old: []int32{1, 2}, newSet: []int32{}, expected: []int32{1, 2},
		},
		"agents added (not removed)": {
			old: []int32{1, 2}, newSet: []int32{1, 2, 3}, expected: nil,
		},
		"empty old set": {
			old: nil, newSet: []int32{1, 2}, expected: nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			set := make(map[int32]struct{}, len(tc.newSet))
			for _, id := range tc.newSet {
				set[id] = struct{}{}
			}
			got := diffRemovedAgents(tc.old, set)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestAgentPool_StrategyConcurrent(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(4)
	)

	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))
		client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)

		pool := NewAgentPool(client)
		_, err = pool.Refresh(t.Context())
		require.NoError(t, err)

		// Spin up concurrent readers and let them overlap. The value of this test
		// is genuinely-concurrent Strategy()/Candidates() reads under -race, so we
		// do NOT insert a synctest.Wait() between spawning and running them (that
		// would serialise them); we only join at the end.
		const goroutines = 20
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				strategy := pool.Strategy()
				for part := int32(0); part < numPartitions; part++ {
					_ = strategy.Candidates(topicName, part, 2)
				}
			}()
		}
		wg.Wait()
	})
}
