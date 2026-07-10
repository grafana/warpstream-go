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

	"github.com/grafana/warpstream-go/pkg/wgo/internal/testkafka"
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

		removed, err := pool.Refresh(t.Context())
		require.NoError(t, err)
		assert.Empty(t, removed)
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

		// Set kgo's metadata cache to its minimum allowed value (10ms; kgo rejects
		// MetadataMinAge=0) so the test sees CreateTopics / DeleteTopics changes on
		// the next Refresh without waiting for the default 5-second cache window.
		// In production the longer default is desirable; the test just needs
		// deterministic visibility.
		client, err := kgo.NewClient(
			kgo.SeedBrokers(clusterAddr),
			kgo.Dialer(vnet.DialContext),
			kgo.MetadataMinAge(10*time.Millisecond),
		)
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

		// Advance past kgo's metadata cache (MetadataMinAge=10ms above) so the next
		// Refresh fetches fresh metadata reflecting the deletion. Under the fake
		// clock this sleep is instant and deterministic.
		time.Sleep(20 * time.Millisecond)

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
		respTopics   []kmsg.MetadataResponseTopic
		prevTopicIDs map[string][16]byte
		wantLeaders  map[topicPartition]int32
		wantTopicIDs map[string][16]byte
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
			wantTopicIDs: map[string][16]byte{"a": idA},
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
			wantTopicIDs: map[string][16]byte{"a": idA, "b": idB},
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
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
		"topic absent from response is evicted (deletion is authoritative)": {
			respTopics:   []kmsg.MetadataResponseTopic{},
			prevTopicIDs: map[string][16]byte{"a": idA},
			wantLeaders:  map[topicPartition]int32{},
			wantTopicIDs: map[string][16]byte{},
		},
		"topic with non-zero error code: previous UUID preserved": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:      stringPtr("a"),
				ErrorCode:  5,          // LEADER_NOT_AVAILABLE
				TopicID:    [16]byte{}, // brokers often return zero UUID alongside the error
				Partitions: nil,
			}},
			prevTopicIDs: map[string][16]byte{"a": idA},
			wantLeaders:  map[topicPartition]int32{},
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
		"all leaders unknown: empty leader map but UUID still picked up": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:      stringPtr("a"),
				TopicID:    idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 99}},
			}},
			wantLeaders:  map[topicPartition]int32{},
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			leaders, topicIDs := buildLeadersAndTopicIDs(tc.respTopics, knownAgents, tc.prevTopicIDs)
			assert.Equal(t, tc.wantLeaders, leaders)
			assert.Equal(t, tc.wantTopicIDs, topicIDs)
		})
	}
}

func stringPtr(s string) *string { return &s }

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
