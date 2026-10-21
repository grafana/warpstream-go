package runner

import (
	"cmp"
	"context"
	"encoding/binary"
	"net"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

func TestTransportPhases_ReplacementChangesAddress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		env := &environment{topic: topicName, numPartitions: 3}
		for index := range 2 {
			var virtual kfake.VirtualNetwork
			replacementPort := 10000
			cluster, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(3, topicName),
				kfake.Ports(9092, 9093, 9094), kfake.ListenFn(func(network, address string) (net.Listener, error) {
					host, port, splitErr := net.SplitHostPort(address)
					if splitErr != nil {
						return nil, splitErr
					}
					if port == "0" {
						address = net.JoinHostPort(host, strconv.Itoa(replacementPort))
						replacementPort++
					}
					return virtual.Listen(network, address)
				}))
			require.NoError(t, err)
			t.Cleanup(cluster.Close)
			for partition := range int32(3) {
				require.NoError(t, cluster.MoveTopicPartition(topicName, partition, partition))
			}
			network := newTransportNetwork(virtual.DialContext)
			t.Cleanup(network.Close)
			for nodeID, address := range cluster.ListenAddrs() {
				network.register(address, int32(nodeID))
			}
			if index == 0 {
				env.wgoCluster, env.wgoNetwork = cluster, network
			} else {
				env.kgoCluster, env.kgoNetwork = cluster, network
			}
		}
		behaviours := newBrokersBehaviourProvider(healthyBehaviours())
		installFailureControl(env.wgoCluster, behaviours, clientTypeWgo)
		client, err := wgo.NewWarpstreamClient(nil, prometheus.NewPedanticRegistry(),
			wgo.WithAddress(env.wgoCluster.ListenAddrs()[0]), wgo.WithTopic(topicName), wgo.WithDialer(env.wgoNetwork.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)
		kgoClient, err := newKgoClient(env.kgoCluster.ListenAddrs()[0], kgo.Dialer(env.kgoNetwork.DialContext))
		require.NoError(t, err)
		t.Cleanup(kgoClient.Close)
		for index, target := range []struct {
			cluster *kfake.Cluster
			client  interface {
				Request(context.Context, kmsg.Request) (kmsg.Response, error)
			}
		}{
			{env.wgoCluster, client}, {env.kgoCluster, kgoClient},
		} {
			raw, captureErr := target.client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
			require.NoError(t, captureErr)
			view := installMetadataControl(target.cluster, raw.(*kmsg.MetadataResponse), behaviours)
			if index == 0 {
				env.wgoMetadata = view
			} else {
				env.kgoMetadata = view
			}
			target.cluster.ShufflePartitionLeaders()
			response, fetchErr := target.client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
			require.NoError(t, fetchErr)
			assert.Equal(t, raw.(*kmsg.MetadataResponse).Topics, response.(*kmsg.MetadataResponse).Topics)
		}

		write := func() {
			results := client.ProduceSync(t.Context(), []*kgo.Record{{Topic: topicName, Partition: 1, Value: []byte("event")}})
			require.NoError(t, results.FirstErr())
		}
		write()
		require.NoError(t, env.applyTransportPhase(transportPhase{nodeID: 1, action: removePod}))
		removed, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.NoError(t, err)
		require.Len(t, removed.(*kmsg.MetadataResponse).Brokers, 2)
		for _, topic := range removed.(*kmsg.MetadataResponse).Topics {
			for _, partition := range topic.Partitions {
				expected := partition.Partition
				if expected == 1 {
					expected = partition.ISR[0]
				}
				assert.Equal(t, expected, partition.Leader)
				assert.NotContains(t, partition.Replicas, int32(1))
				assert.NotContains(t, partition.ISR, int32(1))
				assert.Len(t, partition.Replicas, 2)
			}
		}
		require.NoError(t, env.applyTransportPhase(transportPhase{nodeID: 1, action: replacePod}))
		_, err = env.wgoNetwork.DialContext(t.Context(), "tcp", "127.0.0.1:9093")
		require.Error(t, err)
		raw, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.NoError(t, err)
		for _, topic := range raw.(*kmsg.MetadataResponse).Topics {
			for _, partition := range topic.Partitions {
				assert.Len(t, partition.Replicas, 3)
				assert.Len(t, partition.ISR, 3)
				assert.Contains(t, partition.ISR, partition.Leader)
			}
		}
		for _, broker := range raw.(*kmsg.MetadataResponse).Brokers {
			if broker.NodeID == 1 {
				assert.Equal(t, int32(10000), broker.Port)
			}
		}
		time.Sleep(clientMetadataRefresh)
		synctest.Wait()
		write()
		env.wgoNetwork.mu.Lock()
		assert.Contains(t, env.wgoNetwork.brokers, "127.0.0.1:10000")
		assert.Positive(t, env.wgoNetwork.closedConnections)
		env.wgoNetwork.mu.Unlock()
	})
}

func TestTransportPhases_Cancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, runTransportPhases(ctx, nil, time.Now(), []transportPhase{{time.Second, 1, killPod}}), context.Canceled)
	})
}

func concurrentRestartScenarioForTest(t *testing.T) scenario {
	t.Helper()
	for _, candidate := range transportScenarios() {
		if candidate.name == "concurrent restart" {
			return candidate
		}
	}
	require.FailNow(t, "concurrent restart scenario not found")
	return scenario{}
}

func TestTransportScenarios_ConcurrentRestart(t *testing.T) {
	t.Parallel()
	sc := concurrentRestartScenarioForTest(t)
	require.True(t, sc.requireCascadeContinuation)
	assert.True(t, slices.IsSortedFunc(sc.transportPhases, func(a, b transportPhase) int { return cmp.Compare(a.at, b.at) }))

	leaving := map[int32]time.Duration{}
	returning := map[int32]time.Duration{}
	for _, phase := range sc.transportPhases {
		switch phase.action {
		case killPod, withdrawPod:
			leaving[phase.nodeID] = phase.at
		case restorePod, replacePod:
			returning[phase.nodeID] = phase.at
		}
	}
	assert.Len(t, leaving, 4*len(restartWaves))
	assert.Equal(t, len(leaving), len(returning))
	var lastReturn time.Duration
	for nodeID, left := range leaving {
		assert.GreaterOrEqual(t, returning[nodeID]-left, 2*clientMetadataRefresh)
		lastReturn = max(lastReturn, returning[nodeID])
	}
	var mostAway int
	for _, at := range leaving {
		var away int
		for nodeID, left := range leaving {
			if left <= at && at < returning[nodeID] {
				away++
			}
		}
		mostAway = max(mostAway, away)
	}
	assert.LessOrEqual(t, mostAway, int(clusterSize/5))
	assert.Greater(t, mostAway, int(clusterSize/10))

	require.NotNil(t, sc.expect.recoveryFrom)
	assert.Greater(t, *sc.expect.recoveryFrom-lastReturn, clientMetadataRefresh)
	assert.Greater(t, sc.duration(), *sc.expect.recoveryFrom)

	last := sc.behaviourPhases[len(sc.behaviourPhases)-1]
	assert.Equal(t, lastReturn, last.at)
	assert.Empty(t, last.behaviours.brokerOnlyMissing)
	for _, phase := range sc.behaviourPhases {
		for _, nodeID := range phase.behaviours.brokerOnlyMissing {
			assert.Contains(t, leaving, nodeID)
		}
	}
}

// firstFallback mirrors the hash walk of wgo's default partition assignment
// for a pool of 0..agents-1 where partition p is led by agent p.
func firstFallback(topic string, partition, agents int32) int32 {
	var d xxhash.Digest
	_, _ = d.WriteString(topic)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(partition))
	_, _ = d.Write(b[:])
	skip := int32(d.Sum64() % uint64(agents-1))
	for id := range agents {
		if id == partition {
			continue
		}
		if skip == 0 {
			return id
		}
		skip--
	}
	return -1
}

func TestTransportScenarios_ConcurrentRestartPairsLeaderWithItsFirstFallback(t *testing.T) {
	t.Parallel()
	seen := map[int32]bool{}
	for _, pairs := range restartWaves {
		for _, pair := range pairs {
			assert.Equal(t, pair.fallback, firstFallback(topicName, pair.leader, clusterSize))
			for _, nodeID := range []int32{pair.leader, pair.fallback} {
				assert.False(t, seen[nodeID], "agent %d appears in two pairs", nodeID)
				seen[nodeID] = true
			}
		}
	}
}

func faultOf(n *transportNetwork, nodeID int32) transportFault {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.faults[nodeID]
}

func TestRunPhases_AppliesTransportAndBehaviourPhasesTogether(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		healthy := healthyBehaviours()
		skewed := healthyBehaviours()
		skewed.brokerOnlyMissing = []int32{1}
		sc := scenario{
			transportPhases: []transportPhase{{2 * time.Second, 1, killPod}, {6 * time.Second, 1, restorePod}},
			behaviourPhases: []behaviourPhase{{4 * time.Second, skewed}, {8 * time.Second, healthy}},
		}
		env := &environment{
			behaviours: newBrokersBehaviourProvider(healthy),
			wgoNetwork: newTransportNetwork(nil), kgoNetwork: newTransportNetwork(nil),
		}
		done := make(chan error, 1)
		go func() { done <- runPhases(t.Context(), env, time.Now(), sc) }()

		time.Sleep(3 * time.Second)
		synctest.Wait()
		assert.Equal(t, transportKilled, faultOf(env.wgoNetwork, 1))
		assert.Empty(t, env.behaviours.get().brokerOnlyMissing)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		assert.Equal(t, []int32{1}, env.behaviours.get().brokerOnlyMissing)
		assert.Equal(t, transportKilled, faultOf(env.kgoNetwork, 1))
		time.Sleep(5 * time.Second)
		require.NoError(t, <-done)
		assert.Equal(t, transportHealthy, faultOf(env.wgoNetwork, 1))
		assert.Empty(t, env.behaviours.get().brokerOnlyMissing)
	})
}

func TestTransportScenarios_RollingRestartRefreshWindows(t *testing.T) {
	t.Parallel()
	for _, sc := range transportScenarios() {
		if sc.name != "rolling restart" {
			continue
		}
		for i := 0; i < len(sc.transportPhases); i += 2 {
			withdraw, replace := sc.transportPhases[i], sc.transportPhases[i+1]
			assert.GreaterOrEqual(t, replace.at-withdraw.at, 2*clientMetadataRefresh)
			if i > 0 {
				assert.Greater(t, withdraw.at, sc.transportPhases[i-1].at)
			}
		}
		lastReplacement := sc.transportPhases[len(sc.transportPhases)-1].at
		require.NotNil(t, sc.expect.recoveryFrom)
		assert.Greater(t, *sc.expect.recoveryFrom-lastReplacement, clientMetadataRefresh)
		assert.Greater(t, sc.duration(), *sc.expect.recoveryFrom)
	}
}
