package runner

import (
	"context"
	"net"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

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
