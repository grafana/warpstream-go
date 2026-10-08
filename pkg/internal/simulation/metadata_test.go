package main

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
	"github.com/grafana/warpstream-go/pkg/wgo"
)

func TestMetadata_WithoutBrokers(t *testing.T) {
	t.Parallel()
	template := kmsg.NewPtrMetadataResponse()
	template.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0}, {NodeID: 1}, {NodeID: 2}}
	template.Topics = []kmsg.MetadataResponseTopic{{Topic: kmsg.StringPtr(topicName), TopicID: [16]byte{1},
		Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}}}}

	response := metadataWithoutBrokers(template, 11, []int32{1})
	require.Len(t, response.Brokers, 2)
	assert.Equal(t, int32(0), response.Brokers[0].NodeID)
	assert.Equal(t, int32(2), response.Brokers[1].NodeID)
	assert.Equal(t, template.Topics, response.Topics)
	assert.Equal(t, int16(11), response.Version)
	assert.Len(t, template.Brokers, 3)

	decoded := kmsg.NewPtrMetadataResponse()
	decoded.SetVersion(11)
	require.NoError(t, decoded.ReadFrom(response.AppendTo(nil)))
	assert.Equal(t, response.Topics, decoded.Topics)
	assert.Equal(t, response.Brokers, decoded.Brokers)
}

func TestMetadata_DroppedLeaderWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var network kfake.VirtualNetwork
		cluster, addr := testkafka.CreateCluster(t, 3, topicName,
			testkafka.WithNumBrokers(3), testkafka.WithVirtualNetwork(&network))
		behaviours := newBrokersBehaviourProvider(healthyBehaviours())
		installFailureControl(cluster, behaviours, clientTypeWgo)
		registry := prometheus.NewPedanticRegistry()
		client, err := wgo.NewWarpstreamClient(nil, registry,
			wgo.WithAddress(addr), wgo.WithTopic(topicName), wgo.WithDialer(network.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)
		raw, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.NoError(t, err)
		installMetadataControl(cluster, raw.(*kmsg.MetadataResponse), behaviours)
		behaviours.set(missingBrokerBehaviours(1))

		time.Sleep(clientMetadataRefresh)
		synctest.Wait()
		require.Positive(t, gatherCounter(registry, "warpstream_agentpool_leader_dropped_total"))

		var fallbackWrites atomic.Int64
		client.SetTestProduceResponseHook(func(_ context.Context, nodeID int32, response *kmsg.ProduceResponse, err error) {
			if err != nil || response == nil {
				return
			}
			for _, topic := range response.Topics {
				for _, partition := range topic.Partitions {
					if partition.Partition == 1 && nodeID != 1 {
						fallbackWrites.Add(1)
					}
				}
			}
		})
		observations := runScenarioEvents(t.Context(), client,
			buildScenarioEvents(topicName, 3, time.Second, eventSpacing)).snapshot()
		assert.Equal(t, 2, observations.summary().successes)
		assert.Positive(t, fallbackWrites.Load())
	})
}

func TestBrokersBehaviourProvider_MissingBrokers(t *testing.T) {
	t.Parallel()
	b := missingBrokerBehaviours(1, 2)
	p := newBrokersBehaviourProvider(b)
	b.missingBrokers[0] = 9
	assert.Equal(t, []int32{1, 2}, p.get().missingBrokers)
	p.set(healthyBehaviours())
	assert.Empty(t, p.get().missingBrokers)
}
