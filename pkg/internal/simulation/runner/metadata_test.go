package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
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
		metrics, err := registry.Gather()
		require.NoError(t, err)
		var excludedLeaders float64
		for _, metric := range metrics {
			if metric.GetName() == "warpstream_agentpool_excluded_leaders" {
				require.Len(t, metric.Metric, 1)
				excludedLeaders = metric.Metric[0].GetGauge().GetValue()
			}
		}
		require.Equal(t, float64(1), excludedLeaders)

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
			buildScenarioEvents(topicName, 3, time.Second, eventSpacing), time.Now()).snapshot()
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

func TestMetadataView_UpdateBrokerPreservesSnapshots(t *testing.T) {
	t.Parallel()
	original := kmsg.NewPtrMetadataResponse()
	original.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0}, {NodeID: 1, Port: 9093}, {NodeID: 2}}
	original.Topics = []kmsg.MetadataResponseTopic{{Topic: kmsg.StringPtr(topicName), Partitions: []kmsg.MetadataResponseTopicPartition{
		{Partition: 0, Leader: 0, LeaderEpoch: 2, Replicas: []int32{0, 1, 2}, ISR: []int32{0, 1, 2}},
		{Partition: 1, Leader: 1, LeaderEpoch: 3, Replicas: []int32{1, 2, 0}, ISR: []int32{1, 2, 0}},
		{Partition: 2, Leader: 2, LeaderEpoch: 4, Replicas: []int32{2, 0, 1}, ISR: []int32{2, 0, 1}},
	}}}
	view := &metadataView{template: original}
	view.current.Store(original)
	view.updateBroker(1, nil)
	removed := view.current.Load()
	require.Len(t, removed.Brokers, 2)
	assert.Equal(t, int32(2), removed.Topics[0].Partitions[1].Leader)
	assert.Equal(t, int32(4), removed.Topics[0].Partitions[1].LeaderEpoch)
	for _, partition := range removed.Topics[0].Partitions {
		assert.NotContains(t, partition.Replicas, int32(1))
		assert.NotContains(t, partition.ISR, int32(1))
		assert.Contains(t, partition.ISR, partition.Leader)
		require.Len(t, partition.Replicas, 2)
		require.Len(t, partition.ISR, 2)
	}
	view.updateBroker(1, &kmsg.MetadataResponseBroker{NodeID: 1, Port: 10000})
	replaced := view.current.Load()
	require.Len(t, replaced.Brokers, 3)
	assert.Equal(t, int32(10000), replaced.Brokers[1].Port)
	for index, partition := range replaced.Topics[0].Partitions {
		assert.Equal(t, original.Topics[0].Partitions[index].Replicas, partition.Replicas)
		assert.Equal(t, original.Topics[0].Partitions[index].ISR, partition.ISR)
		assert.Equal(t, original.Topics[0].Partitions[index].Leader, partition.Leader)
	}
	assert.Equal(t, int32(5), replaced.Topics[0].Partitions[1].LeaderEpoch)
	for _, partition := range []int{0, 2} {
		assert.Equal(t, original.Topics[0].Partitions[partition].Leader, removed.Topics[0].Partitions[partition].Leader)
		assert.Equal(t, original.Topics[0].Partitions[partition].LeaderEpoch, removed.Topics[0].Partitions[partition].LeaderEpoch)
		assert.Equal(t, original.Topics[0].Partitions[partition], replaced.Topics[0].Partitions[partition])
	}
	require.Len(t, original.Brokers, 3)
	assert.Equal(t, int32(1), original.Topics[0].Partitions[1].Leader)
	assert.Equal(t, []int32{0, 1, 2}, original.Topics[0].Partitions[0].Replicas)
	assert.Equal(t, []int32{0, 2}, removed.Topics[0].Partitions[0].Replicas)
}

func TestMetadataView_OverlappingRestarts(t *testing.T) {
	t.Parallel()
	original := kmsg.NewPtrMetadataResponse()
	original.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0}, {NodeID: 1}, {NodeID: 2}}
	original.Topics = []kmsg.MetadataResponseTopic{{Partitions: []kmsg.MetadataResponseTopicPartition{
		{Partition: 0, Leader: 0, Replicas: []int32{0, 1, 2}, ISR: []int32{0, 1}, OfflineReplicas: []int32{2}},
	}}}
	view := &metadataView{template: original}
	view.current.Store(original)
	view.updateBroker(1, nil)
	view.updateBroker(2, nil)
	removed := view.current.Load()
	assert.Equal(t, []int32{0}, removed.Topics[0].Partitions[0].Replicas)
	assert.Equal(t, []int32{0}, removed.Topics[0].Partitions[0].ISR)
	assert.Empty(t, removed.Topics[0].Partitions[0].OfflineReplicas)
	view.updateBroker(1, &kmsg.MetadataResponseBroker{NodeID: 1})
	partial := view.current.Load().Topics[0].Partitions[0]
	assert.Equal(t, []int32{0, 1}, partial.Replicas)
	assert.Equal(t, []int32{0, 1}, partial.ISR)
	assert.Empty(t, partial.OfflineReplicas)
	view.updateBroker(2, &kmsg.MetadataResponseBroker{NodeID: 2})
	assert.Equal(t, original.Topics, view.current.Load().Topics)
	assert.Equal(t, []int32{0}, removed.Topics[0].Partitions[0].Replicas)
}

func TestMetadataView_NoLiveReplica(t *testing.T) {
	t.Parallel()
	original := kmsg.NewPtrMetadataResponse()
	original.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0}, {NodeID: 1}}
	original.Topics = []kmsg.MetadataResponseTopic{{Partitions: []kmsg.MetadataResponseTopicPartition{
		{Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
	}}}
	view := &metadataView{template: original}
	view.current.Store(original)
	view.updateBroker(1, nil)
	unavailable := view.current.Load().Topics[0].Partitions[0]
	assert.Equal(t, int32(-1), unavailable.Leader)
	assert.Empty(t, unavailable.Replicas)
	assert.Empty(t, unavailable.ISR)
	assert.Equal(t, kerr.LeaderNotAvailable.Code, unavailable.ErrorCode)
	view.updateBroker(1, &kmsg.MetadataResponseBroker{NodeID: 1})
	restored := view.current.Load().Topics[0].Partitions[0]
	assert.Equal(t, int32(1), restored.Leader)
	assert.Zero(t, restored.ErrorCode)
	assert.Equal(t, []int32{1}, restored.Replicas)
}

func TestMetadataView_WithdrawBroker(t *testing.T) {
	t.Parallel()
	original := kmsg.NewPtrMetadataResponse()
	original.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0}, {NodeID: 1}}
	original.Topics = []kmsg.MetadataResponseTopic{{Partitions: []kmsg.MetadataResponseTopicPartition{
		{Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
		{Partition: 1, Leader: 1, Replicas: []int32{1, 0}, ISR: []int32{1, 0}},
	}}}
	view := &metadataView{template: original}
	view.current.Store(original)
	view.withdrawBroker(1)
	withdrawn := view.current.Load()
	require.Len(t, withdrawn.Brokers, 1)
	assert.Equal(t, original.Topics, withdrawn.Topics)
	assert.Len(t, original.Brokers, 2)
	view.observeMissingLeaders(withdrawn)
	assert.Equal(t, map[int32]int64{1: 1}, view.missingLeaders())
	view.updateBroker(1, &kmsg.MetadataResponseBroker{NodeID: 1, Port: 10000})
	view.observeMissingLeaders(view.current.Load())
	assert.Equal(t, map[int32]int64{1: 1}, view.missingLeaders())
	assert.Len(t, withdrawn.Brokers, 1)
	assert.Equal(t, original.Topics, withdrawn.Topics)
}
