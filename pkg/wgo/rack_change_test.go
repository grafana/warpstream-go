package wgo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
)

func TestKafkaDirectProducer_MetadataRackChange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		changes  bool
		deadline bool
	}{
		{name: "unchanged rack preserves in-flight produces"},
		{name: "one rack change retries in-flight produces across the pool", changes: true},
		{name: "retry shares the original deadline", changes: true, deadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const topic = "test-topic"
				var vnet kfake.VirtualNetwork
				cluster, addr := testkafka.CreateCluster(t, 3, topic,
					testkafka.WithNumBrokers(3), testkafka.WithVirtualNetwork(&vnet))
				client, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.Dialer(vnet.DialContext), kgo.MetadataMaxAge(time.Hour))
				require.NoError(t, err)
				t.Cleanup(client.Close)
				pool := NewAgentPool(client)
				_, err = pool.Refresh(t.Context())
				require.NoError(t, err)
				meta, err := client.RequestCachedMetadata(t.Context(), kmsg.NewPtrMetadataRequest(), time.Hour)
				require.NoError(t, err)
				require.Len(t, meta.Brokers, 3)
				require.NotNil(t, meta.Brokers[0].Rack)
				originalRack := *meta.Brokers[0].Rack
				var rack atomic.Pointer[string]
				rack.Store(&originalRack)
				cluster.ControlKey(int16(kmsg.Metadata), func(req kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()
					resp := *meta
					resp.SetVersion(req.GetVersion())
					resp.Brokers = append([]kmsg.MetadataResponseBroker(nil), meta.Brokers...)
					for i := range resp.Brokers {
						resp.Brokers[i].Rack = rack.Load()
					}
					return &resp, nil, true
				})
				m := newMetrics(prometheus.NewPedanticRegistry())
				producer := NewKafkaDirectProducer(client, pool.TopicID, 9, KafkaDirectProducerConfig{
					ProduceRequestTimeout: time.Second, ProduceRequestTimeoutOverhead: time.Second,
				}, m)
				produce := func(nodeID int32) ProduceResult {
					return producer.ProduceSync(t.Context(), Agent{NodeID: nodeID}, partitionsForTest([]*kgo.Record{{
						Topic: topic, Partition: nodeID, Value: []byte("value"), Timestamp: time.Now(),
					}}))
				}
				var hold atomic.Pointer[chan struct{}]
				started := make(chan int32, 3)
				cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()
					if release := hold.Load(); release != nil {
						started <- cluster.CurrentNode()
						// Metadata must run while produce responses remain outstanding.
						cluster.SleepControl(func() { <-*release })
					}
					return nil, nil, false
				})

				nextRack := originalRack + "-new"
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				t.Cleanup(unblock)
				hold.Store(&release)
				results := make(chan ProduceResult, 3)
				attemptStart := time.Now()
				for nodeID := range int32(3) {
					go func() { results <- produce(nodeID) }()
				}
				synctest.Wait()
				require.Len(t, started, 3)
				var nodes []int32
				for range 3 {
					nodes = append(nodes, <-started)
				}
				assert.ElementsMatch(t, []int32{0, 1, 2}, nodes)
				require.Empty(t, results)

				if tc.deadline {
					time.Sleep(1500 * time.Millisecond)
				}
				if tc.changes {
					rack.Store(&nextRack)
				}
				// AgentPool.Refresh must bypass its one-nanosecond metadata cache.
				time.Sleep(2 * time.Nanosecond)
				removed, err := pool.Refresh(t.Context())
				require.NoError(t, err)
				assert.Empty(t, removed)
				assert.Equal(t, []int32{0, 1, 2}, pool.Agents())
				synctest.Wait()

				require.Empty(t, results)
				if tc.changes {
					require.Len(t, started, 3)
					for range 3 {
						<-started
					}
				}
				if tc.deadline {
					time.Sleep(time.Until(attemptStart.Add(2 * time.Second)))
					synctest.Wait()
					require.Len(t, results, 3)
				}
				hold.Store(nil)
				unblock()
				synctest.Wait()
				for range 3 {
					result := <-results
					if tc.deadline {
						require.ErrorIs(t, result.err, context.DeadlineExceeded)
					} else {
						require.NoError(t, result.error())
					}
				}
				expectedRequests := float64(3)
				if tc.changes {
					expectedRequests = 6
				}
				assert.Equal(t, expectedRequests, testutil.ToFloat64(m.produceDirectRequestsTotal.WithLabelValues("healthy")))
				if tc.changes {
					assert.Equal(t, float64(3), testutil.ToFloat64(m.produceDirectRequestsFailedTotal.WithLabelValues("transport", "healthy")))
				}

				for nodeID := range int32(3) {
					require.NoError(t, produce(nodeID).error())
				}
			})
		})
	}
}
