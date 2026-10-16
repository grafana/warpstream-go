package wgo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
)

func TestKafkaDirectProducer_ImplementsDirectProducer(t *testing.T) {
	var _ DirectProducer = (*KafkaDirectProducer)(nil)
}

type directProducerWriteHook struct{ wrote chan struct{} }

func (h directProducerWriteHook) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if key == int16(kmsg.Produce) && err == nil {
		h.wrote <- struct{}{}
	}
}

func blockedDirectProducer(t *testing.T) (*KafkaDirectProducer, *kgo.Client, <-chan struct{}, func()) {
	t.Helper()
	var vnet kfake.VirtualNetwork
	cluster, address := testkafka.CreateCluster(t, 1, "test-topic", testkafka.WithVirtualNetwork(&vnet))
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.SleepControl(func() { <-hold })
		return nil, nil, false
	})
	writes := make(chan struct{}, 8)
	client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.Dialer(vnet.DialContext), kgo.WithHooks(directProducerWriteHook{writes}))
	require.NoError(t, err)
	t.Cleanup(client.Close)
	metadata, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
	require.NoError(t, err)
	id := metadata.(*kmsg.MetadataResponse).Topics[0].TopicID
	producer := NewKafkaDirectProducer(client, func(string) ([16]byte, bool) { return id, true }, 9, KafkaDirectProducerConfig{
		ProduceRequestTimeout: time.Second, ProduceRequestTimeoutOverhead: time.Second,
	}, newMetrics(prometheus.NewPedanticRegistry()))
	return producer, client, writes, release
}

func TestKafkaDirectProducer_CancellationPreservesPeerRequests(t *testing.T) {
	for _, expireGrace := range []bool{false, true} {
		name := "response drains"
		if expireGrace {
			name = "drain grace expires"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				producer, _, writes, release := blockedDirectProducer(t)
				parts := partitionsForTest([]*kgo.Record{{Topic: "test-topic", Value: []byte("value"), Timestamp: time.Now()}})
				ctx, cancel := context.WithCancel(t.Context())
				first, second := make(chan ProduceResult, 1), make(chan ProduceResult, 1)
				go func() { first <- producer.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
				<-writes
				go func() { second <- producer.ProduceSync(t.Context(), Agent{NodeID: 0}, parts) }()
				<-writes
				cancel()
				synctest.Wait()
				require.ErrorIs(t, (<-first).error(), context.Canceled)
				if expireGrace {
					time.Sleep(time.Second)
					synctest.Wait()
					<-writes
				}
				release()
				require.NoError(t, (<-second).error())
			})
		})
	}
}

func TestKafkaDirectProducer_CancellationAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "caller cancellation"
		if shutdown {
			name = "client shutdown"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				producer, client, writes, release := blockedDirectProducer(t)
				drained := make(chan error, 1)
				producer.onProduceResponse = func(_ context.Context, _ int32, _ *kmsg.ProduceResponse, err error) { drained <- err }
				ctx, cancel := context.WithCancel(t.Context())
				result := make(chan ProduceResult, 1)
				go func() {
					result <- producer.ProduceSync(ctx, Agent{NodeID: 0}, partitionsForTest([]*kgo.Record{{Topic: "test-topic", Value: []byte("value"), Timestamp: time.Now()}}))
				}()
				<-writes
				stoppedAt := time.Now()
				if shutdown {
					client.Close()
				} else {
					cancel()
				}
				synctest.Wait()
				require.Error(t, (<-result).error())
				require.Equal(t, stoppedAt, time.Now())
				require.Error(t, <-drained)
				if !shutdown {
					require.Equal(t, producer.cfg.ProduceRequestTimeoutOverhead, time.Since(stoppedAt))
				}
				cancel()
				release()
			})
		})
	}
}

func TestKafkaDirectProducer_Produce(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(1)
		partition     = int32(0)
		// kfake assigns NodeID 0 to the single broker in a one-broker cluster.
		brokerNodeID = int32(0)
	)

	t.Run("produces a record successfully and round-trips via consumer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vnet kfake.VirtualNetwork
			_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))

			client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Fetch topic metadata to obtain the topic UUID. Both Topic (name) and TopicID
			// (UUID) are set in the ProduceRequest so the request is valid regardless of
			// which API version the connection negotiates: v0-v12 use the name, v13+ the UUID.
			metaResp, err := client.Request(t.Context(), &kmsg.MetadataRequest{
				Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
			})
			require.NoError(t, err)
			meta := metaResp.(*kmsg.MetadataResponse)
			require.Len(t, meta.Topics, 1)
			topicID := meta.Topics[0].TopicID

			reg := prometheus.NewPedanticRegistry()
			topicIDFn := func(name string) ([16]byte, bool) {
				if name == topicName {
					return topicID, true
				}
				return [16]byte{}, false
			}
			m := newMetrics(reg)
			producer := NewKafkaDirectProducer(client, topicIDFn, 9, KafkaDirectProducerConfig{
				ProduceRequestTimeout:         time.Second,
				ProduceRequestTimeoutOverhead: time.Second,
			}, m)

			records := []*kgo.Record{{
				Topic:     topicName,
				Partition: partition,
				Key:       []byte("test-key"),
				Value:     []byte("test-value"),
				Timestamp: time.Now(),
			}}

			res := producer.ProduceSync(t.Context(), Agent{NodeID: brokerNodeID}, partitionsForTest(records))
			require.NoError(t, res.err)
			require.NoError(t, parseProduceResponse(res.resp))

			// Verify the record was actually stored by consuming it back.
			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(clusterAddr),
				kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topicName: {partition: kgo.NewOffset().AtStart()},
				}),
			)
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetches := consumer.PollFetches(t.Context())
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)
			assert.Equal(t, []byte("test-key"), fetches.Records()[0].Key)
			assert.Equal(t, []byte("test-value"), fetches.Records()[0].Value)

			// Counter ticked once on the successful Produce; no failures.
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP warpstream_produce_direct_requests_total Total number of direct Produce requests issued to a Warpstream agent, by routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Hedge retries count as separate attempts; connection-level retries are included in the attempt.
				# TYPE warpstream_produce_direct_requests_total counter
				warpstream_produce_direct_requests_total{agent_state="healthy"} 1
			`), "warpstream_produce_direct_requests_total", "warpstream_produce_direct_requests_failed_total"))

			res = producer.ProduceSync(t.Context(), Agent{NodeID: brokerNodeID, State: AgentStateDemoted}, partitionsForTest([]*kgo.Record{{
				Topic:     topicName,
				Partition: partition,
				Value:     []byte("probe"),
				Timestamp: time.Now(),
			}}))
			require.NoError(t, res.err)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP warpstream_produce_direct_requests_total Total number of direct Produce requests issued to a Warpstream agent, by routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Hedge retries count as separate attempts; connection-level retries are included in the attempt.
				# TYPE warpstream_produce_direct_requests_total counter
				warpstream_produce_direct_requests_total{agent_state="demoted"} 1
				warpstream_produce_direct_requests_total{agent_state="healthy"} 1
			`), "warpstream_produce_direct_requests_total", "warpstream_produce_direct_requests_failed_total"))

			// The latency histogram carries no agent_state label, so both
			// successful calls land on the one success series.
			successCount, _ := histogramCountSum(t, m.produceDirectRequestLatencySuccess.(prometheus.Histogram))
			assert.Equal(t, uint64(2), successCount)
			assert.Equal(t, 1, testutil.CollectAndCount(reg, "warpstream_produce_direct_request_latency_seconds"))
		})
	})

	t.Run("merged same-partition batches round-trip as a single batch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vnet kfake.VirtualNetwork
			_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))

			client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
			require.NoError(t, err)
			t.Cleanup(client.Close)

			metaResp, err := client.Request(t.Context(), &kmsg.MetadataRequest{
				Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
			})
			require.NoError(t, err)
			topicID := metaResp.(*kmsg.MetadataResponse).Topics[0].TopicID

			topicIDFn := func(name string) ([16]byte, bool) {
				if name == topicName {
					return topicID, true
				}
				return [16]byte{}, false
			}
			producer := NewKafkaDirectProducer(client, topicIDFn, 9, KafkaDirectProducerConfig{
				ProduceRequestTimeout:         time.Second,
				ProduceRequestTimeoutOverhead: time.Second,
			}, newMetrics(prometheus.NewPedanticRegistry()))

			// Two separately-encoded batches for the same partition, as the hedge
			// buffer accumulates across concurrent produces. Merging them must yield a
			// single RecordBatch: a partition carrying more than one batch is not a
			// valid produce payload (kfake rejects it). Values are large and
			// compressible so each batch takes the Snappy path, exercising the
			// decode+re-encode merge end to end.
			rec := func(key string) *kgo.Record {
				return &kgo.Record{Topic: topicName, Partition: partition, Key: []byte(key), Value: bytes.Repeat([]byte(key+"|"), 512), Timestamp: time.Now()}
			}
			batch1 := newEncodedTopicPartitionRecords(topicName, partition, []*kgo.Record{rec("k1"), rec("k2")})
			batch2 := newEncodedTopicPartitionRecords(topicName, partition, []*kgo.Record{rec("k3")})

			merged := mergePromisedRoutedBatchByTopicPartition([]promised[routedEncodedTopicPartitionRecords]{
				{item: routedEncodedTopicPartitionRecords{encodedTopicPartitionRecords: batch1, nodeID: brokerNodeID}},
				{item: routedEncodedTopicPartitionRecords{encodedTopicPartitionRecords: batch2, nodeID: brokerNodeID}},
			})
			require.Len(t, merged, 1)

			res := producer.ProduceSync(t.Context(), Agent{NodeID: brokerNodeID}, unrouteEncodedTopicPartitionRecords(merged))
			require.NoError(t, res.err)
			require.NoError(t, parseProduceResponse(res.resp))

			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(clusterAddr),
				kgo.Dialer(vnet.DialContext),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
					topicName: {partition: kgo.NewOffset().AtStart()},
				}),
			)
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			var got []*kgo.Record
			for len(got) < 3 {
				fetches := consumer.PollFetches(t.Context())
				require.NoError(t, fetches.Err())
				got = append(got, fetches.Records()...)
			}
			require.Len(t, got, 3)
			assert.Equal(t, bytes.Repeat([]byte("k1|"), 512), got[0].Value)
			assert.Equal(t, bytes.Repeat([]byte("k2|"), 512), got[1].Value)
			assert.Equal(t, bytes.Repeat([]byte("k3|"), 512), got[2].Value)
		})
	})

	t.Run("applies per-attempt timeout (TimeoutMillis on wire + client-side ctx deadline)", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var vnet kfake.VirtualNetwork
			cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))

			// Capture every Produce request that reaches the broker.
			type captured struct {
				timeoutMillis int32
			}
			var (
				seenMu sync.Mutex
				seen   []captured
				hold   = make(chan struct{})
				holdCh = sync.OnceFunc(func() { close(hold) })
			)
			t.Cleanup(holdCh)

			cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
				pr := req.(*kmsg.ProduceRequest)
				seenMu.Lock()
				seen = append(seen, captured{timeoutMillis: pr.TimeoutMillis})
				seenMu.Unlock()
				// Block to force the client-side deadline to fire.
				<-hold
				return nil, nil, false
			})

			client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
			require.NoError(t, err)
			t.Cleanup(client.Close)

			metaResp, err := client.Request(t.Context(), &kmsg.MetadataRequest{
				Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
			})
			require.NoError(t, err)
			topicID := metaResp.(*kmsg.MetadataResponse).Topics[0].TopicID

			const (
				produceTimeout         = 250 * time.Millisecond
				produceTimeoutOverhead = 250 * time.Millisecond
			)
			reg := prometheus.NewPedanticRegistry()
			topicIDFn := func(name string) ([16]byte, bool) {
				if name == topicName {
					return topicID, true
				}
				return [16]byte{}, false
			}
			producer := NewKafkaDirectProducer(client, topicIDFn, 9, KafkaDirectProducerConfig{
				ProduceRequestTimeout:         produceTimeout,
				ProduceRequestTimeoutOverhead: produceTimeoutOverhead,
			}, newMetrics(reg))

			startedAt := time.Now()
			err = producer.ProduceSync(t.Context(), Agent{NodeID: brokerNodeID}, partitionsForTest([]*kgo.Record{{
				Topic: topicName, Partition: partition, Value: []byte("v"), Timestamp: time.Now(),
			}})).error()
			elapsed := time.Since(startedAt)

			// Verify (a) the per-attempt deadline fires on the client side as
			// context.DeadlineExceeded, and (b) under the fake clock it does so at
			// exactly the configured budget — no wall-clock slack to guess at.
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Equal(t, produceTimeout+produceTimeoutOverhead, elapsed)

			holdCh()

			// Verify req.TimeoutMillis was set from cfg.ProduceRequestTimeout (not
			// the kmsg default 15000).
			seenMu.Lock()
			var firstTimeoutMillis int32
			if len(seen) > 0 {
				firstTimeoutMillis = seen[0].timeoutMillis
			}
			seenMu.Unlock()
			assert.Equal(t, int32(produceTimeout.Milliseconds()), firstTimeoutMillis, "req.TimeoutMillis must reflect cfg.ProduceRequestTimeout")

			// Failure was an attempt-timeout (DeadlineExceeded from inner ctx).
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP warpstream_produce_direct_requests_total Total number of direct Produce requests issued to a Warpstream agent, by routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Hedge retries count as separate attempts; connection-level retries are included in the attempt.
				# TYPE warpstream_produce_direct_requests_total counter
				warpstream_produce_direct_requests_total{agent_state="healthy"} 1
				# HELP warpstream_produce_direct_requests_failed_total Total number of direct Produce requests issued to a Warpstream agent that failed, by failure reason and routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Hedge retries count as separate attempts; connection-level retries are included in the attempt.
				# TYPE warpstream_produce_direct_requests_failed_total counter
				warpstream_produce_direct_requests_failed_total{agent_state="healthy",reason="timeout"} 1
			`), "warpstream_produce_direct_requests_total", "warpstream_produce_direct_requests_failed_total"))
		})
	})

	// A transport-OK response carrying a per-partition error code is a failure:
	// it must increment the failed counter (by classified reason) and observe
	// failure — not success — latency. Both the retriable and non-retriable
	// reason routings are covered.
	t.Run("per-partition error code with no transport error counts as a failure", func(t *testing.T) {
		cases := map[string]struct {
			errorCode int16
			wantErr   *kerr.Error
			reason    string
		}{
			"non-retriable code": {kerr.MessageTooLarge.Code, kerr.MessageTooLarge, "kafka_non_retriable_error"},
			"retriable code":     {kerr.NotLeaderForPartition.Code, kerr.NotLeaderForPartition, "kafka_retriable_error"},
		}
		for name, tc := range cases {
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var vnet kfake.VirtualNetwork
					cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))
					cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
						pr := req.(*kmsg.ProduceRequest)
						resp := pr.ResponseKind().(*kmsg.ProduceResponse)
						resp.Topics = []kmsg.ProduceResponseTopic{{
							Topic: topicName,
							Partitions: []kmsg.ProduceResponseTopicPartition{
								{Partition: partition, ErrorCode: tc.errorCode},
							},
						}}
						return resp, nil, true
					})

					client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.Dialer(vnet.DialContext))
					require.NoError(t, err)
					t.Cleanup(client.Close)

					metaResp, err := client.Request(t.Context(), &kmsg.MetadataRequest{
						Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
					})
					require.NoError(t, err)
					topicID := metaResp.(*kmsg.MetadataResponse).Topics[0].TopicID

					reg := prometheus.NewPedanticRegistry()
					topicIDFn := func(n string) ([16]byte, bool) {
						if n == topicName {
							return topicID, true
						}
						return [16]byte{}, false
					}
					m := newMetrics(reg)
					producer := NewKafkaDirectProducer(client, topicIDFn, 9, KafkaDirectProducerConfig{
						ProduceRequestTimeout:         time.Second,
						ProduceRequestTimeoutOverhead: time.Second,
					}, m)

					res := producer.ProduceSync(t.Context(), Agent{NodeID: brokerNodeID}, partitionsForTest([]*kgo.Record{{
						Topic: topicName, Partition: partition, Value: []byte("v"), Timestamp: time.Now(),
					}}))

					assert.False(t, res.succeeded())
					assert.ErrorIs(t, res.error(), tc.wantErr)

					require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
						# HELP warpstream_produce_direct_requests_failed_total Total number of direct Produce requests issued to a Warpstream agent that failed, by failure reason and routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Hedge retries count as separate attempts; connection-level retries are included in the attempt.
						# TYPE warpstream_produce_direct_requests_failed_total counter
						warpstream_produce_direct_requests_failed_total{agent_state="healthy",reason="%s"} 1
					`, tc.reason)), "warpstream_produce_direct_requests_failed_total"))

					successCount, _ := histogramCountSum(t, m.produceDirectRequestLatencySuccess.(prometheus.Histogram))
					assert.Equal(t, uint64(0), successCount)
					failureCount, _ := histogramCountSum(t, m.produceDirectRequestLatencyFailure.WithLabelValues(tc.reason).(prometheus.Histogram))
					assert.Equal(t, uint64(1), failureCount)
				})
			})
		}
	})
}

// partitionsForTest groups records by (topic, partition) and encodes each into
// an encodedTopicPartitionRecords, the input the DirectProducer chain takes.
func partitionsForTest(records []*kgo.Record) []encodedTopicPartitionRecords {
	groups := make(map[topicPartition]*topicPartitionRecords)
	var order []topicPartition
	for _, r := range records {
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		g, ok := groups[key]
		if !ok {
			g = &topicPartitionRecords{topic: r.Topic, partition: r.Partition}
			groups[key] = g
			order = append(order, key)
		}
		g.records = append(g.records, r)
	}
	out := make([]encodedTopicPartitionRecords, 0, len(order))
	for _, k := range order {
		g := groups[k]
		out = append(out, newEncodedTopicPartitionRecords(g.topic, g.partition, g.records))
	}
	return out
}

func TestKafkaDirectProducer_UnreachablePrimaryAllowsColdFailover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		cluster, address := testkafka.CreateCluster(t, 1, "test-topic", testkafka.WithNumBrokers(2), testkafka.WithVirtualNetwork(&vnet))
		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			req := request.(*kmsg.ProduceRequest)
			resp := req.ResponseKind().(*kmsg.ProduceResponse)
			resp.Version = req.Version
			for _, topic := range req.Topics {
				out := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
				for _, partition := range topic.Partitions {
					out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{Partition: partition.Partition})
				}
				resp.Topics = append(resp.Topics, out)
			}
			return resp, nil, true
		})
		var reject atomic.Bool
		versions := kversion.Stable()
		versions.SetMaxKeyVersion(kmsg.Produce.Int16(), 9)
		client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.MaxVersions(versions), kgo.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
			if reject.Load() && addr == cluster.ListenAddrs()[0] {
				return nil, &net.OpError{Op: "dial", Net: network, Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
			}
			return vnet.DialContext(ctx, network, addr)
		}))
		require.NoError(t, err)
		t.Cleanup(client.Close)
		metadata, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.NoError(t, err)
		id := metadata.(*kmsg.MetadataResponse).Topics[0].TopicID
		m := newMetrics(prometheus.NewPedanticRegistry())
		producer := NewKafkaDirectProducer(client, func(string) ([16]byte, bool) { return id, true }, 9, KafkaDirectProducerConfig{ProduceRequestTimeout: 8 * time.Second, ProduceRequestTimeoutOverhead: 2 * time.Second}, m)
		strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{"test-topic", 0}: healthyAgents(0, 1)}}
		hedger := NewHedger(producer, NewAverageAgentStatsTracker(), strategy, HealthCheckConfig{}, HedgerConfig{MinHedgeDelay: time.Millisecond, MaxHedgeAgents: 2}, 0, 1<<20, m, nil)
		t.Cleanup(hedger.Close)
		reject.Store(true)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		parts := partitionsForTest([]*kgo.Record{{Topic: "test-topic", Partition: 0, Value: []byte("value"), Timestamp: time.Now()}})
		started := time.Now()
		result := hedger.ProduceSync(ctx, 0, []routedEncodedTopicPartitionRecords{{encodedTopicPartitionRecords: parts[0], nodeID: 0, nodeState: AgentStateHealthy}})
		require.NoError(t, result.error())
		assert.Less(t, time.Since(started), time.Second)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoAgentStats)))
	})
}

func TestKafkaDirectProducer_MissingBrokerRemainsRetryable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork
		_, address := testkafka.CreateCluster(t, 1, "test-topic", testkafka.WithVirtualNetwork(&vnet))
		client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.MaxVersions(produceMaxVersions()), kgo.Dialer(vnet.DialContext))
		require.NoError(t, err)
		t.Cleanup(client.Close)
		producer := NewKafkaDirectProducer(client, func(string) ([16]byte, bool) { return [16]byte{}, true }, ProduceAPIVersion, KafkaDirectProducerConfig{ProduceRequestTimeout: time.Second, ProduceRequestTimeoutOverhead: time.Second}, newMetrics(prometheus.NewPedanticRegistry()))
		parts := partitionsForTest([]*kgo.Record{{Topic: "test-topic", Value: []byte("value"), Timestamp: time.Now()}})
		result := producer.ProduceSync(t.Context(), Agent{NodeID: 123}, parts)
		require.Error(t, result.error())
		classified := getProduceResultErr(result.error())
		assert.True(t, classified.retriable)
		assert.Equal(t, kerr.BrokerNotAvailable.Code, classified.code)
		m := newMetrics(prometheus.NewPedanticRegistry())
		strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{"test-topic", 0}: healthyAgents(123, 0)}}
		hedger := NewHedger(producer, NewAverageAgentStatsTracker(), strategy, HealthCheckConfig{}, HedgerConfig{MinHedgeDelay: time.Millisecond, MaxHedgeAgents: 2}, 0, 1<<20, m, nil)
		t.Cleanup(hedger.Close)
		response := hedger.ProduceSync(t.Context(), 123, []routedEncodedTopicPartitionRecords{{encodedTopicPartitionRecords: parts[0], nodeID: 123, nodeState: AgentStateHealthy}})
		require.NoError(t, response.error())
	})
}

func TestKafkaDirectProducer_ConnectionRetryReservesFailoverBudget(t *testing.T) {
	for _, budget := range []time.Duration{time.Second, 10 * time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var vnet kfake.VirtualNetwork
				cluster, address := testkafka.CreateCluster(t, 1, "test-topic", testkafka.WithNumBrokers(2), testkafka.WithVirtualNetwork(&vnet))
				var primaryCalls atomic.Int32
				cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()
					if cluster.CurrentNode() == 0 {
						if primaryCalls.Add(1) == 1 {
							return nil, io.EOF, true
						}
						return nil, nil, true
					}
					req := request.(*kmsg.ProduceRequest)
					resp := req.ResponseKind().(*kmsg.ProduceResponse)
					resp.Version = req.Version
					for _, topic := range req.Topics {
						out := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
						for _, partition := range topic.Partitions {
							out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{Partition: partition.Partition})
						}
						resp.Topics = append(resp.Topics, out)
					}
					return resp, nil, true
				})
				client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.Dialer(vnet.DialContext), kgo.MaxVersions(produceMaxVersions()), kgo.AlwaysRetryEOF())
				require.NoError(t, err)
				t.Cleanup(client.Close)
				m := newMetrics(prometheus.NewPedanticRegistry())
				producer := NewKafkaDirectProducer(client, func(string) ([16]byte, bool) { return [16]byte{}, true }, ProduceAPIVersion, KafkaDirectProducerConfig{ProduceRequestTimeout: 8 * time.Second, ProduceRequestTimeoutOverhead: 2 * time.Second}, m)
				strategy := &mockPartitionAssignmentStrategy{candidates: map[partitionKey][]Agent{{"test-topic", 0}: healthyAgents(0, 1)}}
				hedger := NewHedger(producer, NewAverageAgentStatsTracker(), strategy, HealthCheckConfig{}, HedgerConfig{MinHedgeDelay: time.Millisecond, MaxHedgeAgents: 2}, 0, 1<<20, m, nil)
				t.Cleanup(hedger.Close)
				ctx, cancel := context.WithTimeout(t.Context(), budget)
				defer cancel()
				parts := partitionsForTest([]*kgo.Record{{Topic: "test-topic", Value: []byte("value"), Timestamp: time.Now()}})
				started := time.Now()
				result := hedger.ProduceSync(ctx, 0, []routedEncodedTopicPartitionRecords{{encodedTopicPartitionRecords: parts[0], nodeID: 0, nodeState: AgentStateHealthy}})
				require.NoError(t, result.error())
				assert.Equal(t, min(2*time.Second, budget/2), time.Since(started))
				assert.Equal(t, int32(2), primaryCalls.Load())
			})
		})
	}
}
