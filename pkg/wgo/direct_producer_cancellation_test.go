package wgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
)

func TestKafkaDirectProducer_CancellationDoesNotFailSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer, _, parts, started, release := gatedKafkaDirectProducer(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first, second := make(chan ProduceResult, 1), make(chan ProduceResult, 1)
		go func() { first <- producer.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
		<-started
		go func() { second <- producer.ProduceSync(t.Context(), Agent{NodeID: 0}, parts) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		release()
		firstResult, secondResult := <-first, <-second
		require.NoError(t, secondResult.error())
		require.NoError(t, firstResult.error())
		require.Equal(t, float64(2), testutil.ToFloat64(producer.metrics.produceDirectRequestsTotal.WithLabelValues("healthy")))
	})
}

func TestKafkaDirectProducer_AlreadyCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer, _, parts, started, _ := gatedKafkaDirectProducer(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result := producer.ProduceSync(ctx, Agent{NodeID: 0}, parts)
		require.ErrorIs(t, result.error(), context.Canceled)
		synctest.Wait()
		select {
		case <-started:
			t.Fatal("a canceled attempt reached the broker")
		default:
		}
		require.Zero(t, testutil.ToFloat64(producer.metrics.produceDirectRequestsTotal.WithLabelValues("healthy")))
	})
}

func TestKafkaDirectProducer_DeadlineFailsSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer, _, parts, started, release := gatedKafkaDirectProducer(t)
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		first, second := make(chan ProduceResult, 1), make(chan ProduceResult, 1)
		go func() { first <- producer.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
		<-started
		go func() { second <- producer.ProduceSync(t.Context(), Agent{NodeID: 0}, parts) }()
		synctest.Wait()
		cancel()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		require.ErrorIs(t, (<-first).error(), context.DeadlineExceeded)
		// The sibling's own deadline is still 1.5s away; this is collateral connection failure.
		select {
		case result := <-second:
			require.Error(t, result.error())
			require.NotErrorIs(t, result.error(), context.DeadlineExceeded)
		default:
			t.Fatal("the shared connection deadline did not fail the sibling")
		}
		release()
	})
}

func TestKafkaDirectProducer_CanceledAttemptKeepsDeadline(t *testing.T) {
	for name, parentTimeout := range map[string]time.Duration{"parent deadline": 500 * time.Millisecond, "attempt deadline": 5 * time.Second} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				producer, _, parts, started, release := gatedKafkaDirectProducer(t)
				ctx, cancel := context.WithTimeout(t.Context(), parentTimeout)
				defer cancel()
				result := make(chan ProduceResult, 1)
				begin := time.Now()
				go func() { result <- producer.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
				<-started
				cancel()
				synctest.Wait()
				wantTimeout := min(parentTimeout, 2*time.Second)
				time.Sleep(wantTimeout)
				synctest.Wait()
				require.ErrorIs(t, (<-result).error(), context.DeadlineExceeded)
				require.Equal(t, wantTimeout, time.Since(begin))
				release()
			})
		})
	}
}

func TestKafkaDirectProducer_CloseInterruptsDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer, client, parts, started, release := gatedKafkaDirectProducer(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan ProduceResult, 1)
		begin := time.Now()
		go func() { result <- producer.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
		<-started
		cancel()
		synctest.Wait()
		select {
		case <-result:
			t.Fatal("drain completed before the client closed")
		default:
		}
		client.Close()
		err := (<-result).error()
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed))
		require.Zero(t, time.Since(begin))
		release()
	})
}

func gatedKafkaDirectProducer(t *testing.T) (*KafkaDirectProducer, *kgo.Client, []encodedTopicPartitionRecords, <-chan struct{}, func()) {
	t.Helper()
	const topic = "cancel-test"
	var vnet kfake.VirtualNetwork
	cluster, address := testkafka.CreateCluster(t, 1, topic, testkafka.WithVirtualNetwork(&vnet))
	client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.Dialer(vnet.DialContext))
	require.NoError(t, err)
	t.Cleanup(client.Close)
	response, err := client.Request(t.Context(), &kmsg.MetadataRequest{Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topic)}}})
	require.NoError(t, err)
	id := response.(*kmsg.MetadataResponse).Topics[0].TopicID
	producer := NewKafkaDirectProducer(client, func(name string) ([16]byte, bool) { return id, name == topic }, 9, KafkaDirectProducerConfig{
		ProduceRequestTimeout:         time.Second,
		ProduceRequestTimeoutOverhead: time.Second,
	}, newMetrics(prometheus.NewPedanticRegistry()))
	started, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var seen atomic.Bool
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if seen.CompareAndSwap(false, true) {
			close(started)
			cluster.SleepControl(func() { <-gate })
		}
		return nil, nil, false
	})
	parts := partitionsForTest([]*kgo.Record{{Topic: topic, Partition: 0, Value: []byte("record"), Timestamp: time.Now()}})
	return producer, client, parts, started, release
}
