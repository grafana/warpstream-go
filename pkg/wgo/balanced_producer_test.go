package wgo

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBalancedProducer_CanceledDrainKeepsConnectionBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, _, parts, started, release := gatedKafkaDirectProducer(t)
		cfg := DefaultConfig()
		cfg.Address = first.client.OptValue(kgo.SeedBrokers).([]string)
		cfg.Dialer = first.client.OptValue(kgo.Dialer).(func(context.Context, string, string) (net.Conn, error))
		primary, clients, err := newKgoClients(cfg, nopLogger{}, prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		defer primary.Close()
		for _, client := range clients {
			defer client.Close()
		}
		second := NewKafkaDirectProducer(clients[0], first.topicID, first.version, first.cfg, first.metrics)
		pool := newBalancedProducer([]DirectProducer{first, second})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan ProduceResult, 1)
		go func() { result <- pool.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
		<-started
		cancel()
		synctest.Wait()
		begin := time.Now()
		require.NoError(t, pool.ProduceSync(t.Context(), Agent{NodeID: 0}, parts).error())
		require.Zero(t, time.Since(begin))
		select {
		case <-result:
			t.Fatal("cancellation released a started wire request")
		default:
		}
		release()
		require.NoError(t, (<-result).error())
		require.Empty(t, pool.active)
	})
}

func TestBalancedProducer_LoadIsPerAgent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, second := newMockDirectProducer(), newMockDirectProducer()
		first.delays[0] = time.Hour
		pool := newBalancedProducer([]DirectProducer{first, second})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan ProduceResult, 1)
		go func() { done <- pool.ProduceSync(ctx, Agent{NodeID: 0}, nil) }()
		synctest.Wait()
		require.NoError(t, pool.ProduceSync(t.Context(), Agent{NodeID: 1}, nil).error())
		require.NoError(t, pool.ProduceSync(t.Context(), Agent{NodeID: 0}, nil).error())
		require.Equal(t, []int32{1}, first.recordedCallNodeIDs())
		require.Equal(t, []int32{0}, second.recordedCallNodeIDs())
		cancel()
		require.ErrorIs(t, (<-done).error(), context.Canceled)
		require.Empty(t, pool.active)
	})
}
