package wgo

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func TestKafkaDirectProducer_FallbackConnectionIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		original, _, parts, started, release := gatedKafkaDirectProducer(t)
		cfg := DefaultConfig()
		cfg.Address = original.client.OptValue(kgo.SeedBrokers).([]string)
		cfg.Dialer = original.client.OptValue(kgo.Dialer).(func(context.Context, string, string) (net.Conn, error))
		hooks := &transportLifecycleHook{}
		cfg.Hooks = []kgo.Hook{hooks}
		primaryClient, fallbackClients, err := newKgoClients(cfg, nopLogger{}, prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		defer primaryClient.Close()
		for _, client := range fallbackClients {
			defer client.Close()
		}
		fallbackClient := fallbackClients[0]
		require.Equal(t, int64(1), hooks.created.Load())
		primary := NewKafkaDirectProducer(primaryClient, original.topicID, original.version, original.cfg, original.metrics)
		fallback := NewKafkaDirectProducer(fallbackClient, original.topicID, original.version, original.cfg, original.metrics)
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		first := make(chan ProduceResult, 1)
		go func() { first <- primary.ProduceSync(ctx, Agent{NodeID: 0}, parts) }()
		<-started
		second := make(chan ProduceResult, 1)
		go func() { second <- fallback.ProduceSync(t.Context(), Agent{NodeID: 0}, parts) }()
		synctest.Wait()
		select {
		case result := <-second:
			require.NoError(t, result.error())
		default:
			t.Fatal("fallback blocked behind the primary connection")
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		require.ErrorIs(t, (<-first).error(), context.DeadlineExceeded)
		require.NoError(t, fallback.ProduceSync(t.Context(), Agent{NodeID: 0}, parts).error())
		release()
		fallbackClient.Close()
		require.Zero(t, hooks.closed.Load())
		primaryClient.Close()
		require.Equal(t, int64(1), hooks.closed.Load())
		require.Equal(t, int64(3), hooks.produces.Load())
	})
}

type transportLifecycleHook struct{ created, closed, produces atomic.Int64 }

func (h *transportLifecycleHook) OnNewClient(*kgo.Client)    { h.created.Add(1) }
func (h *transportLifecycleHook) OnClientClosed(*kgo.Client) { h.closed.Add(1) }
func (h *transportLifecycleHook) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	if key == 0 {
		h.produces.Add(1)
	}
}

func TestFallbackTransport_EmbeddedClientClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		original, _, _, _, _ := gatedKafkaDirectProducer(t)
		cfg := DefaultConfig()
		cfg.Address = original.client.OptValue(kgo.SeedBrokers).([]string)
		cfg.Dialer = original.client.OptValue(kgo.Dialer).(func(context.Context, string, string) (net.Conn, error))
		primary, fallbacks, err := newKgoClients(cfg, nopLogger{}, prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		for _, fallback := range fallbacks {
			defer fallback.Close()
		}
		primary.Close()
		for _, fallback := range fallbacks {
			require.ErrorIs(t, fallback.Context().Err(), context.Canceled)
		}
	})
}

func TestFallbackTransport_SASLClosesOnce(t *testing.T) {
	mechanism := &closingSASLMechanism{}
	cfg := DefaultConfig()
	cfg.Address = []string{"127.0.0.1:1"}
	cfg.SASLOptions = []kgo.Opt{kgo.SASL(mechanism)}
	primary, fallbacks, err := newKgoClients(cfg, nopLogger{}, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	for _, fallback := range fallbacks {
		fallback.Close()
	}
	require.Zero(t, mechanism.closed.Load())
	primary.Close()
	require.Equal(t, int64(1), mechanism.closed.Load())
}

type closingSASLMechanism struct{ closed atomic.Int64 }

func (*closingSASLMechanism) Name() string { return "PLAIN" }
func (*closingSASLMechanism) Authenticate(context.Context, string) (sasl.Session, []byte, error) {
	return nil, nil, nil
}
func (m *closingSASLMechanism) Close() { m.closed.Add(1) }
