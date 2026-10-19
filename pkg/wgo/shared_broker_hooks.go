package wgo

import (
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Lifecycle hooks own collector registration; only transport events are shared.
type sharedBrokerHooks struct{ hooks []kgo.Hook }

func (s sharedBrokerHooks) OnBrokerConnect(meta kgo.BrokerMetadata, duration time.Duration, conn net.Conn, err error) {
	for _, hook := range s.hooks {
		if h, ok := hook.(kgo.HookBrokerConnect); ok {
			h.OnBrokerConnect(meta, duration, conn, err)
		}
	}
}
func (s sharedBrokerHooks) OnBrokerDisconnect(meta kgo.BrokerMetadata, conn net.Conn) {
	for _, hook := range s.hooks {
		if h, ok := hook.(kgo.HookBrokerDisconnect); ok {
			h.OnBrokerDisconnect(meta, conn)
		}
	}
}
func (s sharedBrokerHooks) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, size int, wait, duration time.Duration, err error) {
	for _, hook := range s.hooks {
		if h, ok := hook.(kgo.HookBrokerWrite); ok {
			h.OnBrokerWrite(meta, key, size, wait, duration, err)
		}
	}
}
func (s sharedBrokerHooks) OnBrokerRead(meta kgo.BrokerMetadata, key int16, size int, wait, duration time.Duration, err error) {
	for _, hook := range s.hooks {
		if h, ok := hook.(kgo.HookBrokerRead); ok {
			h.OnBrokerRead(meta, key, size, wait, duration, err)
		}
	}
}
func (s sharedBrokerHooks) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	for _, hook := range s.hooks {
		if h, ok := hook.(kgo.HookBrokerE2E); ok {
			h.OnBrokerE2E(meta, key, e2e)
		}
	}
}
func (s sharedBrokerHooks) OnBrokerThrottle(meta kgo.BrokerMetadata, interval time.Duration, after bool) {
	for _, hook := range s.hooks {
		if h, ok := hook.(kgo.HookBrokerThrottle); ok {
			h.OnBrokerThrottle(meta, interval, after)
		}
	}
}
