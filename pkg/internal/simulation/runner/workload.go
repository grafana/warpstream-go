package runner

import (
	"context"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Close()
}

type WorkloadConfig struct {
	Backend, Address, Topic                         string
	Partitions                                      int32
	Registry                                        *prometheus.Registry
	Dialer                                          func(context.Context, string, string) (net.Conn, error)
	ObserveProduce                                  func(address string)
	ProduceLatency                                  func(address string) time.Duration
	WriteTimeout, WriteTimeoutOverhead, DialTimeout time.Duration
}

type WorkloadFactory func(WorkloadConfig) (Producer, error)

type eventBuilder interface {
	EventRecords(topic string, partitions int32, at time.Duration) []*kgo.Record
	MetricsPrefix() string
}

func eventsForClient(client produceClient, topic string, partitions int32, duration, spacing time.Duration) []scenarioEvent {
	events := buildScenarioEvents(topic, partitions, duration, spacing)
	if builder, ok := client.(eventBuilder); ok {
		for i := range events {
			events[i].records = builder.EventRecords(topic, partitions, events[i].at)
		}
	}
	return events
}
