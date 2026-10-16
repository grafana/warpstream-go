package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

type distributorWriters struct{ clients []*distributorClient }

func newDistributorWriters(binary string, cfg runner.WorkloadConfig, options distributorOptions, writers int) (runner.Producer, error) {
	if writers == 0 {
		writers = 1
		if cfg.SecondaryTopic != "" {
			writers = 2
		}
	}
	if writers < 1 || writers > 2 {
		return nil, fmt.Errorf("mimir-writers must be 1 or 2")
	}
	if writers == 1 {
		return newDistributorWithOptions(binary, cfg, options)
	}
	if cfg.SecondaryTopic == "" {
		return nil, fmt.Errorf("two independent writers require -profile=mimir-investigation")
	}
	group := &distributorWriters{}
	for i, topic := range []string{cfg.Topic, cfg.SecondaryTopic} {
		writerCfg := cfg
		writerCfg.Topic = topic
		if i > 0 && cfg.SecondaryPartitions > 0 {
			writerCfg.Partitions = cfg.SecondaryPartitions
		}
		if i > 0 {
			writerCfg.RemoteTimeout = cfg.WriteTimeout
		}
		client, err := newDistributorWithOptions(binary, writerCfg, options)
		if err != nil {
			group.Close()
			return nil, err
		}
		group.clients = append(group.clients, client)
	}
	return group, nil
}

func (g *distributorWriters) EventRecords(_ string, _ int32, at time.Duration) []*kgo.Record {
	var records []*kgo.Record
	for _, c := range g.clients {
		records = append(records, c.EventRecords(c.config.Topic, c.config.Partitions, at)...)
	}
	return records
}

func (g *distributorWriters) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	for _, c := range g.clients {
		if c.config.Topic != record.Topic {
			continue
		}
		c.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil {
				err = fmt.Errorf("writer %s: %w", record.Topic, err)
			}
			promise(r, err)
		})
		return
	}
	promise(record, fmt.Errorf("no writer for topic %q", record.Topic))
}

func (g *distributorWriters) Gather() ([]*dto.MetricFamily, error) {
	merged := map[string]*dto.MetricFamily{}
	for _, c := range g.clients {
		families, err := c.Gather()
		if err != nil {
			return nil, err
		}
		for _, family := range families {
			copy := proto.Clone(family).(*dto.MetricFamily)
			for _, metric := range copy.Metric {
				metric.Label = append(metric.Label, &dto.LabelPair{Name: proto.String("simulation_writer"), Value: proto.String(c.config.Topic)})
			}
			if existing := merged[copy.GetName()]; existing != nil {
				existing.Metric = append(existing.Metric, copy.Metric...)
			} else {
				merged[copy.GetName()] = copy
			}
		}
	}
	out := make([]*dto.MetricFamily, 0, len(merged))
	for _, family := range merged {
		out = append(out, family)
	}
	return out, nil
}

func (*distributorWriters) MetricsPrefix() string { return "cortex_ingest_storage_writer_" }
func (g *distributorWriters) Validate() error {
	for _, c := range g.clients {
		if err := c.Validate(); err != nil {
			return fmt.Errorf("writer %s: %w", c.config.Topic, err)
		}
	}
	return nil
}
func (g *distributorWriters) Close() {
	for i := len(g.clients) - 1; i >= 0; i-- {
		g.clients[i].Close()
	}
}
func (*distributorWriters) RecoveryResponseDescription() string {
	return (*distributorClient)(nil).RecoveryResponseDescription()
}
func (g *distributorWriters) Describe() (string, string) {
	_, configuration := g.clients[0].Describe()
	var deadlines []string
	for _, c := range g.clients {
		deadlines = append(deadlines, fmt.Sprintf("%s: %s, %d partitions", c.config.Topic, c.remoteTimeout(), c.config.Partitions))
	}
	return fmt.Sprintf("Workload: %d independent, unmodified distributor processes per client lane share agents but write distinct topics. Each application event sends a remote-write request to every process and succeeds only if all requests succeed. This models coupled independent writer outcomes. Each request has %d series; partition counts are listed below. Fake brokers acknowledge without durable storage.", len(g.clients), g.clients[0].series), configuration + " Independent-writer HTTP budgets: " + strings.Join(deadlines, ", ") + "."
}
