package runner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

type metadataRequestSource struct {
	apiVersions    atomic.Int32
	metadataWrites atomic.Pointer[chan int32]
}

func (s *metadataRequestSource) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if err != nil {
		return
	}
	switch key {
	case int16(kmsg.ApiVersions):
		s.apiVersions.Store(meta.NodeID)
	case int16(kmsg.Metadata):
		if writes := s.metadataWrites.Load(); writes != nil {
			select {
			case *writes <- meta.NodeID:
			default:
			}
		}
	}
}

type delayedMetadataReply struct {
	started     chan struct{}
	release     chan struct{}
	markStarted func()
	unblock     func()
}

func newDelayedMetadataReply() *delayedMetadataReply {
	d := &delayedMetadataReply{started: make(chan struct{}), release: make(chan struct{})}
	d.markStarted = sync.OnceFunc(func() { close(d.started) })
	d.unblock = sync.OnceFunc(func() { close(d.release) })
	return d
}

type seedMetadataRequest struct {
	client *kgo.Client
	source *metadataRequestSource
	view   *metadataView
	delay  *delayedMetadataReply
	done   chan metadataQueryResult
}

type metadataQueryResult struct {
	response *kmsg.MetadataResponse
	err      error
}

type seedMetadataReordering struct{ requests []*seedMetadataRequest }

func (r *seedMetadataReordering) release() {
	for _, request := range r.requests {
		if request.delay != nil {
			request.delay.unblock()
		}
	}
}

func (env *environment) prepareSeedMetadata(ctx context.Context) (_ *seedMetadataReordering, err error) {
	wgoClient, ok := env.wgoClient.(*wgo.WarpstreamClient)
	if !ok {
		return nil, fmt.Errorf("seed metadata ordering requires the direct workload")
	}
	kgoClient, ok := env.kgoClient.(*latencyDelayedKgoClient)
	if !ok {
		return nil, fmt.Errorf("seed metadata ordering requires the direct Kafka baseline")
	}
	r := &seedMetadataReordering{requests: []*seedMetadataRequest{
		{client: wgoClient.Client, source: env.wgoMetadataSource, view: env.wgoMetadata},
		{client: kgoClient.Client, source: env.kgoMetadataSource, view: env.kgoMetadata},
	}}
	defer func() {
		if err != nil {
			r.release()
		}
	}()
	for _, request := range r.requests {
		request.delay = newDelayedMetadataReply()
		request.done = make(chan metadataQueryResult, 1)
		if err := nextMetadataRequestFromSeed(ctx, request.client, request.source); err != nil {
			return nil, err
		}
		request.view.delayedReply.Store(request.delay)
		writes := make(chan int32, 1)
		request.source.metadataWrites.Store(&writes)
		go func() {
			query := kmsg.NewPtrMetadataRequest()
			query.IncludeTopicAuthorizedOperations = true
			raw, err := request.client.Request(ctx, query)
			result := metadataQueryResult{err: err}
			if err == nil {
				result.response = raw.(*kmsg.MetadataResponse)
			}
			request.done <- result
		}()
		select {
		case node := <-writes:
			request.source.metadataWrites.Store(nil)
			if node >= 0 {
				return nil, fmt.Errorf("delayed metadata did not use a seed connection: %d", node)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-request.delay.started:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r, nil
}

func nextMetadataRequestFromSeed(ctx context.Context, client *kgo.Client, source *metadataRequestSource) error {
	if source == nil {
		return fmt.Errorf("metadata source hook was not installed")
	}
	brokers := len(client.DiscoveredBrokers())
	// ApiVersions queries advance the public client's broker rotation without
	// replacing its broker list. The following metadata query uses its seed.
	for i := 0; ; i++ {
		if i > 2*(brokers+1) {
			return fmt.Errorf("could not select a seed metadata connection")
		}
		if _, err := client.Request(ctx, kmsg.NewPtrApiVersionsRequest()); err != nil {
			return err
		}
		if source.apiVersions.Load() < 0 {
			break
		}
	}
	for range brokers {
		if _, err := client.Request(ctx, kmsg.NewPtrApiVersionsRequest()); err != nil {
			return err
		}
		if source.apiVersions.Load() < 0 {
			return fmt.Errorf("broker rotation changed during metadata preparation")
		}
	}
	return nil
}

func (r *seedMetadataReordering) run(ctx context.Context, start time.Time) error {
	defer r.release()
	waitUntil := func(offset time.Duration) error {
		timer := time.NewTimer(max(time.Until(start.Add(offset)), 0))
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	refresh := func() error {
		var wg sync.WaitGroup
		errs := make([]error, len(r.requests))
		for i, request := range r.requests {
			wg.Go(func() { _, errs[i] = request.client.Request(ctx, kmsg.NewPtrMetadataRequest()) })
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := waitUntil(100 * time.Millisecond); err != nil {
		return err
	}
	if err := refresh(); err != nil {
		return err
	}
	if err := waitUntil(1100 * time.Millisecond); err != nil {
		return err
	}
	r.release()
	for _, request := range r.requests {
		select {
		case result := <-request.done:
			if result.err != nil {
				return result.err
			}
			if len(result.response.Brokers) != len(request.view.template.Brokers) {
				return fmt.Errorf("delayed metadata changed broker membership")
			}
			for i, broker := range result.response.Brokers {
				expected := request.view.template.Brokers[i].Rack
				if (broker.Rack == nil) != (expected == nil) || broker.Rack != nil && *broker.Rack != *expected {
					return fmt.Errorf("delayed metadata did not retain its pre-transition Rack")
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := waitUntil(2100 * time.Millisecond); err != nil {
		return err
	}
	return refresh()
}
