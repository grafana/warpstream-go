package wgo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DirectProducer produces pre-encoded partitions to a specific Warpstream agent
// by its Kafka NodeID. Implementations must be safe for concurrent use. The wire
// request is assembled at the lowest layer (KafkaDirectProducer) from the
// already-encoded batches; upper layers just shuffle partition groups around.
//
// The signature takes bare encodedTopicPartitionRecords (without nodeID, done,
// or nodeState) so the "which agent does this batch go to" decision lives
// in exactly one place — the target parameter — and the function cannot
// be called with a mismatched per-partition nodeID. target is an Agent rather
// than a bare NodeID so routing-time State travels with the attempt; State is
// not sent on the wire.
type DirectProducer interface {
	ProduceSync(ctx context.Context, target Agent, partitions []encodedTopicPartitionRecords) ProduceResult
}

// KafkaDirectProducerConfig holds the per-request timings for
// KafkaDirectProducer.
type KafkaDirectProducerConfig struct {
	// ProduceRequestTimeout is the agent-side deadline written into
	// ProduceRequest.TimeoutMillis. The agent replies with REQUEST_TIMED_OUT
	// after this much wall-clock without committing.
	ProduceRequestTimeout time.Duration

	// ProduceRequestTimeoutOverhead is added on top of ProduceRequestTimeout
	// to compute the client-side ctx deadline. Always strictly greater than zero
	// so the client never kills a connection while the agent is still legitimately
	// working.
	ProduceRequestTimeoutOverhead time.Duration
}

// Validate returns an error if the config is invalid.
func (c *KafkaDirectProducerConfig) Validate() error {
	if c.ProduceRequestTimeout <= 0 {
		return errors.New("produce request timeout must be positive")
	}
	if c.ProduceRequestTimeoutOverhead <= 0 {
		return errors.New("produce request timeout overhead must be positive")
	}
	return nil
}

// KafkaDirectProducer implements DirectProducer using kgo.Client. Wire request
// assembly and per-request timing are enforced here, not at a higher layer.
//
// This is the only type in this package that depends on kgo.Client directly.
type KafkaDirectProducer struct {
	client  *kgo.Client
	topicID func(string) ([16]byte, bool)
	version int16
	cfg     KafkaDirectProducerConfig
	metrics *metrics

	// onProduceResponse is a generic post-response hook invoked after
	// broker.Request returns and before the response is propagated up
	// the chain. The hook receives the per-attempt ctx, the response (may
	// be nil on transport error) and the error. Used by tests to inject
	// behavior such as artificial latency. Production code leaves it nil.
	onProduceResponse func(ctx context.Context, nodeID int32, resp *kmsg.ProduceResponse, err error)
}

// NewKafkaDirectProducer returns a KafkaDirectProducer backed by client.
// topicID resolves a topic name to its Kafka UUID at request-build time.
func NewKafkaDirectProducer(client *kgo.Client, topicID func(string) ([16]byte, bool), version int16, cfg KafkaDirectProducerConfig, m *metrics) *KafkaDirectProducer {
	return &KafkaDirectProducer{client: client, topicID: topicID, version: version, cfg: cfg, metrics: m}
}

// ProduceSync implements DirectProducer.
func (s *KafkaDirectProducer) ProduceSync(ctx context.Context, target Agent, partitions []encodedTopicPartitionRecords) (retResult ProduceResult) {
	req, reqStats, err := buildMultiTopicProduceRequestFromEncoded(s.version, s.topicID, partitions)
	if err != nil {
		return ProduceResult{err: err}
	}
	req.SetTimeout(int32(s.cfg.ProduceRequestTimeout.Milliseconds()))

	// The wire request must not inherit ctx's cancellation: franz-go kills the
	// whole connection when a request is canceled while its response is being
	// awaited, failing every other produce in flight to that agent (e.g. other
	// primaries sharing the connection when a hedge wins). Only the per-attempt
	// deadline may interrupt the wire; a canceled caller abandons the attempt
	// instead and its response is discarded.
	attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ProduceRequestTimeout+s.cfg.ProduceRequestTimeoutOverhead)

	agentState := agentStateLabel(target.State)
	s.metrics.produceDirectRequestsTotal.WithLabelValues(agentState).Inc()

	// Observe per-attempt latency and the failure reason at the single exit.
	// Classify via the returned ProduceResult, not the transport error, so a
	// response carrying a per-partition error code (with no transport error)
	// is counted as a failure.
	reqStart := time.Now()
	defer func() {
		latencySeconds := time.Since(reqStart).Seconds()
		if retResult.succeeded() {
			s.metrics.produceDirectRequestLatencySuccess.Observe(latencySeconds)

			// Producer-state counters: count the request's batches once, only
			// when the whole request is acked. The request is all-or-nothing
			// (see produceResultAccumulator), so a failed attempt counts
			// nothing and is retried; this mirrors franz-go, which counts each
			// batch once on success.
			s.metrics.produceWireRecordsTotal.Add(float64(reqStats.records))
			s.metrics.produceWireBatchesTotal.Add(float64(reqStats.batches))
			s.metrics.produceWireBytesTotal.Add(float64(reqStats.uncompressedBytes))
			s.metrics.produceWireCompressedBytesTotal.Add(float64(reqStats.compressedBytes))
		} else {
			reason := getProduceResultErr(retResult.error()).reason
			s.metrics.produceDirectRequestLatencyFailure.WithLabelValues(reason).Observe(latencySeconds)
			s.metrics.produceDirectRequestsFailedTotal.WithLabelValues(reason, agentState).Inc()
		}
	}()

	// Buffered so an abandoned attempt's goroutine never blocks on send.
	done := make(chan ProduceResult, 1)
	go func() {
		defer cancel()
		done <- s.request(attemptCtx, target, req)
	}()

	// The caller's deadline is honoured like a cancellation: the attempt is
	// abandoned, not interrupted, so ctx.Err() keeps DeadlineExceeded vs
	// Canceled for classification.
	select {
	case res := <-done:
		return res
	case <-ctx.Done():
		return ProduceResult{err: ctx.Err()}
	}
}

// request sends req to target and decodes the response. attemptCtx bounds
// the wire round trip only.
func (s *KafkaDirectProducer) request(attemptCtx context.Context, target Agent, req *kmsg.ProduceRequest) ProduceResult {
	// Uses Broker.Request (not RetriableRequest) so franz-go's internal retry
	// loop stays out of the way: retries are owned by the caller.
	resp, err := s.client.Broker(int(target.NodeID)).Request(attemptCtx, req)

	// Decode the response up front so the hook always observes the same
	// typed value the caller would see.
	var pr *kmsg.ProduceResponse
	if err == nil {
		var ok bool
		pr, ok = resp.(*kmsg.ProduceResponse)
		if !ok {
			return ProduceResult{err: fmt.Errorf("unexpected response type %T", resp)}
		}
	}

	if s.onProduceResponse != nil {
		s.onProduceResponse(attemptCtx, target.NodeID, pr, err)
		// The hook may have slept past the attempt deadline.
		if ctxErr := attemptCtx.Err(); ctxErr != nil {
			return ProduceResult{err: ctxErr}
		}
	}

	// Wrap so classification can tell this from a request we failed to build.
	if err != nil {
		return ProduceResult{err: &wireError{err: err}}
	}
	return ProduceResult{resp: pr}
}
