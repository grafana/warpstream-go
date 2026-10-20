package wgo

import (
	"context"
	"errors"
	"time"
)

// TrackingProducer is a DirectProducer decorator that records the outcome —
// observed latency and error — of every Produce call into an AgentStatsTracker.
type TrackingProducer struct {
	inner   DirectProducer
	tracker AgentStatsTracker
}

// NewTrackingProducer returns a TrackingProducer that forwards to inner and
// records each call's latency and error to tracker.
func NewTrackingProducer(inner DirectProducer, tracker AgentStatsTracker) *TrackingProducer {
	return &TrackingProducer{inner: inner, tracker: tracker}
}

// ProduceSync implements DirectProducer.
func (p *TrackingProducer) ProduceSync(ctx context.Context, target Agent, partitions []encodedTopicPartitionRecords) ProduceResult {
	start := time.Now()
	res := p.inner.ProduceSync(ctx, target, partitions)
	// Per-partition broker errors must count even when the transport returns no error.
	healthErr := res.error()
	var wire *wireError
	if errors.Is(ctx.Err(), context.Canceled) && errors.As(healthErr, &wire) {
		reason := getProduceResultErr(healthErr).reason
		if reason == "transport" {
			// After cancellation, connection failures can come from a sibling's retained deadline.
			healthErr = context.Canceled
		}
	}
	p.tracker.TrackAgentRequest(time.Now(), target.NodeID, time.Since(start), healthErr)
	return res
}
