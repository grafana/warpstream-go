package runner

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type staleFallbackEvidence struct {
	BrokerOnlyWithdrawals int64            `json:"broker_only_withdrawals"`
	StaleRoutingResponses int64            `json:"stale_routing_responses"`
	FallbackErrors        int64            `json:"fallback_errors"`
	FallbackErrorReasons  map[string]int64 `json:"fallback_error_reasons"`
	ThirdAttemptSuccesses int64            `json:"third_attempt_successes"`
	TerminalErrors        int64            `json:"terminal_errors"`
}

func readStaleFallbackEvidence(metrics prometheus.Gatherer, prefix string, view *metadataView) (*staleFallbackEvidence, error) {
	families, err := metrics.Gather()
	if err != nil {
		return nil, err
	}
	view.mu.Lock()
	evidence := &staleFallbackEvidence{BrokerOnlyWithdrawals: view.brokerOnlyWithdrawals, StaleRoutingResponses: view.staleRoutingResponses, FallbackErrorReasons: map[string]int64{}}
	view.mu.Unlock()
	for _, family := range families {
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			switch family.GetName() {
			case prefix + "warpstream_produce_direct_requests_failed_total":
				switch labels["reason"] {
				case "unknown_broker", "unknown", "transport":
					evidence.FallbackErrors += int64(metric.GetCounter().GetValue())
					evidence.FallbackErrorReasons[labels["reason"]] += int64(metric.GetCounter().GetValue())
				}
			case prefix + "warpstream_produce_requests_failed_total":
				if labels["reason"] == "terminal_error" {
					evidence.TerminalErrors += int64(metric.GetCounter().GetValue())
				}
			case prefix + "warpstream_produce_requests_attempts":
				if labels["outcome"] == "success" {
					evidence.ThirdAttemptSuccesses += thirdAttempts(metric.GetHistogram())
				}
			}
		}
	}
	return evidence, nil
}

func thirdAttempts(h *dto.Histogram) int64 {
	for _, bucket := range h.GetBucket() {
		// Attempt counts are integers, so any boundary in [2, 3) separates
		// the first two attempts from later attempts exactly.
		if bound := bucket.GetUpperBound(); bound >= 2 && bound < 3 {
			return int64(h.GetSampleCount() - bucket.GetCumulativeCount())
		}
	}
	if h == nil || h.Schema == nil {
		return 0
	}
	var result, bucketCount int64
	index, position := int32(0), 0
	for _, span := range h.PositiveSpan {
		index += span.GetOffset()
		for range span.GetLength() {
			if position >= len(h.PositiveDelta) {
				return 0
			}
			// Native deltas are differences between adjacent bucket populations.
			// Summing them recovers this bucket's count, not a cumulative count.
			bucketCount += h.PositiveDelta[position]
			lower := math.Exp2(float64(index-1) * math.Exp2(-float64(h.GetSchema())))
			if lower >= 2 {
				result += bucketCount
			}
			index++
			position++
		}
	}
	return result
}
