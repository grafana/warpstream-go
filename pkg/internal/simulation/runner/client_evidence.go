package runner

import "github.com/prometheus/client_golang/prometheus"

type clientEvidence struct {
	WriteFailures    map[string]int64 `json:"write_failures"`
	DirectFailures   map[string]int64 `json:"direct_failures"`
	HedgeSuppression map[string]int64 `json:"hedge_suppression"`
	HedgeTriggers    map[string]int64 `json:"hedge_triggers"`
}

func readClientEvidence(metrics prometheus.Gatherer, prefix string) (*clientEvidence, error) {
	families, err := metrics.Gather()
	if err != nil {
		return nil, err
	}
	evidence := &clientEvidence{
		WriteFailures: map[string]int64{}, DirectFailures: map[string]int64{},
		HedgeSuppression: map[string]int64{}, HedgeTriggers: map[string]int64{},
	}
	for _, family := range families {
		var counts map[string]int64
		labelName := "reason"
		switch family.GetName() {
		case prefix + "warpstream_produce_requests_failed_total":
			counts = evidence.WriteFailures
		case prefix + "warpstream_produce_direct_requests_failed_total":
			counts = evidence.DirectFailures
		case prefix + "warpstream_hedge_attempts_suppressed_total":
			counts = evidence.HedgeSuppression
		case prefix + "warpstream_produce_hedge_triggers_total":
			counts, labelName = evidence.HedgeTriggers, "trigger"
		default:
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == labelName {
					counts[label.GetValue()] += int64(metric.GetCounter().GetValue())
				}
			}
		}
	}
	return evidence, nil
}

func (e *clientEvidence) subtract(before *clientEvidence) {
	for _, pair := range [][2]map[string]int64{
		{e.WriteFailures, before.WriteFailures}, {e.DirectFailures, before.DirectFailures},
		{e.HedgeSuppression, before.HedgeSuppression}, {e.HedgeTriggers, before.HedgeTriggers},
	} {
		for reason, count := range pair[1] {
			pair[0][reason] -= count
		}
	}
}
