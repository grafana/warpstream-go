package runner

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type hedgeDiagnostics struct {
	faultyFraction *float64
	suppressed     map[string]int64
	failures       map[string]int64
}

type hedgeDiagnosticInterval struct {
	FromSeconds    float64          `json:"from_seconds"`
	ToSeconds      float64          `json:"to_seconds"`
	FaultyFraction *float64         `json:"max_writer_faulty_fraction,omitempty"`
	Suppressed     map[string]int64 `json:"suppressed_by_reason,omitempty"`
	Failures       map[string]int64 `json:"direct_failures_by_reason,omitempty"`
}

func readCounterSample(reg prometheus.Gatherer, prefix string) counterSample {
	families, err := reg.Gather()
	if err != nil {
		panic(fmt.Errorf("read simulation counters: %w", err))
	}
	d := &hedgeDiagnostics{suppressed: map[string]int64{}, failures: map[string]int64{}}
	sample := counterSample{diagnostics: d}
	foundPrimary, foundHedge := false, false
	for _, family := range families {
		name := strings.TrimPrefix(family.GetName(), prefix)
		for _, metric := range family.Metric {
			value := int64(metric.GetCounter().GetValue())
			switch name {
			case "warpstream_produce_requests_primary_total":
				sample.primary += value
				foundPrimary = true
			case "warpstream_produce_requests_hedge_total":
				sample.hedge += value
				foundHedge = true
			case "warpstream_cluster_faulty_fraction":
				// Writers classify health independently; summing fractions invents a cluster view.
				fraction := metric.GetGauge().GetValue()
				if d.faultyFraction == nil || fraction > *d.faultyFraction {
					d.faultyFraction = &fraction
				}
			case "warpstream_hedge_attempts_suppressed_total", "warpstream_produce_direct_requests_failed_total":
				for _, label := range metric.Label {
					if label.GetName() == "reason" {
						counts := d.suppressed
						if name == "warpstream_produce_direct_requests_failed_total" {
							counts = d.failures
						}
						counts[label.GetValue()] += value
					}
				}
			}
		}
	}
	if !foundPrimary || !foundHedge {
		panic("simulation metrics missing primary or hedge counter")
	}
	return sample
}

func diagnosticIntervals(samples []counterSample) []hedgeDiagnosticInterval {
	var intervals []hedgeDiagnosticInterval
	for i := 1; i < len(samples); i++ {
		previous, current := samples[i-1], samples[i]
		if previous.diagnostics == nil || current.diagnostics == nil {
			continue
		}
		intervals = append(intervals, hedgeDiagnosticInterval{
			FromSeconds:    previous.at.Sub(samples[0].at).Seconds(),
			ToSeconds:      current.at.Sub(samples[0].at).Seconds(),
			FaultyFraction: current.diagnostics.faultyFraction,
			Suppressed:     counterDifference(previous.diagnostics.suppressed, current.diagnostics.suppressed),
			Failures:       counterDifference(previous.diagnostics.failures, current.diagnostics.failures),
		})
	}
	return intervals
}

func counterDifference(previous, current map[string]int64) map[string]int64 {
	delta := map[string]int64{}
	for reason, count := range current {
		if change := count - previous[reason]; change != 0 {
			delta[reason] = change
		}
	}
	return delta
}

func (rr *resultsReport) writeHedgeDiagnostics(b *strings.Builder, intervals []hedgeDiagnosticInterval) {
	if len(intervals) == 0 {
		return
	}
	b.WriteString("\n### Hedge health diagnostics\n\nCounter deltas exclude warmup. Faulty fraction is the largest writer's last computed gauge at each sample; it is not a peak over the interval. Gauges retain their last value when stats are unavailable. JSON includes every suppression and direct-failure reason. Intervals past observation include drain activity.\n\n")
	b.WriteString("| Interval | Max writer faulty fraction | Hedges suppressed: faulty fraction | Direct timeouts | Direct transport errors |\n| --- | --- | --- | --- | --- |\n")
	for _, interval := range intervals {
		fraction := "unavailable"
		if interval.FaultyFraction != nil {
			fraction = fmt.Sprintf("%.1f%%", 100**interval.FaultyFraction)
		}
		fmt.Fprintf(b, "| %.1fs–%.1fs | %s | %d | %d | %d |\n", interval.FromSeconds, interval.ToSeconds, fraction, interval.Suppressed["faulty_fraction_exceeded"], interval.Failures["timeout"], interval.Failures["transport"])
	}
}
