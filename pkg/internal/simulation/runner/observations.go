package runner

import (
	"slices"
	"sync"
	"time"
)

// observation is one produce call's outcome.
type observation struct {
	// Recovery membership must not depend on scheduler delays.
	scheduled time.Time
	issued    time.Time
	latency   time.Duration
	err       error

	// records is how many produce records the event fanned out and recordsOK how
	// many of those were acknowledged before the application deadline. Zero records
	// means the observation is a single write whose outcome is err.
	records, recordsOK int
}

// observations is a thread-safe collector.
type observations struct {
	mu   sync.Mutex
	list []observation
}

// record appends one observation from its fields.
func (o *observations) record(scheduled, issued time.Time, latency time.Duration, err error) {
	o.add(observation{scheduled: scheduled, issued: issued, latency: latency, err: err})
}

// recordWrites appends one observation together with its per-record outcome.
func (o *observations) recordWrites(scheduled, issued time.Time, latency time.Duration, err error, records, recordsOK int) {
	o.add(observation{scheduled: scheduled, issued: issued, latency: latency, err: err, records: records, recordsOK: recordsOK})
}

// writes returns the observation's record counts, treating a plain observation as
// one write.
func (o observation) writes() (total, ok int) {
	if o.records > 0 {
		return o.records, o.recordsOK
	}
	if o.err == nil {
		return 1, 1
	}
	return 1, 0
}

// add appends an already-formed observation.
func (o *observations) add(obs observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.list = append(o.list, obs)
}

// snapshot captures the recorded observations into a read-only view. Every
// aggregation derives from one snapshot, so a summary's mean, p50 and p99 always
// describe the same population even if recording continues.
func (o *observations) snapshot() observationsSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]observation, len(o.list))
	copy(out, o.list)
	return observationsSnapshot{list: out}
}

// observationsSnapshot is a read-only set of observations and the source of every
// aggregation below.
type observationsSnapshot struct {
	list []observation
}

// latencyQuantile returns the q-quantile (q in [0,1]) of latency across all
// observations, or 0 when there are none.
func (s observationsSnapshot) latencyQuantile(q float64) time.Duration {
	if len(s.list) == 0 {
		return 0
	}
	latencies := make([]time.Duration, len(s.list))
	for i, ob := range s.list {
		latencies[i] = ob.latency
	}
	slices.Sort(latencies)
	idx := int(float64(len(latencies)) * q)
	if idx >= len(latencies) {
		idx = len(latencies) - 1
	}
	return latencies[idx]
}

// slowFraction returns the fraction of observations (successes and failures
// alike — a failure's latency is always at least the caller's own deadline,
// so it counts as slow too) whose latency exceeds budget. Returns 0 when
// there are no observations.
func (s observationsSnapshot) slowFraction(budget time.Duration) float64 {
	if len(s.list) == 0 {
		return 0
	}
	var slow int
	for _, ob := range s.list {
		if ob.latency > budget {
			slow++
		}
	}
	return float64(slow) / float64(len(s.list))
}

// errorCounts groups failed observations by error string, returning error → count.
// Successful observations (nil error) are ignored.
func (s observationsSnapshot) errorCounts() map[string]int {
	counts := map[string]int{}
	for _, ob := range s.list {
		if ob.err == nil {
			continue
		}
		counts[ob.err.Error()]++
	}
	return counts
}

// summary aggregates the snapshot.
func (s observationsSnapshot) summary() observationsSummary {
	out := observationsSummary{total: len(s.list)}
	if len(s.list) == 0 {
		return out
	}
	var sum, successSum time.Duration
	var succeeded []observation
	for _, ob := range s.list {
		total, ok := ob.writes()
		out.writes += total
		out.writesOK += ok
		if ob.err == nil {
			out.successes++
			successSum += ob.latency
			succeeded = append(succeeded, ob)
		} else {
			out.failures++
		}
		sum += ob.latency
	}
	out.meanLatency = sum / time.Duration(len(s.list))
	out.p50Latency = s.latencyQuantile(0.50)
	out.p99Latency = s.latencyQuantile(0.99)
	if len(succeeded) > 0 {
		successes := observationsSnapshot{list: succeeded}
		out.successMeanLatency = successSum / time.Duration(len(succeeded))
		out.successP50Latency = successes.latencyQuantile(0.50)
		out.successP99Latency = successes.latencyQuantile(0.99)
	}
	return out
}

// bucketedSummary splits the observations into windowSize-wide buckets keyed by
// issue time and returns one summary per bucket, including empty buckets as gaps.
// The first bucket is anchored at the earliest issued time; returns nil when there
// are no observations.
func (s observationsSnapshot) bucketedSummary(windowSize time.Duration) []observationsSummary {
	if len(s.list) == 0 {
		return nil
	}
	var start time.Time
	for _, ob := range s.list {
		if start.IsZero() || ob.issued.Before(start) {
			start = ob.issued
		}
	}
	byIdx := map[int][]observation{}
	maxIdx := 0
	for _, ob := range s.list {
		idx := int(ob.issued.Sub(start) / windowSize)
		byIdx[idx] = append(byIdx[idx], ob)
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	out := make([]observationsSummary, maxIdx+1)
	for i := 0; i <= maxIdx; i++ {
		if len(byIdx[i]) > 0 {
			out[i] = observationsSnapshot{list: byIdx[i]}.summary()
		}
	}
	return out
}

// observationsSummary aggregates a set of observations.
type observationsSummary struct {
	total     int
	successes int
	failures  int

	// writes counts the produce records of every event and writesOK those
	// acknowledged before the deadline. With one record per event they equal
	// total and successes; with a fan-out they show how much of a partly failed
	// event still got through.
	writes, writesOK int

	meanLatency time.Duration
	p50Latency  time.Duration
	p99Latency  time.Duration

	// The latency of the successful observations alone. A failure's latency is its
	// caller's deadline, so it hides how long the writes that did succeed took.
	// All three are zero when nothing succeeded.
	successMeanLatency time.Duration
	successP50Latency  time.Duration
	successP99Latency  time.Duration
}

func (s observationsSnapshot) scheduledSince(start time.Time) observationsSnapshot {
	out := observationsSnapshot{}
	for _, observation := range s.list {
		if !observation.scheduled.Before(start) {
			out.list = append(out.list, observation)
		}
	}
	return out
}
