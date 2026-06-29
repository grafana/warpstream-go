package main

import (
	"sort"
	"sync"
	"time"
)

// observation is one produce call's outcome.
type observation struct {
	issued  time.Time
	latency time.Duration
	err     error
}

// observations is a thread-safe collector.
type observations struct {
	mu   sync.Mutex
	list []observation
}

// record appends one observation from its fields.
func (o *observations) record(issued time.Time, latency time.Duration, err error) {
	o.add(observation{issued: issued, latency: latency, err: err})
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
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	idx := int(float64(len(latencies)) * q)
	if idx >= len(latencies) {
		idx = len(latencies) - 1
	}
	return latencies[idx]
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
	var sum time.Duration
	for _, ob := range s.list {
		if ob.err == nil {
			out.successes++
		} else {
			out.failures++
		}
		sum += ob.latency
	}
	out.meanLatency = sum / time.Duration(len(s.list))
	out.p50Latency = s.latencyQuantile(0.50)
	out.p99Latency = s.latencyQuantile(0.99)
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
	total       int
	successes   int
	failures    int
	meanLatency time.Duration
	p50Latency  time.Duration
	p99Latency  time.Duration
}
