package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// counterSample is one snapshot of (primary, hedge) Produce request counts at a
// known wall-clock time.
type counterSample struct {
	at      time.Time
	primary int64
	hedge   int64
}

// counterSampler captures (primary, hedge) snapshots at exact bucket boundaries.
// Per-bucket deltas are computed from consecutive snapshots so the boundaries are
// aligned regardless of ticker jitter. The samples always include:
//
//   - t = start (initial sample, before the observed phase begins)
//   - t = start + k*bucketSize, for k = 1, 2, … until stop is called
//   - t = stop time (a final snapshot at whatever wall-clock stop runs at)
//
// The "exact at boundary" guarantee comes from sleeping to absolute target times
// instead of using a periodic ticker.
type counterSampler struct {
	bucketSize time.Duration
	read       func() (primary, hedge int64)

	mu            sync.Mutex
	start         time.Time
	liveSnapshots []counterSample

	cancel context.CancelFunc
	wg     sync.WaitGroup

	stopOnce         sync.Once
	stoppedSnapshots []counterSample
}

// startCounterSampler takes the initial sample synchronously at start and then
// schedules background samples at start + N*bucketSize.
func startCounterSampler(start time.Time, bucketSize time.Duration, read func() (primary, hedge int64)) *counterSampler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &counterSampler{
		bucketSize: bucketSize,
		read:       read,
		start:      start,
		cancel:     cancel,
	}
	s.record(start)
	s.wg.Add(1)
	go s.run(ctx)
	return s
}

func (s *counterSampler) record(at time.Time) {
	p, h := s.read()
	s.mu.Lock()
	s.liveSnapshots = append(s.liveSnapshots, counterSample{at: at, primary: p, hedge: h})
	s.mu.Unlock()
}

func (s *counterSampler) run(ctx context.Context) {
	defer s.wg.Done()
	for n := int64(1); ; n++ {
		target := s.start.Add(time.Duration(n) * s.bucketSize)
		wait := time.Until(target)
		if wait <= 0 {
			// Target already passed (slow scheduler). Record immediately to keep
			// the bucket sequence dense, then continue to the next target.
			s.record(target)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		s.record(target)
	}
}

// stop cancels the background goroutine and records one final snapshot at the
// current wall-clock time. Idempotent: the first call captures the defensive copy;
// later calls return that same copy without recording again.
func (s *counterSampler) stop() []counterSample {
	s.stopOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.record(time.Now())
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stoppedSnapshots = make([]counterSample, len(s.liveSnapshots))
		copy(s.stoppedSnapshots, s.liveSnapshots)
	})
	return s.stoppedSnapshots
}

// counterBucketDelta is one bucket's primary + hedge wire-request totals.
type counterBucketDelta struct {
	primary, hedge int64
}

// bucketDeltas turns the consecutive (sample[i], sample[i+1]) pairs into
// per-bucket primary/hedge deltas. The final pair (anchored at stop time rather
// than a bucket boundary) is included so trailing wire requests during scenario
// drain are accounted for.
func bucketDeltas(samples []counterSample) []counterBucketDelta {
	if len(samples) < 2 {
		return nil
	}
	out := make([]counterBucketDelta, 0, len(samples)-1)
	for i := 1; i < len(samples); i++ {
		out = append(out, counterBucketDelta{
			primary: samples[i].primary - samples[i-1].primary,
			hedge:   samples[i].hedge - samples[i-1].hedge,
		})
	}
	return out
}

// readProduceCounters returns the (primary, hedge) Produce wire-request counts
// from a wgo registry. Lookup is by metric name, which is the public Prometheus
// contract.
func readProduceCounters(reg *prometheus.Registry) (primary, hedge int64) {
	return gatherCounter(reg, "produce_requests_primary_total"),
		gatherCounter(reg, "produce_requests_hedge_total")
}

// gatherCounter returns the value of the named counter from reg. It panics if the
// metric is not registered — that's a contract bug the simulation should fail
// fast on.
func gatherCounter(reg *prometheus.Registry, name string) int64 {
	mfs, err := reg.Gather()
	if err != nil {
		panic(fmt.Errorf("gatherCounter(%q): %w", name, err))
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return int64(sum)
	}
	panic(fmt.Errorf("gatherCounter: metric %q not registered", name))
}
