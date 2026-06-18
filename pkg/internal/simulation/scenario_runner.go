package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// runScenario runs warmup + observed phases against a fresh environment and
// returns the collected result.
//
// Warmup runs every agent at healthy latency (regardless of the scenario) so
// wgo's stats tracker qualifies a baseline before the scenario behaviours are
// swapped in; its observations are discarded. The observed phase then drives the
// comparison.
func runScenario(ctx context.Context, sc scenario) (scenarioResult, error) {
	env, err := newEnvironment(healthyBehaviours())
	if err != nil {
		return scenarioResult{}, err
	}
	defer env.Close()

	budget := warmupDuration + observedDuration + 60*time.Second
	runCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	runScenarioWarmup(runCtx, env)

	// Sample the Hedger's primary/hedge counters at exact bucket boundaries,
	// anchored at observedStart so the per-bucket deltas line up with the
	// per-event observation buckets in the report.
	observedStart := time.Now()
	sampler := startCounterSampler(observedStart, reportBucket, func() (int64, int64) {
		return readProduceCounters(env.wgoRegistry)
	})
	wgoObservations, kgoObservations := runScenarioObserved(runCtx, env, sc.behaviours)
	samples := sampler.stop()

	// A cancelled runCtx (parent timeout or a hung scenario) means the events
	// returned early and the observations are partial; gating on them would be a
	// false pass/fail, so fail loudly instead.
	if err := runCtx.Err(); err != nil {
		return scenarioResult{}, fmt.Errorf("scenario %q did not complete: %w", sc.name, err)
	}

	wgoSnapshot := wgoObservations.snapshot()
	kgoSnapshot := kgoObservations.snapshot()
	res := scenarioResult{
		sc:             sc,
		wgoSummary:     wgoSnapshot.summary(),
		kgoSummary:     kgoSnapshot.summary(),
		wgoBuckets:     wgoSnapshot.bucketedSummary(reportBucket),
		kgoBuckets:     kgoSnapshot.bucketedSummary(reportBucket),
		produceDeltas:  bucketDeltas(samples),
		wgoErrorCounts: wgoSnapshot.errorCounts(),
		kgoErrorCounts: kgoSnapshot.errorCounts(),
	}
	if len(samples) >= 2 {
		first, last := samples[0], samples[len(samples)-1]
		res.totalPrimary = last.primary - first.primary
		res.totalHedge = last.hedge - first.hedge
	}
	return res, nil
}

func runScenarioWarmup(ctx context.Context, env *environment) {
	// Each client gets its own records: kgo writes back to the *kgo.Record it
	// produces, so sharing one slice across the two concurrent clients would race.
	wgoEvents := buildScenarioEvents(env.topic, env.numPartitions, warmupDuration, eventSpacing)
	kgoEvents := buildScenarioEvents(env.topic, env.numPartitions, warmupDuration, eventSpacing)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runScenarioEvents(ctx, env.wgoClient, wgoEvents) }()
	go func() { defer wg.Done(); runScenarioEvents(ctx, env.kgoClient, kgoEvents) }()
	wg.Wait()
}

func runScenarioObserved(ctx context.Context, env *environment, behaviours brokersBehaviour) (wgoObservations, kgoObservations *observations) {
	env.behaviours.set(behaviours)
	// Each client gets its own records: kgo writes back to the *kgo.Record it
	// produces, so sharing one slice across the two concurrent clients would race.
	wgoEvents := buildScenarioEvents(env.topic, env.numPartitions, observedDuration, eventSpacing)
	kgoEvents := buildScenarioEvents(env.topic, env.numPartitions, observedDuration, eventSpacing)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); wgoObservations = runScenarioEvents(ctx, env.wgoClient, wgoEvents) }()
	go func() { defer wg.Done(); kgoObservations = runScenarioEvents(ctx, env.kgoClient, kgoEvents) }()
	wg.Wait()
	return wgoObservations, kgoObservations
}

// scenarioEvent is one application request fired at scenarioEvent.at, relative to
// the start passed to runScenarioEvents. records is the per-partition fan-out for
// that request.
type scenarioEvent struct {
	at      time.Duration
	records []*kgo.Record
}

// buildScenarioEvents produces one scenarioEvent every spacing for the given
// duration. Each event carries one record per partition so the application request
// fans out to every partition simultaneously.
func buildScenarioEvents(topic string, numPartitions int32, duration, spacing time.Duration) []scenarioEvent {
	var events []scenarioEvent
	for at := time.Duration(0); at < duration; at += spacing {
		recs := make([]*kgo.Record, 0, numPartitions)
		for p := int32(0); p < numPartitions; p++ {
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Value: []byte("event")})
		}
		events = append(events, scenarioEvent{at: at, records: recs})
	}
	return events
}

// runScenarioEvents fires every event on its schedule against one client, each in
// its own goroutine, and returns the recorded per-event observations.
func runScenarioEvents(ctx context.Context, client produceClient, events []scenarioEvent) *observations {
	obs := &observations{}
	start := time.Now()
	var wg sync.WaitGroup
	for _, ev := range events {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runScenarioEvent(ctx, client, start, ev, obs)
		}()
	}
	wg.Wait()
	return obs
}

// runScenarioEvent waits until the event's scheduled time, produces its records
// concurrently, and records the application-request outcome into obs: the latency
// from issue to the slowest partition's promise, and the first non-nil error.
func runScenarioEvent(ctx context.Context, client produceClient, start time.Time, event scenarioEvent, obs *observations) {
	if d := time.Until(start.Add(event.at)); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return
		}
	}
	issued := time.Now()
	// Per-event ctx caps the application request: if the slowest partition hasn't
	// acked within appRequestTimeout the in-flight Produce calls observe ctx
	// cancellation and fire with ctx.Err.
	eventCtx, cancel := context.WithTimeout(ctx, appRequestTimeout)
	defer cancel()

	var (
		innerWG  sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, rec := range event.records {
		innerWG.Add(1)
		client.Produce(eventCtx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
			innerWG.Done()
		})
	}
	innerWG.Wait()
	obs.record(issued, time.Since(issued), firstErr)
}
