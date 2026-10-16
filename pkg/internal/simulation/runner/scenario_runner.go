package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runScenario runs warmup + observed phases against a fresh environment and
// returns the collected result.
//
// Warmup runs every agent at healthy latency (regardless of the scenario) so
// wgo's stats tracker qualifies a baseline before the scenario behaviours are
// swapped in; its observations are discarded. The observed phase then drives the
// comparison.
func runScenario(ctx context.Context, sc scenario, factory WorkloadFactory) (scenarioResult, error) {
	env, err := newScenarioEnvironment(sc.healthyBehaviours(), factory, sc)
	if err != nil {
		return scenarioResult{}, err
	}
	defer env.Close()

	budget := warmupDuration + warmupCoverageTimeout + sc.duration() + 60*time.Second
	runCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	// Preparing payloads can be expensive; keep that pause before warmup so
	// observed traffic starts with freshly collected health statistics.
	wgoEvents := eventsForClient(env.wgoClient, env.topic, env.numPartitions, sc.duration(), sc.spacing())
	kgoEvents := eventsForClient(env.kgoClient, env.topic, env.numPartitions, sc.duration(), sc.spacing())

	if err := runScenarioWarmupWithSpacing(runCtx, env, sc.spacing()); err != nil {
		return scenarioResult{}, fmt.Errorf("scenario %q warmup: %w", sc.name, err)
	}
	if err := validateWorkloads(env); err != nil {
		return scenarioResult{}, fmt.Errorf("scenario %q warmup: %w", sc.name, err)
	}

	// Sample the Hedger's primary/hedge counters at exact bucket boundaries,
	// anchored at observedStart so the per-bucket deltas line up with the
	// per-event observation buckets in the report.
	observedStart := time.Now()
	metrics := prometheus.Gatherer(env.wgoRegistry)
	if external, ok := env.wgoClient.(prometheus.Gatherer); ok {
		metrics = external
	}
	var initialFallback *staleFallbackEvidence
	if sc.requireStaleFallback {
		prefix := ""
		if builder, ok := env.wgoClient.(eventBuilder); ok {
			prefix = builder.MetricsPrefix()
		}
		initialFallback, err = readStaleFallbackEvidence(metrics, prefix, env.wgoMetadata)
		if err != nil {
			return scenarioResult{}, err
		}
	}
	sampler := startCounterSampler(observedStart, reportBucket, func() (int64, int64) {
		if builder, ok := env.wgoClient.(eventBuilder); ok {
			return gatherCounter(metrics, builder.MetricsPrefix()+"warpstream_produce_requests_primary_total"), gatherCounter(metrics, builder.MetricsPrefix()+"warpstream_produce_requests_hedge_total")
		}
		return readProduceCounters(env.wgoRegistry)
	})
	defer sampler.stop()
	if sc.expect.recoveryFrom != nil {
		env.wgoNetwork.mu.Lock()
		env.wgoNetwork.recoveryStart = observedStart.Add(*sc.expect.recoveryFrom)
		env.wgoNetwork.mu.Unlock()
	}
	env.behaviours.set(sc.behaviours)
	phaseCtx, stopPhases := context.WithCancel(runCtx)
	defer stopPhases()
	phaseDone := make(chan error, 1)
	go func() {
		err := runPhases(phaseCtx, env, observedStart, sc)
		if err != nil {
			cancel()
		}
		phaseDone <- err
	}()
	wgoObservations, kgoObservations := runScenarioObservedEvents(runCtx, env, observedStart, wgoEvents, kgoEvents)
	stopPhases()
	if err := <-phaseDone; err != nil {
		return scenarioResult{}, fmt.Errorf("scenario %q transport phases: %w", sc.name, err)
	}
	samples := sampler.stop()

	// A cancelled runCtx (parent timeout or a hung scenario) means the events
	// returned early and the observations are partial; gating on them would be a
	// false pass/fail, so fail loudly instead.
	if err := runCtx.Err(); err != nil {
		return scenarioResult{}, fmt.Errorf("scenario %q did not complete: %w", sc.name, err)
	}
	if err := validateWorkloads(env); err != nil {
		return scenarioResult{}, fmt.Errorf("scenario %q workload: %w", sc.name, err)
	}

	wgoSnapshot := wgoObservations.snapshot()
	kgoSnapshot := kgoObservations.snapshot()
	res := scenarioResult{
		wgoMissingLeaders: env.wgoMetadata.missingLeaders(), kgoMissingLeaders: env.kgoMetadata.missingLeaders(),
		wgoTransport: env.wgoNetwork.snapshot(), kgoTransport: env.kgoNetwork.snapshot(),
		sc:                   sc,
		wgoLatencySelections: env.behaviours.selectedLatencies(clientTypeWgo), kgoLatencySelections: env.behaviours.selectedLatencies(clientTypeKgo),
		windows:        summarizeWindows(sc.windows, observedStart, wgoSnapshot, kgoSnapshot),
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
	if sc.expect.slowBudget != nil {
		f := wgoSnapshot.slowFraction(*sc.expect.slowBudget)
		res.wgoSlowFraction = &f
	}
	if sc.expect.recoveryFrom != nil {
		recovery := wgoSnapshot.scheduledSince(observedStart.Add(*sc.expect.recoveryFrom))
		res.wgoRecovery = recovery.summary()
		if sc.expect.slowBudget != nil {
			res.wgoRecoverySlowFraction = recovery.slowFraction(*sc.expect.slowBudget)
		}
	}
	if profile, ok := env.wgoClient.(interface{ RecoveryResponseDescription() string }); ok {
		res.recoveryResponseDescription = profile.RecoveryResponseDescription()
	}
	if profile, ok := env.wgoClient.(interface{ Describe() (string, string) }); ok {
		res.workloadDescription, res.workloadConfiguration = profile.Describe()
	}
	if sc.requireStaleFallback {
		prefix := ""
		if builder, ok := env.wgoClient.(eventBuilder); ok {
			prefix = builder.MetricsPrefix()
		}
		res.staleFallback, err = readStaleFallbackEvidence(metrics, prefix, env.wgoMetadata)
		if err != nil {
			return scenarioResult{}, err
		}
		res.staleFallback.FallbackErrors -= initialFallback.FallbackErrors
		for reason, count := range initialFallback.FallbackErrorReasons {
			res.staleFallback.FallbackErrorReasons[reason] -= count
		}
		res.staleFallback.ThirdAttemptSuccesses -= initialFallback.ThirdAttemptSuccesses
		res.staleFallback.TerminalErrors -= initialFallback.TerminalErrors
	}
	return res, nil
}

func validateWorkloads(env *environment) error {
	for _, client := range []produceClient{env.wgoClient, env.kgoClient} {
		if checked, ok := client.(interface{ Validate() error }); ok {
			if err := checked.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

const warmupCoverageTimeout = 30 * time.Second

func runScenarioWarmupWithSpacing(ctx context.Context, env *environment, spacing time.Duration) error {
	// Each client gets its own records: kgo writes back to the *kgo.Record it
	// produces, so sharing one slice across the two concurrent clients would race.
	wgoEvents := eventsForClient(env.wgoClient, env.topic, env.numPartitions, warmupDuration, spacing)
	kgoEvents := eventsForClient(env.kgoClient, env.topic, env.numPartitions, warmupDuration, spacing)
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runScenarioEvents(ctx, env.wgoClient, wgoEvents, start) }()
	go func() { defer wg.Done(); runScenarioEvents(ctx, env.kgoClient, kgoEvents, start) }()
	wg.Wait()
	coverageCtx, cancel := context.WithTimeout(ctx, warmupCoverageTimeout)
	defer cancel()
	for at := warmupDuration; !warmupComplete(env); {
		if err := coverageCtx.Err(); err != nil {
			return fmt.Errorf("partition coverage incomplete: %w", err)
		}
		iterationStart := time.Now()
		next := iterationStart.Add(spacing)
		// Continue the series sequence; replaying the first batch cannot reach a
		// partition that its hashes missed.
		events := func(client produceClient) []scenarioEvent {
			if builder, ok := client.(eventBuilder); ok {
				return []scenarioEvent{{records: builder.EventRecords(env.topic, env.numPartitions, at)}}
			}
			return buildScenarioEvents(env.topic, env.numPartitions, spacing, spacing)
		}
		runScenarioObservedEvents(coverageCtx, env, time.Now(), events(env.wgoClient), events(env.kgoClient))
		if delay := time.Until(next); delay > 0 {
			select {
			case <-time.After(delay):
			case <-coverageCtx.Done():
				return fmt.Errorf("partition coverage incomplete: %w", coverageCtx.Err())
			}
		}
		// Slow HTTP completion must not make the payload sequence lag behind
		// elapsed warmup time and repeatedly hash the same sparse series.
		at += max(spacing, time.Since(iterationStart))
	}
	return ctx.Err()
}

func warmupComplete(env *environment) bool {
	for _, client := range []produceClient{env.wgoClient, env.kgoClient} {
		if checked, ok := client.(interface{ WarmupComplete() bool }); ok && !checked.WarmupComplete() {
			return false
		}
	}
	return true
}

func runScenarioObservedEvents(ctx context.Context, env *environment, start time.Time, wgoEvents, kgoEvents []scenarioEvent) (wgoObservations, kgoObservations *observations) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); wgoObservations = runScenarioEvents(ctx, env.wgoClient, wgoEvents, start) }()
	go func() { defer wg.Done(); kgoObservations = runScenarioEvents(ctx, env.kgoClient, kgoEvents, start) }()
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
		for p := range numPartitions {
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Value: []byte("event")})
		}
		events = append(events, scenarioEvent{at: at, records: recs})
	}
	return events
}

// runScenarioEvents fires every event on its schedule against one client, each in
// its own goroutine, and returns the recorded per-event observations.
func runScenarioEvents(ctx context.Context, client produceClient, events []scenarioEvent, start time.Time) *observations {
	obs := &observations{}
	var wg sync.WaitGroup
	for _, ev := range events {
		wg.Go(func() {
			runScenarioEvent(ctx, client, start, ev, obs)
		})
	}
	wg.Wait()
	return obs
}

// Kafka callbacks can outlive an application deadline. Record the caller's
// outcome independently, then drain every callback before returning.
func runScenarioEvent(ctx context.Context, client produceClient, start time.Time, event scenarioEvent, obs *observations) {
	scheduled := start.Add(event.at)
	if d := time.Until(scheduled); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return
		}
	}
	issued := time.Now()
	// Kafka callbacks may outlive cancellation; the caller's deadline still
	// determines the application outcome.
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
	done := make(chan struct{})
	go func() { innerWG.Wait(); close(done) }()
	select {
	case <-done:
		if eventCtx.Err() != nil {
			firstErr = eventCtx.Err()
		}
		obs.record(scheduled, issued, time.Since(issued), firstErr)
	case <-eventCtx.Done():
		obs.record(scheduled, issued, time.Since(issued), eventCtx.Err())
	}
	// Drain callbacks before cleanup so they cannot access a closed environment.
	<-done
}
