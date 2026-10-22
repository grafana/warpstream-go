package runner

import (
	"context"
	"sync"
	"time"
)

func observeWorkloads(ctx context.Context, env *environment, start time.Time, duration time.Duration, wgoEvents, kgoEvents []scenarioEvent) (*observations, *observations, error) {
	clients := []produceClient{env.wgoClient, env.kgoClient}
	out := []*observations{{}, {}}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, client := range clients {
		wg.Go(func() {
			if external, ok := client.(ObservedWorkload); ok {
				var attempts []ApplicationObservation
				attempts, errs[i] = external.Observe(ctx, start, duration)
				for _, attempt := range attempts {
					out[i].record(attempt.Started, attempt.Started, attempt.Duration, attempt.Error)
				}
				return
			}
			events := wgoEvents
			if i == 1 {
				events = kgoEvents
			}
			out[i] = runScenarioEvents(ctx, client, events, start)
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, err
		}
	}
	return out[0], out[1], nil
}
