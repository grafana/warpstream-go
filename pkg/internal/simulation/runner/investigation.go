package runner

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

const secondaryTopicName = "wgo-simulation-secondary"

type workloadProfile struct {
	brokers, partitions                int32
	spacing, writeTimeout, dialTimeout time.Duration
}

type behaviourPhase struct {
	at         time.Duration
	behaviours brokersBehaviour
}

func (sc scenario) topology() (int32, int32) {
	if sc.profile != nil {
		return sc.profile.brokers, sc.profile.partitions
	}
	return clusterSize, clusterSize
}

func (sc scenario) spacing() time.Duration {
	if sc.profile != nil {
		return sc.profile.spacing
	}
	return eventSpacing
}

func (sc scenario) healthyBehaviours() brokersBehaviour {
	if sc.profile == nil {
		return healthyBehaviours()
	}
	brokers, _ := sc.topology()
	bh := brokersBehaviour{byBroker: make(map[int32]brokerBehaviour, brokers)}
	for id := range brokers {
		bh.byBroker[id] = brokerBehaviour{latencyFn: constantLatency(50 * time.Millisecond)}
	}
	return bh
}

func constantLatency(delay time.Duration) func(*rand.Rand) time.Duration {
	return func(*rand.Rand) time.Duration { return delay }
}

func profileScenarios(profile string, external bool) ([]scenario, error) {
	switch profile {
	case "standard":
		return scenarios(), nil
	case "mimir-investigation":
		if !external {
			return nil, fmt.Errorf("mimir-investigation requires an external workload")
		}
		return investigationScenarios(), nil
	default:
		return nil, fmt.Errorf("unknown scenario profile %q", profile)
	}
}

func investigationScenarios() []scenario {
	profile := &workloadProfile{brokers: 10, partitions: 50, spacing: 50 * time.Millisecond, writeTimeout: 4 * time.Second, dialTimeout: 500 * time.Millisecond}
	healthy := scenario{profile: profile}.healthyBehaviours()
	degraded := scenario{profile: profile}.healthyBehaviours()
	degraded.byBroker[0] = brokerBehaviour{latencyFn: constantLatency(9 * time.Second), injectedLatency: true}
	for id := int32(1); id <= 4; id++ {
		degraded.byBroker[id] = brokerBehaviour{latencyFn: constantLatency(3 * time.Second), injectedLatency: true}
	}
	return []scenario{
		{name: "investigation: concurrent healthy", profile: profile, observedFor: 30 * time.Second, behaviours: healthy,
			description: "Ten agents serve fifty partitions at 50ms response latency. Twenty application events per second exercise concurrent independent writers and topic metadata. Every event must succeed.", expect: scenarioExpectations{minWgoSuccessRate: 1}},
		{name: "investigation: sustained stalls and recovery", profile: profile, observedFor: 180 * time.Second, behaviours: healthy,
			description:     "At 10s, one of ten agents stalls Produce responses for 9s and four stall for 3s; five remain healthy. At 130s every agent returns to 50ms latency. Application writes continue at twenty events per second through recovery. Degraded success rates are measured without assuming either client wins; every write scheduled from 145s must succeed.",
			expect:          scenarioExpectations{recoveryFrom: ptr(145 * time.Second)},
			windows:         []observationWindow{{"before fault", 0, 10 * time.Second}, {"degraded", 10 * time.Second, 130 * time.Second}, {"recovery", 145 * time.Second, 180 * time.Second}},
			behaviourPhases: []behaviourPhase{{10 * time.Second, degraded}, {130 * time.Second, healthy}}},
	}
}

func runPhases(ctx context.Context, env *environment, start time.Time, sc scenario) error {
	if len(sc.behaviourPhases) == 0 {
		return runTransportPhases(ctx, env, start, sc.transportPhases)
	}
	if len(sc.transportPhases) != 0 {
		return fmt.Errorf("mixed transport and behaviour phases are unsupported")
	}
	var phaseErr error
	for _, phase := range sc.behaviourPhases {
		if err := ctx.Err(); err != nil {
			return err
		}
		if wait := time.Until(start.Add(phase.at)); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				phaseErr = ctx.Err()
			}
		}
		if phaseErr != nil {
			break
		}
		env.behaviours.set(phase.behaviours)
	}
	return phaseErr
}
