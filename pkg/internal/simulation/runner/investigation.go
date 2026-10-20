package runner

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

const secondaryTopicName = "wgo-simulation-secondary"

type workloadProfile struct {
	prometheus                         bool
	enforceProduceTimeout              bool
	brokers, partitions                int32
	singleAgent, dynamicPlacement      bool
	advertisedAgents                   int
	secondaryPartitions                int32
	seriesPerRequest                   int
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
	case "all":
		if !external {
			return nil, fmt.Errorf("all requires an external workload")
		}
		return append(append(scenarios(), investigationScenarios()...), storagePressureScenarios()...), nil
	case "mimir-prometheus":
		if !external {
			return nil, fmt.Errorf("mimir-prometheus requires an external workload")
		}
		return storagePressureScenarios(), nil
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
	profile := &workloadProfile{brokers: 3, partitions: 1001, secondaryPartitions: 60, seriesPerRequest: 200, singleAgent: true, spacing: 500 * time.Millisecond, writeTimeout: 4 * time.Second, dialTimeout: 500 * time.Millisecond}
	healthy := scenario{profile: profile}.healthyBehaviours()
	degraded := scenario{profile: profile}.healthyBehaviours()
	degraded.byBroker[0] = brokerBehaviour{latencyFn: constantLatency(3 * time.Second), injectedLatency: true}
	degraded.byBroker[1] = brokerBehaviour{latencyFn: constantLatency(9 * time.Second), injectedLatency: true}
	faultCase := scenario{
		name: "investigation: slow preferred agent", profile: profile, observedFor: 180 * time.Second, behaviours: healthy,
		description:     "Every partition prefers agent 0. At 10s it delays Produce responses for 3s, one fallback delays for 9s, and one fallback stays at 50ms. At 130s all agents recover. Two application events per second each send independent HTTP writes to both topics. An available healthy fallback should preserve successful writes and keep at least 95% within 2s; every recovery write must succeed within 2s.",
		expect:          scenarioExpectations{minWgoSuccessRate: 1, slowBudget: ptr(2 * time.Second), maxWgoSlowFraction: ptr(0.05), recoveryFrom: ptr(145 * time.Second)},
		windows:         []observationWindow{{"before fault", 0, 8 * time.Second}, {"degraded", 10 * time.Second, 130 * time.Second}, {"recovery", 145 * time.Second, 180 * time.Second}},
		behaviourPhases: []behaviourPhase{{10 * time.Second, degraded}, {130 * time.Second, healthy}},
	}
	control := scenario{name: "investigation: concurrent healthy", profile: profile, observedFor: 30 * time.Second, behaviours: healthy,
		description: "Three advertised agents serve a 1001-partition topic and a separate 60-partition topic. All partition leaders point to one preferred agent. Two application events per second each send 200-series remote-write requests to both unmodified Mimir processes. Every event must succeed.", expect: scenarioExpectations{minWgoSuccessRate: 1}}
	changing := faultCase
	changingProfile := *profile
	changingProfile.brokers = 4
	changingProfile.dynamicPlacement = true
	changingProfile.spacing = 50 * time.Millisecond
	changing.profile = &changingProfile
	changing.name = "investigation: changing preferred placement"
	changing.behaviours = changing.healthyBehaviours()
	changingDegraded := changing.healthyBehaviours()
	changingDegraded.byBroker[0] = brokerBehaviour{latencyFn: constantLatency(9 * time.Second), injectedLatency: true}
	for id := int32(1); id <= 2; id++ {
		changingDegraded.byBroker[id] = brokerBehaviour{latencyFn: constantLatency(3 * time.Second), injectedLatency: true}
	}
	changing.behaviourPhases = []behaviourPhase{{10 * time.Second, changingDegraded}, {130 * time.Second, changing.healthyBehaviours()}}
	changing.description = "Four physical agents provide three-agent discovery views. Each Metadata response assigns all partitions to its responding agent and advertises two fallbacks, always retaining the healthy agent. Under twenty application events per second, one agent stalls responses for 9s and two for 3s, leaving one healthy agent. All recover after 120s of impairment."
	intermittent := faultCase
	intermittentProfile := *profile
	intermittentProfile.spacing = 50 * time.Millisecond
	intermittent.profile = &intermittentProfile
	intermittent.name = "investigation: intermittent fallback stalls"
	intermittentDegraded := intermittent.healthyBehaviours()
	for id := int32(0); id < 3; id++ {
		base := 50 * time.Millisecond
		if id == 0 {
			base = 3 * time.Second
		}
		if id == 1 {
			base = 9 * time.Second
		}
		intermittentDegraded.byBroker[id] = brokerBehaviour{latencyFn: burstyLatency(constantLatency(base), 0.01, 9*time.Second), injectedLatency: true, minInjectedLatency: 50 * time.Millisecond}
	}
	intermittent.behaviourPhases = []behaviourPhase{{10 * time.Second, intermittentDegraded}, {130 * time.Second, intermittent.healthyBehaviours()}}
	intermittent.description = "Single-agent placement with a 3s preferred agent, a 9s fallback, and a normally 50ms fallback. Every agent independently encounters 1% additional 9s stalls during the fault window. Twenty application events per second exercise shared Kafka connections while both independent HTTP writers remain active."
	intermittent.expect.minWgoSuccessRate = 0.95
	intermittent.requiredWgoDelayBrokers = []int32{2}
	restart := faultCase
	restart.name = "investigation: stale fallback during rolling restart"
	restart.observedFor = 90 * time.Second
	restart.description = "Three agents start healthy with every partition preferring agent 0. From 10s to 50s, agent 0 rejects 10% of Produce requests with NotLeaderForPartition. Concurrent broker-only discovery omits fallback 1 while topic discovery still advertises it with a 2s response delay. Requests must continue past the stale fallback to healthy agent 2. Discovery converges at 50s; recovery from 70s must be complete. Real distributor metrics must show fallback wire errors, and both discovery response types must be observed."
	restarting := scenario{profile: profile}.healthyBehaviours()
	restarting.byBroker[0] = brokerBehaviour{failRate: 0.1, latencyFn: constantLatency(50 * time.Millisecond)}
	restarting.brokerOnlyMissing = []int32{1}
	restarting.topicMetadataDelay = 2 * time.Second
	restart.behaviourPhases = []behaviourPhase{{10 * time.Second, restarting}, {50 * time.Second, healthy}}
	restart.windows = []observationWindow{{"before fault", 0, 8 * time.Second}, {"degraded", 10 * time.Second, 50 * time.Second}, {"recovery", 70 * time.Second, 90 * time.Second}}
	restart.expect = scenarioExpectations{minWgoSuccessRate: 1, slowBudget: ptr(2 * time.Second), maxWgoSlowFraction: ptr(0.05), recoveryFrom: ptr(70 * time.Second)}
	restart.requireStaleFallback = true
	return []scenario{control, faultCase, changing, intermittent, restart}
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
