package runner

import "time"

func storagePressureScenarios() []scenario {
	profile := &workloadProfile{
		prometheus: true, brokers: 10, advertisedAgents: 10, enforceProduceTimeout: true, partitions: 1001, secondaryPartitions: 60,
		singleAgent: true, dynamicPlacement: true, seriesPerRequest: 200,
		spacing: 70 * time.Millisecond, writeTimeout: 4 * time.Second, dialTimeout: 500 * time.Millisecond,
	}
	healthy := scenario{profile: profile}.healthyBehaviours()
	control := scenario{
		name: "prometheus: healthy control", profile: profile, behaviours: healthy, observedFor: 30 * time.Second,
		description:           "Ten agents advertise all peers and assign partitions to the responding agent. Two actual Prometheus processes each scrape 32,000 fixed series every second and remote-write to independent Mimir distributors using 1001- and 60-partition topics and a 4s Kafka write budget. Real Prometheus senders scrape a fixed metric source and remote-write through their WAL and queues. Reports count actual HTTP attempts, including retries.",
		expect:                scenarioExpectations{minWgoSuccessRate: 1},
		collectClientEvidence: true,
	}
	degraded := scenario{profile: profile}.healthyBehaviours()
	degraded.byBroker[0] = brokerBehaviour{latencyFn: constantLatency(6 * time.Second), injectedLatency: true}
	for id := int32(1); id < 5; id++ {
		degraded.byBroker[id] = brokerBehaviour{latencyFn: constantLatency(3 * time.Second), injectedLatency: true}
	}
	fault := control
	fault.name = "prometheus: sustained partial outage"
	fault.observedFor = 180 * time.Second
	fault.description = "The healthy control workload continues while one agent delays every Produce response for 6s and four delay every response for 3s, from 10s through 130s. Five agents remain healthy. Agent work exceeding the request’s wire timeout returns REQUEST_TIMED_OUT at that timeout. Mimir sends a 2s broker timeout through wgo and 4s through franz-go. Delays overlap across requests but preserve TCP response order. This models sustained agent acknowledgement stalls, not independent per-request latency draws or an object-storage packet emulator. The 4s write budget leaves little time to recover after a slow primary. Availability and latency targets require the client to route around the impairment."
	fault.behaviourPhases = []behaviourPhase{{10 * time.Second, degraded}, {130 * time.Second, healthy}}
	fault.windows = []observationWindow{{"before fault", 0, 8 * time.Second}, {"degraded", 10 * time.Second, 130 * time.Second}, {"recovery", 145 * time.Second, 180 * time.Second}}
	fault.expect = scenarioExpectations{minWgoSuccessRate: 1, slowBudget: ptr(2 * time.Second), maxWgoSlowFraction: ptr(0.05), maxSenderLag: ptr(5 * time.Second), recoveryFrom: ptr(145 * time.Second)}
	return []scenario{control, fault}
}
