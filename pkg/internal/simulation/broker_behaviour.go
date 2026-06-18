package main

import (
	"context"
	"maps"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// clientType identifies which of the two clients under comparison a draw is for,
// so each gets its own per-broker random stream.
type clientType int

const (
	clientTypeWgo clientType = iota
	clientTypeKgo
)

// brokerBehaviour overrides per-broker Produce response behaviour.
type brokerBehaviour struct {
	// latencyFn draws an artificial response latency per request.
	latencyFn func(*rand.Rand) time.Duration

	// failRate is the per-request probability of failing with
	// NotLeaderForPartition; 1.0 forces every request to fail.
	failRate float64
}

// brokersBehaviour holds the Produce behaviour for every broker in the cluster,
// keyed by node ID.
type brokersBehaviour struct {
	byBroker map[int32]brokerBehaviour
}

// forBroker returns the behaviour for nodeID, and whether one is configured.
func (b brokersBehaviour) forBroker(nodeID int32) (brokerBehaviour, bool) {
	bh, ok := b.byBroker[nodeID]
	return bh, ok
}

// healthyBehaviours returns a baseline where every broker runs at production-like
// healthy latency. Scenarios mutate a copy.
func healthyBehaviours() brokersBehaviour {
	byBroker := map[int32]brokerBehaviour{}
	for i := int32(0); i < clusterSize; i++ {
		byBroker[i] = brokerBehaviour{latencyFn: normalLatency}
	}
	return brokersBehaviour{byBroker: byBroker}
}

// failingBehavioursFor returns healthy behaviours with the given brokers forced to
// fail every request.
func failingBehavioursFor(nodeIDs ...int32) brokersBehaviour {
	bh := healthyBehaviours()
	for _, id := range nodeIDs {
		bh.byBroker[id] = brokerBehaviour{failRate: 1.0}
	}
	return bh
}

// highLatencyBehavioursFor returns healthy behaviours with the given brokers set to
// high latency.
func highLatencyBehavioursFor(nodeIDs ...int32) brokersBehaviour {
	bh := healthyBehaviours()
	for _, id := range nodeIDs {
		bh.byBroker[id] = brokerBehaviour{latencyFn: highLatency}
	}
	return bh
}

// burstyLatencyBehaviours returns behaviours where every broker runs at normal
// latency with a burstRate chance of an extra burst per request.
func burstyLatencyBehaviours(burstRate float64, burst time.Duration) brokersBehaviour {
	bh := healthyBehaviours()
	for i := int32(0); i < clusterSize; i++ {
		bh.byBroker[i] = brokerBehaviour{latencyFn: burstyLatency(normalLatency, burstRate, burst)}
	}
	return bh
}

// rngKey identifies a per-client, per-broker random stream.
type rngKey struct {
	client clientType
	broker int32
}

// brokersBehaviourProvider holds the currently-active brokersBehaviour.
type brokersBehaviourProvider struct {
	current atomic.Pointer[brokersBehaviour]

	// One mutex guards the generator map and every draw: draws are infrequent
	// enough that a single lock is simpler than per-stream locking and not a
	// contention point.
	rngMu sync.Mutex
	rngs  map[rngKey]*rand.Rand
}

func newBrokersBehaviourProvider(initial brokersBehaviour) *brokersBehaviourProvider {
	p := &brokersBehaviourProvider{rngs: map[rngKey]*rand.Rand{}}
	p.set(initial)
	return p
}

// set swaps in a copy of b's map so a caller that keeps mutating its own map can't
// race the concurrent produce paths reading the live behaviour.
func (p *brokersBehaviourProvider) set(b brokersBehaviour) {
	p.current.Store(&brokersBehaviour{byBroker: maps.Clone(b.byBroker)})
}

func (p *brokersBehaviourProvider) get() brokersBehaviour {
	if b := p.current.Load(); b != nil {
		return *b
	}
	return brokersBehaviour{}
}

// nextLatencyFor returns the next simulated response latency for a broker as seen
// by client, or 0 when the broker has no latency configured. It only draws; the
// caller decides whether/how to sleep.
func (p *brokersBehaviourProvider) nextLatencyFor(client clientType, nodeID int32) time.Duration {
	b, ok := p.get().forBroker(nodeID)
	if !ok || b.latencyFn == nil {
		return 0
	}
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return b.latencyFn(p.rngFor(client, nodeID))
}

// nextLatencySleepFor draws client's next latency for a broker and sleeps for it,
// returning early when ctx is cancelled. A zero/absent latency returns immediately.
func (p *brokersBehaviourProvider) nextLatencySleepFor(ctx context.Context, client clientType, nodeID int32) {
	d := p.nextLatencyFor(client, nodeID)
	if d <= 0 {
		return
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// nextFailureFor reports whether a broker's next request should fail, as seen by
// client. Brokers with a zero failRate never fail and don't draw.
func (p *brokersBehaviourProvider) nextFailureFor(client clientType, nodeID int32) bool {
	b, ok := p.get().forBroker(nodeID)
	if !ok || b.failRate <= 0 {
		return false
	}
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rngFor(client, nodeID).Float64() < b.failRate
}

// rngFor returns client's random generator for a broker, creating it on first use.
// Seeded by (broker, client) for reproducibility and per-client independence.
// Callers must hold rngMu.
func (p *brokersBehaviourProvider) rngFor(client clientType, nodeID int32) *rand.Rand {
	key := rngKey{client, nodeID}
	r := p.rngs[key]
	if r == nil {
		r = rand.New(rand.NewPCG(uint64(nodeID), uint64(client)+1))
		p.rngs[key] = r
	}
	return r
}

// normalLatency models a Warpstream agent under normal load: avg ≈ 400 ms,
// min 100 ms, max 1 s.
func normalLatency(rng *rand.Rand) time.Duration {
	// mode=min squeezes mass toward the floor so the long tail tops out near 1s.
	return triangular(rng, 100*time.Millisecond, 100*time.Millisecond, time.Second)
}

// highLatency models a degraded agent: avg ≈ 1.5 s, max 3 s.
func highLatency(rng *rand.Rand) time.Duration {
	return triangular(rng, 500*time.Millisecond, time.Second, 3*time.Second)
}

// burstyLatency wraps base so each draw has burstRate probability of being
// extended by burst.
func burstyLatency(base func(*rand.Rand) time.Duration, burstRate float64, burst time.Duration) func(*rand.Rand) time.Duration {
	return func(rng *rand.Rand) time.Duration {
		d := base(rng)
		if rng.Float64() < burstRate {
			d += burst
		}
		return d
	}
}

// triangular draws a duration from a triangular distribution defined by
// (min, mode, max): clustered around the mode with a long right tail, a
// reasonable approximation of real Warpstream agent response times.
func triangular(rng *rand.Rand, min, mode, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	u := rng.Float64()
	span := float64(max - min)
	mid := float64(mode-min) / span
	var pick float64
	if u < mid {
		pick = float64(min) + math.Sqrt(u*span*float64(mode-min))
	} else {
		pick = float64(max) - math.Sqrt((1-u)*span*float64(max-mode))
	}
	return time.Duration(pick)
}
