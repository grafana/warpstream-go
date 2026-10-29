package runner

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type transportAction int

const (
	killPod transportAction = iota
	blackholePod
	restorePod
	removePod
	replacePod
	withdrawPod
)

type transportPhase struct {
	at     time.Duration
	nodeID int32
	action transportAction
}

func runTransportPhases(ctx context.Context, env *environment, start time.Time, phases []transportPhase) error {
	for _, phase := range phases {
		if err := ctx.Err(); err != nil {
			return err
		}
		if delay := time.Until(start.Add(phase.at)); delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := env.applyTransportPhase(phase); err != nil {
			return err
		}
	}
	return nil
}

func (env *environment) applyTransportPhase(phase transportPhase) error {
	for _, target := range []struct {
		cluster  *kfake.Cluster
		network  *transportNetwork
		metadata *metadataView
	}{
		{env.wgoCluster, env.wgoNetwork, env.wgoMetadata}, {env.kgoCluster, env.kgoNetwork, env.kgoMetadata},
	} {
		switch phase.action {
		case killPod:
			target.network.setFault(phase.nodeID, transportKilled)
		case blackholePod:
			target.network.setFault(phase.nodeID, transportBlackhole)
		case restorePod:
			target.network.setFault(phase.nodeID, transportHealthy)
		case removePod, withdrawPod:
			target.network.setFault(phase.nodeID, transportKilled)
			if err := target.cluster.RemoveNode(phase.nodeID); err != nil {
				return fmt.Errorf("remove broker %d: %w", phase.nodeID, err)
			}
			if phase.action == withdrawPod {
				target.metadata.withdrawBroker(phase.nodeID)
			} else {
				target.metadata.updateBroker(phase.nodeID, nil)
			}
		case replacePod:
			_, port, err := target.cluster.AddNode(phase.nodeID, 0)
			if err != nil {
				return fmt.Errorf("replace broker %d: %w", phase.nodeID, err)
			}
			target.network.register(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), phase.nodeID)
			target.network.setFault(phase.nodeID, transportHealthy)
			target.metadata.updateBroker(phase.nodeID, &kmsg.MetadataResponseBroker{NodeID: phase.nodeID, Host: "127.0.0.1", Port: int32(port)})
		}
	}
	return nil
}

// restartPair is a leaving partition leader and the agent that is that
// partition's first fallback in wgo's fixed hash walk over the pool. The fallback
// is still advertised by topic discovery but unknown to the client's broker map,
// so a cascade that reaches it fails with an unknown-broker error.
type restartPair struct {
	leader, fallback int32
	leaderReplaced   bool
}

// restartWaves lists the pairs that leave together. A random choice of leaving
// agents rarely pairs a leader with its first fallback, and the client's health
// tracking steers around agents it has already seen fail, so new pairs have to keep
// arriving for the cascade to keep meeting an agent it has not learned about.
var restartWaves = [][]restartPair{
	{{15, 37, false}, {7, 18, true}},
	{{39, 34, false}, {1, 33, true}},
	{{8, 20, false}, {48, 6, true}},
	{{16, 38, false}, {47, 49, true}},
	{{9, 17, false}, {45, 27, true}},
}

const (
	restartFirstWave   = 5 * time.Second
	restartWaveSpacing = 10 * time.Second
	restartWaveLength  = 20 * time.Second
)

// concurrentRestartScenario rolls restartWaves through the cluster so that two
// waves, 8 of 50 agents (16%), are away at once, in the ways a terminating pod
// looks to a client: refusing connections while still advertised, refusing them
// while only some discovery responses list it, and leaving discovery before
// returning on a new address.
func concurrentRestartScenario() scenario {
	var phases []transportPhase
	var boundaries []time.Duration
	for wave, pairs := range restartWaves {
		leave := restartFirstWave + time.Duration(wave)*restartWaveSpacing
		rejoin := leave + restartWaveLength
		boundaries = append(boundaries, leave, rejoin)
		for _, pair := range pairs {
			if pair.leaderReplaced {
				phases = append(phases, transportPhase{leave, pair.leader, withdrawPod}, transportPhase{rejoin, pair.leader, replacePod})
			} else {
				phases = append(phases, transportPhase{leave, pair.leader, killPod}, transportPhase{rejoin, pair.leader, restorePod})
			}
			phases = append(phases, transportPhase{leave, pair.fallback, killPod}, transportPhase{rejoin, pair.fallback, restorePod})
		}
	}
	slices.SortStableFunc(phases, func(a, b transportPhase) int { return cmp.Compare(a.at, b.at) })
	slices.Sort(boundaries)
	boundaries = slices.Compact(boundaries)
	var behaviourPhases []behaviourPhase
	for _, at := range boundaries {
		skewed := healthyBehaviours()
		for wave, pairs := range restartWaves {
			leave := restartFirstWave + time.Duration(wave)*restartWaveSpacing
			if at >= leave && at < leave+restartWaveLength {
				for _, pair := range pairs {
					skewed.brokerOnlyMissing = append(skewed.brokerOnlyMissing, pair.fallback)
				}
			}
		}
		behaviourPhases = append(behaviourPhases, behaviourPhase{at, skewed})
	}
	awayAtOnce := int(restartWaveLength/restartWaveSpacing) * 2 * len(restartWaves[0])
	return scenario{
		name:        "concurrent restart",
		observedFor: 90 * time.Second,
		description: fmt.Sprintf("%d waves of agents leave, each for %s and starting %s after the last, so %d of %d agents (%.0f%%) are away at once. Each wave takes two partition leaders and, for each, the agent that is the partition's first fallback. "+
			"One leader refuses connections while still advertised and the other leaves discovery and returns on a new address; both fallbacks refuse connections while broker-only discovery omits them but topic discovery still lists them. "+
			"A write whose leader and first fallback are both gone must continue to a third agent. The evidence section counts failed flushes by stop reason; a cascade that stops at a terminal error with candidates left fails the scenario.",
			len(restartWaves), restartWaveLength, restartWaveSpacing, awayAtOnce, clusterSize, 100*float64(awayAtOnce)/float64(clusterSize)),
		behaviours:      healthyBehaviours(),
		transportPhases: phases,
		behaviourPhases: behaviourPhases,
		expect:          scenarioExpectations{minWgoSuccessRate: 0.5, recoveryFrom: ptr(80 * time.Second), minSuccessDeltaVsKgo: ptr(0.2)},

		requireCascadeContinuation: true,
		collectClientEvidence:      true,
	}
}

func transportScenarios() []scenario {
	expectation := scenarioExpectations{
		minWgoSuccessRate: 1,
		slowBudget:        ptr(2 * time.Second), maxWgoSlowFraction: ptr(0.15),
		recoveryFrom: ptr(50 * time.Second), maxHedgeSurge: ptr(0.25),
	}
	return []scenario{
		concurrentRestartScenario(),
		metadataUnavailableScenario(),
		{
			name:        "pod killed during writes",
			description: "At 10s broker 1 closes every established connection and refuses new dials. Metadata continues advertising it. At 30s the broker accepts connections again. Writes must reroute within budget and recover by 50s.",
			behaviours:  healthyBehaviours(), expect: expectation,
			transportPhases: []transportPhase{{10 * time.Second, 1, killPod}, {30 * time.Second, 1, restorePod}},
		},
		{
			name:        "network blackhole",
			description: "From 10s to 30s broker 1 drops request and response bytes while keeping connections open. This exercises actual socket deadlines and hedging. Clearing the fault forces reconnection; recovery is required by 50s.",
			behaviours:  healthyBehaviours(), expect: expectation,
			transportPhases: []transportPhase{{10 * time.Second, 1, blackholePod}, {30 * time.Second, 1, restorePod}},
		},
		{
			name:        "rolling restart",
			observedFor: 90 * time.Second,
			description: "Roll brokers 1, 2 and 3 sequentially. Each pod closes connections and leaves discovery for 20s while partition assignments still name it. " +
				"Each withdrawal spans two metadata refresh periods; both backends must be served Metadata naming the missing leader and every wgo write must succeed within 2s. " +
				"Replacement pods use new addresses; assignments converge when they return. Recovery from 80s must receive successful Produce responses from all three replacements, including probes.",
			behaviours: healthyBehaviours(), expect: scenarioExpectations{minWgoSuccessRate: 1, slowBudget: ptr(2 * time.Second), maxWgoSlowFraction: ptr(0.0), recoveryFrom: ptr(80 * time.Second), maxHedgeSurge: ptr(0.05)},
			transportPhases: []transportPhase{
				{5 * time.Second, 1, withdrawPod}, {25 * time.Second, 1, replacePod},
				{27 * time.Second, 2, withdrawPod}, {47 * time.Second, 2, replacePod},
				{49 * time.Second, 3, withdrawPod}, {69 * time.Second, 3, replacePod},
			},
		},
	}
}
