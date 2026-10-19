package main

import (
	"context"
	"fmt"
	"net"
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

func transportScenarios() []scenario {
	expectation := scenarioExpectations{
		minWgoSuccessRate: 1,
		slowBudget:        ptr(2 * time.Second), maxWgoSlowFraction: ptr(0.15),
		recoveryFrom: ptr(50 * time.Second), maxHedgeSurge: ptr(0.25),
	}
	return []scenario{
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
