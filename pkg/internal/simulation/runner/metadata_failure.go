package runner

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

const (
	metadataFailureWithdrawAt = 10 * time.Second
	metadataFailureFrom       = 25 * time.Second
	metadataFailureReplaceAt  = 35 * time.Second
	metadataFailureUntil      = 65 * time.Second

	// Two refresh intervals after Metadata returns, so both clients have learned the new addresses.
	metadataFailureRecoverFrom = metadataFailureUntil + 2*clientMetadataRefresh
)

// metadataFailureAgents are spread out so that none is another's first fallback;
// this scenario is about discovery, not paired failures.
var metadataFailureAgents = []int32{3, 14, 27, 41}

// metadataUnavailableScenario fails Metadata while agents are replaced. The
// failure starts after the clients have seen the agents leave, so the new
// addresses stay undiscoverable until Metadata recovers.
func metadataUnavailableScenario() scenario {
	var phases []transportPhase
	for _, id := range metadataFailureAgents {
		phases = append(phases, transportPhase{metadataFailureWithdrawAt, id, withdrawPod}, transportPhase{metadataFailureReplaceAt, id, replacePod})
	}
	// The runner applies phases in list order, so they must be sorted by time.
	slices.SortStableFunc(phases, func(a, b transportPhase) int { return cmp.Compare(a.at, b.at) })
	failing := healthyBehaviours()
	failing.metadataFailing = true
	return scenario{
		name:        "metadata unavailable while agents are replaced",
		observedFor: 100 * time.Second,
		description: fmt.Sprintf("%d of %d agents (%.0f%%) leave discovery at %s. Metadata requests fail from %s to %s while Produce keeps working, and the agents come back on new addresses at %s, which neither client can learn until Metadata recovers. "+
			"The partitions they led must be written through other agents in between. WarpStream's discovery can fail while produce works, but the payload of that error is not documented, so the harness fails the Metadata request and leaves each client with the view it had. Recovery is measured from %s.",
			len(metadataFailureAgents), clusterSize, 100*float64(len(metadataFailureAgents))/float64(clusterSize), metadataFailureWithdrawAt, metadataFailureFrom, metadataFailureUntil, metadataFailureReplaceAt, metadataFailureRecoverFrom),
		behaviours:      healthyBehaviours(),
		transportPhases: phases,
		behaviourPhases: []behaviourPhase{{metadataFailureFrom, failing}, {metadataFailureUntil, healthyBehaviours()}},
		expect:          scenarioExpectations{minWgoSuccessRate: 0.5, recoveryFrom: ptr(metadataFailureRecoverFrom), minSuccessDeltaVsKgo: ptr(0.2)},

		collectClientEvidence: true,
	}
}
