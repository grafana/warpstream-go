package runner

import (
	"cmp"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
)

func TestMetadata_FailingWhileProduceKeepsWorking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var network kfake.VirtualNetwork
		cluster, addr := testkafka.CreateCluster(t, 3, topicName, testkafka.WithNumBrokers(3), testkafka.WithVirtualNetwork(&network))
		client, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.Dialer(network.DialContext), kgo.RequestRetries(0))
		require.NoError(t, err)
		t.Cleanup(client.Close)
		raw, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.NoError(t, err)

		behaviours := newBrokersBehaviourProvider(healthyBehaviours())
		view := installMetadataControl(cluster, raw.(*kmsg.MetadataResponse), behaviours)
		failing := healthyBehaviours()
		failing.metadataFailing = true
		behaviours.set(failing)
		_, err = client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.Error(t, err)
		assert.EqualValues(t, 1, view.failedMetadata())

		behaviours.set(healthyBehaviours())
		_, err = client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
		require.NoError(t, err)
		assert.EqualValues(t, 1, view.failedMetadata())
	})
}

func TestMetadataFailure_UnavailableScenario(t *testing.T) {
	t.Parallel()
	sc := metadataUnavailableScenario()
	require.True(t, sc.failsMetadata())
	require.Len(t, sc.behaviourPhases, 2)
	assert.True(t, sc.behaviourPhases[0].behaviours.metadataFailing)
	assert.False(t, sc.behaviourPhases[1].behaviours.metadataFailing)
	assert.Equal(t, metadataFailureFrom, sc.behaviourPhases[0].at)
	assert.Equal(t, metadataFailureUntil, sc.behaviourPhases[1].at)

	assert.True(t, slices.IsSortedFunc(sc.transportPhases, func(a, b transportPhase) int { return cmp.Compare(a.at, b.at) }))
	for _, phase := range sc.transportPhases {
		assert.Less(t, phase.nodeID, int32(clusterSize))
		switch phase.action {
		case withdrawPod:
			// Clients must see the agent leave before discovery fails, or they would not know it left.
			assert.Less(t, phase.at, metadataFailureFrom)
		case replacePod:
			// The new address can only be learned after Metadata recovers.
			assert.GreaterOrEqual(t, phase.at, metadataFailureFrom)
			assert.Less(t, phase.at, metadataFailureUntil)
		default:
			assert.Fail(t, "unexpected transport action", "%v", phase.action)
		}
	}
	assert.Len(t, sc.transportPhases, 2*len(metadataFailureAgents))
	assert.Less(t, float64(len(metadataFailureAgents))/float64(clusterSize), wgoHealthCheckMaxFaultyFraction)

	require.NotNil(t, sc.expect.recoveryFrom)
	assert.GreaterOrEqual(t, *sc.expect.recoveryFrom-metadataFailureUntil, 2*clientMetadataRefresh)
	assert.Greater(t, sc.duration(), *sc.expect.recoveryFrom)

	// A run in which no Metadata request failed did not exercise the scenario.
	res := scenarioResult{sc: sc}
	var found bool
	for _, failure := range checkResult(res) {
		found = found || failure.detail == "wgo: no Metadata request failed during the failure window"
	}
	assert.True(t, found)
}
