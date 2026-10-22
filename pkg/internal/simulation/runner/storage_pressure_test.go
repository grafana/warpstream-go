package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestStoragePressure_Discovery(t *testing.T) {
	sc := storagePressureScenarios()[1]
	template := kmsg.NewPtrMetadataResponse()
	for id := range int32(10) {
		template.Brokers = append(template.Brokers, kmsg.MetadataResponseBroker{NodeID: id})
	}
	template.Topics = []kmsg.MetadataResponseTopic{{Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0}}}}
	for preferred := range int32(10) {
		response := metadataPreferredPlacement(template, preferred, sc.profile.advertisedAgents, -1)
		require.Len(t, response.Brokers, 10)
		require.Equal(t, preferred, response.Topics[0].Partitions[0].Leader)
	}
	_, err := profileScenarios("mimir-prometheus", false)
	require.ErrorContains(t, err, "external workload")
	selected, err := profileScenarios("mimir-prometheus", true)
	require.NoError(t, err)
	require.Len(t, selected, 2)
}

func TestStoragePressure_FaultWindows(t *testing.T) {
	sc := storagePressureScenarios()[1]
	provider := newBrokersBehaviourProvider(sc.behaviourPhases[0].behaviours)
	for _, lane := range []clientType{clientTypeWgo, clientTypeKgo} {
		for id := range int32(10) {
			want := 50 * time.Millisecond
			if id == 0 {
				want = 6 * time.Second
			} else if id < 5 {
				want = 3 * time.Second
			}
			require.Equal(t, want, provider.nextLatencyFor(lane, id))
		}
	}
	require.True(t, sc.profile.enforceProduceTimeout)
	require.True(t, sc.collectClientEvidence)
	require.Equal(t, 4*time.Second, sc.profile.writeTimeout)
}
