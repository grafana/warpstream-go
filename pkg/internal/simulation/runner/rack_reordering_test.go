package runner

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
	"github.com/grafana/warpstream-go/pkg/wgo"
)

func TestMetadata_RackChangeWithDelayedOldReply(t *testing.T) {
	for _, changeRack := range []bool{false, true} {
		sc := seedMetadataScenario(changeRack)
		t.Run(sc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				initial := sc.healthyBehaviours()
				behaviours := newBrokersBehaviourProvider(initial)
				env := &environment{topic: topicName, numPartitions: 3, behaviours: behaviours}
				for lane := range 2 {
					var network kfake.VirtualNetwork
					cluster, addr := testkafka.CreateCluster(t, 3, topicName, testkafka.WithNumBrokers(3), testkafka.WithVirtualNetwork(&network))
					source := &metadataRequestSource{}
					installFailureControl(cluster, behaviours, clientType(lane))
					var client *kgo.Client
					if lane == 0 {
						producer, registry, err := newWarpstreamClient(addr, wgo.WithDialer(network.DialContext), wgo.WithHooks(source))
						require.NoError(t, err)
						t.Cleanup(producer.Close)
						env.wgoClient, env.wgoRegistry, env.wgoMetadataSource = producer, registry, source
						client = producer.Client
					} else {
						producer, err := newKgoClient(addr, kgo.Dialer(network.DialContext), kgo.WithHooks(source))
						require.NoError(t, err)
						t.Cleanup(producer.Close)
						env.kgoClient = &latencyDelayedKgoClient{Client: producer, behaviours: behaviours, client: clientTypeKgo}
						env.kgoMetadataSource = source
						client = producer
					}
					raw, err := client.Request(t.Context(), kmsg.NewPtrMetadataRequest())
					require.NoError(t, err)
					view := installMetadataControl(cluster, raw.(*kmsg.MetadataResponse), behaviours)
					if lane == 0 {
						env.wgoMetadata = view
					} else {
						env.kgoMetadata = view
					}
				}
				pending, err := env.prepareSeedMetadata(t.Context())
				require.NoError(t, err)
				t.Cleanup(pending.release)
				env.behaviours.set(sc.behaviours)
				start := time.Now()
				scheduled := make(chan error, 1)
				go func() { scheduled <- pending.run(t.Context(), start) }()
				events := func() []scenarioEvent { return buildScenarioEvents(topicName, 3, time.Nanosecond, eventSpacing) }
				wgoObservations, kgoObservations := runScenarioObservedEvents(t.Context(), env, start, events(), events())
				require.NoError(t, <-scheduled)
				// Drain requests abandoned on replaced connections before closing kfake.
				time.Sleep(3 * time.Second)
				synctest.Wait()
				snapshot := wgoObservations.snapshot()
				assert.Equal(t, 1, snapshot.summary().successes)
				assert.Empty(t, snapshot.errorCounts())
				evidence, err := readClientEvidence(env.wgoRegistry, "")
				require.NoError(t, err)
				assert.Zero(t, evidence.WriteFailures["candidates_exhausted_budget_cap"])
				if changeRack {
					assert.Positive(t, evidence.DirectFailures["transport"])
				} else {
					assert.Empty(t, evidence.DirectFailures)
					assert.Equal(t, 1, kgoObservations.snapshot().summary().successes)
				}
			})
		})
	}
}

func TestScenarios_SeedMetadataRequiresDirectWorkload(t *testing.T) {
	t.Parallel()
	direct, err := profileScenarios("standard", false)
	require.NoError(t, err)
	external, err := profileScenarios("all", true)
	require.NoError(t, err)
	var directCases int
	for _, sc := range direct {
		if sc.reorderSeedMetadata {
			directCases++
		}
	}
	assert.Equal(t, 2, directCases)
	for _, sc := range external {
		assert.False(t, sc.reorderSeedMetadata)
	}
}
