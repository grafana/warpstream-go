package runner

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

// wgoSimulationChains is wgo's own candidate list (leader, first fallback, second
// fallback) for each partition of the simulation's topic, with agents 0 to 49 and
// agent p leading partition p. pkg/wgo pins the same table, so a change to its
// fallback walk fails there and here; regenerate both from wgo's output.
var wgoSimulationChains = [][]int32{
	{0, 2, 3}, {1, 33, 34}, {2, 14, 15}, {3, 37, 38}, {4, 10, 11},
	{5, 30, 31}, {6, 5, 7}, {7, 18, 19}, {8, 20, 21}, {9, 17, 18},
	{10, 38, 39}, {11, 6, 7}, {12, 7, 8}, {13, 47, 48}, {14, 24, 25},
	{15, 37, 38}, {16, 38, 39}, {17, 48, 49}, {18, 28, 29}, {19, 1, 2},
	{20, 49, 0}, {21, 22, 23}, {22, 35, 36}, {23, 34, 35}, {24, 9, 10},
	{25, 32, 33}, {26, 27, 28}, {27, 37, 38}, {28, 49, 0}, {29, 47, 48},
	{30, 36, 37}, {31, 16, 17}, {32, 43, 44}, {33, 8, 9}, {34, 15, 16},
	{35, 8, 9}, {36, 24, 25}, {37, 42, 43}, {38, 11, 12}, {39, 34, 35},
	{40, 36, 37}, {41, 28, 29}, {42, 17, 18}, {43, 10, 11}, {44, 25, 26},
	{45, 27, 28}, {46, 19, 20}, {47, 49, 0}, {48, 6, 7}, {49, 18, 19},
}

func TestFallbackChain_MatchesWgo(t *testing.T) {
	t.Parallel()
	require.Len(t, wgoSimulationChains, int(clusterSize))
	require.Equal(t, candidateChainLength, len(wgoSimulationChains[0]))
	for partition, want := range wgoSimulationChains {
		assert.Equal(t, want, fallbackChain(topicName, int32(partition), clusterSize), "partition %d", partition)
	}
}

func TestStalledAgents_RulesHoldForEveryShareAndRunID(t *testing.T) {
	t.Parallel()
	for _, gate := range []struct {
		name string
		rule stalledRule
	}{{"fast first fallback", fastFirstFallback}, {"fast second fallback", fastSecondFallback}} {
		for _, above := range []bool{false, true} {
			count := stalledFastFirstFallbackGate(defaultRunID).agents(above)
			sets := map[string]bool{}
			for runID := 1; runID <= 5; runID++ {
				ids := pickStalledAgents(count, gate.rule, runID)
				require.Len(t, ids, int(count), gate.name)
				require.True(t, slices.IsSorted(ids))
				first, whole := chainExposure(ids)
				assert.Zero(t, whole, "%s above=%v run %d: a partition has its whole chain stalled", gate.name, above, runID)
				if gate.rule == fastFirstFallback {
					assert.Zero(t, first, "%s above=%v run %d", gate.name, above, runID)
				} else {
					assert.GreaterOrEqual(t, first, minNeedingSecondFallback, "%s above=%v run %d", gate.name, above, runID)
				}
				// Determinism: the same id gives the same agents every time.
				assert.Equal(t, ids, pickStalledAgents(count, gate.rule, runID))
				sets[fmt.Sprint(ids)] = true
			}
			// Each run id places the stalled agents differently.
			assert.Len(t, sets, 5, "%s above=%v", gate.name, above)
		}
	}
}

// The ids of the default run are pinned so that a change to the search, the walk or
// the shares shows up as a diff in what the scenarios stall.
func TestStalledAgents_PickedIDs(t *testing.T) {
	t.Parallel()
	below, above := stalledFastFirstFallbackGate(defaultRunID).agents(false), stalledFastFirstFallbackGate(defaultRunID).agents(true)
	assert.Equal(t, []int32{4, 11, 13, 16, 22, 24, 25, 29, 30, 34, 41, 49}, pickStalledAgents(below, fastFirstFallback, defaultRunID))
	assert.Equal(t, []int32{1, 7, 8, 9, 10, 11, 15, 21, 23, 26, 28, 32, 36, 39, 42, 44, 47}, pickStalledAgents(above, fastFirstFallback, defaultRunID))
	assert.Equal(t, []int32{1, 3, 12, 16, 17, 19, 26, 27, 32, 38, 42, 45}, pickStalledAgents(below, fastSecondFallback, defaultRunID))
	assert.Equal(t, []int32{0, 1, 5, 10, 12, 13, 16, 22, 24, 25, 29, 30, 41, 42, 43, 44, 46}, pickStalledAgents(above, fastSecondFallback, defaultRunID))
}

func TestStalledAgents_ScenariosStallThePickedAgents(t *testing.T) {
	t.Parallel()
	for _, runID := range []int{1, 2, 3} {
		for _, gate := range []struct {
			g    fractionGate
			rule stalledRule
		}{{stalledFastFirstFallbackGate(runID), fastFirstFallback}, {stalledFastSecondFallbackGate(runID), fastSecondFallback}} {
			for _, above := range []bool{false, true} {
				sc := fractionScenario(gate.g, above)
				var stalled []int32
				for id, behaviour := range sc.behaviours.byBroker {
					if behaviour.latencyFn(nil) == stalledSlowLatency {
						stalled = append(stalled, id)
					}
				}
				slices.Sort(stalled)
				assert.Equal(t, pickStalledAgents(gate.g.agents(above), gate.rule, runID), stalled, "%s run %d", sc.name, runID)
				// Only the stalled agents are counted as delayed in the report.
				for id, behaviour := range sc.behaviours.byBroker {
					assert.Equal(t, slices.Contains(stalled, id), behaviour.injectedLatency, "%s agent %d", sc.name, id)
				}
				// The description says which run placed which agents.
				assert.Contains(t, sc.description, fmt.Sprintf("Run id %d places the stalled agents at %v", runID, stalled))
			}
		}
	}
}

// Run ids change only where the stalled agents sit: names, counts and every other
// scenario stay the same, so results stay comparable across ids.
func TestScenarios_RunIDOnlyMovesTheStalledAgents(t *testing.T) {
	t.Parallel()
	first, second := scenarios(1), scenarios(2)
	require.Len(t, second, len(first))
	var moved int
	for i := range first {
		assert.Equal(t, first[i].name, second[i].name)
		if first[i].description != second[i].description {
			moved++
			assert.Contains(t, first[i].name, "stalled agents with a fast")
		}
	}
	assert.Equal(t, 4, moved, "the four stalled scenarios are the only ones that use the run id")
}

// A rescue has to finish inside the application deadline with room to spare. The
// hedge delay is the cluster's mean latency, which the stalled agents raise.
func TestStalledAgents_RescueFitsTheDeadline(t *testing.T) {
	t.Parallel()
	for _, above := range []bool{false, true} {
		share := float64(stalledFastFirstFallbackGate(defaultRunID).agents(above)) / float64(clusterSize)
		baseline := time.Duration(share*float64(stalledSlowLatency) + (1-share)*float64(moderateHealthyLatency))
		delay := max(baseline, wgo.DefaultHedgerMinHedgeDelay)
		assert.Less(t, delay+moderateHealthyLatency, appRequestTimeout, "one hedge delay, above=%v", above)
		assert.Less(t, 2*delay+moderateHealthyLatency, appRequestTimeout, "two hedge delays, above=%v", above)
	}
}

func TestStalledAgents_NamesDistinguishTheVariants(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "stalled agents with a fast first fallback just below MaxSlowFraction", stalledFastFirstFallbackGate(defaultRunID).name(false))
	assert.Equal(t, "stalled agents with a fast second fallback just above MaxSlowFraction", stalledFastSecondFallbackGate(defaultRunID).name(true))
	// The stalled agents are past the deadline for kgo and short of the wire timeout.
	require.Greater(t, stalledSlowLatency, appRequestTimeout)
	require.Less(t, stalledSlowLatency, clientProduceRequestTimeout)
	below := scenarioNamed(t, stalledFastFirstFallbackGate(defaultRunID).name(false))
	above := scenarioNamed(t, stalledFastFirstFallbackGate(defaultRunID).name(true))
	require.Contains(t, above.requireSuppression, suppressedSlowFraction)
	require.NotNil(t, below.expect.minSuccessDeltaVsKgo)
	require.Nil(t, above.expect.minSuccessDeltaVsKgo)
}
