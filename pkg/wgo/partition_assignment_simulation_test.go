package wgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// simulationChains is the candidate list (leader, first fallback, second fallback)
// for each partition of the simulation: topic "wgo-simulation", agents 0 to 49,
// agent p leading partition p. The simulation scenarios in
// pkg/internal/simulation/runner choose their slow agents from it and keep a copy
// of this table. If this test fails the fallback walk changed: regenerate the table
// there and re-check which agents the scenarios stall.
var simulationChains = [][]int32{
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

func TestDefaultPartitionAssignmentStrategy_CandidatesSimulationChains(t *testing.T) {
	t.Parallel()
	const topic, agentCount = "wgo-simulation", 50
	agents := make([]int32, agentCount)
	leaders := make(map[topicPartition]int32, agentCount)
	for i := range agents {
		agents[i] = int32(i)
		leaders[topicPartition{topic: topic, partition: int32(i)}] = int32(i)
	}
	strategy := newDefaultPartitionAssignmentStrategy(agents, leaders, nil, nil, map[string]int32{topic: agentCount})

	require.Len(t, simulationChains, agentCount)
	for partition, want := range simulationChains {
		var got []int32
		for _, agent := range strategy.Candidates(topic, int32(partition), len(want)) {
			got = append(got, agent.NodeID)
		}
		assert.Equal(t, want, got, "partition %d", partition)
	}
}
