package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScenarioBatches_CoverEveryScenarioOnce(t *testing.T) {
	available, err := profileScenarios("all", true, defaultRunID)
	require.NoError(t, err)
	seen := make(map[string]bool)
	for batch := 1; batch <= 5; batch++ {
		selected, err := batchScenarios(available, batch, 5)
		require.NoError(t, err)
		expected := len(available) / 5
		if batch <= len(available)%5 {
			expected++
		}
		require.Len(t, selected, expected)
		for _, sc := range selected {
			assert.False(t, seen[sc.name])
			seen[sc.name] = true
		}
	}
	assert.Len(t, seen, len(available))
}

func TestScenarioBatches_FilterAndUnevenBatches(t *testing.T) {
	available, err := filterScenarios("a", []scenario{{name: "a1"}, {name: "b"}, {name: "a2"}, {name: "a3"}})
	require.NoError(t, err)
	first, err := batchScenarios(available, 1, 2)
	require.NoError(t, err)
	require.Len(t, first, 2)
	assert.Equal(t, "a1", first[0].name)
	assert.Equal(t, "a3", first[1].name)
	second, err := batchScenarios(available, 2, 2)
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, "a2", second[0].name)
	all, err := batchScenarios(available, 1, 1)
	require.NoError(t, err)
	assert.Len(t, all, len(available))
}

func TestScenarioBatches_RejectInvalidBatch(t *testing.T) {
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {-1, 1}, {1, -1}, {2, 1}, {1, 3}} {
		_, err := batchScenarios([]scenario{{name: "a"}, {name: "b"}}, pair[0], pair[1])
		require.ErrorContains(t, err, "invalid scenario batch")
	}
}
