package runner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBenchmarkScenarios_AreRealScenariosWithAFloor(t *testing.T) {
	t.Parallel()
	all, err := profileScenarios("all", true, defaultRunID)
	require.NoError(t, err)
	byName := map[string]scenario{}
	for _, sc := range all {
		byName[sc.name] = sc
	}
	for name := range benchmarkScenarios {
		sc, ok := byName[name]
		require.True(t, ok, "benchmark scenario %q does not exist", name)
		require.Greater(t, sc.expect.minWgoSuccessRate, 0.0, "%q gates on nothing", name)
	}
}
