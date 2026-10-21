package runner

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/warpstream-go/pkg/wgo"
)

func TestResearchScenarios_AreInvestigationScenarios(t *testing.T) {
	t.Parallel()
	var investigation []string
	for _, sc := range investigationScenarios() {
		investigation = append(investigation, sc.name)
	}
	for _, sc := range researchScenarios() {
		assert.True(t, strings.HasPrefix(sc.name, "investigation: "), sc.name)
		assert.Contains(t, investigation, sc.name)
		for _, standard := range scenarios() {
			assert.NotEqual(t, sc.name, standard.name)
		}
	}
}

func TestResearchScenarios_NamesStateTheSetup(t *testing.T) {
	t.Parallel()
	// An agent is slow when it draws what the scenario's slow agents draw; the others
	// use the suite's healthy latency.
	slowAgents := func(sc scenario, slow func(*rand.Rand) time.Duration) (n int) {
		for _, b := range sc.behaviours.byBroker {
			if b.latencyFn(rand.New(rand.NewPCG(1, 1))) == slow(rand.New(rand.NewPCG(1, 1))) {
				n++
			}
		}
		return n
	}
	all := researchScenarios()
	require.Len(t, all, 2)
	for i, slow := range []func(*rand.Rand) time.Duration{highLatency, uniformLatency(researchSpreadFastest, researchSpreadSlowest)} {
		count := slowAgents(all[i], slow)
		assert.Contains(t, all[i].name, fmt.Sprintf("%d of %d agents (%.0f%%)", count, clusterSize, 100*float64(count)/float64(clusterSize)))
		assert.Contains(t, all[i].description, fmt.Sprintf("%d of %d agents", count, clusterSize))
	}
	assert.Contains(t, all[0].name, "in 500ms to 3s, mean 1.5s")
	assert.Contains(t, all[1].name, "uniformly in 250ms to 3s")
}

// The setups are fixed on purpose. These checks fail when a default moves so far
// that a description stops being true.
func TestResearchScenarios_StillSitOnTheirSideOfTheLimits(t *testing.T) {
	t.Parallel()
	share := float64(researchSlowAgents) / float64(clusterSize)
	require.Greater(t, share, wgo.DefaultHealthCheckMaxSlowFraction)
	require.Greater(t, share, wgo.DefaultHealthCheckMaxFaultyFraction)
	// Main's "25% slow agents" has 12 agents and stays under both.
	require.Less(t, 12.0/float64(clusterSize), wgo.DefaultHealthCheckMaxFaultyFraction)

	// The slow agents still count as slow when they answer fastest, answer later than
	// the hedge delay when slowest, and never reach a deadline or timeout.
	require.Less(t, requiredSlowRatio(share), float64(researchSpreadFastest)/float64(researchHealthyLatency))
	require.Less(t, researchSpreadFastest, shortestHedgeDelay)
	require.Greater(t, researchSpreadSlowest, longestHedgeDelay)
	require.Less(t, researchSpreadSlowest, min(appRequestTimeout, clientProduceRequestTimeout))
}

func TestResearchScenarios_UseTheLatencyOfTheExistingSlowAgentsScenario(t *testing.T) {
	t.Parallel()
	sc := researchScenarios()[0]
	fn := sc.behaviours.byBroker[0].latencyFn
	require.NotNil(t, fn)
	a, b := rand.New(rand.NewPCG(7, 7)), rand.New(rand.NewPCG(7, 7))
	for range 200 {
		require.Equal(t, highLatency(a), fn(b))
	}
	assert.Equal(t, 1500*time.Millisecond, (researchTriangularFast+researchTriangularMode+researchTriangularSlow)/3)
	assert.Contains(t, sc.description, `"25% slow agents"`)
	_ = scenarioNamed(t, "25% slow agents")
}

func TestResearchScenarios_ExpectWhatCorrectHandlingLooksLike(t *testing.T) {
	t.Parallel()
	all := researchScenarios()
	// Every agent answers, so no gate may act and wgo has to do as well as kgo.
	assert.Equal(t, []string{suppressedFaultyFraction}, all[0].forbidSuppression)
	assert.Empty(t, all[0].requireSuppression)
	assert.Equal(t, 1.0, all[0].expect.minWgoSuccessRate)
	// Above MaxSlowFraction only the slow-fraction gate may stop hedging.
	assert.Equal(t, []string{suppressedSlowFraction}, all[1].requireSuppression)
	assert.Equal(t, []string{suppressedFaultyFraction}, all[1].forbidSuppression)
	assert.Equal(t, suppressedSlowFraction, all[1].expectedHedgeStop)
}
