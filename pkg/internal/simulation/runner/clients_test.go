package runner

import (
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientSelection_Parse(t *testing.T) {
	for _, name := range []string{"both", "wgo", "kgo"} {
		selected, err := parseClients(name)
		require.NoError(t, err)
		assert.Equal(t, name, selected.String())
		assert.Equal(t, name != "kgo", selected.includes("wgo"))
		assert.Equal(t, name != "wgo", selected.includes("kgo"))
	}
	_, err := parseClients("other")
	require.ErrorContains(t, err, "unknown client")
}

func TestClientSelection_OnlySelectedLaneProduces(t *testing.T) {
	for _, selected := range []clientSelection{clientWgoOnly, clientKgoOnly} {
		t.Run(selected.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				env := &environment{topic: "t", numPartitions: 1}
				if selected == clientWgoOnly {
					env.wgoClient = &fakeProduceClient{}
				} else {
					env.kgoClient = &fakeProduceClient{}
				}
				require.NoError(t, runScenarioWarmupWithSpacing(t.Context(), env, eventSpacing))
				wgo, kgo := runScenarioObservedEvents(t.Context(), env, time.Now(),
					eventsForClient(env.wgoClient, "t", 1, time.Second, eventSpacing),
					eventsForClient(env.kgoClient, "t", 1, time.Second, eventSpacing))
				if selected == clientWgoOnly {
					assert.Len(t, wgo.snapshot().list, 2)
					assert.Empty(t, kgo.snapshot().list)
				} else {
					assert.Len(t, kgo.snapshot().list, 2)
					assert.Empty(t, wgo.snapshot().list)
				}
				env.Close()
			})
		})
	}
}

func TestClientSelection_GatesOnlyMeasuredClients(t *testing.T) {
	result := scenarioResult{
		clients: clientKgoOnly,
		sc: scenario{name: "fault", requireStaleFallback: true,
			expect:          scenarioExpectations{minWgoSuccessRate: 1, minSuccessDeltaVsKgo: ptr(1.0), maxHedgeSurge: ptr(0.0), recoveryFrom: ptr(time.Second)},
			transportPhases: []transportPhase{{0, 1, killPod}}},
		kgoSummary:   observationsSummary{total: 4, successes: 1},
		kgoTransport: transportStats{byBroker: map[int32]brokerTransportStats{1: {closedConnections: 1}}},
	}
	assert.Empty(t, checkResult(result))
	result.kgoTransport = transportStats{}
	require.Len(t, checkResult(result), 1)
	result.clients = clientWgoOnly
	assert.NotEmpty(t, checkResult(result))
}

func TestResultsReport_SingleClientOmitsUnmeasuredCounts(t *testing.T) {
	for _, selected := range []clientSelection{clientWgoOnly, clientKgoOnly} {
		t.Run(selected.String(), func(t *testing.T) {
			result := scenarioResult{clients: selected, sc: scenario{name: "healthy"},
				wgoSummary: observationsSummary{total: 4, successes: 3},
				kgoSummary: observationsSummary{total: 4, successes: 2},
				windows:    []windowResult{{window: observationWindow{name: "observed"}}},
			}
			report := newResultsReport(scenarioResults{entries: []scenarioResult{result}})
			data, err := report.generateJSON()
			require.NoError(t, err)
			var rows []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &rows))
			for _, client := range []string{"wgo", "kgo"} {
				assert.Equal(t, selected.includes(client), rows[0][client] != nil)
				if !selected.includes(client) {
					assert.NotContains(t, report.generateMarkdown(), "- app requests ("+client+")")
				}
			}
			assert.Contains(t, report.generateMarkdown(), "Client: "+selected.String())
		})
	}
}
