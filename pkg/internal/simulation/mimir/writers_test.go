package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

func TestWriters_IndependentOutcomes(t *testing.T) {
	group := &distributorWriters{}
	for _, topic := range []string{"primary", "secondary"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if topic == "secondary" {
				http.Error(w, "timed out", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		group.clients = append(group.clients, &distributorClient{config: runner.WorkloadConfig{Topic: topic, Partitions: 1}, series: 32, base: server.URL, http: server.Client()})
	}
	records := group.EventRecords("", 0, 0)
	require.Len(t, records, 2)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, record := range records {
		wg.Add(1)
		group.Produce(t.Context(), record, func(_ *kgo.Record, err error) { results <- err; wg.Done() })
	}
	wg.Wait()
	close(results)
	var successes, failures int
	for err := range results {
		if err == nil {
			successes++
		} else {
			failures++
			require.ErrorContains(t, err, "writer secondary: mimir HTTP 500")
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, failures)
	called := 0
	group.Produce(context.Background(), &kgo.Record{Topic: "unknown"}, func(_ *kgo.Record, err error) { called++; require.ErrorContains(t, err, "no writer") })
	require.Equal(t, 1, called)
}

func TestWriters_MetricsKeepWriterIdentity(t *testing.T) {
	group := &distributorWriters{}
	for _, topic := range []string{"primary", "secondary"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "# TYPE cortex_ingest_storage_writer_warpstream_produce_requests_primary_total counter\ncortex_ingest_storage_writer_warpstream_produce_requests_primary_total 42\n")
		}))
		defer server.Close()
		group.clients = append(group.clients, &distributorClient{config: runner.WorkloadConfig{Topic: topic}, base: server.URL, http: server.Client()})
	}
	for range 2 {
		families, err := group.Gather()
		require.NoError(t, err)
		require.Len(t, families, 1)
		require.Len(t, families[0].Metric, 2)
		for i, metric := range families[0].Metric {
			require.Len(t, metric.Label, 1)
			require.Equal(t, "simulation_writer", metric.Label[0].GetName())
			require.Equal(t, group.clients[i].config.Topic, metric.Label[0].GetValue())
		}
		for _, client := range group.clients {
			require.Empty(t, client.metrics[0].Metric[0].Label)
		}
	}
}

func TestDistributorWriters_WarmupComplete(t *testing.T) {
	first := &distributorClient{expectedPartitions: 2, partitions: map[int32]bool{0: true, 1: true}}
	second := &distributorClient{expectedPartitions: 2, partitions: map[int32]bool{0: true}}
	group := &distributorWriters{clients: []*distributorClient{first, second}}
	require.True(t, first.WarmupComplete())
	require.False(t, second.WarmupComplete())
	require.False(t, group.WarmupComplete())
	second.partitions[1] = true
	require.True(t, group.WarmupComplete())
}
