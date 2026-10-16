package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

func TestDistributor_RemoteWrite(t *testing.T) {
	c := &distributorClient{series: 1600, http: &http.Client{}}
	record := c.EventRecords("test", 50, 0)[0]
	received := make(chan prompb.WriteRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/push" || r.Header.Get("X-Scope-OrgID") != tenant || r.Header.Get("Content-Encoding") != "snappy" || r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" {
			http.Error(w, "bad request", 400)
			return
		}
		var request prompb.WriteRequest
		body, err := io.ReadAll(r.Body)
		if err == nil {
			body, err = snappy.Decode(nil, body)
		}
		if err == nil {
			err = request.Unmarshal(body)
		}
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		received <- request
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c.base = server.URL
	require.NoError(t, c.push(t.Context(), record.Value))
	request := <-received
	require.Len(t, request.Timeseries, 1600)
	names := map[string]bool{}
	for _, series := range request.Timeseries {
		require.Len(t, series.Samples, 1)
		names[series.Labels[0].Value] = true
	}
	require.Len(t, names, 1600)
}

func TestDistributor_HTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "write timeout", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	c := &distributorClient{base: server.URL, http: server.Client()}
	require.ErrorContains(t, c.push(t.Context(), nil), "mimir HTTP 503: write timeout")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.push(ctx, nil), context.Canceled)
}

func TestDistributor_MetricsFailure(t *testing.T) {
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "# TYPE cortex_ingest_storage_writer_warpstream_produce_requests_primary_total counter\ncortex_ingest_storage_writer_warpstream_produce_requests_primary_total 42\n")
	}))
	defer server.Close()
	c := &distributorClient{base: server.URL, http: server.Client(), done: make(chan struct{})}
	metrics, err := c.Gather()
	require.NoError(t, err)
	require.Equal(t, 42.0, metrics[0].Metric[0].GetCounter().GetValue())
	fail.Store(true)
	cached, err := c.Gather()
	require.NoError(t, err)
	require.Equal(t, metrics, cached)
	require.ErrorContains(t, c.Validate(), "metrics HTTP 503")
}

func TestDistributor_MissingCounters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "# TYPE unrelated counter\nunrelated 1\n")
	}))
	defer server.Close()
	c := &distributorClient{base: server.URL, http: server.Client(), backend: "warpstream"}
	require.ErrorContains(t, c.Validate(), "metrics missing")
}

func TestDistributor_HTTPAddressFromLog(t *testing.T) {
	data := []byte("partial startup log\n" + `{"msg":"unrelated","http":"127.0.0.1:9999"}` + "\n" + `{"msg":"server listening on addresses","http":"127.0.0.1:43127","grpc":"127.0.0.1:43128"}` + "\n" + `{"msg":`)
	require.Equal(t, "127.0.0.1:43127", httpAddressFromLog(data))
	require.Empty(t, httpAddressFromLog([]byte(`{"msg":"server listening on addresses","http":"127.0.0.1:0"}`)))
	require.Empty(t, httpAddressFromLog([]byte(`{"msg":"server listening on addresses","http":"192.0.2.1:1234"}`)))

	require.Empty(t, httpAddressFromLog([]byte(`{"msg":"server listening on addresses","http":"127.0.0.1:invalid"}`)))

	require.Empty(t, httpAddressFromLog([]byte(`{"msg":"server listening on addresses","http":"127.0.0.1:65536"}`)))
}

func TestDistributor_LoggedTCPAddress(t *testing.T) {
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43127}
	var output bytes.Buffer
	require.NoError(t, log.NewJSONLogger(&output).Log("msg", "server listening on addresses", "http", address))
	require.Equal(t, address.String(), httpAddressFromLog(output.Bytes()))

	structured, err := json.Marshal(map[string]any{"msg": "server listening on addresses", "http": address})
	require.NoError(t, err)
	require.Equal(t, address.String(), httpAddressFromLog(structured))
	address.Port = 0
	structured, err = json.Marshal(map[string]any{"msg": "server listening on addresses", "http": address})
	require.NoError(t, err)
	require.Empty(t, httpAddressFromLog(structured))
}

func TestDistributor_ConfigurationReport(t *testing.T) {
	t.Parallel()
	cfg := runner.WorkloadConfig{Backend: "warpstream", Topic: "test", WriteTimeout: 7 * time.Second, WriteTimeoutOverhead: 1500 * time.Millisecond, DialTimeout: 3 * time.Second}
	config := distributorConfiguration(cfg, "127.0.0.1:9092", "http://127.0.0.1:8500", "/tmp/mimir")
	client := &distributorClient{config: cfg, series: 96, expectedPartitions: 3}
	workload, description := client.Describe()
	for _, setting := range []struct {
		key, label string
		value      time.Duration
	}{
		{"remote_timeout", "Distributor remote timeout", distributorRemoteTimeout},
		{"write_timeout", "Kafka write timeout", cfg.WriteTimeout},
		{"write_timeout_overhead", "overhead", cfg.WriteTimeoutOverhead},
		{"dial_timeout", "dial timeout", cfg.DialTimeout},
	} {
		require.Contains(t, config, fmt.Sprintf("%s: %s", setting.key, setting.value))
		require.Contains(t, description, fmt.Sprintf("%s %s", setting.label, setting.value))
	}
	pin, err := os.ReadFile("mimir-version.txt")
	require.NoError(t, err)
	require.Contains(t, description, "source pin "+strings.TrimSpace(string(pin)))
	require.Contains(t, workload, "96 series per request across 3 ingest partitions")
	require.Contains(t, client.RecoveryResponseDescription(), "wire responses read")
	require.Contains(t, client.RecoveryResponseDescription(), "after HTTP completion")
}
