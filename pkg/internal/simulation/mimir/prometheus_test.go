package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPrometheus_RetryRemoteWrite(t *testing.T) {
	binary := os.Getenv("PROMETHEUS_BINARY")
	if binary == "" {
		t.Skip("set PROMETHEUS_BINARY to run the real sender integration test")
	}
	var failed, succeeded atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, tenant, r.Header.Get("X-Scope-OrgID"))
		_, _ = io.Copy(io.Discard, r.Body)
		if failed.Load() < 3 {
			failed.Add(1)
			http.Error(w, "retry", http.StatusInternalServerError)
			return
		}
		succeeded.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	client := &distributorClient{base: receiver.URL, http: &http.Client{Timeout: 5 * time.Second}}
	sender, err := newPrometheusSender(binary, client, t.TempDir())
	require.NoError(t, err)
	defer sender.Close()
	workload := &prometheusWorkload{senders: []*prometheusSender{sender}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	attempts, err := workload.Observe(ctx, time.Now(), 15*time.Second)
	require.NoError(t, err)
	require.GreaterOrEqual(t, failed.Load(), int64(3))
	require.Positive(t, succeeded.Load())
	var failures int
	for _, attempt := range attempts {
		if attempt.Error != nil {
			failures++
		}
	}
	require.Positive(t, failures)
	evidence, err := workload.SenderEvidence()
	require.NoError(t, err)
	require.Positive(t, evidence["samples_retried_total"])
	require.Zero(t, evidence["samples_failed_total"])
	require.Positive(t, evidence["samples_total"])
	var accepted, rejected int64
	for _, attempt := range attempts {
		if attempt.Error == nil {
			accepted += attempt.Samples
		} else {
			rejected += attempt.Samples
		}
	}
	require.Positive(t, accepted)
	require.Positive(t, rejected)
	require.Equal(t, float64(accepted), evidence["samples_accepted_observed_total"])
}

func TestPrometheus_CountSamplesPreservesPayload(t *testing.T) {
	request := prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{Samples: []prompb.Sample{{Value: 1}, {Value: 2}}},
		{Samples: []prompb.Sample{{Value: 3}}},
	}}
	data, err := request.Marshal()
	require.NoError(t, err)
	body := snappy.Encode(nil, data)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/push", bytes.NewReader(body))
	count, err := remoteWriteSamples(r)
	require.NoError(t, err)
	require.Equal(t, int64(3), count)
	forwarded, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	require.Equal(t, body, forwarded)
}

func TestPrometheus_RejectInvalidSampleMeasurement(t *testing.T) {
	for _, body := range [][]byte{[]byte("bad snappy"), snappy.Encode(nil, []byte{0xff})} {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/push", bytes.NewReader(body))
		_, err := remoteWriteSamples(r)
		require.Error(t, err)
		forwarded, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, body, forwarded)
	}
}

func TestPrometheus_RecordsDoNotInjectHTTP(t *testing.T) {
	p := &prometheusWorkload{}
	require.Empty(t, p.EventRecords("", 0, 0))
	called := false
	p.Produce(context.Background(), &kgo.Record{}, func(_ *kgo.Record, err error) { require.NoError(t, err); called = true })
	require.True(t, called)
}
