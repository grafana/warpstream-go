package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

type prometheusSender struct {
	keepFiles          bool
	lastScrape         atomic.Int64
	client             *distributorClient
	command            *exec.Cmd
	done               chan struct{}
	source, receiver   *httptest.Server
	directory, address string
	log                *os.File
	mu                 sync.Mutex
	attempts           []runner.ApplicationObservation
	active             map[*http.Request]time.Time
	measurementErr     error
	once               sync.Once
}

type prometheusWorkload struct {
	*distributorWriters
	senders         []*prometheusSender
	observedSamples atomic.Int64
}

func newPrometheusWorkload(binary, prometheusBinary string, cfg runner.WorkloadConfig, options distributorOptions) (_ runner.Producer, err error) {
	producer, err := newDistributorWriters(binary, cfg, options, 2)
	if err != nil {
		return nil, err
	}
	group := &prometheusWorkload{distributorWriters: producer.(*distributorWriters)}
	defer func() {
		if err != nil {
			group.Close()
		}
	}()
	for _, client := range group.clients {
		sender, senderErr := newPrometheusSender(prometheusBinary, client, options.logDirectory)
		if senderErr != nil {
			return nil, senderErr
		}
		group.senders = append(group.senders, sender)
	}
	return group, nil
}

func newPrometheusSender(binary string, client *distributorClient, logs string) (_ *prometheusSender, err error) {
	s := &prometheusSender{keepFiles: logs != "", client: client, active: map[*http.Request]time.Time{}}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	s.directory, err = os.MkdirTemp(logs, "prometheus-simulation-")
	if err != nil {
		return nil, err
	}
	var exposition strings.Builder
	for id := range 32000 {
		fmt.Fprintf(&exposition, "simulation_%d 1\n", id)
	}
	body := exposition.String()
	s.source = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.lastScrape.Store(time.Now().UnixMilli())
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, body)
	}))
	target, err := url.Parse(client.base)
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{ResponseHeaderTimeout: 30 * time.Second}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "distributor unavailable", http.StatusBadGateway)
	}
	s.receiver = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		s.mu.Lock()
		s.active[r] = start
		s.mu.Unlock()
		tracked := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		samples, measurementErr := remoteWriteSamples(r)
		proxy.ServeHTTP(tracked, r)
		var attemptErr error
		if tracked.status < 200 || tracked.status >= 300 {
			attemptErr = fmt.Errorf("mimir HTTP %d", tracked.status)
		}
		s.mu.Lock()
		if measurementErr != nil {
			s.measurementErr = measurementErr
		}
		s.attempts = append(s.attempts, runner.ApplicationObservation{Started: start, Duration: time.Since(start), Error: attemptErr, Samples: samples})
		delete(s.active, r)
		s.mu.Unlock()
	}))
	config := fmt.Sprintf(`global:
  scrape_interval: 1s
scrape_configs:
  - job_name: simulation
    static_configs:
      - targets: [%q]
        labels:
          instance: simulation
remote_write:
  - url: %s/api/v1/push
    headers:
      X-Scope-OrgID: %s
`, strings.TrimPrefix(s.source.URL, "http://"), s.receiver.URL, tenant)
	configPath := filepath.Join(s.directory, "prometheus.yaml")
	if err = os.WriteFile(configPath, []byte(config), 0600); err != nil {
		return nil, err
	}
	s.log, err = os.Create(filepath.Join(s.directory, "prometheus.log"))
	if err != nil {
		return nil, err
	}
	s.command = exec.Command(binary, "--agent", "--config.file="+configPath, "--storage.agent.path="+filepath.Join(s.directory, "wal"), "--web.listen-address=127.0.0.1:0", "--log.format=json")
	s.command.Stdout, s.command.Stderr = s.log, s.log
	if err = s.command.Start(); err != nil {
		s.command = nil
		return nil, err
	}
	s.done = make(chan struct{})
	go func() { _ = s.command.Wait(); close(s.done) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		data, _ := os.ReadFile(s.log.Name())
		for _, line := range strings.Split(string(data), "\n") {
			var entry struct {
				Message string `json:"msg"`
				Address string `json:"address"`
			}
			if json.Unmarshal([]byte(line), &entry) == nil && entry.Message == "Listening on" {
				s.address = "http://" + entry.Address
			}
		}
		if s.address != "" {
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.address+"/-/ready", nil)
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return s, nil
				}
			}
		}
		select {
		case <-s.done:
			return nil, fmt.Errorf("prometheus exited: %s", data)
		case <-ctx.Done():
			return nil, fmt.Errorf("prometheus readiness: %w; %s", ctx.Err(), data)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func remoteWriteSamples(r *http.Request) (int64, error) {
	body, err := io.ReadAll(r.Body)
	// Restore the exact wire payload so measurement does not change Mimir's input.
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	decoded, err := snappy.Decode(nil, body)
	if err != nil {
		return 0, err
	}
	var request prompb.WriteRequest
	if err := request.Unmarshal(decoded); err != nil {
		return 0, err
	}
	var samples int64
	for _, series := range request.Timeseries {
		samples += int64(len(series.Samples))
	}
	return samples, nil
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (s *prometheusSender) Close() {
	s.once.Do(func() {
		if s.command != nil {
			_ = s.command.Process.Signal(syscall.SIGTERM)
			select {
			case <-s.done:
			case <-time.After(10 * time.Second):
				_ = s.command.Process.Kill()
				<-s.done
			}
		}
		if s.receiver != nil {
			s.receiver.Close()
		}
		if s.source != nil {
			s.source.Close()
		}
		if s.log != nil {
			_ = s.log.Close()
		}
		if !s.keepFiles && s.directory != "" {
			_ = os.RemoveAll(s.directory)
		}
	})
}

func (p *prometheusWorkload) Close() {
	for _, sender := range p.senders {
		sender.Close()
	}
	p.distributorWriters.Close()
}
func (*prometheusWorkload) EventRecords(string, int32, time.Duration) []*kgo.Record { return nil }
func (*prometheusWorkload) Produce(_ context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	promise(record, nil)
}
func (*prometheusWorkload) ObservationDescription() string {
	return "Each event is one real Prometheus remote-write HTTP attempt, including retries. Prometheus controls batching, sharding, backoff and its WAL. A failed attempt is not proof of lost samples; eventual delivery and backlog must be assessed separately. Attempts are grouped by actual issue time and counts may differ between clients."
}
func (p *prometheusWorkload) Observe(ctx context.Context, start time.Time, duration time.Duration) ([]runner.ApplicationObservation, error) {
	end := start.Add(duration)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Until(end)):
	}
	deadline := time.NewTimer(31 * time.Second)
	defer deadline.Stop()
	for {
		var attempts []runner.ApplicationObservation
		pending := false
		for _, sender := range p.senders {
			select {
			case <-sender.done:
				return nil, fmt.Errorf("prometheus exited during observation")
			default:
			}
			sender.mu.Lock()
			for _, at := range sender.active {
				if !at.Before(start) && at.Before(end) {
					pending = true
				}
			}
			for _, attempt := range sender.attempts {
				if !attempt.Started.Before(start) && attempt.Started.Before(end) {
					attempts = append(attempts, attempt)
				}
			}
			sender.mu.Unlock()
		}
		if !pending {
			if len(attempts) == 0 {
				return nil, fmt.Errorf("prometheus issued no remote-write attempts")
			}
			var samples int64
			for _, attempt := range attempts {
				if attempt.Error == nil {
					samples += attempt.Samples
				}
			}
			p.observedSamples.Store(samples)
			return attempts, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("remote-write attempts did not drain")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func (p *prometheusWorkload) Describe() (string, string) {
	_, config := p.distributorWriters.Describe()
	return "Workload: two actual Prometheus processes per client lane run in agent mode, each scraping 32,000 fixed series every second and remote-writing to an unmodified Mimir distributor. Remote-write queue, batching, retries and 30s HTTP timeout use Prometheus defaults. HTTP outcomes include retries and do not measure durable storage or queryability.", config
}

func (p *prometheusWorkload) SenderEvidence() (map[string]float64, error) {
	out := map[string]float64{"samples_total": 0, "samples_failed_total": 0, "samples_retried_total": 0, "samples_dropped_total": 0, "samples_pending": 0}
	out["samples_accepted_observed_total"] = float64(p.observedSamples.Load())
	for _, sender := range p.senders {
		response, err := sender.client.http.Get(sender.address + "/metrics")
		if err != nil {
			return nil, err
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, parseErr := parser.TextToMetricFamilies(response.Body)
		_ = response.Body.Close()
		if parseErr != nil {
			return nil, parseErr
		}
		highest, sent := float64(0), float64(0)
		for name, family := range families {
			for _, metric := range family.Metric {
				switch name {
				case "prometheus_remote_storage_samples_total", "prometheus_remote_storage_samples_failed_total", "prometheus_remote_storage_samples_retried_total", "prometheus_remote_storage_samples_dropped_total":
					out[strings.TrimPrefix(name, "prometheus_remote_storage_")] += metric.GetCounter().GetValue()
				case "prometheus_remote_storage_samples_pending":
					out["samples_pending"] += metric.GetGauge().GetValue()
				case "prometheus_remote_storage_queue_highest_timestamp_seconds":
					highest = max(highest, metric.GetGauge().GetValue())
				case "prometheus_remote_storage_queue_highest_sent_timestamp_seconds":
					sent = max(sent, metric.GetGauge().GetValue())
				}
			}
		}
		out["max_enqueue_lag_seconds"] = max(out["max_enqueue_lag_seconds"], highest-sent)
		out["max_source_lag_seconds"] = max(out["max_source_lag_seconds"], float64(sender.lastScrape.Load())/1000-sent)
	}
	return out, nil
}

func (p *prometheusWorkload) Validate() error {
	if err := p.distributorWriters.Validate(); err != nil {
		return err
	}
	for _, sender := range p.senders {
		sender.mu.Lock()
		measurementErr := sender.measurementErr
		sender.mu.Unlock()
		if measurementErr != nil {
			return fmt.Errorf("count remote-write samples: %w", measurementErr)
		}
		select {
		case <-sender.done:
			return fmt.Errorf("prometheus exited before completion")
		default:
		}
	}
	return nil
}
