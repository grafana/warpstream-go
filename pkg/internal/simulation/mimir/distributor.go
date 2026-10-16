package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/grafana/dskit/ring"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/warpstream-go/pkg/internal/simulation/runner"
)

const (
	tenant                   = "simulation"
	distributorRemoteTimeout = 5 * time.Second
)

//go:embed mimir-version.txt
var mimirVersion string

type distributorClient struct {
	config             runner.WorkloadConfig
	base               string
	series             int
	http               *http.Client
	command            *exec.Cmd
	done               chan struct{}
	cleanup            []func()
	once               sync.Once
	mu                 sync.Mutex
	metrics            []*dto.MetricFamily
	failure            error
	partitions         map[int32]bool
	expectedPartitions int
	lastWriteError     error
	backend            string
}

func newDistributor(binary string, cfg runner.WorkloadConfig) (_ runner.Producer, err error) {
	c := &distributorClient{config: cfg, http: &http.Client{Timeout: distributorRemoteTimeout}, series: 32 * int(cfg.Partitions), partitions: make(map[int32]bool), expectedPartitions: int(cfg.Partitions), backend: cfg.Backend}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()
	proxy := newBrokerProxy(observedDialer(cfg.Dialer, cfg.ObserveProduce, func(partition int32) {
		c.mu.Lock()
		c.partitions[partition] = true
		c.mu.Unlock()
	}))
	proxy.latency = cfg.ProduceLatency
	c.cleanup = append(c.cleanup, proxy.Close)
	address, err := proxy.address(cfg.Address)
	if err != nil {
		return nil, err
	}
	fixture, err := partitionFixture(cfg.Partitions)
	if err != nil {
		return nil, err
	}
	c.cleanup = append(c.cleanup, fixture.Close)
	dir, err := os.MkdirTemp("", "mimir-simulation-")
	if err != nil {
		return nil, err
	}
	c.cleanup = append(c.cleanup, func() { _ = os.RemoveAll(dir) })
	config := distributorConfiguration(cfg, address, fixture.URL, dir)
	configPath := filepath.Join(dir, "mimir.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, "mimir.log")
	log, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	c.cleanup = append(c.cleanup, func() { _ = log.Close() })
	c.command = exec.Command(binary, "-config.file="+configPath)
	c.command.Dir, c.command.Stdout, c.command.Stderr = dir, log, log
	if err := c.command.Start(); err != nil {
		return nil, err
	}
	c.done = make(chan struct{})
	go func() { _ = c.command.Wait(); close(c.done) }()
	c.cleanup = append(c.cleanup, func() {
		_ = c.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-c.done:
		case <-time.After(5 * time.Second):
			_ = c.command.Process.Kill()
			<-c.done
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for {
		if c.base == "" {
			data, readErr := os.ReadFile(logPath)
			if readErr != nil {
				return nil, readErr
			}
			if address := httpAddressFromLog(data); address != "" {
				c.base = "http://" + address
			}
		}
		if c.base != "" {
			response, requestErr := c.http.Get(c.base + "/ready")
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return c, nil
				}
			}
		}
		select {
		case <-c.done:
			data, _ := os.ReadFile(logPath)
			return nil, fmt.Errorf("mimir exited during startup: %s", data)
		case <-ctx.Done():
			data, _ := os.ReadFile(logPath)
			return nil, fmt.Errorf("mimir readiness: %w; %s", ctx.Err(), data)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func distributorConfiguration(cfg runner.WorkloadConfig, address, fixtureURL, dir string) string {
	return fmt.Sprintf(`target: distributor
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
  log_level: info
  log_format: json
distributor:
  remote_timeout: %s
  ring:
    kvstore:
      store: inmemory
    instance_addr: 127.0.0.1
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_addr: 127.0.0.1
  partition_ring:
    kvstore:
      store: consul
      consul:
        host: %s
ingest_storage:
  enabled: true
  migration:
    distributor_send_to_ingesters_enabled: false
  kafka:
    backend: %s
    address: %s
    topic: %s
    write_timeout: %s
    write_timeout_overhead: %s
    dial_timeout: %s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: %s
memberlist:
  bind_addr: [127.0.0.1]
  bind_port: 0
usage_stats:
  enabled: false
`, distributorRemoteTimeout, strings.TrimPrefix(fixtureURL, "http://"), cfg.Backend, address, cfg.Topic, cfg.WriteTimeout, cfg.WriteTimeoutOverhead, cfg.DialTimeout, filepath.Join(dir, "blocks"))
}

func partitionFixture(partitions int32) (*httptest.Server, error) {
	desc := ring.NewPartitionRingDesc()
	for id := int32(0); id < partitions; id++ {
		desc.AddPartition(id, ring.PartitionActive, time.Now())
	}
	encoded, err := ring.GetPartitionRingCodec().Encode(desc)
	if err != nil {
		return nil, err
	}
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/kv/collectors/ingester-partitions" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("index") == "1" {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Second):
			}
		}
		w.Header().Set("X-Consul-Index", "1")
		w.Header().Set("X-Consul-Knownleader", "true")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"Key": "collectors/ingester-partitions", "Value": encoded, "ModifyIndex": 1, "CreateIndex": 1}})
	}))
	return fixture, nil
}

func httpAddressFromLog(data []byte) string {
	// Let the child bind port zero; reserving then releasing a port races other listeners.
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		var entry struct {
			Message string          `json:"msg"`
			HTTP    json.RawMessage `json:"http"`
		}
		if json.Unmarshal(line, &entry) != nil || entry.Message != "server listening on addresses" {
			continue
		}
		var address string
		if json.Unmarshal(entry.HTTP, &address) != nil {
			var tcpAddress net.TCPAddr
			if json.Unmarshal(entry.HTTP, &tcpAddress) != nil {
				continue
			}
			address = tcpAddress.String()
		}
		host, port, err := net.SplitHostPort(address)
		portNumber, portErr := strconv.Atoi(port)
		if err == nil && portErr == nil && host == "127.0.0.1" && portNumber > 0 && portNumber <= 65535 {
			return address
		}
	}
	return ""
}

func (*distributorClient) MetricsPrefix() string { return "cortex_ingest_storage_writer_" }
func (c *distributorClient) EventRecords(topic string, _ int32, at time.Duration) []*kgo.Record {
	request := prompb.WriteRequest{}
	// Spread metric-name hashes so requests exercise the whole partition ring.
	for series := range c.series {
		name := fmt.Sprintf("simulation_%x", sha256.Sum256([]byte(fmt.Sprint(series))))
		request.Timeseries = append(request.Timeseries, prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: name}}, Samples: []prompb.Sample{{Timestamp: time.Now().Add(at).UnixMilli(), Value: 1}}})
	}
	data, err := request.Marshal()
	if err != nil {
		panic(err)
	}
	return []*kgo.Record{{Topic: topic, Value: snappy.Encode(nil, data)}}
}
func (c *distributorClient) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	go func() {
		err := c.push(ctx, record.Value)
		if err != nil {
			c.mu.Lock()
			c.lastWriteError = err
			c.mu.Unlock()
		}
		promise(record, err)
	}()
}
func (c *distributorClient) push(ctx context.Context, data []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/api/v1/push", bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Content-Encoding", "snappy")
	request.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	request.Header.Set("X-Scope-OrgID", tenant)
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("mimir HTTP %d: %s", response.StatusCode, body)
	}
	return nil
}

func (c *distributorClient) Gather() ([]*dto.MetricFamily, error) {
	families, err := c.scrape()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if c.failure == nil {
			c.failure = err
		}
	} else {
		c.metrics = families
	}
	// Keep sampling alive so the runner can clean up every child before reporting a scrape failure.
	return c.metrics, nil
}
func (c *distributorClient) scrape() ([]*dto.MetricFamily, error) {
	response, err := c.http.Get(c.base + "/metrics")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mimir metrics HTTP %d", response.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	parsed, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return nil, err
	}
	if c.backend == "warpstream" {
		for _, counter := range []string{"primary", "hedge"} {
			name := c.MetricsPrefix() + "warpstream_produce_requests_" + counter + "_total"
			if parsed[name] == nil {
				return nil, fmt.Errorf("mimir metrics missing %q", name)
			}
		}
	}
	families := make([]*dto.MetricFamily, 0, len(parsed))
	for _, family := range parsed {
		families = append(families, family)
	}
	return families, nil
}
func (c *distributorClient) Validate() error {
	_, err := c.Gather()
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if len(c.partitions) != c.expectedPartitions {
		return fmt.Errorf("mimir %s successful writes reached %d/%d ingest partitions; last write error: %v", c.backend, len(c.partitions), c.expectedPartitions, c.lastWriteError)
	}
	select {
	case <-c.done:
		return fmt.Errorf("mimir exited before scenario completion")
	default:
		return nil
	}
}
func (c *distributorClient) Close() {
	c.once.Do(func() {
		for i := len(c.cleanup) - 1; i >= 0; i-- {
			c.cleanup[i]()
		}
		c.http.CloseIdleConnections()
	})
}
func (*distributorClient) RecoveryResponseDescription() string {
	return "successful wgo Produce wire responses read (including probes and responses after HTTP completion)"
}

func (c *distributorClient) Describe() (string, string) {
	return fmt.Sprintf("Workload: separate, unmodified Mimir distributor processes receive Prometheus remote-write HTTP requests for one tenant, with %d series per request across %d ingest partitions. Mimir validates, shards, serializes and writes using its warpstream or kafka backend. Fake brokers acknowledge without durable storage; this measures HTTP completion, not queryability.", c.series, c.expectedPartitions),
		fmt.Sprintf("Configuration: cmd/mimir source pin %s, built with a go.mod replacement for this checkout. Distributor remote timeout %s; Kafka write timeout %s, overhead %s and dial timeout %s. Other client settings use the pinned Mimir defaults. Ingester migration writes are disabled. External TCP proxies apply faults and rewrite advertised broker addresses; counters are scraped from /metrics. Produce response latency is injected on the TCP wire using the shared per-broker distributions; response order is preserved.", strings.TrimSpace(mimirVersion), distributorRemoteTimeout, c.config.WriteTimeout, c.config.WriteTimeoutOverhead, c.config.DialTimeout)
}
