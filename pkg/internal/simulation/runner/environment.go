package runner

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/grafana/warpstream-go/pkg/internal/testkafka"
	"github.com/grafana/warpstream-go/pkg/wgo"
)

// produceAPIVersion mirrors the unexported produceAPIVersion pinned in
// pkg/wgo/client.go, so the kgo baseline negotiates the same wire format wgo
// itself emits and parses. Kept as a local literal rather than an exported
// wgo symbol: this simulation is the only external consumer that would ever
// need it, and duplicating one constant is cheaper than growing wgo's public
// surface for it.
const produceAPIVersion int16 = 11

const (
	// topicName is the single topic every scenario produces to.
	topicName = "wgo-simulation"

	// clusterSize is both the number of brokers and the number of partitions:
	// the kgo baseline keys per-request latency by rec.Partition, which is only
	// equivalent to keying by broker while partitions and brokers are 1:1 (each
	// broker leads exactly one partition).
	//
	// Scenarios derive affected-agent counts as fractions of this and guard against
	// a degenerate split via agentCount; the fractions need not divide it evenly
	// (e.g. "25% slow" rounds to 12 of 50).
	clusterSize = int32(50)

	// Scenario timing. Warmup is sized so wgo's stats tracker accumulates enough
	// 10s buckets to qualify a cluster baseline before the observed phase begins;
	// see pkg/wgo/agent_stats_tracker.go.
	warmupDuration   = 30 * time.Second
	observedDuration = 60 * time.Second
	eventSpacing     = 500 * time.Millisecond

	// appRequestTimeout caps each simulated application request: an event whose
	// partitions don't all complete within this window is recorded as failed
	// with context.DeadlineExceeded. Models a caller that won't wait forever.
	appRequestTimeout = 5 * time.Second

	// Shared settings keep the two clients comparable.
	clientLinger                 = 50 * time.Millisecond
	clientBatchMaxBytes          = 16_000_000
	clientMaxInflight            = 20
	clientMetadataRefresh        = 10 * time.Second
	clientDialTimeout            = 2 * time.Second
	clientWriteTimeout           = 10 * time.Second
	clientRequestTimeoutOverhead = 2 * time.Second
	// wgo.Config requires WriteTimeout >= ProduceRequestTimeout + overhead, so
	// derive the per-attempt produce timeout from the write budget rather than
	// reusing WriteTimeout (which would leave no room for the overhead).
	clientProduceRequestTimeout = clientWriteTimeout - clientRequestTimeoutOverhead

	// wgo-only configuration: the Hedger/Demoter/health-check knobs that have no
	// kgo equivalent. Named (rather than left as inline literals in
	// newWarpstreamClient) so the report can print the exact values the
	// simulation ran with alongside the shared client config above.
	wgoHealthCheckSlowMultiplier    = 2.0
	wgoHealthCheckMaxSlowFraction   = 0.3
	wgoHealthCheckFaultyThreshold   = 0.05
	wgoHealthCheckMaxFaultyFraction = 0.3
	wgoHedgerMinHedgeDelay          = time.Second
	wgoHedgerMaxHedgeAgents         = 3
	wgoDemoterProbeInterval         = time.Second
	wgoClusterStatsTTL              = time.Second
)

// produceClient is the minimum clients surface the simulation exercises.
type produceClient = Producer

// environment owns one kfake cluster per client so the two clients never share
// broker capacity. Each cluster is configured identically (same broker count,
// same per-broker behaviour overrides) so the comparison stays apples-to-apples.
type environment struct {
	topic         string
	numPartitions int32

	wgoCluster  *kfake.Cluster
	wgoClient   produceClient
	wgoRegistry *prometheus.Registry

	kgoCluster *kfake.Cluster
	kgoClient  produceClient

	// behaviours is the atomically-settable cluster behaviour; the kfake control
	// function and the latency hooks read through it so a single set retargets
	// every produce path.
	behaviours               *brokersBehaviourProvider
	wgoNetwork, kgoNetwork   *transportNetwork
	wgoMetadata, kgoMetadata *metadataView
}

func newScenarioEnvironment(initial brokersBehaviour, factory WorkloadFactory, sc scenario) (_ *environment, err error) {
	if sc.profile != nil && factory == nil {
		return nil, fmt.Errorf("investigation topology requires an external workload")
	}
	brokers, partitions := sc.topology()
	behaviourProvider := newBrokersBehaviourProvider(initial)
	metadataPartitions, secondaryPartitions := partitions, partitions
	if sc.profile != nil {
		if sc.profile.secondaryPartitions > 0 {
			secondaryPartitions = sc.profile.secondaryPartitions
		}
	}
	createCluster := func() (*kfake.Cluster, string, error) {
		if sc.profile != nil && sc.profile.singleAgent {
			// Seed both topics: Cluster.CreateTopic needs a kfake that requires franz-go >= 1.21.7.
			cluster, createErr := kfake.NewCluster(kfake.NumBrokers(int(brokers)),
				kfake.SeedTopics(metadataPartitions, topicName), kfake.SeedTopics(secondaryPartitions, secondaryTopicName))
			if createErr != nil {
				return nil, "", createErr
			}
			return cluster, cluster.ListenAddrs()[0], nil
		}
		return testkafka.NewCluster(partitions, topicName, testkafka.WithNumBrokers(int(brokers)))
	}
	installMetadata := func(cluster *kfake.Cluster, address string, network *transportNetwork) (*metadataView, error) {
		ctx, cancel := context.WithTimeout(context.Background(), appRequestTimeout)
		defer cancel()
		admin, createErr := kgo.NewClient(kgo.SeedBrokers(address), kgo.Dialer(network.DialContext))
		if createErr != nil {
			return nil, createErr
		}
		defer admin.Close()
		raw, requestErr := admin.Request(ctx, kmsg.NewPtrMetadataRequest())
		if requestErr != nil {
			return nil, fmt.Errorf("capture metadata: %w", requestErr)
		}
		retainedFallback := int32(-1)
		if sc.profile != nil && sc.profile.dynamicPlacement {
			retainedFallback = brokers - 1
		}
		advertised := 0
		if sc.profile != nil {
			advertised = sc.profile.advertisedAgents
		}
		return installMetadataPlacementControl(cluster, raw.(*kmsg.MetadataResponse), behaviourProvider, sc.profile != nil && sc.profile.singleAgent, sc.profile != nil && sc.profile.dynamicPlacement, retainedFallback, advertised), nil
	}
	workload := func(backend, address string, registry *prometheus.Registry, network *transportNetwork, behaviours *brokersBehaviourProvider) WorkloadConfig {
		cfg := workloadConfig(backend, address, registry, network, behaviours)
		cfg.Partitions = partitions
		cfg.SplitWrites = sc.splitWrites
		if sc.profile != nil {
			cfg.SecondaryTopic = secondaryTopicName
			cfg.SecondaryPartitions = secondaryPartitions
			cfg.SeriesPerRequest = sc.profile.seriesPerRequest
			cfg.EnforceProduceTimeout = sc.profile.enforceProduceTimeout
			cfg.Prometheus = sc.profile.prometheus
			cfg.WriteTimeout, cfg.DialTimeout = sc.profile.writeTimeout, sc.profile.dialTimeout
			cfg.RemoteTimeout, cfg.ArtificialDelay = appRequestTimeout, 500*time.Millisecond
		}
		return cfg
	}
	beh := behaviourProvider

	var clusters []*kfake.Cluster
	var clients []interface{ Close() }
	var networks []*transportNetwork

	// On any error before we hand ownership to the caller, tear down whatever
	// we already created.
	defer func() {
		if err != nil {
			for _, c := range clients {
				c.Close()
			}
			for _, network := range networks {
				network.Close()
			}
			for _, c := range clusters {
				c.Close()
			}
		}
	}()

	//
	// Warpstream client testing environment.
	//

	wgoCluster, wsAddr, err := createCluster()
	if err != nil {
		return nil, fmt.Errorf("create warpstream cluster: %w", err)
	}
	clusters = append(clusters, wgoCluster)
	installFailureControl(wgoCluster, beh, clientTypeWgo)

	wgoNetwork := newTransportNetwork((&net.Dialer{Timeout: clientDialTimeout}).DialContext)
	networks = append(networks, wgoNetwork)
	for nodeID, address := range wgoCluster.ListenAddrs() {
		wgoNetwork.register(address, int32(nodeID))
	}

	wgoMetadata, err := installMetadata(wgoCluster, wsAddr, wgoNetwork)
	if err != nil {
		return nil, err
	}
	wgoRegistry := prometheus.NewPedanticRegistry()
	var wgoClient produceClient
	if factory == nil {
		var raw *wgo.WarpstreamClient
		raw, wgoRegistry, err = newWarpstreamClient(wsAddr, wgo.WithDialer(wgoNetwork.DialContext))
		if err == nil {
			raw.SetTestProduceResponseHook(func(ctx context.Context, nodeID int32, response *kmsg.ProduceResponse, hookErr error) {
				beh.nextLatencySleepFor(ctx, clientTypeWgo, nodeID)
				if hookErr == nil && ctx.Err() == nil && response != nil {
					for _, topic := range response.Topics {
						for _, partition := range topic.Partitions {
							if partition.ErrorCode == 0 {
								wgoNetwork.observeProduce(nodeID)
								return
							}
						}
					}
				}
			})
			wgoClient = raw
		}
	} else {
		wgoClient, err = factory(workload("warpstream", wsAddr, wgoRegistry, wgoNetwork, beh))
	}
	if err != nil {
		return nil, fmt.Errorf("create warpstream workload: %w", err)
	}
	clients = append(clients, wgoClient)

	//
	// franz-go client testing environment.
	//

	kgoCluster, kgoAddr, err := createCluster()
	if err != nil {
		return nil, fmt.Errorf("create kgo cluster: %w", err)
	}
	clusters = append(clusters, kgoCluster)
	installFailureControl(kgoCluster, beh, clientTypeKgo)

	// The kgo baseline keys latency by rec.Partition (not the underlying broker
	// NodeID). Correct for this baseline because vanilla kgo with
	// ManualPartitioner does not hedge or re-route: records for partition P
	// always go to P's one designated leader, so "partition P is slow" and "P's
	// leader is slow" are equivalent from the producer's perspective.
	kgoNetwork := newTransportNetwork((&net.Dialer{Timeout: clientDialTimeout}).DialContext)
	networks = append(networks, kgoNetwork)
	for nodeID, address := range kgoCluster.ListenAddrs() {
		kgoNetwork.register(address, int32(nodeID))
	}

	kgoMetadata, err := installMetadata(kgoCluster, kgoAddr, kgoNetwork)
	if err != nil {
		return nil, err
	}
	var kgoClient produceClient
	if factory == nil {
		var inner *kgo.Client
		inner, err = newKgoClient(kgoAddr, kgo.Dialer(kgoNetwork.DialContext))
		if err == nil {
			kgoClient = &latencyDelayedKgoClient{Client: inner, behaviours: beh, client: clientTypeKgo}
		}
	} else {
		kgoClient, err = factory(workload("kafka", kgoAddr, prometheus.NewPedanticRegistry(), kgoNetwork, beh))
	}
	if err != nil {
		return nil, fmt.Errorf("create kafka workload: %w", err)
	}
	clients = append(clients, kgoClient)

	return &environment{
		topic:         topicName,
		numPartitions: partitions,
		wgoCluster:    wgoCluster,
		wgoClient:     wgoClient,
		wgoRegistry:   wgoRegistry,
		kgoCluster:    kgoCluster,
		kgoClient:     kgoClient,
		behaviours:    beh,
		wgoNetwork:    wgoNetwork, kgoNetwork: kgoNetwork,
		wgoMetadata: wgoMetadata, kgoMetadata: kgoMetadata,
	}, nil
}

// Close releases both clients and both clusters.
func (env *environment) Close() {
	env.wgoClient.Close()
	env.kgoClient.Close()
	env.wgoNetwork.Close()
	env.kgoNetwork.Close()
	env.wgoCluster.Close()
	env.kgoCluster.Close()
}

// installFailureControl installs the per-broker FAILURE-injection control
// function on the kfake cluster. Latency is NOT applied here: kfake serializes
// requests per broker, so cluster-side latency would cap per-broker throughput at
// 1/latency. Latency is applied client-side via the latency hooks so kfake
// responds instantly and broker parallelism is preserved.
func installFailureControl(cluster *kfake.Cluster, behaviours *brokersBehaviourProvider, client clientType) {
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		nodeID := cluster.CurrentNode()
		shouldFail := behaviours.nextFailureFor(client, nodeID)
		preq := req.(*kmsg.ProduceRequest)
		presp := preq.ResponseKind().(*kmsg.ProduceResponse)
		presp.Version = preq.Version
		var partitionErrCode int16
		if shouldFail {
			partitionErrCode = kerr.NotLeaderForPartition.Code
		}
		for _, rt := range preq.Topics {
			outTopic := rt.Topic
			if outTopic == "" {
				outTopic = topicName
			}
			out := kmsg.ProduceResponseTopic{Topic: outTopic, TopicID: rt.TopicID}
			for _, rp := range rt.Partitions {
				out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition: rp.Partition,
					ErrorCode: partitionErrCode,
				})
			}
			presp.Topics = append(presp.Topics, out)
		}
		return presp, nil, true
	})
}

// latencyDelayedKgoClient wraps a kgo.Client and delays every Produce promise by
// the latency drawn from the per-partition behaviour. See newScenarioEnvironment for why
// partition-keying is equivalent to broker-keying for this baseline.
//
// Latency is applied after kgo's promise fires, not on the wire, so kgo's own
// retry/timeout machinery never observes it. That's deliberate: with
// ManualPartitioner kgo cannot reroute, so its only reaction to a slow leader is
// to wait out that same leader — the app-level outcome (the per-event deadline
// trips) is identical whether the wait happens inside kgo or here. This isolates
// the single capability under test: wgo reroutes/hedges, kgo cannot.
type latencyDelayedKgoClient struct {
	*kgo.Client
	behaviours *brokersBehaviourProvider
	client     clientType
}

func (c *latencyDelayedKgoClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	c.Client.Produce(ctx, r, func(rec *kgo.Record, err error) {
		go func() {
			c.behaviours.nextLatencySleepFor(ctx, c.client, rec.Partition)
			// If the simulated latency was cut short because the caller's ctx
			// (e.g. app-request timeout) fired, surface that as the record's
			// outcome — kgo itself would have observed the broker not respond
			// in time.
			if err == nil && ctx.Err() != nil {
				err = ctx.Err()
			}
			promise(rec, err)
		}()
	})
}

func newWarpstreamClient(addr string, extra ...wgo.Opt) (*wgo.WarpstreamClient, *prometheus.Registry, error) {
	reg := prometheus.NewPedanticRegistry()
	opts := []wgo.Opt{
		wgo.WithAddress(addr),
		wgo.WithTopic(topicName),
		wgo.WithClientID("ws-simulation"),
		wgo.WithDialTimeout(clientDialTimeout),
		wgo.WithWriteTimeout(clientWriteTimeout),
		wgo.WithLinger(clientLinger),
		wgo.WithBatchMaxBytes(clientBatchMaxBytes),
		wgo.WithHealthCheckSlowMultiplier(wgoHealthCheckSlowMultiplier),
		wgo.WithHealthCheckMaxSlowFraction(wgoHealthCheckMaxSlowFraction),
		wgo.WithHealthCheckFaultyThreshold(wgoHealthCheckFaultyThreshold),
		wgo.WithHealthCheckMaxFaultyFraction(wgoHealthCheckMaxFaultyFraction),
		wgo.WithHedgerMinHedgeDelay(wgoHedgerMinHedgeDelay),
		wgo.WithHedgerMaxHedgeAgents(wgoHedgerMaxHedgeAgents),
		wgo.WithDemoterProbeInterval(wgoDemoterProbeInterval),
		wgo.WithClusterStatsTTL(wgoClusterStatsTTL),
		wgo.WithMetadataRefreshInterval(clientMetadataRefresh),
		wgo.WithProduceRequestTimeout(clientProduceRequestTimeout),
		wgo.WithProduceRequestTimeoutOverhead(clientRequestTimeoutOverhead),
	}
	c, err := wgo.NewWarpstreamClient(nil, reg, append(opts, extra...)...)
	if err != nil {
		return nil, nil, err
	}
	return c, reg, nil
}

// Keep shared settings in lockstep so configuration differences do not bias
// the comparison.
func newKgoClient(addr string, extra ...kgo.Opt) (*kgo.Client, error) {
	// Cap Produce at the version wgo pins so request and response payloads carry
	// the topic name on the wire, matching what the client emits.
	v := kversion.Stable()
	v.SetMaxKeyVersion(kmsg.Produce.Int16(), produceAPIVersion)

	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.ClientID("kgo-simulation"),
		kgo.DialTimeout(clientDialTimeout),
		kgo.MetadataMinAge(clientMetadataRefresh),
		kgo.MetadataMaxAge(clientMetadataRefresh),

		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(clientBatchMaxBytes),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(clientLinger),
		kgo.MaxProduceRequestsInflightPerBroker(clientMaxInflight),

		// Bound retries by delivery time rather than a fixed attempt count.
		kgo.RecordRetries(math.MaxInt64),
		// Retry jitter changes the fault draws consumed before the app deadline.
		kgo.RetryBackoffFn(simulationRetryBackoff),
		kgo.RecordDeliveryTimeout(clientWriteTimeout),
		kgo.ProduceRequestTimeout(clientProduceRequestTimeout),
		kgo.RequestTimeoutOverhead(clientRequestTimeoutOverhead),

		kgo.MaxBufferedRecords(math.MaxInt),
		kgo.MaxBufferedBytes(0),

		kgo.MaxVersions(v),
	}
	return kgo.NewClient(append(opts, extra...)...)
}

func simulationRetryBackoff(failures int) time.Duration {
	const minBackoff = 250 * time.Millisecond
	const maxBackoff = 5 * time.Second
	if failures <= 1 {
		return minBackoff
	}
	if failures > 5 {
		return maxBackoff
	}
	return min(minBackoff*time.Duration(1<<(failures-1)), maxBackoff)
}

func workloadConfig(backend, address string, registry *prometheus.Registry, network *transportNetwork, behaviours *brokersBehaviourProvider) WorkloadConfig {
	return WorkloadConfig{Backend: backend, Address: address, Topic: topicName, Partitions: clusterSize, Registry: registry, Dialer: network.DialContext,
		WriteTimeout: clientWriteTimeout, WriteTimeoutOverhead: clientRequestTimeoutOverhead, DialTimeout: clientDialTimeout,
		ProduceLatency: func(address string) time.Duration {
			network.mu.Lock()
			nodeID, ok := network.brokers[address]
			network.mu.Unlock()
			if !ok {
				return 0
			}
			client := clientTypeWgo
			if backend == "kafka" {
				client = clientTypeKgo
			}
			return behaviours.nextLatencyFor(client, nodeID)
		},
		ObserveProduce: func(address string) {
			network.mu.Lock()
			nodeID, ok := network.brokers[address]
			network.mu.Unlock()
			if ok {
				network.observeProduce(nodeID)
			}
		},
	}
}
