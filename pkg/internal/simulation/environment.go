package main

import (
	"context"
	"fmt"
	"math"
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

	// Client configuration, matching pkg/storage/ingest/writer_client.go so the
	// comparison reflects production behaviour. Differences from production are
	// limited to test-only concerns (no SASL/TLS, no metrics hooks, no logger).
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
)

// produceClient is the minimum clients surface the simulation exercises.
type produceClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Close()
}

// environment owns one kfake cluster per client so the two clients never share
// broker capacity. Each cluster is configured identically (same broker count,
// same per-broker behaviour overrides) so the comparison stays apples-to-apples.
type environment struct {
	topic         string
	numPartitions int32

	wgoCluster  *kfake.Cluster
	wgoClient   *wgo.WarpstreamClient
	wgoRegistry *prometheus.Registry

	kgoCluster *kfake.Cluster
	kgoClient  produceClient

	// behaviours is the atomically-settable cluster behaviour; the kfake control
	// function and the latency hooks read through it so a single set retargets
	// every produce path.
	behaviours *brokersBehaviourProvider
}

// newEnvironment builds two identically-configured clusters (one per client) with
// the given initial per-broker behaviours. The caller owns the result and must
// Close it.
func newEnvironment(initial brokersBehaviour) (_ *environment, err error) {
	beh := newBrokersBehaviourProvider(initial)

	var clusters []*kfake.Cluster
	var clients []interface{ Close() }

	// On any error before we hand ownership to the caller, tear down whatever
	// we already created.
	defer func() {
		if err != nil {
			for _, c := range clients {
				c.Close()
			}
			for _, c := range clusters {
				c.Close()
			}
		}
	}()

	//
	// Warpstream client testing environment.
	//

	wgoCluster, wsAddr, err := testkafka.NewCluster(clusterSize, topicName, testkafka.WithNumBrokers(int(clusterSize)))
	if err != nil {
		return nil, fmt.Errorf("create warpstream cluster: %w", err)
	}
	clusters = append(clusters, wgoCluster)
	installFailureControl(wgoCluster, beh, clientTypeWgo)

	wgoClient, wgoRegistry, err := newWarpstreamClient(wsAddr)
	if err != nil {
		return nil, fmt.Errorf("create warpstream client: %w", err)
	}
	clients = append(clients, wgoClient)

	// Latency is injected at KafkaDirectProducer's post-response hook so the
	// TrackingProducer below it observes the simulated latency and feeds the
	// Hedger/Demoter realistic stats. Installed once at construction so the hook
	// is never reinstalled concurrently with in-flight produce traffic.
	wgoClient.SetTestProduceResponseHook(func(ctx context.Context, nodeID int32, _ *kmsg.ProduceResponse, _ error) {
		beh.nextLatencySleepFor(ctx, clientTypeWgo, nodeID)
	})

	//
	// franz-go client testing environment.
	//

	kgoCluster, kgoAddr, err := testkafka.NewCluster(clusterSize, topicName, testkafka.WithNumBrokers(int(clusterSize)))
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
	kgoInner, err := newKgoClient(kgoAddr)
	if err != nil {
		return nil, fmt.Errorf("create kgo client: %w", err)
	}
	clients = append(clients, kgoInner)
	kgoClient := &latencyDelayedKgoClient{Client: kgoInner, behaviours: beh, client: clientTypeKgo}

	return &environment{
		topic:         topicName,
		numPartitions: clusterSize,
		wgoCluster:    wgoCluster,
		wgoClient:     wgoClient,
		wgoRegistry:   wgoRegistry,
		kgoCluster:    kgoCluster,
		kgoClient:     kgoClient,
		behaviours:    beh,
	}, nil
}

// Close releases both clients and both clusters.
func (env *environment) Close() {
	env.wgoClient.Close()
	env.kgoClient.Close()
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
// the latency drawn from the per-partition behaviour. See newEnvironment for why
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
			if err != nil {
				// kgo's own machinery already resolved this record against the
				// real (instant) cluster — e.g. it retried a dead leader until
				// RecordDeliveryTimeout. That time was really spent, so the
				// latency model has nothing to add. Still draw, so the failure
				// scenarios consume the same RNG stream as before.
				c.behaviours.nextLatencyFor(c.client, rec.Partition)
				promise(rec, err)
				return
			}
			promise(rec, c.awaitSimulatedLatency(ctx, rec.Partition))
		}()
	})
}

// kgoAttemptDeadline is the per-attempt budget kgo enforces on a produce
// request: the timeout written into the request plus the read overhead. Mirrors
// what newKgoClient passes kgo, and equals wgo's own attemptCtx budget in
// KafkaDirectProducer.ProduceSync, so both clients abandon a stalled attempt at
// the same point.
const kgoAttemptDeadline = clientProduceRequestTimeout + clientRequestTimeoutOverhead

// awaitSimulatedLatency replays kgo's own attempt loop against the simulated
// latency and returns the record's outcome.
//
// Why this exists: kfake answers instantly and the draw is applied client-side
// (see the type comment), so kgo's real retry machinery never observes the
// simulated latency. wgo's does — its hook runs on attemptCtx inside
// KafkaDirectProducer.ProduceSync, which converts an overrunning draw into a
// failed attempt the Hedger can route around. Replaying the loop here restores
// the one recovery path that asymmetry removed: because the model redraws
// latency per request, an attempt abandoned at the deadline is retried against
// the same leader with a fresh draw — the same "the tail is per-request, not
// per-broker" property wgo's hedge exploits. Without it the baseline is
// handicapped in a way wgo is not, which matters now that minSuccessDeltaVsKgo
// gates the run on kgo's measured rate.
func (c *latencyDelayedKgoClient) awaitSimulatedLatency(ctx context.Context, partition int32) error {
	// RecordDeliveryTimeout caps the record across every attempt, so the budget
	// starts now rather than per attempt.
	deliveryDeadline := time.Now().Add(clientWriteTimeout)

	for {
		draw := c.behaviours.nextLatencyFor(c.client, partition)
		if draw <= 0 {
			return nil
		}

		// The attempt resolves when the broker answers, or is abandoned at the
		// per-attempt deadline, whichever comes first.
		attempt := min(draw, kgoAttemptDeadline)

		// Either way it can't outlive the record's remaining delivery budget.
		if remaining := time.Until(deliveryDeadline); attempt >= remaining {
			if err := sleepCtx(ctx, remaining); err != nil {
				return err
			}
			return kgo.ErrRecordTimeout
		}
		if err := sleepCtx(ctx, attempt); err != nil {
			return err
		}
		if draw <= kgoAttemptDeadline {
			return nil
		}
		// Attempt abandoned mid-flight. ManualPartitioner pins the record to the
		// same leader, so the retry is another draw against the same broker.
	}
}

// sleepCtx sleeps for d, returning ctx.Err() if ctx fires first. A non-positive
// d still reports a already-cancelled ctx, so callers can treat the returned
// error as the record's outcome unconditionally.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newWarpstreamClient(addr string) (*wgo.WarpstreamClient, *prometheus.Registry, error) {
	reg := prometheus.NewPedanticRegistry()
	c, err := wgo.NewWarpstreamClient(nil, reg,
		wgo.WithAddress(addr),
		wgo.WithTopic(topicName),
		wgo.WithClientID("ws-simulation"),
		wgo.WithDialTimeout(clientDialTimeout),
		wgo.WithWriteTimeout(clientWriteTimeout),
		wgo.WithLinger(clientLinger),
		wgo.WithBatchMaxBytes(clientBatchMaxBytes),
		wgo.WithHealthCheckSlowMultiplier(2.0),
		wgo.WithHealthCheckMaxSlowFraction(0.3),
		wgo.WithHealthCheckFaultyThreshold(0.05),
		wgo.WithHealthCheckMaxFaultyFraction(0.3),
		wgo.WithHedgerMinHedgeDelay(time.Second),
		wgo.WithHedgerMaxHedgeAgents(3),
		wgo.WithDemoterProbeInterval(time.Second),
		wgo.WithClusterStatsTTL(time.Second),
		wgo.WithMetadataRefreshInterval(clientMetadataRefresh),
		wgo.WithProduceRequestTimeout(clientProduceRequestTimeout),
		wgo.WithProduceRequestTimeoutOverhead(clientRequestTimeoutOverhead),
	)
	if err != nil {
		return nil, nil, err
	}
	return c, reg, nil
}

// newKgoClient mirrors the kgo.Client configuration that
// pkg/storage/ingest/writer_client.go applies in production. Any drift would
// make the comparison misleading, so the two option sets are kept in lockstep.
func newKgoClient(addr string) (*kgo.Client, error) {
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

		// Mirrors writer_client.go: unlimited retries, deadline on the max time
		// a record can take to be delivered.
		kgo.RecordRetries(math.MaxInt64),
		kgo.RecordDeliveryTimeout(clientWriteTimeout),
		kgo.ProduceRequestTimeout(clientProduceRequestTimeout),
		kgo.RequestTimeoutOverhead(clientRequestTimeoutOverhead),

		kgo.MaxBufferedRecords(math.MaxInt),
		kgo.MaxBufferedBytes(0),

		kgo.MaxVersions(v),
	}
	return kgo.NewClient(opts...)
}
