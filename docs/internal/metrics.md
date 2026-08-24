# Metrics design

This client aims to be a **drop-in replacement** for a franz-go producer from a
metrics standpoint: a dashboard or alert written against a standard franz-go
client (instrumented with [`kprom`](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kprom))
should keep working when the producer is swapped for this client.

To make that possible, the client's metrics fall into three groups.

## 1. Transport metrics — from kprom

Connection and request-transport metrics (`connects_total`,
`connect_errors_total`, `disconnects_total`, `write_bytes_total`,
`write_errors_total`, `read_bytes_total`, `read_errors_total`, the request
E2E/throttle histograms, …) come from kprom, attached as a hook on the embedded
franz-go client.

The client's produce requests still travel over franz-go's broker connections,
so kprom's connection and transport hooks fire normally and these metrics need
no special handling.

## 2. Producer-state metrics — tracked by the client, under kprom names

kprom derives a second group from franz-go's producer state machine and
producer buffer:

- `produce_records_total`, `produce_batches_total`, `produce_bytes_total`,
  `produce_compressed_bytes_total`
- `buffered_produce_records_total`, `buffered_produce_bytes`

This client does **not** use franz-go's producer (it batches and produces
itself), so kprom would report all of these as a constant zero. Instead the
client tracks them itself and registers them under the **same names**:

- the counters count the records, batches and bytes written to the wire, once
  per request and only when the whole request is acked. A request is
  all-or-nothing, so a failed attempt counts nothing and is retried — matching
  franz-go, which counts each batch once on success;
- the buffered gauges report the client's own in-flight buffer.

kprom registers all of its metrics unconditionally and offers no way to disable
individual ones, so the client gives kprom a filtering registerer that drops
this group (matched by bare metric name, independent of any prefix a caller
applies) and registers its own versions in their place. This keeps the names
identical without a duplicate registration.

## 3. Warpstream-specific metrics — `warpstream_` prefix

Metrics with no franz-go counterpart describe behaviour unique to this client:
hedging, agent demotion, direct-request and attempt accounting, client-boundary
record counters, metadata refresh, and the cluster view the Hedger and Demoter
read. They carry a `warpstream_` prefix so they never collide with
franz-go/kprom names and are unambiguously backend-specific.

### Direct-request counters and `agent_state`

`warpstream_produce_direct_requests_total` and
`warpstream_produce_direct_requests_failed_total` are labeled
`agent_state="healthy|demoted"`. The value is routing-time policy state at the
DirectProducer, not a health check of the remote agent:

- `healthy` — the attempt is ordinary traffic.
- `demoted` — at least one partition group in the request is a demoted-agent
  probe. Per-partition linger merge is last-wins: a later healthy routing
  for the same partition overwrites an earlier probe, and that is the
  `nodeState` `shouldHedge` uses for zero-delay. Across partitions in one
  wire attempt, both `shouldHedge` and this counter treat any remaining
  demoted partition as a probe, so a mixed flush is not delayed or counted
  as healthy traffic.

Adding `agent_state` is a Prometheus descriptor change: existing unlabeled
series disappear and are replaced by labeled ones. Histograms
(`warpstream_produce_direct_request_latency_seconds`,
`warpstream_produce_requests_attempts`) stay unlabeled by `agent_state`.

Failed-request reasons are unchanged (`timeout`, `canceled`,
`kafka_retriable_error`, `kafka_non_retriable_error`, `unknown`).

### Metadata refresh results

`warpstream_metadata_refresh_results_total{trigger,result}` counts every
AgentPool Metadata refresh this client owns:

- `trigger`: `periodic` (the background ticker), `on_demand` (`refreshNowCh`,
  currently routing-miss only). The synchronous constructor Refresh is not
  counted: failure is "client never started", success is empty→first Agents.
- `result`: `membership_changed` when the sorted `AgentPool.Agents()` NodeID
  list differs after a successful refresh; `unchanged` when membership is
  identical (leader-only or topic-only Metadata updates count as unchanged);
  `failed` when Refresh returns an error and the previous snapshot is kept.

### Demotion

`warpstream_demoter_demoted_agents` remains the aggregate count of agents this
client is currently routing around (0 while demotion is globally suppressed).

`warpstream_demoter_demotion_suppressed{reason}` is one 0/1 series per
suppression reason (`no_cluster_stats`, `many_faulty_agents`,
`many_faulty_agents_small_cluster`). At most one series is 1; `sum()` is the
plain suppressed signal.

`warpstream_agent_demoted{node_id}` is the same signal per current pool Agent:
`1` means this client is routing around that NodeID, `0` means it is eligible
for normal routing. During global suppression every series is `0`. Series for
NodeIDs that have left the pool disappear on the next scrape.

`demotion_suppressed` and `agent_demoted` are collected together from one
ClusterStats snapshot so a scrape cannot see suppression on one and leftover
demotion on the other. `demoted_agents` is a GaugeFunc and may see its own
snapshot. Collection does not call `Candidates`, `shouldProbe`, or
`observeClusterStats`.

`warpstream_demoter_transitions_total{transition="demoted|restored"}` counts
policy edges. Purging a departed agent on Metadata refresh is not a restore.

### Cluster stats (policy-time gauges)

These gauges snapshot the `ClusterStats` value Hedger.shouldHedge and
Demoter.Candidates actually read. They are not updated by Prometheus scrapes of
the Demoter gauges (those also call `ClusterStats`, but wrapping the tracker
would mix scrape traffic into the policy view).

On `WarpstreamClient.Produce` (Mimir's path: one `Produce` per record),
`Demoter.Candidates` reads + observes at **routing time**, once per record,
before linger. `Hedger.shouldHedge` does the same once per **flush**.
High ingest is therefore per-record observe, not per-batch. That is cheap:
both sites share the `CachedAgentStatsTracker` key (`SlowMultiplier`,
`FaultyThreshold`), so the O(agents) walk runs at most once per
`ClusterStatsTTL` (Mimir: 1s); other calls are an RLock + map lookup.
`observeClusterStats` is eight atomic `Gauge.Set`s, no alloc. Scrapes
read the last values; extra writes do not grow cardinality.
`last_observed` is the later of those reads (often the same Unix second),
not two independent samples. shouldHedge can return before `ClusterStats`
(demoted probe, no per-agent stats) — then only Candidates stamps.

When the last policy read returned no cluster view, `warpstream_cluster_stats_available`
is `0` and the other gauges are `NaN`. `warpstream_cluster_stats_last_observed_timestamp_seconds`
is the wall clock of that read (`NaN` until the first one).

