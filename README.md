# wgo – A Warpstream-aware Kafka client

> [!WARNING]
> **Experimental.** This client is not battle tested yet. Use it at your own risk, and please don't run it in production unless you've extensively tested it first. Testing and feedback are very welcome!

A Kafka client tailored to [Warpstream](https://www.warpstream.com/)'s stateless-agent architecture.

## What it is

`WarpstreamClient` produces records to a Kafka-protocol-compatible cluster — specifically Warpstream — using a custom orchestration layer that sits on top of [franz-go](https://github.com/twmb/franz-go). It reuses franz-go for connection management, Metadata refresh, TLS/SASL, and wire encoding (`pkg/kmsg`), but bypasses franz-go's standard producer state machine. The public surface mirrors `kgo.Client`'s producer methods (`Produce`, `ProduceSync`, `Close`), and `WarpstreamClient` embeds `*kgo.Client` so non-producer methods (`Ping`, `MetadataRequest`, …) work transparently.

## Why we built it

In vanilla Kafka every partition has exactly one leader broker. The Kafka protocol requires the client to send each partition's batch to its current leader; sending the same batch to a non-leader broker fails with `NotLeaderForPartition`. This makes retries on a different broker, or hedging, impossible.

Warpstream is different. Its agents are fully stateless — every agent can serve every partition at any time. The "leader" returned in a Metadata response is a Kafka-protocol concession, not a real routing constraint.

In some applications – like [Grafana Mimir](https://github.com/grafana/mimir) – the ingestion path tolerates duplicates: writes are at-least-once and have no in-partition sequencing requirement. So the cost franz-go pays to honor the Kafka protocol — pinning every partition to one specific broker — buys these applications nothing on Warpstream. In this custom client, we trade that guarantee away and get something more valuable in return: the ability to **race a slow primary against a different agent and accept whichever responds first**, plus the ability to **route away from a sick agent entirely** while it's degraded.

That's the thing this client does that franz-go can't.

## Non-negotiable principles

This client has been designed around the following non-negotiable assumptions:

1. **Warpstream-specific.** Hedging the same batch across agents only works because any agent can serve any partition. Pointed at vanilla Kafka, the secondary leg would fail with `NotLeaderForPartition`.
2. **At-least-once delivery only.** Duplicates are tolerable. Any code that assumes exactly-once or in-partition record ordering must stay on franz-go.
3. **No transactional or idempotent producer support.** `DisableIdempotentWrite()` semantics are baked in — no `producerId`/`producerEpoch`/`baseSequence` handshake.
4. **Background-only Metadata refresh.** Produce requests never block on Metadata; an out-of-date pool view is preferred to an in-flight stall.

## How it works

The client is a small pipeline of independent components, each with a narrow concern. From caller to wire:

```
Produce(record) ──► routing ──► cluster buffer ──► per-agent buffer ──► Hedger ──► DirectProducer ──► kgo broker.Request
                    (Demoter:                      (linger, batch)              (race, retry)        (wire)
                     skip/probe
                     sick agents)
```

The `Demoter` is not a separate pipeline stage: it wraps the routing strategy, so "route away from a sick agent" happens inside the routing step that every Produce call and every hedge retry consults.

### Routing: pick a primary per partition

For every record the client asks a `PartitionAssignmentStrategy` for an ordered candidate list. The first entry is the primary, the rest are fallbacks. Two properties matter:

- **Deterministic.** Given the same Metadata view, every client instance picks the same primary and the same secondary for a given partition. Hedge load is predictable and analysable instead of randomly smeared across agents.
- **State-aware.** A wrapper around the base strategy (the **Demoter**, see below) can mark an agent as demoted so it's elided from the candidate list or surfaced as a probe.

### Buffering: linger by destination agent, not by partition

Records are buffered through a `ClusterRecordBuffer`, which bins them by the destination agent picked at routing time, then through a per-agent `AgentRecordBuffer`, which applies a configurable linger window before flushing. Each flush ships one Produce request to one agent carrying batches for as many partitions as the buffer accumulated.

Linger by agent (not by partition) is what lets a single wire request fan out across many partitions, and what makes the hedge cascade efficient: a hedge wave sends one request per fallback-agent too, not one per partition.

### Hedger: race the primary against a fallback

When a per-agent buffer flushes, the resulting batch goes to the `Hedger`, which decides whether to race the primary against a secondary agent. Per call it produces one of three outcomes:

- **Primary wins outright.** The primary leg returns first with a clean result; we surface it and the secondary never fires.
- **Primary fails, cascade retries.** A leg counts as failed if *any* partition in its response errors (per-leg outcome is all-or-nothing — successful partitions are not credited when a sibling fails). The Hedger walks down the candidate list and re-attempts the unresolved partitions, up to `MaxHedgeAgents` total per partition. Different partitions can land on different agents in the same wave when their candidate orderings diverge.
- **Hedge timer fires first.** The primary is taking longer than expected. The Hedger fires a fallback alongside the in-flight primary; whichever returns first with a usable result wins, and the loser is cancelled.

The hedge decision is **data-driven**, not unconditional. The Hedger consults rolling per-agent latency and error stats (the same window the `Demoter` uses, so both components agree on "is agent X bad?"). When the primary looks unhealthy the hedge delay is the cluster's baseline latency, so the fallback fires quickly. When the primary looks healthy the delay is multiplied so we don't stampede the cluster during normal operation — but we still hedge, because tail-latency amplification (every application request fans out to all partitions; one slow primary stalls the whole request) is the dominant cost we're trying to avoid. Either way the computed delay is floored at the configured `MinHedgeDelay` (probes are the exception — they fire the fallback immediately with no delay).

Two cluster-wide guard rails suppress hedging entirely:
- **Slow-fraction guard.** Too many agents are slower than baseline → hedging would amplify the problem; back off.
- **Faulty-fraction guard.** Too many agents are erroring → ditto.

These guards make the client fail safely under correlated outages instead of multiplying produce traffic just when the cluster can least afford it.

### Demoter: route away from sick agents

The `Demoter` wraps the routing strategy and adds a "skip demoted agents, occasionally probe them" policy. An agent crosses into the demoted set when its rolling error rate exceeds a configured absolute threshold (`FaultyThreshold`), gated by a volume-adaptive minimum request count so a fluke or two on a barely-used agent can't trip it. While demoted:

- Most routing decisions skip the demoted agent to the next healthy candidate — no traffic, no further damage.
- A small fraction of decisions (governed by a per-agent `ProbeInterval`) are routed back as **probes**: explicit hedge-eligible requests where we already expect the primary to fail, so the `Hedger` fires the fallback immediately with no delay.

Demotion is driven only by error rate, not by latency — the `Hedger` already covers "slow but working" agents, so the Demoter only has to handle "really broken" ones. Two properties make error rate the safer demotion signal:

- **It doesn't cascade.** Routing around a faulty agent moves its traffic to healthy agents without making *them* faulty. Routing around a *slow* agent adds that load to the others, which can push the next-slowest over the line and demote it too, and so on. Latency is coupled across agents through load; error rate is not.
- **Its threshold is absolute.** Faulty means "windowed error rate over a fixed `FaultyThreshold`", so the bar to recover never moves. A latency threshold would have to be relative to the cluster baseline, which itself drifts as load redistributes — a moving target for a recovering agent.

Recovery is *not* instantaneous, and this is symmetric between error- and latency-based signals: both read the same rolling stats window (~60s), so a demoted agent only clears once its bad observations age out of the window and the probe traffic refills it with good ones. What the error path adds is hysteresis that keeps recovery from stalling on low probe volume: once an agent is demoted, its per-agent sample gate relaxes to a single request, so even sparse probes are enough to re-evaluate it, and it flips back to healthy the moment the windowed rate falls below the threshold. If probes are so sparse the agent drops below the minimum filled-bucket count, the Demoter fails open and treats it as healthy rather than leaving it stuck demoted.

The `Hedger` handles the "slow but working" case; the Demoter handles the "really broken" case. Together they cover the spectrum without either component having to decide both.

### Stats: a single rolling-window view shared by both controllers

Every produce attempt (primary or hedge wave) feeds latency and error data into a rolling per-agent stats tracker. Both the `Hedger` and the `Demoter` read from the same tracker through the same `HealthCheckConfig`, so they always agree on which agents are slow and which are faulty. The tracker is bucketed over a rolling 60s window (6 buckets, 10s each) so a transient blip doesn't trigger immediate intervention and a recovery is reflected quickly.

### Wire layer: franz-go, used as a transport

The bottom layer is a thin `KafkaDirectProducer` that hands a built `ProduceRequest` to `kgo.Client.Broker().Request()`. We do not use `kgo.Client.Produce()` because that's where franz-go's leader-pinning lives. By dropping into the raw `Broker.Request` path we keep all of franz-go's connection pooling, TLS/SASL, and wire encoding while taking complete control of which broker each request actually goes to.

### Hooks

The client accepts [franz-go hooks](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Hook)
via `WithHooks`, so you can attach your own metrics, tracing, or connection
instrumentation. However, the following hooks are currently **not supported**:

- `HookProduceBatchWritten`
- `HookProduceRecordPartitioned`

### Tracing

The client is a drop-in for a franz-go producer traced with
[`kotel`](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel): pass your kotel hooks
(or any hook implementing the produce-record hooks) via `WithHooks`. On each produce your
tracer starts a producer span and injects the trace-context header into the record, so the
record carries the trace to downstream consumers; the span ends when the produce is
acknowledged or fails. Consume-side tracing works through the embedded client with no extra
wiring. See [`docs/internal/tracing.md`](docs/internal/tracing.md) for the design.

## FAQ

### Is this a replacement for franz-go?

No. [franz-go](https://github.com/twmb/franz-go) is an excellent, full-featured Kafka client, and we recommend it for the vast majority of use cases. This client is not a general-purpose client and it is not trying to compete with franz-go — in fact it is built *on top of* franz-go, which it relies on for connection management, Metadata, TLS/SASL, and wire encoding.

### When should I use this client instead?

Only when **all** of the following hold:

- You're producing to a [Warpstream](https://www.warpstream.com/) cluster (or another backend with fully stateless, any-agent-serves-any-partition brokers).
- You want better tail-latency and resilience on Produce requests via hedging and routing around sick agents.
- Your workload tolerates at-least-once delivery and does not need ordering, idempotent, or transactional producers.

If any of these don't apply, use franz-go. See [Non-negotiable principles](#non-negotiable-principles) for the full list of assumptions.
