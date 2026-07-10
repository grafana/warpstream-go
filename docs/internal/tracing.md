# Tracing design

This client is a drop-in replacement for a franz-go producer, including distributed
tracing: a caller that instruments its franz-go client with [`kotel`](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel) gets the
same producer spans and `traceparent` propagation after swapping in this client.

## The problem

kotel traces produces through two franz-go hooks:

- `OnProduceRecordBuffered`: start the "publish" span, inject the trace-context header
  into the record.
- `OnProduceRecordUnbuffered`: end the span.

franz-go fires these from its producer state machine. This client bypasses that machine
and produces via `Broker.Request`, so those hooks would never fire for a produce here,
and a kotel tracer passed via `WithHooks` would trace nothing on the produce path.

## The approach

The client drives the two produce-record hooks itself, at the same lifecycle points
franz-go uses:

- **Buffered** fires at the top of `Produce`/`ProduceSync`, before the size and routing
  gates and before encoding — mirroring franz-go, which fires the buffered hook before a
  record can be failed. The record's parent context is seeded from the produce ctx when
  unset, so a sampled-only tracer can read the sampling decision. A tracer injects its
  `traceparent` header here, while the record is still mutable; the header is then encoded
  into the wire batch and travels to the consumer.
- **Unbuffered** fires before the caller observes a record's outcome. For `Produce` the
  promise is wrapped so the hook runs just before it, mirroring franz-go (unbuffered hook,
  then promise). For `ProduceSync` the hooks fire, in input order, on the calling goroutine —
  after it has finalized every result.

Callers pass their tracer via the existing `WithHooks`, exactly as for a franz-go client,
so this needs no new API and no OpenTelemetry dependency in the client — it relies only on
the `kgo.HookProduceRecordBuffered`/`kgo.HookProduceRecordUnbuffered` interfaces.

## Scope

Produce spans only. Consume-side tracing is unchanged franz-go behaviour via the embedded
`*kgo.Client`. There are no per-hedge-leg or per-wire-request spans — matching kotel, which
emits one span per record regardless of how many wire attempts a produce takes underneath.
