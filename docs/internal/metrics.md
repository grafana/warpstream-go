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
hedging, agent demotion, direct-request and attempt accounting, and
client-boundary record counters. They carry a `warpstream_` prefix so they never
collide with franz-go/kprom names and are unambiguously backend-specific.

## Caller hooks

Callers can supply their own franz-go hooks; they are attached to the embedded
client alongside kprom, so a caller can add its own transport-layer metrics or
tracing.
