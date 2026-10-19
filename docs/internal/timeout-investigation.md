# Timeout amplification with draining requests

The `1% timeouts across all agents` scenario adds about 10 seconds of latency to
1% of Produce responses. Each Mimir HTTP event writes across 50 partitions and
succeeds only if every partition completes before its 5-second HTTP deadline.
This measures HTTP completion, not durability.

## Comparison

CI uses three runs and reports each client's minimum successful write count:

- Before draining losing attempts: **80/120** Mimir writes
  ([#125 run](https://github.com/grafana/warpstream-go/actions/runs/39301769342),
  client `33cfe5b`).
- With draining: **15/120** Mimir writes
  ([#127 run](https://github.com/grafana/warpstream-go/actions/runs/39314930336),
  client `6367ade`). The direct-client result remains **120/120**.

Matched local runs with the unchanged Mimir harness, pinned Mimir source, one
scenario at a time, 50 partitions, 30-second warmup and 60-second observation:

- Before #127: **101/120**.
- #127: **19/120**.
- #127 with only the Hedger's faulty-fraction suppression disabled: **15/120**.

These local values are individual runs, not worst-of-three estimates. The
suppression-disabled build is a diagnostic control, not a proposed policy.

## Mechanism

The Mimir proxy injects latency before forwarding ordered Kafka responses. One
10-second stall therefore blocks later responses on that TCP connection,
including responses to otherwise healthy requests. The direct runner sleeps
after `Broker.Request` returns, so a stalled call does not block sibling calls.
Its perfect result does not exercise the same transport failure mode.

Draining a losing request preserves its connection until the original deadline.
That fixes early hedge cancellation closing connections during ordinary slow
requests, but also lets these much longer stalls block the connection for longer.
When the retained deadline expires, franz-go still closes the shared connection
and fails its other in-flight requests. The deadline regression test demonstrates
this with a sibling whose own deadline has not expired.

Fallback requests use the same per-agent Produce connection as primaries. A
fallback can therefore land behind another write's stalled primary. Fallback
waves wait for their responses before advancing to another candidate; the
5-second HTTP deadline can expire before either leg resolves. Fifty-partition
fanout magnifies these correlated stalls into application failures.

The resulting connection failures also feed the health tracker. A local scrape
observed **32 healthy-state direct timeouts, 812 healthy-state transport errors**
and **54% of agents classified faulty**. The default 30% faulty-fraction gate then
suppresses proactive hedges. Disabling that gate alone did not improve success,
so it does not explain the regression by itself.

## Scope and follow-up

The stacked investigation adds interval diagnostics and deterministic coverage of
ordered-response blocking and deadline collateral failures. It preserves the
transport and health policy from #127; the timeout regression remains open.

Further experiments should isolate stalled connections from fallback traffic or
let an unresolved fallback race another candidate within the existing attempt
budget. Any policy change needs matched timeout and 25%-slow-agent comparisons:
restoring early connection cancellation would reintroduce the failure #127 fixes.

To reproduce using binaries built from each client revision:

```sh
GOMAXPROCS=2 go run . -mimir-binary /absolute/path/to/mimir \
  -scenario '^1% timeouts across all agents$' -parallel 1 -check=false \
  -report-filepath /tmp/timeouts.md -json-report-filepath /tmp/timeouts.json
```

Run from `pkg/internal/simulation/mimir`. Use `build-mimir.sh` for the pinned
source and record the client revision with each output. `-check=false` retains
missed scenario expectations in the report; an exit-zero run is not a passed
success-rate gate.
