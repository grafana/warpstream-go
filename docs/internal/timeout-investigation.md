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

## Fix

Primary and fallback requests use separate franz-go clients and broker
connections. A blocked primary cannot hold a fallback behind it. All transports
use the same authentication, deadlines and broker hooks. Only the primary owns
SASL shutdown and lifecycle hooks, so sharing does not double-close credential
resources or register metrics twice. Two additional Produce connections may be
opened per agent used by fallbacks. Each fallback flush chooses the connection
with fewer outstanding requests to its target agent. Started losing requests
retain their slot until they finish draining; cancellation cannot make a blocked
connection appear idle.

When the original hedge decision permits racing, an unresolved fallback starts
another candidate after the same hedge delay. This also applies after an early
primary failure. Earlier legs stay eligible to win; exhausting the candidate
budget stops dispatch but still waits for outstanding legs. Suppressed hedge
decisions keep sequential failure retries. The maximum distinct-agent budget and
bounded draining remain unchanged.

Canceled losing primary legs can still hit retained deadlines and close their
connections. Collateral connection errors after cancellation remain in raw metrics but are
excluded from agent health. Actual request deadlines, live leg failures and
broker-reported errors still count. This prevents completed
writes' losing drains from building a false cluster-wide fault signal.

The diagnostics and deterministic tests cover response ordering, primary/fallback
connection isolation, deadline collateral failures, late fallback wins, primary
failure followed by a stalled fallback, and shared-resource shutdown.

Isolation alone passed 59/120 HTTP writes in an initial local timeout run.
Isolation plus timed races passed 95, 80 and 49 out of 120 across three runs;
the last still failed the success gate after collateral errors suppressed
hedging. The final health correction prevents that false signal while retaining
genuine deadline failures. The complete matched comparison appears below.

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

## Initial validation

Three local runs per revision used the same pinned Mimir binary source,
50-partition workloads, 30-second warmup, 60-second observation, `GOMAXPROCS=2`
and `-parallel 2` for the timeout and 25%-slow scenarios. The client revisions
were #127 (`6367ade`) and the complete fix (`0087567`).

- Timeout HTTP successes: **29, 18, 15 / 120** before; **86, 74, 120 / 120** after.
  Worst-of-three improves from **15/120 to 74/120**. Every fixed run meets the
  scenario's 50% success floor and 20-point advantage over kgo.
- 25%-slow HTTP successes: **120/120 in all three runs** for both revisions.
- No faulty-fraction hedge suppression occurred in the three corrected timeout
  runs. Raw transport failures still appear in the diagnostic counters.
- Slow-agent hedge overhead increased from **22.7–22.9%** to **27.2–27.7%**
  above primary attempts; each fallback agent can also need an extra connection.

Passing that 50% scenario floor did not recover historical performance. The
single fallback transport could still stall behind another fallback, even with
zero health-gate suppression. This motivated balancing across two fallback
connections per agent. The pool is bounded; it cannot guarantee success if both
connections stall, the candidate budget runs out, or the HTTP deadline expires.

## Released baseline and fallback balancing

The same paired workload was repeated three times with released v0.1.4
(`fc5c579`) and the same pinned Mimir source. v0.1.4 completed **107, 116, 110 / 120**
timeout writes, worst **107/120**, confirming that the initial **74/120** result
still regressed. Its slow-agent results were **113, 120, 118 / 120**.


With two fallback connections balanced by outstanding requests, the timeout
results were **120, 120, 117 / 120**, worst **117/120**. Slow-agent results were
**120/120 in all three runs**. No faulty-fraction suppression occurred in the
balanced timeout runs. Timeout hedge overhead was **19.7–21.2%**; slow-agent hedge
overhead was **27.4–27.7%**, with **10.8–12.5%** of slow-agent HTTP writes taking
over 2.5 seconds. The third timeout run still lost three writes to the HTTP
deadline, so these runs establish an improvement over the released baseline,
not a guarantee of lossless HTTP completion under injected stalls.

The deterministic pool test holds a wire request open after its caller is
canceled and verifies that the next fallback uses the other connection without
waiting. A separate test verifies that load on one agent does not affect another
agent's connection choice. Pool accounting is released only after the actual
wire attempt returns, and idle agent entries are removed.
