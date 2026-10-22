# Mimir distributor simulations

Run `make simulation-mimir` from the repository root. The build downloads the
revision pinned in `mimir-version.txt` and builds its actual `cmd/mimir` with a
`go.mod` replacement pointing at this checkout. Mimir source is unchanged. Build
artifacts and the linked module manifest live under `.build/`.

Each standard scenario launches two separate distributor processes, using Mimir's
`warpstream` and `kafka` ingest backends. Requests enter through the public
Prometheus remote-write HTTP endpoint, with one tenant and 1600 series per request
across 50 ingest partitions. The harness imports no Mimir implementation code.
Warmup must receive successful Kafka acknowledgements for every partition. After
the initial 30 seconds, it continues with fresh series for up to 30 more seconds
until every writer has covered every partition. Missing coverage still fails the run.
A read-only Consul HTTP fixture supplies the partition ring. Ingester migration
writes are disabled; no ingesters are launched.

TCP proxies connect the processes to the shared runner's fake brokers and fault
transport. Metadata broker addresses are rewritten to proxy listeners, including
replacement addresses, so discovered connections also traverse the fault layer.
All other Metadata fields and Kafka responses remain intact. Successful Produce
responses are observed on the wire; counters are scraped from Mimir's `/metrics`.
Recovery evidence counts wire responses read, including probes and responses after
an HTTP request completes. HTTP success counts are measured separately.

The same 27 scenarios as the direct-client runner cover healthy traffic, missing
leaders, failing agents, slow agents, random failures and latency bursts, storage
degradation, pod kills, network blackholes, and rolling and concurrent replacements. Produce
responses receive the shared per-broker latency distributions on the TCP wire.
Response order is preserved, while delays for concurrent requests overlap.

Four scenarios run at a time by default, launching up to sixteen Mimir processes;
use `-parallel` to change that limit. The default `all` profile includes all 27
standard, nine investigation, and two real Prometheus scenarios
and takes roughly 10–15 minutes plus binary build time. Use `-profile standard`
for only the standard suite. During rolling restarts,
each of three pods disappears for 20s while partition assignments still name it.
Both backends must be served Metadata responses naming each missing leader.
This scenario observes traffic for 90s, with recovery measured from 80s. Every Warpstream-backed
HTTP request must complete within 2s, with at most 5% extra hedge requests, and the
final recovery window must reach all three replacements.

The concurrent restart scenario rolls five waves of agents through the cluster, each away
for 20s and 10s apart, so 8 of 50 agents (16%) are away at once. A wave takes two partition
leaders and the agent that is each one's first fallback in wgo's hash walk, so a cascade has
to continue to a third agent. Its report counts failed flushes by stop reason, fallback wire
errors by reason and cascades that reached a third attempt; the scenario fails when a
cascade stops at a terminal error.

The rotating leader investigation scenario uses WarpStream's single-leader Metadata
placement: one agent is named leader of every partition. 20 agents are all advertised and
six waves each take two agents away for 20s; for the first 10s after an agent dies, Metadata
still names it as the leader. Its report lists write failures by stop reason, so cascades
that run out of candidates show up separately from those that stop at an error.

The rolling stalls investigation scenario stalls every Produce response of two agents per
wave by 9s (four of 20 at once) without errors, while Metadata keeps listing them and for 10s
names a newly stalled agent as leader. Each wave stalls agents the client has not seen fail,
so it keeps hedging and learning.

The metadata unavailable scenario fails Metadata requests from 25s to 65s while four of 50
agents leave discovery at 10s and return on new addresses at 35s. Produce keeps working, so
the new addresses cannot be discovered until Metadata recovers and the partitions must be
written through other agents. WarpStream documents that a discovery failure surfaces as a
Kafka error but not its payload, so the harness fails the request and leaves each client
with its cached view.

Threshold pairs probe wgo's health-check fractions. Each pair injects the same fault into a
share of agents just below the limit (wgo still acts on it) and just above it (wgo stops
acting). Names carry no numbers, so history stays comparable when a limit changes: "failing
agents just below/above MaxFaultyFraction" and, for the slow fraction, "moderately slow",
"barely hedged slow" and "stalled" agents (with a fast first or second fallback) just
below/above MaxSlowFraction (the parameter names of wgo's health-check config). The number of
affected agents derives from the library default limit, 5 points below or above it rounded down to whole agents, and
every latency derives from the default hedge delays and deadlines. The harness refuses to
start when a derived value stops making sense, and a test fails if the shares no longer
straddle the limit. Settings stay at the library defaults (Mimir's flags default to them)
and each description lists them.

- The failing pair (immediate NotLeaderForPartition) isolates the faulty fraction. wgo is
  expected to collapse above the limit, because demotion switches off.
- The moderately slow pair never reaches the hedge delay, so no hedge fires on either side:
  the slow-fraction gate acts and nothing else changes.
- The barely hedged pair answers just after the hedge delay but before a hedge to a healthy
  agent could, so hedges fire but the slow primary wins, so no slow request is cancelled. The hedge that
  loses is cancelled on a healthy agent, which before #127 can close that agent's shared
  connection; the report shows if that raises the faulty-fraction gate. Below the limit wgo
  hedges, above it hedging should stop.
- The stalled pairs answer between the application deadline and the produce wire timeout, so
  kgo misses the deadline on every event that reaches a stalled agent. An event needs every
  write it issues, so each pair places its stalled agents with a fixed search over wgo's real
  candidate walk (a leader followed by consecutive agents from a hash of the topic and
  partition) so that every partition has a way out. "Fast first fallback" leaves every
  stalled partition a fast first fallback, which every build races. "Fast second fallback"
  leaves no partition with its whole chain stalled and at least three whose first fallback
  is stalled but second is fast, which only a client that races another candidate on a
  timer (#128) reaches in time, so older builds are expected to fail it. Below the limit
  wgo should finish near 100 while kgo fails the events that reach a stalled agent, nearly all
  of them; above it hedging stops and both fail. Each event sends 50 writes hashed over the
  ring, so a rare event can miss the partitions a pair is built around. The walk is pinned by a test in `pkg/wgo` and a copy in the runner.

`-run-id N` (default 1, at least 1) numbers the run. CI passes the run number of its three repetitions, and a
local run can pass its round number. Only the four stalled-agent scenarios use it: run N places the stalled agents
at the N-th set, in order of seed, that meets the scenario's rule, so each id is a different but reproducible
placement with the same guarantees, and every build in a run sees the same one. Every other scenario ignores it,
and id 1 gives the placement used before the flag existed. Repeat the same id to measure plain noise. The
descriptions in a report name the id and the agents.

The CI comment compares the PR's wgo results with the current head of the PR's base branch (`main`) and splits each table in two. **Gated checks** are the benchmark scenarios listed in `runner/benchmarks.go`, whose expectations are meant to block a PR later (CI does not enforce them yet); their PR result starts with ✅ when every PR run met the expectations and ❌ when one missed. Checks about the kgo reference run (for example, a fault that never reached kgo's traffic) do not count, because they say nothing about wgo. **Other checks** are informational. Every PR result also starts with 🟩 improved, 🟥 regressed or 🟦 unchanged against main: results within 2 points of each other, or whose three-run ranges overlap, count as unchanged, but a drop from a perfect 100% is always a regression and reaching 100% from below is always an improvement. Bold marks a clear winner only: a result that is better by less than that same margin stays plain, so a bold value is not shown next to 🟦, except a perfect 100% that ties.

An event succeeds only when every one of its writes is acknowledged before the application deadline, which hides how
much of a failed event got through. The reports therefore also count individual writes ("Individual write success"
table, `wgo_writes` and `kgo_writes` in the JSON) for scenarios whose events fan out into several. The direct harness
does so for every scenario (one write per partition). On this harness the four stalled-agent scenarios send each series
as its own write (50 per event, `splitWrites`); the others keep one request per event and show no write table, so their
event results stay comparable with earlier runs.

Slow agents are hedged, never demoted: wgo's demoter acts only on agents that return errors,
so above MaxSlowFraction nothing else helps a slow agent.

Success against kgo is the main result of every scenario. Each report has a "Latency of
successful requests" table: both clients' success ratio first (a failure is a 5xx for the
application), then p50, p99 and mean of the events that succeeded (an event with one failed write is not timed), so a failure's deadline
does not hide how fast the others were. A client that fails its slowest writes looks faster
there, so read it with the success columns. A "Why hedging stopped (secondary signal)" table
then shows the gate a scenario is built to trip next to the hedges each gate suppressed, with
the verdict `as expected`, `mixed` (the expected gate acted and so did another), `differs`,
or `informational` for scenarios that do not isolate a gate. The JSON report carries the same
data as `wgo_success_latency`, `kgo_success_latency` and `hedge_stop`.

Research scenarios are investigation scenarios whose result depends on work in progress
(#127 and #128). They are named by their setup, with the numbers fixed on purpose rather than
derived, so a name never describes a mechanism that a fix would change. "investigation: 17 of
50 agents (34%) answer in 500ms to 3s, mean 1.5s" is main's `25% slow agents` at a larger
share, and "investigation: 17 of 50 agents (34%) answer uniformly in 250ms to 3s" has answers
from under to over the hedge delay, above MaxSlowFraction. Correct handling means no agent is
charged as faulty, wgo does as well as kgo, and only the slow-fraction gate stops hedging. On
current clients, cancelling a hedged slow request closes the shared connection and fails the
requests queued behind it; those failures are charged to the slow agent and the
faulty-fraction gate acts too. Tests fail when a default moves so far that a setup no longer
sits on its side of a limit.

The standard scenarios use a 5s distributor remote timeout and a 10s Kafka write
timeout, with 2s
request overhead and a 2s dial timeout. Other client settings use Mimir defaults,
as provided by the pinned source. The application deadline is 5s; the
rolling-restart gate still requires every write to finish within 2s.

Fake brokers acknowledge without persisting writes. Results measure HTTP
completion, not durability or queryability. Local reports are written under
`.reports/` and ignored by Git. CI runs every scenario three times in separate
jobs and posts each client's observed success-percentage range in one updated PR comment.
The Mimir binary and simulation harness are built once and shared across the
three runs per client variant. Each CI run splits the 38 scenarios into five batches of seven or eight on
separate runners. Both clients run together within each scenario, preserving
all comparison gates. Integration harness unit tests run once in a separate job, in
parallel with the build. The real Prometheus sender retry test runs after the
Prometheus release binary is downloaded for the PR client variant.
Each run's full latency, fault and error report is retained as a CI artifact.
CI freezes one `main` client revision, then runs both clients against the PR's
harness three times per variant. The comment compares PR wgo, main wgo and the
PR-run kgo reference, sorted by PR wgo success rate with the weakest first.
Artifacts record client and harness revisions; mixed revisions or scenario
coverage are rejected. Main-run kgo results remain available in the artifacts.

Results below 100% success can vary because of randomness and wall-clock timing
during testing. The same code can produce 110/120 successful writes in one run
and 80/120 in another.

CI uses `-check=false`: missed scenario expectations are included in the report,
while build, unit-test and harness errors still fail their jobs. Local commands
check expectations by default. PR comments are published for branches in this
repository; fork PRs retain the artifacts and job summaries.

To use an existing build, run `go run . -mimir-binary /absolute/path/to/mimir` in
this directory. CI supplies the same path through `SIMULATION_BINARY`.
Use the pinned source and current client checkout for comparisons with CI.
Reports identify the supplied binary's Go version and linked client version;
the default builder source pin does not assert the source of a supplied binary.
Use `-scenario` with a regular expression to rerun selected scenarios.
Use `-batch 1 -batches 5` to run the first of five batches. Batches select every
fifth scenario after filtering, spreading the investigation cases across runners.
Run `go test -race ./...` here for proxy, remote-write and observer unit tests.

## Investigation profile

`make simulation-mimir-investigation` runs only the investigation profile against unmodified
Mimir distributor processes. These scenarios are also part of the default Mimir
suite and every three-run CI report.
The profile uses single-agent partition placement: all partition leaders point to
one preferred agent, with two advertised fallback agents. It exercises a
1001-partition topic and an independent 60-partition topic, using two distributors
per client lane. Each application event sends an HTTP remote-write request to
both processes and succeeds only if both finish within 5s.

Kafka writes have a 4s budget and 500ms dial timeout. The two distributors have
5s and 4s HTTP budgets and a 500ms minimum ingestion response time. Requests carry
200 series; metric names rotate between requests so sparse traffic eventually
exercises every partition. Wire observations must confirm successful warmup
writes to every partition before the measured workload starts. Payload preparation
finishes before warmup, keeping traffic continuous into the fault window.

The profile includes:

- A healthy control with two application events per second and single-agent
  placement. Every event must succeed.
- A slow preferred agent: at 10s, the preferred agent delays responses for 3s,
  one fallback delays for 9s, and one remains at 50ms. Writes continue twice per
  second; all agents recover at 130s.
- Changing discovery views: four physical agents advertise three at a time;
  the responding agent becomes preferred for every partition, and each view retains
  the healthy fallback. Twenty application
  events per second continue while one agent stalls for 9s and two for 3s.
- Intermittent fallback stalls: the slow-preferred layout at twenty application
  events per second, with an additional independent 1% chance of a 9s stall on
  every agent, including the normally healthy fallback. Wire evidence must show
  that this fallback actually encountered an injected stall in the wgo lane.

- A stale fallback during rolling restart: for 40s, the preferred agent rejects 10% of
  Produce requests, broker-only discovery removes one fallback, and topic discovery
  still advertises it with a 2s delay. Intermittent failures keep the preferred
  agent active so demotion cannot avoid the fallback path. The other fallback stays healthy. Both discovery paths, actual
  fallback wire errors, and successful cascades of at least three attempts must
  be observed. Terminal cascade errors fail the scenario expectation. This case
  observes traffic for 90s and checks recovery from 70s.

Latency fault cases observe traffic for 180s. Reports separate scheduled pre-fault,
degraded, and recovery windows. Cases with an available healthy fallback require
every write to succeed and at least 95% to finish within 2s. The intermittent case
requires 95% success. All recovery writes scheduled from 145s must succeed within
2s. These expectations deliberately expose missed availability or latency targets;
`-check=false` retains the report without failing the command for those misses.
Fake brokers acknowledge without durable storage; measurements cover HTTP
completion, Kafka request behavior, and recovery. They do not measure queryability.

A sequential investigation run takes about fourteen minutes plus build time. To retain each
process's logs and generated configuration locally:

```sh
cd pkg/internal/simulation/mimir
go run . -mimir-binary /absolute/path/to/mimir -profile mimir-investigation \
  -parallel 1 -check=false -timeout 20m \
  -mimir-log-directory /private/tmp/mimir-investigation-logs \
  -report-filepath .reports/investigation.md \
  -json-report-filepath .reports/investigation.json
```

For compatible builds with additional configuration fields, supply
`-mimir-config-overlay /absolute/path/to/overlay.yaml`. The YAML overlay is a Go
text template with `.Backend`, `.Address`, `.Topic`, and `.SecondaryTopic` fields.
It merges over the generated configuration. Harness-owned networking, topology,
rings, write budgets, and artificial delay cannot be overridden. For example:

```yaml
limits:
  max_global_series_per_user: 200000
```

Keep overlays and retained logs outside the repository. Reports include the
SHA-256 of the overlay source, not its contents. Inspect retained configuration
and logs before sharing them. Additional settings must be supported by the
supplied binary.

Build the same pinned Mimir source with another published client version using
`WARPSTREAM_CLIENT_VERSION=<version> bash pkg/internal/simulation/mimir/build-mimir.sh /absolute/build/directory`.
Compare runs with the same machine, profile, writer count, binary source and
overlay, and inspect the linked module manifests for dependency differences.
Use `-mimir-writers=1` to isolate the main writer; the default is two for the
investigation profile and one for the standard suite.

## Real Prometheus remote-write profile

`-profile mimir-prometheus` runs a healthy control and a sustained partial outage.
Both are included in the default `all` profile and its three-run CI report.
The build compiles the actual pinned Mimir binary and downloads the official
Prometheus release pinned in `prometheus-version.txt`, verifying the archive against
`prometheus-sha256sums.txt`. Linux and macOS on amd64 and arm64 are supported.
Supply `-prometheus-binary` to use an existing binary; otherwise the harness looks
beside the Mimir binary. `PROMETHEUS_BINARY` also supplies this path.

Each lane launches two actual Prometheus processes in agent mode. Each scrapes
32,000 fixed series every second, writes its WAL, and remote-writes to an
independent Mimir distributor. Prometheus controls batching, shard scaling,
backoff, retries and the default 30s HTTP timeout. Labels are identical across
lanes; only the Mimir backend changes. The harness observes requests without
changing the sender's payload or implementing its retry logic.

Ten agents advertise all ten peers. Metadata assigns partitions to its responding
agent. The distributors use separate 1001- and 60-partition topics, a 4s Kafka
write budget, 2s overhead and 500ms dial timeout. Mimir derives each backend's
wire timeout. Warmup requires successful Kafka acknowledgements for every
partition before observation starts.

From 10s to 130s, one agent takes 6s to acknowledge and four take 3s; five remain
at 50ms. The proxy reads `Produce.TimeoutMillis` from each real Kafka request.
Agent work exceeding it returns `KAFKA_STORAGE_ERROR` at that deadline. With this
budget Mimir sends 2s through wgo and 4s through franz-go: the same 3s operation
can produce different broker outcomes. Concurrent service times overlap while
TCP response order is preserved. These are acknowledgement-stall inputs, not a
conversion of packet delays or an exact outage replay.

The fault case observes 180s and reports pre-fault, degraded and recovery windows
by HTTP issue time. An event is a real remote-write **attempt**, including retries;
failed attempts do not establish sample loss. Attempts vary by client and run,
so CI normalizes each run to a success percentage. Partial results show the
midpoint ± half the three-run min/max range in percentage points; 100% requires
all three runs to be perfect. Sorting uses the lower bound. Bold marks a range
entirely above its peers; overlapping or touching ranges have no highlighted
winner. Perfect 100% results are highlighted, with ties preferring PR wgo.
Synthetic scenarios retain exact attempt-count validation. No assertion requires a client
to win. HTTP availability targets remain strict and informational in CI. The
fault case also rejects permanently failed or dropped wgo sender samples and requires
the wgo source-to-sent lag to be at most 5s at collection, so successful recent HTTP
requests cannot hide an old WAL backlog.

Reports retain Mimir timeout/hedge-suppression evidence and sender counters since
process start: attempted, permanently failed, retried, dropped and queued samples.
The Prometheus `samples_total` counter includes retries and is not a delivered
sample count. The CI sample table instead counts decoded samples in requests
issued during the observation window that receive HTTP 2xx, including the bounded
request drain and excluding warmup and failed attempts. It shows the midpoint ±
half the three-run min/max range; higher counts mean more acknowledged throughput.
The JSON field is `samples_accepted_observed_total` in each lane's sender evidence.
This does not deduplicate samples or prove durable storage.
Current enqueue-to-sent and source-to-sent timestamp lags are reported separately.
Queue pending counts exclude unread WAL; highest-sent timestamps do not prove
all shards have caught up. These gauges are diagnostics, not a complete sample
loss or delivery guarantee. Fake agents do not persist data or provide queries.

```sh
go run . -mimir-binary /absolute/path/to/mimir \
  -prometheus-binary /absolute/path/to/prometheus -profile mimir-prometheus \
  -parallel 1 -check=false -report-filepath /private/tmp/remote-write.md \
  -json-report-filepath /private/tmp/remote-write.json
```

Run `PROMETHEUS_BINARY=/absolute/path/to/prometheus go test -race -run TestPrometheus`
for the real sender retry integration test. It verifies that temporary HTTP 500s
increase retries while subsequent writes succeed without permanent sample failure.
