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

The same 15 scenarios as the direct-client runner cover healthy traffic, missing
leaders, failing agents, slow agents, random failures and latency bursts, storage
degradation, pod kills, network blackholes, and rolling replacements. Produce
responses receive the shared per-broker latency distributions on the TCP wire.
Response order is preserved, while delays for concurrent requests overlap.

Four scenarios run at a time by default, launching up to sixteen Mimir processes;
use `-parallel` to change that limit. The default `all` profile includes all 15
standard and five investigation scenarios
and takes roughly 10–15 minutes plus binary build time. Use `-profile standard`
for only the standard suite. During rolling restarts,
each of three pods disappears for 20s while partition assignments still name it.
Both backends must be served Metadata responses naming each missing leader.
This scenario observes traffic for 90s, with recovery measured from 80s. Every Warpstream-backed
HTTP request must complete within 2s, with at most 5% extra hedge requests, and the
final recovery window must reach all three replacements.

The standard scenarios use a 5s distributor remote timeout and a 10s Kafka write
timeout, with 2s
request overhead and a 2s dial timeout. Other client settings use Mimir defaults,
as provided by the pinned source. The application deadline is 5s; the
rolling-restart gate still requires every write to finish within 2s.

Fake brokers acknowledge without persisting writes. Results measure HTTP
completion, not durability or queryability. Local reports are written under
`.reports/` and ignored by Git. CI runs every scenario three times in separate
jobs and posts each client's worst success count in one updated PR comment.
The Mimir binary and simulation harness are built once and shared across the
three runs. Each CI run splits the 20 scenarios into five batches of four on
separate runners. Both clients run together within each scenario, preserving
all comparison gates. Integration harness unit tests run once in a separate job, in
parallel with the build.
Each run's full latency, fault and error report is retained as a CI artifact.

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
