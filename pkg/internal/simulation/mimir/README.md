# Mimir distributor simulations

Run `make simulation-mimir` from the repository root. The build downloads the
revision pinned in `mimir-version.txt` and builds its actual `cmd/mimir` with a
`go.mod` replacement pointing at this checkout. Mimir source is unchanged. Build
artifacts and the linked module manifest live under `.build/`.

Each scenario launches two separate distributor processes, using Mimir's
`warpstream` and `kafka` ingest backends. Requests enter through the public
Prometheus remote-write HTTP endpoint, with one tenant and 1600 series per request
across 50 ingest partitions. The harness imports no Mimir implementation code.
Warmup must receive successful Kafka acknowledgements for every partition.
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

Four scenarios run at a time by default, launching up to eight Mimir processes;
use `-parallel` to change that limit. A full run takes approximately six minutes
plus binary build time. During rolling restarts,
each of three pods disappears for 20s while partition assignments still name it.
Both backends must be served Metadata responses naming each missing leader.
This scenario observes traffic for 90s, with recovery measured from 80s. Every Warpstream-backed
HTTP request must complete within 2s, with at most 5% extra hedge requests, and the
final recovery window must reach all three replacements.

The distributor remote timeout is 5s. Kafka write timeout is 10s, with 2s
request overhead and a 2s dial timeout. Other client settings use Mimir defaults,
as provided by the pinned source. The application deadline is 5s; the
rolling-restart gate still requires every write to finish within 2s.

Fake brokers acknowledge without persisting writes. Results measure HTTP
completion, not durability or queryability. Local reports are written under
`.reports/` and ignored by Git. CI runs every scenario three times in separate
jobs and posts each client's worst success count in one updated PR comment.
Each run's full latency, fault and error report is retained as a CI artifact.

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
Run `go test -race ./...` here for proxy, remote-write and observer unit tests.

## Investigation profile

`make simulation-mimir-investigation` runs an opt-in profile against the same
unmodified Mimir binary. The standard suite and CI remain unchanged. The profile
uses ten agents serving fifty partitions, a 4s Kafka write timeout, 500ms dial
timeout, and 500ms minimum ingestion delay. Two independent distributors per
client lane write distinct topics through the same agents. Their HTTP budgets
are 5s and 4s. An application event sends both remote-write requests concurrently
and succeeds only when both succeed within the outer 5s application deadline.

Twenty application events per second produce forty HTTP pushes per second per
lane, each containing 1600 series. A healthy case observes 600 events after warmup.
The sustained-stall case observes 3600 events: ten healthy seconds, 120 seconds
with one agent delaying Produce responses by 9s and four by 3s, then fifty healthy
seconds. Reports separate scheduled pre-fault, degraded, and recovery windows;
all 700 events scheduled from 145s must succeed. The degraded window has no
success-rate floor or assumption that either client wins.

This is a hypothesis-testing profile, not a complete Mimir deployment. Independent
HTTP writers approximate coupled write outcomes, but do not implement an
aggregation pipeline. Delaying Kafka responses does not emulate an object store,
packet loss, shared storage queues, or server-side backpressure. Brokers still
acknowledge without durable storage. Use real services to establish those effects.
The profile takes about five minutes plus build time, running cases sequentially.

To retain each process's logs and generated configuration locally:

```sh
cd pkg/internal/simulation/mimir
go run . -mimir-binary /absolute/path/to/mimir -profile mimir-investigation \
  -parallel 1 -check=false -timeout 15m \
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
and logs before sharing them. Additional features must be explicitly configured
in a compatible binary; an overlay alone does not add them to OSS Mimir.

Build the same pinned Mimir source with another published client version using
`WARPSTREAM_CLIENT_VERSION=<version> bash pkg/internal/simulation/mimir/build-mimir.sh /absolute/build/directory`.
Compare runs with the same machine, profile, writer count, binary source and
overlay. Use `-mimir-writers=1` to isolate the main writer; the default is two for
the investigation profile and one for the standard suite.
