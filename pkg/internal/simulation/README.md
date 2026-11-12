# Simulations

`make simulation` runs every standard scenario against fake brokers and writes
the reports to `.reports/` in this directory. The Mimir workload in
[`mimir/`](mimir/README.md) takes the same flags.

List the scenarios, then run the ones you are working on:

```sh
go run ./pkg/internal/simulation -list
go run ./pkg/internal/simulation -list -scenario 'slow'
go run ./pkg/internal/simulation -scenario 'slow'
go run ./pkg/internal/simulation -scenario '^rolling restart$'
```

`-scenario` takes a regular expression and matches it anywhere in a scenario's
name, so anchor it with `^…$` to pick one scenario. Without `-scenario`, every
scenario in the profile runs. An invalid expression, or one that matches no
scenario, exits with status 1 before any scenario starts.

`-list` prints one name per line and exits without starting a scenario. It
applies `-profile`, `-scenario`, `-batch` and `-batches` the same way a run does,
so it shows exactly what a run with the same flags executes. The summary and the
Markdown and JSON reports cover only the scenarios that ran, in profile order,
with the same expectation checks.

For the Mimir workload, run the same flags from `mimir/`. Listing doesn't need a
Mimir binary:

```sh
cd pkg/internal/simulation/mimir
go run . -list -profile mimir-investigation
go run . -mimir-binary /absolute/path/to/mimir -scenario '^investigation: slow preferred agent$'
```
