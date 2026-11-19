# AGENTS.md — CI & repo automation

See the repo-root [`AGENTS.md`](../AGENTS.md) for build/test/format commands.

## GitHub Actions

- Pin every action to a full commit SHA, with the human-readable version as a
  trailing comment — e.g. `uses: actions/checkout@<sha> # v6.0.3`. Never use a
  bare tag.
- The Go version comes from `go.mod` through `setup-go`'s `go-version-file`;
  don't hardcode a Go version in the workflow.

- The `simulation` suites are the slowest jobs, so they only run when `lint`,
  `test`, `vendor` and workload discovery succeed. When a fast check is added,
  keep these in sync:
  - `simulation`: add it to `needs` and require `== 'success'` in the `if`.
  - `simulation-comment`: add it to `needs` and use the opposite, `!= 'success'`,
    in the `if`. An `== 'success'` there would run the job on green PRs and
    replace the results table.
  - Pass its result to `simulation-comment` as an env var (like `TEST_RESULT`),
    map it in `publishSkipped`, and add it to `gateLabels` in
    `scripts/simulation-report.cjs`. A missing result counts as a failure, so
    without the env var a passing check would be named in the comment.

## Renovate

- [`renovate.json`](renovate.json). Do **not** group dependencies — each
  dependency gets its own PR.
