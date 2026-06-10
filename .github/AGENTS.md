# AGENTS.md — CI & repo automation

See the repo-root [`AGENTS.md`](../AGENTS.md) for build/test/format commands.

## GitHub Actions

- Pin every action to a full commit SHA, with the human-readable version as a
  trailing comment — e.g. `uses: actions/checkout@<sha> # v6.0.3`. Never use a
  bare tag.
- The Go version comes from `go.mod` through `setup-go`'s `go-version-file`;
  don't hardcode a Go version in the workflow.

## Renovate

- [`renovate.json`](renovate.json). Do **not** group dependencies — each
  dependency gets its own PR.
