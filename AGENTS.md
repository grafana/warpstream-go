# AGENTS.md

`github.com/grafana/warpstream-go` is a produce-only Kafka client for
[Warpstream](https://www.warpstream.com/)'s stateless-agent architecture.

The client lives under `pkg/wgo`. See [`README.md`](README.md) for the architecture.

Keep [`README.md`](README.md) up to date whenever the client's logic or behaviour
changes — its "How it works" section describes the routing/buffering/hedging/
demotion design and must stay accurate. If a change alters that behaviour, update
the README in the same change.

This file holds repo-wide conventions. More specific instructions live in nested
`AGENTS.md` files — read the one closest to the files you're editing:

- [`pkg/AGENTS.md`](pkg/AGENTS.md) — Go code: layout, comment style, tests.
- [`.github/AGENTS.md`](.github/AGENTS.md) — CI, golangci-lint, Renovate, CODEOWNERS.

## Common commands

- `make test` — runs the unit tests with the race detector.
- `make lint` — runs the linters.
- `make format` — formats the code to match what `make lint` checks. Run it
  before committing.

## Commits

- Keep PR descriptions short and focused on the "why".
- Commit/push only when explicitly asked.
