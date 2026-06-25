# AGENTS.md

`github.com/grafana/warpstream-go` is a produce-only Kafka client for
[Warpstream](https://www.warpstream.com/)'s stateless-agent architecture.

The client lives under `pkg/wgo`. See [`README.md`](README.md) for the architecture.

Keep [`README.md`](README.md) up to date whenever the client's logic or behaviour
changes — its "How it works" section describes the routing/buffering/hedging/
demotion design and must stay accurate. If a change alters that behaviour, update
the README in the same change.

Design notes that aren't user-facing live under [`docs/internal/`](docs/internal/) —
read it when planning changes to the areas it covers, and keep it current:

- [`docs/internal/metrics.md`](docs/internal/metrics.md) — how metrics are split
  (transport vs producer-state vs warpstream-specific) and the franz-go/`kprom`
  drop-in-compatibility contract. Update it when changing metrics.

This file holds repo-wide conventions. More specific instructions live in nested
`AGENTS.md` files — read the one closest to the files you're editing:

- [`pkg/AGENTS.md`](pkg/AGENTS.md) — Go code: comment style, tests.
- [`.github/AGENTS.md`](.github/AGENTS.md) — GitHub Actions / CI, Renovate.

## Common commands

- `make test` — runs the unit tests with the race detector.
- `make lint` — runs the linters.
- `make format` — formats the code to match what `make lint` checks. Run it
  before committing.

## Commits

- Keep PR descriptions short and focused on the "why".
- Commit/push only when explicitly asked.
