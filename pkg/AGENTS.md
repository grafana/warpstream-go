# AGENTS.md — Go code

Conventions for code under `pkg/`. See the repo-root [`AGENTS.md`](../AGENTS.md)
for commands, and [`README.md`](../README.md) for the architecture.

When you change the client's logic or behaviour, update
[`README.md`](../README.md) in the same change: its "How it works" section
documents the routing, buffering, hedging, and demotion design, and must not
drift from the code.

## Configuration

`Config` (in `config.go`) and its functional options live together. Each `Config`
field has a matching `With…` option in the same file. When you add, remove, or
rename a `Config` field, make the same change to its `With…` option, and keep the
option's doc comment aligned with the field's documentation. A defaulted field
also needs its `Default…` constant and a line in `DefaultConfig`.

## Naming

When two functions do the same thing but one takes a single input and the other
takes many, name the single-input one plainly and give the multi-input one a
`Multi` prefix — e.g. `Add` (one record) / `MultiAdd` (many). The plain name is
the single-item case; the `Multi` prefix signals "same operation, batched".

## Comments

Default to writing **no** comment. Add one only when the *why* is non-obvious: a
hidden constraint, a subtle invariant, a workaround for a specific bug, or
behaviour that would surprise a reader.

- Comment the **why**, never restate the **what** — the code and well-named
  identifiers already say what. Lead with the rule/fact, then the reason. Keep it
  to 1–3 lines (struct- or component-level docs may run longer). When in doubt,
  shorten — never expand.
- Comment the code in front of you, not how *other* components use it. Caller
  behaviour drifts independently and the comment rots; justify a design choice by
  this code's own contract and invariants.
- No decorative separator/divider comments (`// --- label ---`,
  `// === section ===`, trailing dashes, etc.). Group with blank lines or split
  the file instead.
- Inside a function/method doc, do not document what the *caller* uses it for —
  especially for general-purpose helpers. Document the function's own
  contract/behaviour.
- When embedding a struct inside another struct, do not enumerate the embedded
  struct's fields in the outer struct's comment.

## Tests

- Name tests `Test<Component>_<Method>`: an underscore between the
  component-under-test and the method or scenario (same for `Benchmark`). Subtest
  (`t.Run("...")`) names are plain prose, not affected by this rule.
- No message argument on `assert`/`require` unless it explains a non-obvious
  invariant — and prefer a leading `// comment` for that. Format-style diagnostic
  args that surface values at failure time are fine; the required-substring arg of
  `assert.ErrorContains` is not a message, don't strip it.
- `kfake` tests run under `testing/synctest`: wrap the body in `synctest.Test`,
  create the cluster with `testkafka.WithVirtualNetwork(&vnet)`, and give every
  client `vnet.DialContext` (`WithDialer` / `kgo.Dialer`) — a real socket blocks a
  goroutine on network I/O and deadlocks the bubble. Never poll with
  `require.Eventually`; use `synctest.Wait()` to settle goroutines and `time.Sleep`
  only to advance the fake clock past a timer. No `t.Run` inside a bubble (wrap each
  subtest in its own `synctest.Test`).
