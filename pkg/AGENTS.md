# AGENTS.md — Go code

Conventions for code under `pkg/`. See the repo-root [`AGENTS.md`](../AGENTS.md)
for layout and commands, and [`README.md`](../README.md) for the architecture.

When you change the client's logic or behaviour, update
[`README.md`](../README.md) in the same change: its "How it works" section
documents the routing, buffering, hedging, and demotion design, and must not
drift from the code.

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

## Tests

- Name tests `Test<Component>_<Method>`: an underscore between the
  component-under-test and the method or scenario (same for `Benchmark`). Subtest
  (`t.Run("...")`) names are plain prose, not affected by this rule.
- No message argument on `assert`/`require` unless it explains a non-obvious
  invariant — and prefer a leading `// comment` for that. Format-style diagnostic
  args that surface values at failure time are fine; the required-substring arg of
  `assert.ErrorContains` is not a message, don't strip it.
