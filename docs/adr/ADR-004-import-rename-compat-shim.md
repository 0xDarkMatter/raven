# ADR-004: Import root renamed to `raven_bus`; v1 API survives as a compat shim

- Status: accepted
- Date: 2026-08-12
- Touches: src/, pyproject.toml, tests/, README.md, AGENTS.md

## BLUF

v2's import root is **`raven_bus`** (was `claude_bus`). The CLI stays
`raven`. The **distribution name is deliberately undecided** (PyPI `raven`
is occupied by Sentry's legacy client; candidates like `ravenhq` parked —
do not publish under any name without a fresh decision). v1's
`BusClient(session_id, role)` API survives one release as
`raven_bus.compat`, implemented on the v2 store.

## Context

The project carried four identities (repo `claude-bus`, dist `raven`, import
`claude_bus`, CLI `raven`), and `pip install raven` installs the wrong
package. v2 is a breaking rewrite (ADR-001/002) — the only cheap moment to
fix the import root.

## Decision

One rename, now, while nothing external depends on the import. The compat
shim maps v1 addresses onto 2-consumer broadcast channels and keeps the
v1 examples runnable; it is documented as deprecated from day one and
removed after one minor release.

## Alternatives rejected

- **Keep `claude_bus`** — perpetuates the identity split into the rewrite.
- **Decide the dist name now** — publishing is out of scope for P1; a name
  chosen under time pressure sticks forever.
- **No compat shim** — the four worked examples and their integration tests
  are the best regression suite the rewrite has; the shim keeps them alive
  until v2-native examples replace them.

## Consequences

- Every file moves; grep for `claude_bus` must return zero hits outside
  `raven_bus/compat` and the changelog when P1 lands.
- README/AGENTS install + import docs are invalidated and must ship in the
  same run (docs lane).
