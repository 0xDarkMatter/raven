# ADR-002: Full-string addressing, path-style channels, one host DB

- Status: accepted
- Date: 2026-08-12
- Touches: src/, cli/, docs/

## BLUF

Consumer ids are the **full string `<role>@<run>`** (e.g. `lane-3@v0-2`) —
no derived hash anywhere in the address path. Channels are path-style names
(`run/<run>/control`, `run/<run>/queue`, `run/<run>/telemetry`,
`run/<run>/lane/<id>`). There is **one host DB** (`~/.raven/bus.db`,
override `RAVEN_DB`); runs are a naming convention, not a fence, and
teardown is a prefix-scoped delete.

## Context

v1 addressed by `<role>:<session>` compressed to a `role + 6-hex-sha1` alias
(24 bits — silent collision risk, since `INSERT OR IGNORE` on the PK made a
colliding pair resolve to someone else's mailbox). v1 also fenced sends to
the sender's own session, producing a misleading `UnknownRoleError` that
leaked the internal alias, while the README advertised multi-swarm use. v1's
per-cwd DB default meant every run had its own file and no cross-run surface.

## Decision

Drop the alias layer entirely. `@` (not `:`) separates role from run,
deliberately breaking the v1 grammar so misuse fails loudly. Grammar:
role and run match `[a-z0-9][a-z0-9._-]*`; channel segments likewise,
joined by `/`. One DB per host so one `ravend`, one tail, one dashboard feed
cover every run; `raven teardown --run <name>` deletes `run/<name>/%`.

## Alternatives rejected

- **Keep hashed aliases** — collision risk bought nothing; full strings are
  cheap in SQLite.
- **Per-run DB files** — kills the cross-run observability surface and
  requires a ravend per run; isolation is achieved by naming + teardown.
- **Session fence on sends** — the v1 footgun; host-global channels with
  convention-scoped names replace it.

## Consequences

- A v1-compat shim maps `BusClient(session_id, role)` onto a 2-consumer
  broadcast channel during migration.
- All tooling (fleetflow guard preamble, ff-clean) uses the channel-name
  convention; nothing parses hashes.
