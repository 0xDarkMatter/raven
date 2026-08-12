# Changelog

All notable changes to **raven** are recorded here.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — Unreleased

The v2 rewrite. A breaking rewrite of the store, not a feature drop: raven v2
replaces v1's per-message delivery state with an append-only log + per-channel
-kind read-state. Decisions of record: [ADR-001](docs/adr/ADR-001-append-only-log-channel-kind-read-state.md)
(log + channel-kind read-state), [ADR-002](docs/adr/ADR-002-addressing-and-single-host-db.md)
(full-string addressing + one host DB), [ADR-004](docs/adr/ADR-004-import-rename-compat-shim.md)
(rename + compat shim).

### Added — the v2 store

- **Append-only message log partitioned into channels**; read-state lives per
  channel *kind* (ADR-001): `broadcast` → per-consumer `cursors`, `queue` →
  `claims` rows with leases, `stream` → none. The `messages` table has **no
  status column** — reads never mutate, so fan-out, replay, and `tail` are
  trivially correct.
- **Three channel kinds.** `broadcast` (at-least-once, ack = cursor jump),
  `queue` (exactly-one-winner claim with lease + auto-requeue + dead-letter
  after `max_deliveries`), `stream` (observe-only, ring retention, tail-only).
  Kind is decided at creation and immutable after.
- **Lease-backed queues.** Claim is `INSERT … ON CONFLICT DO NOTHING`
  (rowcount 1 wins); a lapsed lease is flipped to a durable `lapsed` state by
  the opportunistic `sweep` and re-won by a guarded `UPDATE` that increments
  `deliveries` — attempt counts survive requeue, dead-letter fires reliably at
  `max_deliveries`, replacing v1's "crashed consumer = message stuck forever".
- **Full-string `<role>@<run>` addressing**; path-style channel names
  (`run/<run>/control`, `run/<run>/queue`, …); one host DB
  (`~/.raven/bus.db`, override `RAVEN_DB`); `raven teardown --run` is a
  prefix-scoped delete (ADR-002).
- **Expiry that works.** Every liveness read filters `expires_at`; `sweep`
  runs opportunistically inside reads (ADR-001). v1 shipped `expires_at` and
  never invoked it.
- **v2 CLI** (`raven`): `send read ack claim done release tail channels doctor
  teardown version`. The DB is created on first use — no `init` step.
- **v1-compat shim** (`raven_bus.compat`): the v1 `BusClient(session_id, role)`
  API on the v2 store, so the v1 examples and their integration tests survive
  as the rewrite's regression net. Deprecated from day one; removed after one
  minor release (ADR-004).

### Changed

- **Import root renamed `claude_bus` → `raven_bus`** (ADR-004). The CLI stays
  `raven`. The **pip distribution name is undecided** — do not `pip install
  raven` (Sentry's legacy client); install from source (`pip install -e .`)
  until naming is settled.

### Removed

- The **`claude_bus` import root** (ADR-004).
- **Per-message `status` column** and its four-state model
  (`sent`/`delivered`/`resolved`/`expired`) — read-state now lives in
  `cursors`/`claims` (ADR-001).
- **Hash aliases** (`role + 6-hex-sha1`, 24-bit collision risk) — full
  `<role>@<run>` strings instead (ADR-002).
- **Session fences** on sends — channels are host-global; run-scoping is a
  naming convention, not a fence (ADR-002).
- The **`raven init` / `session init`** commands and the v1 **HTTP bridge**
  (`serve`, `GET /inbox`, `GET /message/{id}`) — the DB is created on first
  use; ravend HTTP is a P2 item.

### v1-compat narrowings

The shim preserves the v1 surface but three behaviours narrow, because v2 has
no per-message status, no hash alias, and no session fence (documented loudly
in `raven_bus/compat.py`):

- **Case-folding.** v2's grammar is lowercase-only (ADR-002); the shim
  `.lower()`s role and session before mapping. (All v1 examples are lowercase,
  so they keep running unchanged.)
- **Cursor-jump ack.** v1 acked a single message; `compat.ack(id)` jumps the
  cursor to `id`, acking everything up to it.
- **`task_id` in body.** v2 has no `task_id` column; the shim carries it under
  `body["__task_id__"]` on send and strips it on read.

## P2+ roadmap

The pre-v2 "Phase 2" wish-list (archive/search, persistent aliases, sessions
table, schema discovery) is superseded by the v2 design. The real roadmap is
the phasing in [docs/design/raven2-architecture.md §8](docs/design/raven2-architecture.md#8-phasing):

- **P2 — ravend:** loopback HTTP read+write, SSE tail, Process-Compose registration.
- **P3 — adapters:** `raven-acp` harness + Claude Code hook adapter; the shared
  injection-policy module (ADR-003).
- **P4 — fleetflow:** `ff-spawn --acp`, heartbeat switch, `ff-clean` teardown, dashboard SSE.
- **P5 — bridges:** raven↔Buzz relay.

## [0.1.1] — 2026-04-25

Edge-case + QOL polish following the v0.1.0 ship. No public-API breakage.

### Fixed

- `raven read` / `ack` and `GET /message/{id}` no longer add a
  spurious `__cli__` / `__http__` / `reader` row to the `aliases`
  table on every invocation.
- `GET /inbox` now returns 400 on `role=a:` (empty session),
  `role=:b` (empty role), and `max<1` — previously these produced a
  silent empty array, masking caller bugs.
- `BusClient(session_id="", role="alice")` and
  `BusClient(session_id="s", role="bad:role")` now fail fast with a
  clear `ValueError` instead of registering a useless alias.

### Changed

- "Unregistered message type" log demoted from WARNING to DEBUG.
  Permissive mode is the *default* — it shouldn't nag on every send.
  Strict-mode rejections still surface loudly via
  `SchemaValidationError`.
- `init_db()` is now cached per-process. Long-running subscribers and
  bulk CLI usage no longer re-execute the migration script on every
  `BusClient()` instantiation. Pass `force=True` to bypass.
- `cli_main()` renders `ClaudeBusError`, missing-message, missing-file,
  and permission-denied exceptions as one-line `error: ...` messages
  with proper exit codes — no Python tracebacks for users.

### Added

- `raven tail` — live stream observer that watches all bus traffic (or
  one role's messages) without consuming them. Identity-free, never
  competes with subscribers. Flags: `--role`, `--from`, `--follow/--no-follow`,
  `--interval`, `--json`.
- `raven version` subcommand (mirrors `--version`).
- Short flags: `inbox -r/--role -m/--max -j/--json`,
  `send -t/--type`, `read -j/--json`.
- `doctor` checks the bundled `0001_initial.sql` migration is present
  on disk, catching broken installs early.
- `_core.read_by_id()` — identity-free message fetch primitive.
- `_core.list_since()` — id-range query used by `tail`.
- Integration tests for `03-news-desk` and `04-server-incident` pipelines
  (subprocess + SQLite assertions, 100% line coverage).

## [0.1.0] — 2026-04-25

The hackathon ship target — minimum viable bus that tells the
"live bus complement to Pigeon's mailbox" story.

### Added

- **`BusClient` Python API** — identity-bound by `(session_id, role)`, with
  `send` / `inbox` / `read` / `ack` / `subscribe`.
- **`Message` model** — exposes `<role>:<session>` addressing on top of an
  internal alias scheme.
- **`SchemaRegistry`** — opt-in Pydantic body validation per message type;
  permissive by default, switch to strict mode to reject unregistered types.
- **CLI (8 commands)** — `init`, `doctor`, `session init`, `send`, `inbox`,
  `read`, `ack`, `serve`. JSON output mode on read commands.
- **Optional HTTP bridge** (`pip install 'raven[http]'`) — read-only
  Starlette app exposing `GET /health`, `GET /inbox`, `GET /message/{id}`.
- **Async subscribe iterator** — `async for msg in client.subscribe()`
  yields each new unread message exactly once with at-most-once semantics.
- **SQLite store** — WAL-mode single-file DB; idempotent schema apply.
- **Deterministic role aliases** — `(role, session_id)` always resolves to
  the same internal alias, so producers can address recipients that haven't
  booted yet.
- Docs: `README.md`, `docs/QUICKSTART.md`, runnable `examples/01-hello-world/`.

### Phase 1 deviations from the original spec

- **Message ids are integers** rather than UUIDs. Integers play better with
  shells (`raven read 42`) and SQLite autoincrement is the simplest
  store. Wire-stable for v0.1.x.
- **Status enum is `unread` / `read`** at the public API surface; the
  internal store still uses Raven's four-state model (`sent`, `delivered`,
  `resolved`, `expired`) and the BusClient maps between them.

[Unreleased]: https://github.com/0xDarkMatter/raven/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/0xDarkMatter/raven/releases/tag/v0.1.1
[0.1.0]: https://github.com/0xDarkMatter/raven/releases/tag/v0.1.0
