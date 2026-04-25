# Changelog

All notable changes to **claude-bus** are recorded here.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] — 2026-04-25

Edge-case + QOL polish following the v0.1.0 ship. No public-API breakage.

### Fixed

- `claude-bus read` / `ack` and `GET /message/{id}` no longer add a
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

- `claude-bus version` subcommand (mirrors `--version`).
- Short flags: `inbox -r/--role -m/--max -j/--json`,
  `send -t/--type`, `read -j/--json`.
- `doctor` checks the bundled `0001_initial.sql` migration is present
  on disk, catching broken installs early.
- `_core.read_by_id()` — identity-free message fetch primitive.

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
- **Optional HTTP bridge** (`pip install 'claude-bus[http]'`) — read-only
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
  shells (`claude-bus read 42`) and SQLite autoincrement is the simplest
  store. Wire-stable for v0.1.x.
- **Status enum is `unread` / `read`** at the public API surface; the
  internal store still uses Raven's four-state model (`sent`, `delivered`,
  `resolved`, `expired`) and the BusClient maps between them.

## [Unreleased] — Phase 2 plan (target v0.2.0)

Phase 2 is **not** built into v0.1.0 — it is documented here so callers
know what's intentionally deferred.

- `claude-bus archive <id>` + `archived` status; `BusClient.archive()`
- `claude-bus search` + `BusClient.search()` (filter by type, sender,
  recipient, since, status)
- Persistent role aliases (`role_aliases` table) + `claude-bus alias add/list/remove`
- `sessions` table + `claude-bus session close/list`,
  `init_session()` / `teardown_session()` Python API
- `claude-bus schemas list/validate` + entry-point schema discovery
- HTTP write path: `POST /send`, `POST /ack`
- Extended docs: `SCHEMA_REGISTRATION.md`, `HTTP_BRIDGE.md`, `DEPLOYMENT.md`
- Two more example projects (two-session coordination, HTTP bridge consumer)

[0.1.1]: https://github.com/0xDarkMatter/claude-bus/releases/tag/v0.1.1
[0.1.0]: https://github.com/0xDarkMatter/claude-bus/releases/tag/v0.1.0
