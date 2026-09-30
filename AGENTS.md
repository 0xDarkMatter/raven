# AGENTS — raven v2 developer guide

Guide for AI agents and human contributors working in this codebase. v0.2.0 is
a breaking rewrite; this guide describes the v2 end state. Rationale is cited,
not restated — decisions of record live in [docs/adr/](docs/adr/).

## What raven is

**raven** is a zero-infra, single-host coordination substrate for multi-agent
runs: an append-only SQLite log partitioned into channels, with read-state per
channel kind (ADR-001). Org-scoped chat belongs to Buzz; raven does in-run
coordination.

## Naming (ADR-004 — get this right or nothing imports)

| | |
|---|---|
| **Python import** | `raven_bus` — `from raven_bus import db, log, ...`. The v1 import root is gone; v1 code uses `raven_bus.compat`. |
| **CLI** | `raven` (entry point `raven_bus.cli.main:cli_main`) |
| **pip dist name** | **UNDECIDED.** The `raven` PyPI name belongs to Sentry's legacy client. Install from source: `pip install -e .` |
| **Version** | `0.2.0` (`raven_bus.__version__`; single source — pyproject reads it) |

## Run & test

```bash
just check      # THE gate: ruff + full suite at 100% coverage (run before landing)
just test       # full suite, no coverage (fast loop); `just test -k claims` filters
python -m pytest tests/v2/test_claims.py -q   # one module's tests
```

Every code change ships its tests in the same change. New code with no test
fails the 100% gate. Tests: `tests/v2/` (unit, one file per module) and
`tests/integration/` (subprocess runs of `examples/01-04`; 03/04 exercise the
compat shim). `tests/v2/test_invariants.py` turns the landmines below into
failing tests — if it fails, read the landmine before touching its allowlist.

## Architecture map

```
src/raven_bus/
├── models.py     pydantic models + the ADR-002 address grammar
│                 (validate_atom / parse_consumer_id / validate_channel_name)
├── exceptions.py RavenBusError → {InvalidAddressError (also ValueError),
│                 UnknownChannelError, UnknownMessageError, ClaimDeniedError,
│                 WrongChannelKindError, InvalidBodyError (also ValueError),
│                 SchemaMismatchError (also RuntimeError), TeardownBlockedError,
│                 StoreUnavailableError}
│                 — every subclass is exported from `raven_bus` (a test pins it)
├── paths.py      resolve_db_path(): arg > RAVEN_DB > ~/.raven/bus.db
├── db.py         init_db() (idempotent, process-cached; SchemaMismatchError on a
│                 foreign file), connection(create=True) ctx mgr (WAL + foreign_keys
│                 + Row, commit/rollback; create=False never makes a file — ravend),
│                 probe() (read-only health check), is_busy_error(), data_version(),
│                 sweep(count_expired=False)
│                 (the ADR-001 enforcement point), teardown_run(), instance_id()
├── channels.py   ensure_channel (race-safe get-or-create; kind checked) /
│                 get_channel / list_channels — kind is immutable
├── log.py        append() (the ONLY messages writer; ensure=True = get-or-create,
│                 any kind; bodies capped at MAX_BODY_DEPTH=64) + read_after /
│                 read_all_after (cross-channel, id-ordered) / read_by_id / read_thread
├── cursors.py    broadcast: pending() + ack() (cursor-jump only) + get_cursor()
├── claims.py     queue: claim_next / renew / complete / release / get_claim
├── consumers.py  touch() — the ONLY writer of `consumers` (heartbeat + upsert)
├── compat.py     v1 BusClient shim on the v2 store (deprecated, one release)
├── policy.py     THE attention layer (ADR-003/006) — plan() + render() (push form,
│                 the harness) and render_hint() (bounded pull notice, the hook); pure,
│                 deterministic, NO I/O and NO clock reads (now + budget are inputs).
│                 The ONLY composer of injection text; adapters call it and
│                 shuttle bytes, never building framing themselves.
├── http/         ravend — optional loopback HTTP bridge (the `[http]` extra; ADR-005)
│                 app.py = frozen route table + error envelope + exception→status map;
│                 read.py / write.py / sse.py = the 1:1 handlers; cli/serve.py runs it
├── adapters/     deliver bus messages INTO a running agent session (ADR-006 — THIN):
│   ├── acp/      protocol.py = minimal ACP client (JSON-RPC 2.0 over the child's
│   │             stdio: initialize, session/new, session/set_mode (--mode),
│   │             session/prompt, session/update, session/cancel);
│   │             harness.py = the dumb-pipe loop (gather pending → plan → deliver → ack-after)
│   └── hooks/    Claude Code PreToolUse hook: peek.py (runnable as a module) +
│                 raven-inbox-hook.sh (trivial wrapper, forces exit 0). PEEK-ONLY —
│                 never acks; emits render_hint as additionalContext JSON.
├── migrations/0002_v2_schema.sql
└── cli/          Typer `raven`: send read ack claim done release tail
                  channels doctor teardown version serve acp  (_common.py = exit codes + error map)
```

Six tables: `channels`, `messages`, `cursors`, `claims`, `consumers`, `bus_meta`
(`schema_version`, plus a lazily written `instance_id`).
`messages` has **no status column** — read-state lives in `cursors`/`claims`.

## Landmines (ADR-001 — these are hard invariants)

Treat each as a build-breaker if violated. The decision text owns the *why*.

- **Append-only: never `UPDATE` or `DELETE` a `messages` row** outside
  `db.teardown_run` (the one sanctioned prefix-scoped delete; stream retention would be the second, but is not implemented).
  `log.append` is the sole writer. Reads never mutate.
- **Every liveness read filters `expires_at`.** `read_after` filters by default
  (`include_expired=True` is for `tail`/forensics only); `cursors.pending` and
  `claims.claim_next` skip expired. v1 shipped `expires_at` but never enforced
  it — v2 read paths call `db.sweep` first.
- **Ack is a cursor jump, not per-message.** `cursors.ack(up_to_id)` advances
  monotonically (`MAX(current, up_to_id)`); backwards ack is a silent no-op.
  It **clamps** `up_to_id` to the channel's current head first — ids are
  global, so acking a foreign or future id would otherwise hide every later
  message on this channel forever. No gap tracking — `tail` covers forensics.
- **Claims use `INSERT … ON CONFLICT DO NOTHING`.** Rowcount 1 wins; a lost
  race retries the next candidate rather than returning `None`. Never a
  read-modify-write claim.
- **Channel kind is immutable.** Re-ensuring a channel with a different `kind`
  raises `WrongChannelKindError`. Cursor ops require `broadcast`; claim ops
  require `queue`.
- **Lease bookkeeping lives in `claims`, never `messages`.** `sweep` flips a
  lapsed lease to state `lapsed` (the row and its `deliveries` count are kept
  DURABLY — never deleted); at `max_deliveries` it flips to `dead` instead.
  `claim_next` re-wins a lapsed row via a guarded `UPDATE … WHERE
  state='lapsed'` that increments `deliveries` atomically — there is no
  caller-side snapshot, so any sweep call site is harmless to dead-letter
  accounting. A voluntary `release` flips the row to `lapsed` with
  `deliveries - 1` (immediately re-claimable; it undoes only its own
  claim's count — resetting to 0 let a poison message dodge dead-lettering
  forever) — **never delete** a claim row: a deleted row falls below every
  process's claim frontier and the message becomes unclaimable.
- **`teardown_run` refuses rather than orphan.** If a message outside the
  run replies to / threads under one inside it, the schema's foreign keys
  forbid the delete; `teardown_run` raises `TeardownBlockedError` naming the
  blockers and deletes nothing. Don't "fix" this by nulling `reply_to` —
  that's an `UPDATE` on `messages` (append-only).
- **There is no migration chain — `~/.raven/bus.db` is live (fleetflow).**
  `init_db` skips the SQL when the stored `schema_version` matches, so
  editing `0002_v2_schema.sql` in place never reaches existing DBs; and it
  *refuses* any other version, so bumping `SCHEMA_VERSION` bricks every
  existing DB. A schema change needs a real, tested migration step first.

## Landmines — the HTTP bridge (ADR-005)

ravend is a **thin loopback bridge**, not a second store. The wire surface is
frozen in `http/app.py`'s route table (= ADR-005's endpoint table).

- **Thin-bridge rule: a handler maps 1:1 onto one module-contract call.** It
  validates/decodes, opens one `db.connection`, calls one contract function,
  encodes the result. **Adding logic to a handler is a defect** — it belongs in
  the module contract (where it's already at 100% coverage), not in HTTP. The
  one sanctioned pairing is `send`'s `ensure_channel` + `append(ensure=False)`
  when the request names a `kind` (without one it is a single `append(ensure=True)`),
  mirroring `raven send`; everything else is a single call.
- **GET handlers never write.** v1's `GET /inbox` registered alias rows as a
  side effect — that bug class is the reason this line exists. (The one nuance:
  `GET /pending` calls `cursors.pending`, which by module contract runs the
  opportunistic sweep and upserts the consumer's presence row via
  `consumers.touch` — a never-seen consumer gets registered, like
  `/heartbeat` (ADR-005 Amendment). That is the *module's* write,
  not the handler's — the handler still makes exactly one contract call.)
- **The claim frontier is a pure per-process optimisation.** `claims._FRONTIER`
  is an in-memory watermark keyed by `path|instance_id|dev:ino` + channel that
  lets a warm `claim_next` skip a cold backlog scan. `instance_id` is a uuid
  in `bus_meta`, backfilled once inside `claim_next`'s transaction — the key
  includes it because ext4 reuses a replaced file's inode, and a stale
  frontier silently hides every message below it; with no instance id the
  cache is skipped. Never persisted, never read for correctness — a fresh
  process re-derives it. Correctness must never depend on it being populated.
- **SSE tests need the real-uvicorn harness.** Sync `TestClient.stream` over an
  infinite SSE generator hangs on context exit — the portal can't reliably
  deliver `http.disconnect`, so the generator's poll loop never sees the client
  go. A real socket close does. Streaming tests therefore spin up a real uvicorn
  on an ephemeral loopback port (per-test daemon thread); see
  `tests/v2/test_http_sse.py`'s module docstring.

## Landmines — adapters (ADR-003/006 — the P3 attention layer)

`policy` + two thin adapters. Same rule shape as the store landmines: the
decision text owns the *why*; treat each as a build-breaker.

- **Only `policy` composes injection text.** An adapter that builds its
  own framing — even a header line — is a **defect**. The sender-attributed data
  frame IS the prompt-injection defense (ADR-003); two implementations drift.
  The harness calls `policy.plan` then `policy.render`; the hook calls
  `policy.render_hint`; each prints/submits the result verbatim.
- **The hook announces, never re-injects (issue #1).** It fires on every tool
  call and never acks, so it emits `render_hint`'s bounded notice (≤2,000
  chars, no bodies/types) — never `render`'s full blocks, which repeated the
  whole backlog each call. The fyi due-rule is `policy._fyi_due`, shared by
  `plan` and `render_hint`; don't fork it.
- **Pulled content must stay data-framed.** The notice points agents at
  `raven read --framed`, which prints `policy.render`'s frame. Plain
  `raven read` output runs every sender/type through `policy.single_line`
  (all `str.splitlines()` boundaries, incl. U+2028) — a raw `type` with a
  newline forged whole message lines. Never print message fields unsanitised.
- **`policy` stays pure: no clock reads, no I/O, no randomness.** `now` and the
  token budget are *inputs* to `plan` (`datetime` is passed in; the harness
  passes `datetime.now(UTC)`, the hook the same). A `datetime.now()` or file
  read inside `policy.py` is a build-breaker — the functions must stay
  deterministic for identical inputs (it's how they're tested).
- **The hook must NEVER write — not even presence.** It opens the DB
  read-only (`mode=ro`, 250 ms busy timeout) and issues SELECTs only
  (`cursors.get_cursor` + `log.read_after`): no `init_db`, no `sweep`, no
  `consumers.touch`, and a missing DB file stays missing. Under a held write
  lock the old read path stalled every tool call ~6 s. Unknown or
  non-broadcast watched channels are skipped (one stderr line), not fatal.
  `cursors.ack` appears nowhere in `adapters/hooks/`; only the harness moves
  a cursor.
- **A hook inside a harness-driven agent stays quiet on the harness's
  channels.** `raven acp` sets `RAVEN_ACP_CONSUMER` / `RAVEN_ACP_CHANNELS` in
  the agent's env; the hook skips exactly those. Otherwise it re-announces
  ids the harness injected but hasn't acked yet (acks follow the turn).
  Code calling `run_harness` with its own child doesn't get the markers.
- **The harness acks ONLY after the whole boundary's prompts succeed** (none
  on `AcpError`), and **per channel, up to the longest prefix of that
  channel's pending ids delivered this session** — walking from the cursor,
  stopping at the first undelivered id. That never passes a deferred id, and
  one channel's deferral can't pin another's (the old global
  `plan.ack_up_to` cap stalled whole channels once 100 delivered-but-unacked
  ids filled the pending window, and re-injected them on every restart).
  `plan.ack_up_to` stays meaningful for single-channel callers only.
- **The harness's same-session redelivery guard is a SET of delivered ids,
  never a per-channel max.** A max hides lower-id *deferred* messages from
  `policy.plan`, which then computes `ack_up_to` without them — the cursor
  jumps over messages injected zero times (issue #3). Gathering pages past
  already-delivered ids (bounded) so `plan` always sees undelivered ones.
- **The hook's output is `hookSpecificOutput.additionalContext` JSON, never
  bare text.** Claude Code logs plain PreToolUse stdout and never shows it
  to the model (issue #2); `peek._emit` owns the envelope. The wrapper must
  not `exec` python — `exec` makes its exit-0 guarantee unreachable — and
  every command line in it ends in ` #`, so a CRLF copy's stray `\r` lands
  in a comment instead of making bash exit 2 (PreToolUse's *blocking* code).
  Interpreter order: `$RAVEN_PYTHON` alone if set, else `python3`, then
  `python`.
- **The harness is a dumb pipe: no respawn.** `run_harness` exits when the child
  exits or ACP errors (0 only if the child exited 0; 10 otherwise, with one
  `raven-acp: <phase> failed: …` stderr line); lifecycle (spawn/reap/restart)
  belongs to the spawner (design §9 Q4 — ff-spawn's journal). Two owners of
  respawn = orphan factories. The ACP timeout is an **inactivity** limit, off
  by default (`raven acp --timeout S`): ACP is silent during long tool calls,
  and a dead agent is detected without one.
- **Subprocess-driven hook tests are invisible to coverage.** The hook's real
  path runs in a child process (`python -m …peek`), so `--cov` never sees it.
  Exercise the logic through its **in-process twins**: import `peek`/`policy`
  and call `peek()` / `policy.plan`/`render` directly in the same process
  (see `tests/v2/test_hook.py`, `tests/v2/test_policy.py`). A line only hit by
  a subprocess call reads as uncovered and fails the 100% gate.

## Testing patterns

Fixtures, sibling-stub and raw-SQL setup patterns live in
[docs/TESTING.md](docs/TESTING.md). The one landmine-grade rule: **scope any
stand-in for another module via `pytest.MonkeyPatch`, never bare assignment**
— a bare `mod.attr = stub` leaks into every later test file (it caused 8
cross-file failures at the wave-1 landing).

## CLI surface

Frozen — flags may grow, commands may not. The full flag table is
[docs/CLI.md](docs/CLI.md). Contract for every command: exit `0` ok / `2`
usage / `3` not-found / `10` error (`cli/_common.py`); failures are ONE line
`error: …`, tracebacks never reach users. Consumer ids are `<role>@<run>`,
channels path-style, atoms lowercase `[a-z0-9][a-z0-9._-]*` (ADR-002).

## Out of scope

P1-P4 shipped (store, ravend, adapters, fleetflow integration — fleetflow
ADR-022/023). Still out: the Buzz bridge (P5). See
[docs/design/raven2-architecture.md §8](docs/design/raven2-architecture.md#8-phasing).
