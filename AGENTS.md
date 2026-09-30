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
| **Version** | `0.2.0.dev0` (`raven_bus.__version__`) |

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
│                 WrongChannelKindError}
├── paths.py      resolve_db_path(): arg > RAVEN_DB > ~/.raven/bus.db
├── db.py         init_db() (idempotent, process-cached), connection() ctx mgr
│                 (WAL + foreign_keys + Row, commit/rollback), data_version(),
│                 sweep() (the ADR-001 enforcement point), teardown_run()
├── channels.py   ensure_channel / get_channel / list_channels — kind is immutable
├── log.py        append() (the ONLY messages writer) + read_after/read_by_id/read_thread
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

Six tables: `channels`, `messages`, `cursors`, `claims`, `consumers`, `bus_meta`.
`messages` has **no status column** — read-state lives in `cursors`/`claims`.

## Landmines (ADR-001 — these are hard invariants)

Treat each as a build-breaker if violated. The decision text owns the *why*.

- **Append-only: never `UPDATE` or `DELETE` a `messages` row** outside
  `db.teardown_run` (the one sanctioned prefix-scoped delete) and retention.
  `log.append` is the sole writer. Reads never mutate.
- **Every liveness read filters `expires_at`.** `read_after` filters by default
  (`include_expired=True` is for `tail`/forensics only); `cursors.pending` and
  `claims.claim_next` skip expired. v1 shipped `expires_at` but never enforced
  it — v2 read paths call `db.sweep` first.
- **Ack is a cursor jump, not per-message.** `cursors.ack(up_to_id)` advances
  monotonically (`MAX(current, up_to_id)`); backwards ack is a silent no-op.
  No gap tracking — `tail` covers forensics.
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
  `deliveries=0` (immediately re-claimable, doesn't count) — **never
  delete** a claim row: a deleted row falls below every process's claim
  frontier and the message becomes unclaimable.
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
  one sanctioned pairing is `send`'s `ensure_channel` + `append(ensure=False)`,
  mirroring `raven send`; everything else is a single call.
- **GET handlers never write.** v1's `GET /inbox` registered alias rows as a
  side effect — that bug class is the reason this line exists. (The one nuance:
  `GET /pending` calls `cursors.pending`, which by module contract runs the
  opportunistic sweep and upserts the consumer's presence row via
  `consumers.touch` — a never-seen consumer gets registered, like
  `/heartbeat` (ADR-005 Amendment). That is the *module's* write,
  not the handler's — the handler still makes exactly one contract call.)
- **The claim frontier is a pure per-process optimisation.** `claims._FRONTIER`
  is an in-memory `{(db_path, channel_id): id}` watermark that lets a warm
  `claim_next` skip a cold backlog scan. It is never persisted and never read
  for correctness — a fresh process re-derives it from the same query that
  serves the request. Correctness must never depend on it being populated.
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
- **`policy` stays pure: no clock reads, no I/O, no randomness.** `now` and the
  token budget are *inputs* to `plan` (`datetime` is passed in; the harness
  passes `datetime.now(UTC)`, the hook the same). A `datetime.now()` or file
  read inside `policy.py` is a build-breaker — the functions must stay
  deterministic for identical inputs (it's how they're tested).
- **The hook must NEVER ack.** It only peeks (`cursors.pending`, which advances
  nothing) and renders. `cursors.ack` appears nowhere in `adapters/hooks/`. This
  is what lets a hook run *beside* a harness on the same consumer without
  double-delivery — only the harness moves the cursor (ADR-006).
- **The harness acks ONLY after a successful `session/prompt`.** `_deliver`
  submits first, then `cursors.ack`. If the child dies (ACP EOF) before the
  submit completes, the loop returns and the message stays pending for the next
  process/boundary — an undelivered message must never be acked. Every ack is
  also capped at `plan.ack_up_to`, which stops before the first deferred id
  (cursor-jump can't skip an older still-deferred message — ADR-001).
- **The harness's same-session redelivery guard is a SET of delivered ids,
  never a per-channel max.** A max hides lower-id *deferred* messages from
  `policy.plan`, which then computes `ack_up_to` without them — the cursor
  jumps over messages injected zero times (issue #3). `plan` must always
  see every undelivered pending id.
- **The hook's output is `hookSpecificOutput.additionalContext` JSON, never
  bare text.** Claude Code logs plain PreToolUse stdout and never shows it
  to the model (issue #2); `peek._emit` owns the envelope. The wrapper must
  not `exec` python — `exec` makes its exit-0 guarantee unreachable.
- **The harness is a dumb pipe: no respawn.** `run_harness` exits when the child
  exits or ACP errors; lifecycle (spawn/reap/restart) belongs to the spawner
  (design §9 Q4 — ff-spawn's journal). Two owners of respawn = orphan factories.
- **Subprocess-driven hook tests are invisible to coverage.** The hook's real
  path runs in a child process (`python -m …peek`), so `--cov` never sees it.
  Exercise the logic through its **in-process twins**: import `peek`/`policy`
  and call `peek()` / `policy.plan`/`render` directly in the same process
  (see `tests/v2/test_hook.py`, `tests/v2/test_policy.py`). A line only hit by
  a subprocess call reads as uncovered and fails the 100% gate.

## Testing patterns

### Fixtures (tests/v2/conftest.py)

Every test gets an isolated DB under `tmp_path` with the init cache reset:

```python
@pytest.fixture()
def db(tmp_path):
    from raven_bus.db import _reset_init_cache, init_db
    _reset_init_cache()
    path = tmp_path / "bus.db"
    init_db(path, force=True)
    return path
```

`init_db` is process-cached (`force=True` bypasses; `_reset_init_cache()` clears
it for multi-DB-in-one-process tests). Add module-specific fixtures in your own
test file, not in `conftest.py` (a frozen wave-0 artifact).

### Landmine: scope sibling stubs via `monkeypatch`, never bare assignment

Lanes call only sibling modules' public contracts. During the parallel build a
lane's siblings may be stubs, so a test stands them in with faithful contract
implementations. **Always do this through `pytest.MonkeyPatch`:**

```python
def _install_sibling_stubs(monkeypatch):
    monkeypatch.setattr(cursors_mod.db, "sweep", lambda _conn: None)
    monkeypatch.setattr(cursors_mod.channels, "get_channel", get_channel)
    monkeypatch.setattr(cursors_mod.log, "read_after", read_after)
```

A bare `cursors_mod.db.sweep = ...` replaces the **real** module attribute for
every later test file in the process — it caused **8 cross-file failures at the
wave-1 landing**. `monkeypatch` auto-restores on teardown. (`tests/v2/test_cursors.py`
documents this in `_install_sibling_stubs`.)

### Raw-SQL data setup

Sibling-lane stand-ins set up rows with direct SQL (e.g. `_insert_message`
appends a `messages` row matching the `log.append` contract) so the code under
test exercises real v2 semantics against a live schema.

## CLI surface (frozen)

```
raven send      --channel C --from R@RUN -t TYPE --body JSON
                [--urgency U] [--tag T]... [--reply-to ID] [--expires-in S]
                [--kind broadcast|queue|stream]
raven read      --channel C --as R@RUN [-m MAX] [-j]      (broadcast pending)
raven ack       --channel C --as R@RUN --up-to ID          (cursor jump)
raven claim     --channel C --as R@RUN [--lease S] [-j]    (queue: claim next)
raven done      --id ID --as R@RUN                         (complete claim)
raven release   --id ID --as R@RUN
raven tail      [--channel C] [--from ID] [--no-follow] [--json] [--interval S]
raven channels  [--prefix P] [-j]
raven doctor    [--db P]
raven teardown  --run RUN [--yes]
raven version
raven serve    [--host 127.0.0.1] [--port 7713] [--db P] [--yes-expose]
                                                            (run ravend under uvicorn; `[http]` extra;
                                                             non-loopback --host needs --yes-expose)
raven acp      --as R@RUN --channel C [--channel C]... [--reply-to C]
               [--db P] [--poll-interval S] [--budget N] [--cwd .]
               [--mode M] [--initial-prompt-file F] -- <agent cmd...>
                                                            (dumb-pipe ACP harness; ADR-006.
                                                             --mode = session/set_mode after
                                                             session/new; headless lanes need a
                                                             non-prompting permission mode.
                                                             --initial-prompt-file = the task
                                                             packet, VERBATIM boundary 0 —
                                                             trusted spawner input, never
                                                             data-framed; bus messages are)
```

Exit codes (`cli/_common.py`): `0` ok / `2` usage / `3` not-found / `10` error.
Failures render as one-line `error: …`; tracebacks never reach users. Consumer
ids are `<role>@<run>`; channels are path-style; atoms are lowercase
`[a-z0-9][a-z0-9._-]*` (ADR-002).

## Out of scope (P4+, see the design doc)

P3 shipped: `raven acp` + the Claude Code hook share `raven_bus.policy`
(ADR-003/006 — injection lives in adapters, never the store; see the
[adapters landmines](#landmines--adapters-adr-003006--the-p3-attention-layer)).
P4 shipped: fleetflow heartbeats (ADR-022) + `ff-spawn --acp` (fleetflow
ADR-023 — packet as trusted boundary 0 via `--initial-prompt-file`, mode via
`--mode`, verdict from telemetry). Still out: Buzz bridge (P5). See
[docs/design/raven2-architecture.md §8](docs/design/raven2-architecture.md#8-phasing).
