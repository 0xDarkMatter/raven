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
# Full suite (the gate)
python -m pytest tests/ -p no:cacheprovider --tb=short -q

# Coverage is locked at 100%
python -m pytest tests/ --cov=raven_bus --cov-fail-under=100 -q

# One v2 module's tests
python -m pytest tests/v2/test_claims.py -q
```

Every code change ships its tests in the same change. New code with no test
fails the 100% gate. v2 tests live under `tests/v2/`; the legacy v1 suite
(`tests/unit/`, `tests/integration/`) stays untouched until retired.

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
├── compat.py     v1 BusClient shim on the v2 store (deprecated, one release)
├── migrations/0002_v2_schema.sql
└── cli/          Typer `raven`: send read ack claim done release tail
                  channels doctor teardown version  (_common.py = exit codes + error map)
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
  accounting. A voluntary `release` deletes the row and does not count.

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
```

Exit codes (`cli/_common.py`): `0` ok / `2` usage / `3` not-found / `10` error.
Failures render as one-line `error: …`; tracebacks never reach users. Consumer
ids are `<role>@<run>`; channels are path-style; atoms are lowercase
`[a-z0-9][a-z0-9._-]*` (ADR-002).

## Out of scope (P2+, see the design doc)

ravend HTTP (P2), `raven-acp` + hook adapters / injection policy (P3,
ADR-003 — *not built*; injection lives in adapters, never the store), fleetflow
wiring (P4), Buzz bridge (P5). See
[docs/design/raven2-architecture.md §8](docs/design/raven2-architecture.md#8-phasing).
