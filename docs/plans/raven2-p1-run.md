# raven2-p1 — fleetflow run plan (P1: core store rewrite)

> Status: **executed 2026-08-12 — historical; do not re-run.** Disposable run plan — cites decisions, never owns
> them. Governing decisions: [ADR-001](../adr/ADR-001-append-only-log-channel-kind-read-state.md),
> [ADR-002](../adr/ADR-002-addressing-and-single-host-db.md),
> [ADR-003](../adr/ADR-003-injection-in-adapters-messages-are-data.md) (P3 —
> out of scope this run, cited for boundary),
> [ADR-004](../adr/ADR-004-import-rename-compat-shim.md).
> Architecture: [raven2-architecture.md](../design/raven2-architecture.md).

## Scope

P1 only: schema v2, `raven_bus` package, CLI, compat shim, tests, docs.
**Out of scope:** ravend HTTP (P2), adapters (P3), fleetflow wiring (P4),
Buzz bridge (P5). Any lane tempted to start P2+ stops and reports instead.

## Run parameters

- Run name: `raven2-p1` (grammar: `[a-z0-9-]+`, no dots)
- Base: `main`
- Posture at close: `tested` (docs-parity + qa + regression waves)
- Verify: every build lane gets ≥1 cross-provider refuter (ADR-invariant
  prompts below); claims/lease lane gets a race-focused refuter

## Wave 0 — skeleton (orchestrator, inline, BEFORE spawning)

The orchestrator authors and commits the frozen interface skeleton so wave-1
lanes never negotiate interfaces with each other:

- `src/raven_bus/__init__.py`, `models.py` (Message, Channel, Claim, Cursor
  pydantic models), `exceptions.py`, `paths.py`
- `src/raven_bus/migrations/0002_v2_schema.sql` (DDL exactly per design §3)
- Module stubs with full signatures + docstrings: `db.py`, `channels.py`,
  `log.py`, `cursors.py`, `claims.py`, `compat.py`, `cli/` command stubs
- Conftest: tmp-path DB fixture + cache-reset helpers

Wave 1 lanes implement bodies; **signatures are frozen** — a lane that needs
a signature change reports it in FINAL REPLY rather than editing another
lane's file.

## Wave 1 — build (parallel, file-disjoint)

| Lane | Model | Owns (exclusive) | Delivers |
|---|---|---|---|
| `store` | sonnet | `src/raven_bus/db.py`, `tests/v2/test_db.py` | connection mgmt, WAL, init, opportunistic sweep (expiry + lease reap — ADR-001), teardown_run |
| `log` | sonnet | `src/raven_bus/channels.py`, `src/raven_bus/log.py`, `tests/v2/test_log.py` | channel CRUD by kind, append, id-range reads (expires_at filtered — ADR-001) |
| `cursors` | glm | `src/raven_bus/cursors.py`, `tests/v2/test_cursors.py` | broadcast read + cursor-jump ack (jump-only; no gap tracking) |
| `claims` | codex | `src/raven_bus/claims.py`, `tests/v2/test_claims.py` | atomic claim, lease renew, requeue-on-expiry, dead-letter. DO NOT COMMIT (fleetflow ADR-006 — orchestrator commits). Sandbox has no venv/network: write code + tests, do NOT run them; `TESTS: not-run` is the expected FINAL REPLY value, the orchestrator runs the suite at collect |
| `cli` | sonnet | `src/raven_bus/cli/`, `tests/v2/test_cli.py` | `send read ack claim done release tail channels doctor teardown version` over the frozen interfaces |
| `compat` | glm | `src/raven_bus/compat.py`, `tests/v2/test_compat.py` | v1 `BusClient(session_id, role)` on compat broadcast channels (ADR-004) |

File-disjointness holds: no two lanes share a path. `models.py` (including
the implemented address-grammar helpers), `exceptions.py`, `paths.py`, the
migration SQL, module stub signatures, and `tests/v2/conftest.py` are frozen
wave-0 artifacts; lanes import, never edit. v2 tests live under `tests/v2/`
so the live v1 suite (`tests/unit/`, `tests/integration/`) stays untouched
until the `integrate` lane retires it.

Lane environment (claude-family lanes): create a venv inside the lane —
`uv venv .venv && uv pip install -e ".[dev,http]"` — then run only your own
test file: `.venv/Scripts/python.exe -m pytest tests/v2/test_<module>.py -q`
(or `.venv/bin/python` on POSIX paths). Full-suite 100% coverage is the
`integrate` lane's gate, not yours.

## Wave 2 — verify + integrate (after wave-1 collect)

| Lane | Model | Task |
|---|---|---|
| `refute-append-only` | codex or grok | try to refute: "no code path mutates a messages row after insert" (ADR-001). Any UPDATE/DELETE on messages outside teardown/retention = finding |
| `refute-claims` | opus | adversarial race review of claims: two claimants, lease expiry mid-flight, dead-letter off-by-one |
| `refute-expiry` | glm | prove expired messages can still be read/claimed anywhere = finding |
| `integrate` | sonnet | port `examples/` to v2 (01/02 native; 03/04 via compat), integration tests, full suite green at `--cov-fail-under=100` |
| `docs` | glm | README + AGENTS + QUICKSTART rewrite for v2 (ADR-002 addressing, ADR-004 naming); grep-gate: `claude_bus` appears only in compat + changelog |

## Packet constraints (paste into every packet)

- BLUFs of ADR-001, ADR-002, ADR-004 verbatim (workers have no ambient
  knowledge of the decision log — non-Claude lanes cannot read `docs/adr/`
  unless pointed there, and Codex sandbox may not reach it at all).
- Python 3.12+, ruff clean, pytest via
  `python -m pytest tests/ -p no:cacheprovider --tb=short -q`.
- Coverage is locked at 100% (`--cov-fail-under=100`) — new code ships with
  tests in the same lane.
- Relative paths only (guard preamble); no new dependencies without
  reporting.
- FINAL REPLY shape: `TESTS: <passed>/<failed>`, `FILES_CHANGED: <n>`,
  plus one line per deviation from the frozen signatures (or `DEVIATIONS: none`).

## Landing

Through fleet-ops as always: sequential, test-gated, `ff-collect
--check-main-clean` after the run. The compat shim keeps v1 integration
tests as the regression net until `integrate` replaces them.

## Post-run doc duty

- Design doc §9 questions are settled (see doc); if any lane's DEVIATIONS
  force a signature change, record it here and, if it contradicts an ADR,
  supersede the ADR first.
- CHANGELOG `[Unreleased]` gains the v2 rewrite entry; the old Phase-2 HTTP
  items move under P2.
