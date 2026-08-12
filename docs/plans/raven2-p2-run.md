# raven2-p2 — fleetflow run plan (P2: ravend + claim frontier)

> Status: ready to spawn. Disposable plan — cites, never owns.
> Decisions: [ADR-005](../adr/ADR-005-ravend-http-contract.md) (HTTP
> contract), [ADR-001/002/004](../adr/) (store invariants, addressing,
> naming). Architecture: [raven2-architecture.md](../design/raven2-architecture.md) §4/§8.
> Base: post-P1 main (202 tests, 100% coverage over raven_bus).

## Scope

ravend (HTTP read+write+SSE per ADR-005), `raven serve` CLI command,
and the claim-frontier optimisation (P1 waiver: candidate search walks
the terminal-claim backlog). Out of scope: adapters (P3), fleetflow
wiring (P4), Buzz bridge (P5), Process-Compose service registration
(maintainer does it post-run when ravend first runs as a standing
service).

## Run parameters

- Run name: `raven2-p2`; base `main`; orchestrator fable
- Verify: refuter on the HTTP write path + refuter on the frontier
  change (claims.py just survived two adversarial rounds — a change
  there gets fresh eyes by default)

## Wave 0 — orchestrator, inline, before spawning

- This plan + ADR-005 committed
- Skeleton: `src/raven_bus/http/` package — `app.py` with `create_app`
  FULLY IMPLEMENTED (route table frozen so no two lanes touch it),
  error-envelope + JSON helpers, and frozen handler stubs in `read.py`,
  `write.py`, `sse.py`; `cli/serve.py` stub. Signatures frozen as in P1.

## Wave 1 — build (parallel, file-disjoint)

| Lane | Model | Owns (exclusive) | Delivers |
|---|---|---|---|
| `http-read` | glm | `src/raven_bus/http/read.py`, `tests/v2/test_http_read.py` | health, channels list, messages, pending, cursor GETs — read-only, no writes ever (ADR-005) |
| `http-write` | sonnet | `src/raven_bus/http/write.py`, `tests/v2/test_http_write.py` | send/claim/renew/done/release/ack/heartbeat POSTs + full error mapping |
| `http-sse` | codex | `src/raven_bus/http/sse.py`, `src/raven_bus/cli/serve.py`, the one-line command registration in `cli/main.py`, `tests/v2/test_http_sse.py` | SSE tail (data_version fast-poll), `raven serve` (uvicorn, `--port 7713 --db`). DO NOT COMMIT; write tests but don't run them |
| `frontier` | sonnet | `src/raven_bus/claims.py`, `tests/v2/test_claims_frontier.py` | per-channel claim frontier so claim_next stops re-walking terminal backlog; MUST NOT change public signatures or weaken any invariant the P1 verify waves confirmed |

`http/app.py` and all P1 modules are frozen; http lanes call module
contracts only (thin-bridge rule, ADR-005). Lane env: same uv venv
recipe as P1 (`uv venv .venv && uv pip install -e ".[dev,http]"`), run
only your own test file; httpx TestClient for handler tests (no live
server needed except sse, which may use uvicorn in a thread or
starlette TestClient stream support).

## Wave 2 — verify + docs

| Lane | Model | Task |
|---|---|---|
| `refute-http` | opus | attack the write path: GET-writes (forbidden), envelope/status fidelity to ADR-005, percent-encoding edge cases, concurrent POST /claim exactly-one-winner over HTTP, malformed/hostile bodies, SSE consuming or mutating anything |
| `refute-frontier` | grok | refute the frontier: can it skip a claimable message (lapsed below the frontier), regress exactly-one-winner, or diverge from P1's verified semantics? |
| `docs` | glm | README (HTTP bridge section), QUICKSTART step, CHANGELOG, AGENTS structure map + landmines (thin-bridge rule, GETs never write) |

Integrate gate: orchestrator inline — full suite, 100% coverage
(http package included), ruff, ADR-005 table vs implemented routes
parity check, `--check-main-clean`.

## Packet constraints (paste into every packet)

ADR-005 BLUF + thin-bridge rule; ADR-001/002 BLUFs for the frontier
lane; Python 3.12+, ruff clean, tests colocated, coverage is
integrate's gate not yours; FINAL REPLY: `STATUS / TESTS /
FILES_CHANGED / DEVIATIONS / NOTES` exactly as P1.
