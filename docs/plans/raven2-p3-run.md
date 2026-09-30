# raven2-p3 — fleetflow run plan (P3: policy + ACP harness + hook)

> Status: **executed 2026-08-12 — historical; do not re-run.** Disposable plan — cites, never owns.
> Decisions: [ADR-003](../adr/ADR-003-injection-in-adapters-messages-are-data.md)
> (tiers + data framing), [ADR-006](../adr/ADR-006-adapter-architecture.md)
> (thin adapters, dumb-pipe harness, hook peeks / harness acks).
> Base: post-P2 main (321 tests, 100% coverage).

## Scope

`raven_bus.policy`, the `raven acp` harness (minimal ACP client subset
over child stdio), the Claude Code PreToolUse hook adapter, a fake ACP
agent test fixture. Out of scope: pi adapter, fleetflow wiring (P4),
Buzz bridge (P5), respawn logic (dumb pipe — ADR-006).

## Run parameters

Run `raven2-p3`; base `main`; orchestrator fable. Verify wave: an opus
refuter on policy/framing (the injection-safety surface) and a
cross-provider refuter on the protocol/harness; docs lane; integrate
gate inline (100% coverage incl. new modules).

## Wave 0 — orchestrator, inline

ADR-006 + this plan + frozen skeleton: `policy.py` (models + function
signatures), `adapters/acp/{protocol,harness}.py` stubs,
`cli/acp.py` stub, `adapters/hooks/raven-inbox-hook.sh` stub,
`tests/v2/fake_acp_agent.py` CONTRACT (implemented by acp-protocol
lane).

## Wave 1 — build (file-disjoint)

| Lane | Model | Owns | Delivers |
|---|---|---|---|
| `policy` | sonnet | `src/raven_bus/policy.py`, `tests/v2/test_policy.py` | tier planning, token-capped digest, data-framing renderer — pure + deterministic |
| `acp-protocol` | codex | `src/raven_bus/adapters/acp/protocol.py`, `tests/v2/fake_acp_agent.py`, `tests/v2/test_acp_protocol.py` | JSON-RPC 2.0 stdio client (subset per ADR-006) + the fake agent fixture. No-commit, no-run |
| `acp-harness` | sonnet | `src/raven_bus/adapters/acp/harness.py`, `src/raven_bus/cli/acp.py`, one-line registration in `cli/main.py`, `tests/v2/test_acp_harness.py` | the bus↔ACP loop: pending → policy → session/prompt at boundaries → ack after submit → reply/telemetry back to bus; `raven acp` command |
| `hook` | glm | `src/raven_bus/adapters/hooks/` (script + README), `tests/v2/test_hook.py` | PreToolUse hook: silent-on-empty, compact block on pending, PEEKS ONLY (never acks — ADR-006), env config, exit 0 always |

## Wave 2 — verify + docs

| Lane | Model | Task |
|---|---|---|
| `refute-policy` | opus | attack tier semantics, budget math, framing (can crafted message content escape the data frame? digest starvation? ack_up_to skipping undelivered?) |
| `refute-acp` | codex or grok | attack protocol/harness: framing bugs, partial reads, child-death races, ack-before-submit windows, double-delivery vs the hook |
| `docs-p3` | glm | README/QUICKSTART/AGENTS/CHANGELOG for adapters |

Integrate gate inline: full suite, 100% coverage, ruff, escape guard.

## Packet constraints

ADR-003 + ADR-006 BLUFs pasted; FINAL REPLY shape as P1/P2; lane env
recipe as P1/P2; codex lanes no-commit/no-run.
