# ADR-006: Adapters are thin; one policy module owns injection; the ACP harness is a dumb pipe

- Status: accepted
- Date: 2026-08-12
- Touches: src/raven_bus/policy.py, src/raven_bus/adapters/, src/raven_bus/cli/acp.py

## BLUF

Delivery into a running agent session is split into exactly two layers:
**`raven_bus.policy`** (pure functions — no I/O, no clocks read
internally) decides *what* enters a session and *how it is framed*;
**adapters** (`raven acp` harness, the Claude Code PreToolUse hook)
decide only *when* a turn boundary occurs and shuttle bytes. Per
ADR-003's standing rule, injected content is always **sender-attributed
data, never instructions**, and only `policy.render` produces that
framing — adapters MUST NOT compose injection text themselves. The ACP
harness supports the minimal client subset of the Agent Client Protocol
(JSON-RPC 2.0 over the child's stdio: `initialize`, `session/new`,
`session/prompt`, `session/update` notifications, `session/cancel`) and
is a **dumb pipe**: it never respawns its child, exits when the child
exits, and leaves lifecycle to whatever spawned it (design §9 Q4 —
ff-spawn owns lifecycle). The hook adapter mirrors pigeon's proven
shape: silent when nothing is pending, a compact text block when
something is, configuration via `RAVEN_CONSUMER` (+ optional
`RAVEN_CHANNELS`, `RAVEN_DB`), never an error that blocks the tool call.

## Context

ADR-003 fixed the policy tiers (blocking → alone at next boundary;
prompt → batched; fyi → token-capped digest) and the data-framing rule.
P3 implements them. buzz-acp proved the harness shape (batch → one
`session/prompt` per boundary); MCP cannot push (stateless since the
2026-07-28 revision), so turn-boundary delivery is the only semantic.

## Decision

- `policy.plan(pending, budget) -> InjectionPlan{interrupt, batch,
  digest_source, deferred, ack_up_to}` and `policy.render(...) -> str`
  are the entire brain; both are pure and deterministic (timestamps and
  token budgets are inputs). Token estimation is chars/4 — crude is
  fine, the cap is a guardrail not an invoice.
- Adapters live under `src/raven_bus/adapters/` (`acp/` package, the
  hook script in `adapters/hooks/`); the CLI gains `raven acp -- <agent
  cmd...>`.
- The harness acks (cursor-jump) only AFTER a successful
  `session/prompt` submission — an undelivered message must remain
  pending for the next boundary or the next process.
- Testing uses a **fake ACP agent** subprocess (ships in tests/) — the
  protocol client is tested against it, never against a live model.

## Amendment (2026-09-30, issue #3)

An implementation defect, fixed without changing the decision:

- **Harness redelivery guard.** The in-memory guard that stops a session
  re-injecting what it already delivered is a set of exact ids. A
  per-channel max hid deferred lower ids from `plan`, breaking the
  "ack never passes a deferred id" rule above — delivered-zero-times loss.

## Alternatives rejected

- **Harness respawns crashed agents** — buzz-acp does; here lifecycle
  belongs to ff-spawn's journal/reap machinery (design §9 Q4). Two
  owners of respawn = orphan factories.
- **Adapters compose their own injection text** — the framing IS the
  prompt-injection defense; one implementation or it drifts.
- **Full ACP surface** (filesystem, terminals, permissions) — the
  harness only needs prompt/update; the rest is agent-side capability
  we neither grant nor proxy.

## Consequences

- New harnesses (pi, future CLIs) are thin adapters over `policy`.
- Policy changes (tier semantics, budgets) never touch adapter code.
- A hook and a harness can serve the same consumer concurrently without
  double-delivery only via cursor semantics — the hook is read-only
  (peeks, never acks); ONLY the harness acks. Document in both.
