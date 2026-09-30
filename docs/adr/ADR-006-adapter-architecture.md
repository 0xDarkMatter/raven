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

## Amendment (2026-09-30, issues #1, #2 and #3)

Fixed without changing the decision (policy owns all text; the hook peeks
and never acks; the harness acks after delivery):

- **Hook transport (#2).** The hook's output must travel as PreToolUse
  `hookSpecificOutput.additionalContext` JSON. Claude Code writes plain
  PreToolUse stdout to its debug log, never the model's context, so the
  shipped hook rendered correctly and delivered nothing.
- **Hook content (#1).** The hook's "compact text block" is
  `policy.render_hint` — a bounded pull notice (counts, ids, senders, the
  `raven read` command; ≤2,000 chars; no bodies or types) — not
  `policy.render`'s full blocks. A peek-only adapter that fires every tool
  call and never acks re-pushed the entire backlog on every call. It stays
  stateless (no hook-local "already shown" record), so the hook remains
  read-only. `render_hint` shares `plan`'s fyi due-rule. Only the harness,
  which owns the loop and acks, pushes full `render` blocks.
- **Hook config (as built).** `RAVEN_CHANNELS` is *required* whenever
  `RAVEN_CONSUMER` is set (the BLUF's "optional" is superseded): the hook
  derives no channel names, and a consumer with no channels stays silent.
  `RAVEN_PYTHON` optionally names the interpreter the wrapper runs.
- **Client subset (as built).** Besides the BLUF's list, the harness sends
  `session/set_mode` right after `session/new` when `--mode` is given — a
  headless lane must leave a prompting permission mode to use tools.
- **Harness redelivery guard.** The in-memory guard that stops a session
  re-injecting what it already delivered is a set of exact ids. A
  per-channel max hid deferred lower ids from `plan`, breaking the
  "ack never passes a deferred id" rule above — delivered-zero-times loss.

## Amendment (2026-09-30, QA pass)

A hostile QA pass of the adapter layer changed four things the Decision and
Consequences above state differently. The decision itself — pure policy,
thin adapters, dumb-pipe harness, a hook that never acks — stands.

- **Ack rule.** The harness no longer caps acks at the global
  `plan.ack_up_to`. It acks each channel, after the whole boundary succeeds,
  up to the longest prefix of that channel's pending ids delivered this
  session (stopping at the first undelivered id). The global cap let one
  channel's deferral pin another, and once 100 delivered-but-unacked ids
  filled a channel's pending window that channel stalled for the session —
  later blocking messages included. `ack_up_to` remains the rule for a
  single-channel caller.
- **The hook beside a harness *does* double-announce — now mitigated.** The
  Consequences' "can serve the same consumer concurrently without
  double-delivery" was wrong: the harness acks after the turn, so a hook in
  that turn re-announces what was just injected. `raven acp` now sets
  `RAVEN_ACP_CONSUMER` / `RAVEN_ACP_CHANNELS` in its agent's environment and
  the hook stays quiet on exactly those channels. (A caller driving
  `run_harness` with its own child doesn't get the markers.)
- **The hook is read-only, strictly.** It opens the DB read-only and runs
  SELECTs only — no `sweep`, no presence upsert, no DB creation — and skips
  unknown or non-broadcast channels. The old read path wrote on every tool
  call and stalled ~6 s behind another writer's lock. Pulled content stays
  data-framed: the notice points at `raven read --framed` (`policy.render`'s
  frame), and plain `raven read` sanitises sender/type to one line.
- **Harness lifecycle.** `run_harness` returns 0 only when the agent exits
  0 (a non-zero exit is 10, as a mid-prompt crash already was), with one
  stderr breadcrumb per failure. The ACP timeout is an *inactivity* limit,
  off by default (`raven acp --timeout`), not a 600 s total deadline that
  killed long turns while a silent hung agent waited forever. The wrapper's
  interpreter order is `$RAVEN_PYTHON`, else `python3`, then `python`.

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
