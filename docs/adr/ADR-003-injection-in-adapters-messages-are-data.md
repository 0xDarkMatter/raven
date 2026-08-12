# ADR-003: Injection policy lives in adapters; bus messages are data, never instructions

- Status: accepted
- Date: 2026-08-12
- Touches: adapters/ (raven-acp, hooks), docs/

## BLUF

The store never decides *when or how* a message enters an agent's context —
that is the adapter's job (`raven-acp` for ACP-driven sessions, a PreToolUse
hook for interactive Claude Code), sharing **one injection-policy module**:
`blocking` → injected alone at the next turn boundary; `prompt` → batched
into the next natural prompt; `fyi` → held and delivered as a token-capped
digest. Injected content is always framed as **sender-attributed data**
("messages from the bus"), never as instructions to execute.

## Context

MCP (2026-07-28 spec) is stateless request/response and cannot push;
injection is a property of whoever owns the agent loop. buzz-acp
(block/buzz) proved the shape: an ACP harness that batches channel events
into `session/prompt` at turn boundaries. An org-scoped future (Buzz bridge)
means messages will eventually arrive from other people's agents — untrusted
input by definition.

## Decision

Adapters own delivery timing and framing. The data/instruction framing rule
is enforced in the shared policy module, not left to each adapter.
Turn-boundary delivery is the only semantic offered — no adapter may claim
mid-completion interruption (no harness supports it).

## Alternatives rejected

- **Policy in the store** (per-message delivery directives executed by the
  bus) — couples the store to harness capabilities it can't see.
- **MCP server as the push channel** — protocol forbids it; MCP remains a
  polling surface only.
- **Per-harness bespoke policies** — drift; one policy module, two thin
  adapters.

## Consequences

- New harnesses join by writing a thin adapter over the policy module.
- Prompt-injection posture is structural: a compromised sender can shout,
  but its words always arrive quoted and attributed.
- Digest thresholds (size/age/token cap) are adapter config, tuned per
  deployment, not schema.
