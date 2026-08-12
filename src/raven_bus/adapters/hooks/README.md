# raven inbox hook (Claude Code PreToolUse)  — LANE: hook (raven2-p3)

Mirrors pigeon's proven hook shape: on every tool call, PEEK at the
consumer's pending messages and print a compact block when any exist;
print NOTHING when the inbox is empty; always exit 0 (a broken hook
must never block a tool call). The hook NEVER acks (ADR-006 — only a
harness acks; the hook may run beside one on the same consumer).

Config via environment:

- `RAVEN_CONSUMER`  (required to activate; absent → silent no-op)
- `RAVEN_CHANNELS`  (comma-separated; default `run/<run>/lane/<role>`
  derivation is NOT attempted — explicit only)
- `RAVEN_DB`        (optional; the store's normal resolution otherwise)

Output shape when pending (rendered via `raven_bus.policy` — the hook
shells out to `python -m raven_bus.adapters.hooks.peek`, which does the
store read + policy render; the .sh wrapper stays trivial):

    === RAVEN: 2 message(s) for lane-3@v0-2 ===
    <policy.render output — sender-attributed, data-framed>
    Use your raven tooling (or the CLI: raven read/ack) to act.

Installation (documented, not automated): copy `raven-inbox-hook.sh`
somewhere stable and add a PreToolUse hook entry to
`~/.claude/settings.json` (mirror pigeon's block).
