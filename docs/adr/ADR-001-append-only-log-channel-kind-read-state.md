# ADR-001: Append-only message log; read-state lives per channel kind

- Status: accepted
- Date: 2026-08-12
- Touches: src/, migrations/, tests/

## BLUF

`messages` is **append-only and has no status column**. Read-state lives in a
per-channel-kind structure: `broadcast` → per-consumer cursors, `queue` →
claim rows with leases (auto-requeue on expiry, dead-letter after
`max_deliveries`), `stream` → none (ring retention). Never add a mutable
status/read flag back onto the message row.

## Context

raven v1 stored delivery state as a per-message `status` column
(`sent/delivered/resolved/expired`). That shape cannot fan out (read-ness is a
property of the *(consumer, channel)* pair, not the message), stranded
messages forever when a consumer crashed between claim and ack (no requeue
path existed), and made `tail`/replay semantics awkward.

## Decision

Three tables beside the log: `cursors(consumer, channel_id, last_ack_id)`,
`claims(message_id, consumer, state, deliveries, lease_until)`,
`channels(name, kind, retention_s, max_deliveries)`. Queue claim =
`INSERT INTO claims ... ON CONFLICT DO NOTHING`; rowcount 1 wins (v1's proven
atomic-claim pattern relocated). Expired leases are swept opportunistically
inside reads — the same sweep that enforces `expires_at`, which v1 shipped
but never invoked.

## Alternatives rejected

- **Keep per-message status** — cannot support N-subscriber channels.
- **Per-message acks with gap tracking** — chat feature, not coordination;
  cursor-jump ack is sufficient and `tail` covers forensics (settled
  2026-08-12).
- **External queue (Redis etc.)** — violates the zero-infra constraint that
  is raven's reason to exist.

## Amendment (2026-09-30, QA pass)

Stream "ring retention" is decided but **not implemented**: `channels.
retention_s` is stored and never read, nothing prunes stream messages, and no
CLI/HTTP surface sets it. Stream channels therefore grow without bound today.
An implementation is a second sanctioned `messages` delete (beside
`teardown_run`) and must first settle how pruned rows interact with
`reply_to`/`thread_id` foreign keys from surviving messages — the same
constraint that makes `teardown_run` refuse (`TeardownBlockedError`) when
another run's message references it.

## Consequences

- Fan-out, replay, and `tail` are trivially correct (reads never mutate).
- Every read path MUST filter `expires_at` and MAY run the sweep.
- Deleting history is retention/teardown policy, never delivery bookkeeping.
