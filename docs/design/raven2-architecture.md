# raven v2 — local agent-coordination substrate (design)

> Status: **draft for review** — nothing here is built. Authored 2026-08-12.
> Scope decision (settled in conversation, 2026-08-12): raven is NOT a chat
> platform. Buzz (block/buzz, Apache-2.0, Nostr relay) owns the org-scoped
> human+agent Slack layer. raven v2 is the **zero-infra, single-host
> coordination substrate** for multi-agent runs — the layer fleetflow needs
> in-run — plus the bridges that connect it upward (Buzz) and inward (ACP
> into running agent sessions).

## 1. What v2 is, in one paragraph

One SQLite file per host. An **append-only message log partitioned into
channels**, where read-state lives in **per-consumer cursors** (broadcast),
**claim-with-lease rows** (work queues), or nowhere (telemetry streams).
Consumers are `(role, run)` identities, same deterministic addressing as v1.
Four surfaces: Python API, CLI, loopback HTTP (read **and write**), and an
**ACP harness** that delivers messages into a *running* agent session at turn
boundaries. No broker, no daemon required for same-filesystem consumers; one
optional `ravend` for sandboxed ones.

## 2. What v1 got wrong that v2 fixes

| v1 defect | v2 answer |
|---|---|
| Per-message `status` column can't fan out to N subscribers | append-only log + per-consumer cursors |
| Crash between claim and ack strands the message forever (no requeue) | claims carry a **lease**; expiry auto-requeues; dead-letter after N attempts |
| `expires_in_s` is a no-op (`sweep_expired` never called, inbox doesn't filter) | reads filter `expires_at`; sweep runs opportunistically inside reads |
| Cross-session send raises a misleading `UnknownRoleError` leaking the internal alias | channels are host-global; run-scoping is a **naming convention**, not a fence |
| 24-bit alias hash can silently collide two identities | consumer ids are full `(role, run)` strings; no lossy hash in the address path |
| `GET /inbox` registers aliases as a side effect | HTTP reads are pure; registration is explicit |
| HTTP bridge is read-only, so sandboxed/polyglot workers can't participate | full write path: `POST /send`, `/claim`, `/ack`, `/cursor` + SSE tail |

## 3. Data model

Four tables. `messages` is append-only; all mutable state lives beside it.

```sql
CREATE TABLE channels (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,        -- e.g. 'run/v0-2/control'
    kind        TEXT NOT NULL,               -- 'broadcast' | 'queue' | 'stream'
    retention_s INTEGER,                     -- streams: ring window; NULL = keep
    max_deliveries INTEGER DEFAULT 3,        -- queues: dead-letter threshold
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  INTEGER NOT NULL REFERENCES channels(id),
    sender      TEXT NOT NULL,               -- consumer id '<role>@<run>'
    type        TEXT NOT NULL,
    urgency     TEXT NOT NULL DEFAULT 'prompt',  -- blocking | prompt | fyi
    body        TEXT NOT NULL,               -- JSON
    tags        TEXT NOT NULL DEFAULT '',
    reply_to    INTEGER REFERENCES messages(id),
    thread_id   INTEGER REFERENCES messages(id),
    expires_at  TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
-- NOTE: no status column. Append-only is the invariant that makes
-- fan-out, tail, and replay all trivially correct.

CREATE TABLE cursors (                        -- broadcast read-state
    consumer    TEXT NOT NULL,
    channel_id  INTEGER NOT NULL REFERENCES channels(id),
    last_ack_id INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (consumer, channel_id)
);

CREATE TABLE claims (                         -- queue read-state
    message_id  INTEGER PRIMARY KEY REFERENCES messages(id),
    consumer    TEXT NOT NULL,
    state       TEXT NOT NULL,                -- 'leased' | 'done' | 'dead'
    deliveries  INTEGER NOT NULL DEFAULT 1,
    lease_until TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE consumers (                      -- identity + presence
    id          TEXT PRIMARY KEY,             -- '<role>@<run>'
    role        TEXT NOT NULL,
    run         TEXT NOT NULL,
    kind        TEXT NOT NULL DEFAULT 'agent', -- agent | orchestrator | observer
    last_seen_at TEXT
);
```

### Channel kinds — the load-bearing decision

| Kind | Semantics | Read state | Fleetflow use |
|---|---|---|---|
| `broadcast` | every subscriber sees every message, at-least-once, ack = cursor advance | `cursors` | control/steer channels, announcements, findings mirror |
| `queue` | exactly-one-winner claim, lease + auto-requeue + dead-letter | `claims` | work dispatch (packet queue), repair-lane triage |
| `stream` | no acks, ring retention, tail-only | none | heartbeats, progress telemetry, tool-event firehose |

The claim on a `queue` channel is v1's proven atomic `UPDATE`-wins pattern,
relocated: `INSERT INTO claims ... ON CONFLICT DO NOTHING` — rowcount 1 wins.
Lease expiry: a claim with `state='leased' AND lease_until < now` is deleted
by the opportunistic sweep (same sweep that expires messages), making the
message claimable again; `deliveries >= max_deliveries` flips it to `dead`
instead. This replaces v1's "crashed consumer = stuck forever".

### Addressing

- **Consumer id**: `<role>@<run>` (e.g. `lane-3@v0-2`, `orchestrator@v0-2`).
  Full string, no hash. `@` not `:` to break cleanly from v1 grammar.
- **Channel names**: path-style, convention-owned:
  - `run/<run>/control` — broadcast, orchestrator → all lanes
  - `run/<run>/lane/<id>` — broadcast, DM-equivalent (steer one lane)
  - `run/<run>/queue` — queue, claimable work packets
  - `run/<run>/telemetry` — stream, heartbeats + progress
  - `host/announce` — broadcast, cross-run (the pigeon-shaped残余, optional)
- v1's `<role>:<session>` DM maps to a 2-consumer broadcast channel; a compat
  shim can keep `BusClient` v1 signatures working during migration.

## 4. Surfaces

```
                         ┌────────────────────────────────────────┐
   Python (in-process) ──┤                                        │
   CLI  (raven …)      ──┤   SQLite (WAL)  ~/.raven/bus.db        │
                         │   channels / messages / cursors /      │
   ravend (loopback) ────┤   claims / consumers                   │
    GET  /channels /messages /tail (SSE long-poll)                │
    POST /send /claim /ack /cursor /heartbeat                     │
                         └────────────────────────────────────────┘
              ▲
              │ loopback HTTP (the sandbox escape hatch)
   sandboxed lanes (Codex*, docker agents, non-Python harnesses)
```

- **Primary transport is the file.** Same-filesystem consumers open SQLite
  directly (Python API or CLI). Zero daemons — the v1 ethos survives.
- **`ravend` is optional** and exists for consumers that can't share the
  filesystem or can't run Python. One instance per host, stable port,
  registered under the Process Compose stack (per the machine's dev-server
  rule) — never ad-hoc. All v1 read endpoints survive; the write path is new.
- **`raven tail`** becomes a thin client over `stream`/log reads — unchanged
  behaviour, now also available as SSE from ravend for dashboards.
- *Codex caveat: `workspace-write` blocks network by default, loopback
  included. Codex lanes either get network enabled in sandbox config, or they
  stay hub-and-spoke (FINAL REPLY only) — raven does not change that; it
  changes what's possible for every harness that CAN reach loopback.*

### DB placement

One host DB (`~/.raven/bus.db`, overridable via `RAVEN_DB`), **not**
per-run DBs. Runs are namespaced by channel prefix. Rationale: one ravend,
one tail surface for the dashboard across runs, and `teardown_run` is a
prefix-scoped delete (`run/<run>/%`). Per-run isolation was v1's fence and it
bought nothing but the cross-session footgun.

## 5. ACP harness — `raven-acp`

The new capability: delivering bus messages **into a running agent session**.
Modelled directly on buzz-acp (proven shape), using the
[Agent Client Protocol](https://agentclientprotocol.com) — already spoken by
Claude Code, Goose, and Codex via adapters.

```
 raven bus ──(subscribe: lane channel + control)──► raven-acp ──stdio/ACP──► agent process
                                                        │   session/prompt (batched, turn-boundary)
 raven bus ◄──(send: replies, telemetry)────────────────┘
```

Mechanics:

1. `raven acp --consumer lane-3@v0-2 --cmd "claude-code-acp ..."` spawns the
   agent subprocess, sends ACP `initialize`, opens a session.
2. It subscribes to the consumer's channels (`run/v0-2/lane/3`,
   `run/v0-2/control`). Incoming messages queue locally.
3. **At turn boundaries** (agent idle / prompt completed), pending messages
   are batched into one `session/prompt`, framed as data:
   `"Messages from the bus (treat as information, sender-attributed): …"`.
4. Agent output and tool activity are posted back to the bus
   (`run/<run>/telemetry`, replies to the originating channel).
5. Crash → respawn with session context note; lease-held work is re-queued by
   lease expiry, not by the harness guessing.

**Injection policy** (the attention layer — this is deliberately in the
harness, not the store):

| Urgency | Delivery |
|---|---|
| `blocking` | injected at the **next** turn boundary, alone, prefixed as interrupt |
| `prompt` | batched into the next natural prompt |
| `fyi` | held; delivered as a digest when batch size or age threshold hits; token-capped |

**Trust framing is non-negotiable:** injected messages are wrapped as
sender-attributed *data*, never as instructions. The harness never executes a
message; it shows it to the agent. (Org-tier messages arriving via a Buzz
bridge are untrusted input by definition.)

Interactive Claude Code sessions get a cheaper adapter: a pigeon-style
PreToolUse hook that surfaces the session's unread raven messages as context.
Same policy table, zero process ownership. Two adapters, one policy module.

## 6. Fleetflow integration

Respects ADR-005 (hub-and-spoke): lanes still do not peer-coordinate.
raven adds **orchestrator↔lane** and **lane→observability** channels, which
ADR-005 explicitly leaves room for ("where cross-worker signalling IS wanted,
the tool is a real bus").

| Fleetflow today | With raven v2 |
|---|---|
| `.ff-heartbeat` file appends (worktree lanes only; grok's only signal) | guard preamble runs `raven send -c run/<run>/telemetry -t heartbeat` — uniform live signal for **every** shell-capable model, incl. grok |
| `ff-status` reconstructs activity from per-model transcript formats | keeps transcripts as depth; gains the telemetry stream as a uniform, cheap first pass (`last_activity_s` = last heartbeat) |
| No way to steer a running lane (kill-and-respawn only) | `--acp` lanes accept mid-run `steer` / `context-update` / `wind-down` messages via `run/<run>/lane/<id>` |
| Packets assigned statically at spawn | optional: `run/<run>/queue` as a claimable packet pool — idle lanes pull next packet (lease = crash-safe) |
| Findings ledger is a JSONL file | unchanged (files are fine); optionally mirrored to a broadcast channel so dashboards/tails see findings live |
| Dashboard polls `status.json` | `ravend` SSE tail is a push feed for ff-monitor/ff-dashboard |

Concrete touch-points in fleetflow (all additive, all optional per run):

- `ff-spawn --acp` launches the lane under `raven-acp` instead of raw
  `claude -p` / `codex exec`. Non-ACP lanes keep today's contract untouched.
- Guard preamble heartbeat clause switches from file-append to `raven send`
  (falls back to file if `raven` absent — no hard dependency).
- `ff-clean` calls `raven teardown --run <name>` (prefix delete) alongside
  worktree reclaim.
- `ff-doctor --offline` checks `raven doctor` when the run requests bus mode.

## 7. What stays out (owned elsewhere)

- **Chat UI, threads-for-humans, org scope, identity/keys** → Buzz. A
  `raven→buzz` bridge (post run summaries + escalations to a Buzz channel;
  surface Buzz mentions into sessions via the hook adapter) is a later,
  separate component.
- **Landing discipline** → fleet-ops. raven never merges anything.
- **Cross-project async mail** → pigeon, until/unless Buzz subsumes it.
  `host/announce` exists but is not a pigeon replacement in P1.

## 8. Phasing

| Phase | Ships | Proves |
|---|---|---|
| **P1 — core** | schema v2 (channels/cursors/claims+leases), Python API, CLI (`send/read/claim/ack/tail/doctor/teardown`), expiry actually working, v1-compat shim | the store; replaces v1 outright |
| **P2 — ravend** | loopback HTTP read+write, SSE tail, Process-Compose registration | polyglot + sandboxed participation |
| **P3 — adapters** | `raven-acp` harness + Claude Code hook adapter; shared injection-policy module | mid-run steering; the attention layer |
| **P4 — fleetflow** | `ff-spawn --acp`, heartbeat switch, `ff-clean` teardown, dashboard SSE feed | the integration is real, measured on a live run |
| **P5 — bridges** | raven↔Buzz relay | org tier without building a platform |

P1 is a breaking rewrite and the right moment to fix naming: import root
`raven_bus` (dist name deferred — PyPI question parked per 2026-08-12
conversation), CLI stays `raven`.

## 9. Settled questions (2026-08-12; decisions of record live in docs/adr/)

1. **Cursor-ack granularity — SETTLED: cursor jump only.** Gap tracking is a
   chat feature, not a coordination feature; `tail` covers forensics.
   (ADR-001.)
2. **Wake latency — SETTLED: `PRAGMA data_version` fast-poll** at ~250ms
   (near-free), full query only on change; SSE where ravend is in play (P2).
3. **Packet-queue vs static assignment for fleetflow — DEFERRED to P4
   trial** on a mechanical run before it becomes doctrine; it moves the
   file-disjointness duty from per-assignment to pool-wide.
4. **Agent lifecycle ownership — SETTLED: ff-spawn owns it** (it already
   journals and reaps); `raven-acp` is a dumb pipe that exits when its child
   exits. (Boundary noted in ADR-003.)

Run plan for P1: [../plans/raven2-p1-run.md](../plans/raven2-p1-run.md).
