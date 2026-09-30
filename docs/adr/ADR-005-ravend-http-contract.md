# ADR-005: ravend is a thin loopback bridge; the HTTP surface is a 1:1 map of the module contracts

- Status: accepted
- Date: 2026-08-12
- Touches: src/raven_bus/http/, src/raven_bus/cli/serve.py, docs/

## BLUF

ravend (``raven serve``) is a **thin JSON bridge**: every handler maps
1:1 onto exactly one module-contract call (`log`/`cursors`/`claims`/
`channels`/`db`) with **no business logic in the HTTP layer**. It binds
**loopback only, default port 7713, no auth** — TLS/auth terminate at a
reverse proxy for anything beyond one host. `tail` streams over **SSE**.
Errors use the envelope `{"error": <code>, "detail": <human>}` with
400 (invalid input) / 404 (unknown channel or message) / 409 (claim
denied, wrong channel kind). This endpoint table is the wire format of
record:

| Method + path | Maps to | Notes |
|---|---|---|
| GET `/health` | db probe | `{status, db, version, schema}` |
| GET `/channels?prefix=` | `channels.list_channels` | `{channels: [...]}` |
| GET `/channels/{name}/messages?after=0&limit=100&include_expired=false` | `log.read_after` | forensic flag mirrors the Python arg |
| GET `/channels/{name}/pending?consumer=&limit=` | `cursors.pending` | broadcast read; does not move the cursor |
| GET `/channels/{name}/cursor?consumer=` | `cursors.get_cursor` | `null` body when absent |
| GET `/tail?channel=&after=0` | `log.read_after` polling | SSE: `event: message`, message JSON per event; `: ping` comments while idle |
| POST `/send` | `log.append` | body mirrors append kwargs (+`kind` for ensure); 201 |
| POST `/claim` | `claims.claim_next` | 200 message, **204 when queue empty** |
| POST `/claims/{id}/renew` | `claims.renew` | `{consumer, lease_s?}` |
| POST `/claims/{id}/done` | `claims.complete` | `{consumer}` |
| POST `/claims/{id}/release` | `claims.release` | 204 |
| POST `/ack` | `cursors.ack` | `{channel, consumer, up_to_id}` → cursor |
| POST `/heartbeat` | consumers upsert | `{consumer}` → 204; the fleet live-signal |

Channel names appear **percent-encoded** in paths (they contain `/`).

## Context

P1 shipped the store; sandboxed and non-Python workers (Codex lanes,
docker agents, grok/pi harnesses) can only reach it over loopback HTTP
(design doc §4). v1's bridge was read-only and its GET /inbox wrote
alias rows as a side effect — both mistakes to not repeat.

## Decision

Thin-bridge rule: a handler validates/decodes, calls one contract
function inside one `db.connection` context, encodes the result. Any
behaviour worth testing lives in the modules, already at 100% coverage;
HTTP tests assert mapping, status codes, and envelopes. GET handlers
perform NO writes (v1's inbox-registers-aliases bug class). SSE tail
uses the `data_version` fast-poll internally; it is an observer —
never consumes, may serve expired (forensic, like `raven tail`).

## Alternatives rejected

- **WebSockets** — SSE is sufficient for a one-way tail, works through
  plain proxies, trivial to consume from curl.
- **Auth in-process** — loopback + reverse-proxy termination keeps the
  bridge auditable and the attack surface at zero config (v1 precedent,
  README-documented).
- **Fat handlers** (validation/orchestration in HTTP) — duplicates the
  module contracts and drifts.

## Amendment (2026-08-12, post adversarial verify)

The verify wave hardened the contract without changing the route table:

- **Every POST body is strict** — unknown keys → 400 (a `leases_s` typo
  silently applying a default lease was the finding).
- **All caller ints are bounded**: `lease_s` 1..30d, `limit` 1..1000
  (SQLite's LIMIT -1 means unlimited), `after`/ids within int64.
- **Router-level 404/405 wear the same envelope** (405 code:
  `method_not_allowed`).
- **`raven serve` refuses non-loopback hosts** without an explicit
  `--yes-expose` — the posture must not be defeatable by one flag.
- **Handlers run store work in worker threads** (`app.run_db`); the SSE
  tail drains multi-batch bursts and holds a `cross_thread` connection.
- **`consumers.touch()`** is the store contract behind `/heartbeat`,
  ending the raw-SQL drift this ADR's thin-bridge rule forbade.

## Amendment (2026-09-30, QA pass)

Two behaviours the BLUF under-specified, recorded as deliberate:

- **Claim ops on an unheld id return 409, not 404.** `POST
  /claims/{id}/{renew,done,release}` answers 409 `conflict` whenever the
  caller does not hold a leased claim on that id — including when no such
  message exists. The claims module reports "you don't hold it" without a
  separate existence probe; the 404 row in the BLUF applies to unknown
  *channels* and to reads.
- **`GET /pending` touches the consumer's presence row.** Via
  `cursors.pending`'s module contract (the same `consumers.touch` upsert as
  `/heartbeat`), a first read by a never-seen consumer registers it. That is
  a module-contract write, not handler logic — GET handlers themselves still
  never write.

Wire changes from the same pass (the route table is unchanged):

- **Status map extended.** Besides 400/404/409: `503 busy` (a SQLite lock
  outlasted the busy timeout — retryable), `503 unavailable` (the store file
  is missing or unopenable), `503 schema_mismatch` (not a raven v2 store —
  on every route, not just `/health`), and `500 internal_error` for anything
  unexpected (fixed `detail`; the traceback goes to the `raven_bus.http`
  logger). Starlette's plain-text 500 no longer escapes.
- **ravend never creates the DB.** Every connection opens with
  `create=False`; a vanished file is 503 `unavailable`, never a fresh empty
  file. `raven serve`'s `init_db` preflight is the one creator. `GET
  /health` is a real read-only probe (`db.probe`) on every call.
- **`POST /send`'s `kind` is optional.** Absent: one `log.append(ensure=True)`
  (existing channel of any kind; broadcast if new). Present: must be a valid
  kind (else 400), then `ensure_channel(kind)` + `append(ensure=False)` — a
  mismatch is 409. `expires_in_s` must be 1..2,592,000 (1 s..30 days).
- **Responses are encoded inside the transaction.** A result that can't be
  encoded is 500 and rolls back — a `POST /claim` can no longer commit a
  lease for a message it failed to return. (Trade-off: such a row, only
  writable before the store's 64-level body-depth cap, now 500s every HTTP
  claim at the head of its queue instead of dead-lettering unseen.)
- **Input validation is uniform.** A malformed channel name or consumer id is
  400 on every route (it was 404 on some, `200 null` on `/cursor`); query
  ints are strict ASCII digits; an empty `?after=` means the default.
- **`/tail`** honours `Last-Event-ID` (it wins over `?after=`); all-channel
  ids are strictly increasing across polls (one id-ordered store read,
  `log.read_all_after`); a tailed channel torn down mid-stream flushes what
  was read and closes cleanly.

## Consequences

- Port 7713 (unclaimed in the machine port registry as of 2026-08-12).
  Running ravend as a *standing service* goes through the
  Process-Compose stack, never ad-hoc.
- New store capabilities get an endpoint only by first landing as a
  module contract.
- The endpoint table above is append-only in spirit: breaking a
  shipped route requires superseding this ADR.
