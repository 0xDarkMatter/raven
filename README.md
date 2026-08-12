# raven — local agent-coordination substrate

**raven** is a zero-infra, single-host coordination substrate for multi-agent
runs. One SQLite file holds an append-only message log partitioned into
**channels**; read-state lives per channel *kind*. Same-filesystem consumers
(Python API or CLI) open the file directly — no broker, no daemon.

> The org-scoped human+agent chat layer — Slack-style threads, identity, keys,
> cross-project reach — lives in [Buzz](https://github.com/block/buzz). raven
> is the in-run coordination layer fleetflow and similar runners need; a Buzz
> bridge is a later, separate component.

- **Python import:** `raven_bus` (ADR-004 — the package root is `raven_bus`;
  the v1 import root is gone, surviving one release only as `raven_bus.compat`)
- **CLI:** `raven` (unchanged from v1)
- **Store:** one SQLite (WAL) DB per host — `~/.raven/bus.db`, override `RAVEN_DB` (ADR-002)

> **Distribution name is undecided.** Installing by the `raven` name on PyPI
> would pull Sentry's legacy client — install from source until naming is
> settled (ADR-004): `pip install -e .`

## Recent updates

**v0.2.0 (unreleased)** is a breaking rewrite. raven v2 replaces v1's
per-message `status` column with an append-only log + per-channel-kind
read-state, fixes crash-stranded queues with lease auto-requeue, and renames
the import root to `raven_bus`. See [CHANGELOG.md](CHANGELOG.md) and the
[v1→v2 migration](#v1v2-migration) section.

## Why raven?

Agent runners coordinate processes that can't share memory and often can't
share a network. They need to broadcast steering messages, hand out work
packets safely, and stream telemetry — without standing up Redis or a broker.

raven does this with one SQLite file and three channel semantics:

- **broadcast** — every subscriber sees every message; ack = advance your cursor
- **queue** — exactly-one-winner claim with a lease; crashes auto-requeue; dead-letter after N attempts
- **stream** — observe-only firehose; no acks, ring retention

Key properties: append-only log (reads never mutate, so fan-out, replay, and
`tail` are trivially correct); full-string `<role>@<run>` addressing (no
lossy hash); expiry that actually works (every liveness read filters it).

Rationale lives in the decision log — this README cites rather than restates:
[ADR-001](docs/adr/ADR-001-append-only-log-channel-kind-read-state.md) (log +
channel-kind read-state), [ADR-002](docs/adr/ADR-002-addressing-and-single-host-db.md)
(addressing + one host DB), [ADR-004](docs/adr/ADR-004-import-rename-compat-shim.md)
(rename + compat shim).

## Structure

```
src/raven_bus/
├── __init__.py        public re-exports (models, exceptions, __version__)
├── models.py          pydantic models + ADR-002 address grammar (atom/consumer/channel validation)
├── exceptions.py      RavenBusError hierarchy
├── paths.py           resolve_db_path() — RAVEN_DB > arg > ~/.raven/bus.db
├── db.py              init_db(), connection(), data_version(), sweep(), teardown_run()
├── channels.py        channel registry (ensure/get/list by kind)
├── log.py             append() + read primitives (the only writer of messages rows)
├── cursors.py         broadcast read-state: pending() + cursor-jump ack()
├── claims.py          queue read-state: claim_next/renew/complete/release/get_claim
├── compat.py          v1 BusClient shim on the v2 store (ADR-004)
├── http/              ravend — optional loopback HTTP bridge (the `[http]` extra; ADR-005)
├── migrations/
│   └── 0002_v2_schema.sql   channels / messages / cursors / claims / consumers / bus_meta
└── cli/               Typer app `raven` — send read ack claim done release
                      tail channels doctor teardown version serve (see _common.py for exit codes)
```

## Installation

```bash
# From source (the pip distribution name is undecided — ADR-004)
pip install -e .

# Smoke-test
raven version
raven doctor
```

The DB is created on first use — there is no `raven init` step. `RAVEN_DB`
points it elsewhere; default is `~/.raven/bus.db`.

## Quickstart

A broadcast control channel: orchestrator announces, every lane reads.

### CLI

```bash
# Create + send onto a broadcast channel (auto-created as broadcast).
$ raven send --channel run/v0-2/control --from orchestrator@v0-2 \
    -t steer --body '{"note": "prefer the streaming parser"}'
sent #1 orchestrator@v0-2 -> run/v0-2/control type=steer

# A lane reads its unseen messages (reading does NOT ack).
$ raven read --channel run/v0-2/control --as lane-1@v0-2
#1  orchestrator@v0-2 -> run/v0-2/control  type=steer  urgency=prompt  created=...
  body: {"note": "prefer the streaming parser"}

# Ack by jumping the cursor to the highest id handled (ADR-001: ack is a cursor jump, not per-message).
$ raven ack --channel run/v0-2/control --as lane-1@v0-2 --up-to 1
acked lane-1@v0-2 on run/v0-2/control up_to=1

$ raven read --channel run/v0-2/control --as lane-1@v0-2
(no messages)
```

### Python

```python
from raven_bus import db, channels, log, cursors

db.init_db()                       # creates ~/.raven/bus.db on first use (idempotent)

with db.connection() as conn:
    channels.ensure_channel(conn, "run/v0-2/control", kind="broadcast")
    m = log.append(conn, channel="run/v0-2/control",
                   sender="orchestrator@v0-2", type="steer",
                   body={"note": "prefer the streaming parser"})

with db.connection() as conn:
    pending = cursors.pending(conn, "lane-1@v0-2", "run/v0-2/control")  # id > cursor, expiry-filtered
    for msg in pending:
        handle(msg)
        cursors.ack(conn, "lane-1@v0-2", "run/v0-2/control", up_to_id=msg.id)
```

A queue work channel: claim one packet, finish it (or it auto-requeues).

```bash
$ raven send --channel run/v0-2/queue --from orchestrator@v0-2 \
    -t packet --body '{"file": "src/parser.py"}' --kind queue
sent #2 orchestrator@v0-2 -> run/v0-2/queue type=packet

$ raven claim --channel run/v0-2/queue --as lane-2@v0-2     # oldest unclaimed, leases it
#2  orchestrator@v0-2 -> run/v0-2/queue  type=packet  ...

$ raven done --id 2 --as lane-2@v0-2                        # terminal; releases nothing
done #2 as lane-2@v0-2
```

See [docs/QUICKSTART.md](docs/QUICKSTART.md) for the full 5-minute walkthrough
(send/read/ack, claim/done, tail, teardown).

## HTTP bridge (optional)

ravend is an **optional loopback HTTP bridge** over the same store, for consumers
that can't share the filesystem or can't run Python — sandboxed lanes (Codex,
docker agents) and non-Python harnesses. The primary transport stays the file;
ravend is a sandbox escape hatch, not a daemon you need to run (design doc §4).

It is a **thin bridge**: every endpoint maps 1:1 onto one module-contract call
with no business logic in the HTTP layer, binds **loopback only, port 7713, no
auth** (TLS/auth terminate at a reverse proxy for anything beyond one host), and
errors use the envelope `{"error": <code>, "detail": <human>}` (ADR-005).

Install the extra and run it:

```bash
pip install -e ".[http]"        # starlette + uvicorn
raven serve                     # 127.0.0.1:7713; --host/--port/--db override
```

All v2 store operations are exposed. Send, read broadcast pending, ack, claim,
and finish (the `done` claim):

```bash
# send onto a broadcast channel (201 → Message)
curl -s 127.0.0.1:7713/send -H 'content-type: application/json' \
  -d '{"channel":"run/v0-2/control","sender":"orchestrator@v0-2","type":"steer","body":{"note":"prefer streaming"}}'

# broadcast pending for a consumer (does NOT move the cursor)
curl -s '127.0.0.1:7713/channels/run%2Fv0-2%2Fcontrol/pending?consumer=lane-1@v0-2'

# ack (cursor jump) → 200 Cursor
curl -s 127.0.0.1:7713/ack -H 'content-type: application/json' \
  -d '{"channel":"run/v0-2/control","consumer":"lane-1@v0-2","up_to_id":1}'

# claim the oldest packet from a queue → 200 Message (204 when empty)
curl -s 127.0.0.1:7713/claim -H 'content-type: application/json' \
  -d '{"channel":"run/v0-2/queue","consumer":"lane-2@v0-2"}'

# finish the claim → 200 Claim
curl -s 127.0.0.1:7713/claims/2/done -H 'content-type: application/json' \
  -d '{"consumer":"lane-2@v0-2"}'
```

Channel names appear **percent-encoded** in paths (they contain `/`). Tail a
channel as SSE (an observer — never consumes, may serve expired, like
`raven tail`):

```bash
curl -N '127.0.0.1:7713/tail?channel=run/v0-2/control'
# event: message
# id: 1
# data: {"id":1,"channel":"run/v0-2/control",...}
#
# : ping                       ← comment line while idle
```

The wire surface is the endpoint table below (reproduced from
[ADR-005](docs/adr/ADR-005-ravend-http-contract.md), which owns it as the wire
format of record):

| Method + path | Maps to | Notes |
|---|---|---|
| `GET /health` | db probe | `{status, db, version, schema}` |
| `GET /channels?prefix=` | `channels.list_channels` | `{channels: [...]}` |
| `GET /channels/{name}/messages?after=0&limit=100&include_expired=false` | `log.read_after` | forensic flag mirrors the Python arg |
| `GET /channels/{name}/pending?consumer=&limit=` | `cursors.pending` | broadcast read; does not move the cursor |
| `GET /channels/{name}/cursor?consumer=` | `cursors.get_cursor` | `null` body when absent |
| `GET /tail?channel=&after=0` | `log.read_after` polling | SSE: `event: message`, message JSON per event; `: ping` comments while idle |
| `POST /send` | `log.append` | body mirrors append kwargs (+`kind` for ensure); 201 |
| `POST /claim` | `claims.claim_next` | 200 message, **204 when queue empty** |
| `POST /claims/{id}/renew` | `claims.renew` | `{consumer, lease_s?}` |
| `POST /claims/{id}/done` | `claims.complete` | `{consumer}` |
| `POST /claims/{id}/release` | `claims.release` | 204 |
| `POST /ack` | `cursors.ack` | `{channel, consumer, up_to_id}` → cursor |
| `POST /heartbeat` | consumers upsert | `{consumer}` → 204; the fleet live-signal |

## Channel kinds & delivery semantics

Three kinds, decided once at channel creation (`--kind`), immutable after
([ADR-001](docs/adr/ADR-001-append-only-log-channel-kind-read-state.md)).

| Kind | Delivery | Read state | Ack | Failure |
|---|---|---|---|---|
| `broadcast` | every subscriber sees every message, **at-least-once** | per-consumer `cursors` | cursor **jump** to highest id handled — acks everything up to it | un-acked messages stay pending; idempotent re-read |
| `queue` | **exactly-one-winner** claim | `claims` rows with a lease | `done` (terminal) or `release` (voluntary) | lease expiry **auto-requeues**; `deliveries ≥ max_deliveries` → `dead` |
| `stream` | observe-only, **no acks** | none | n/a | n/a (ring retention; tail-only) |

The queue claim is v1's proven atomic-claim pattern relocated:
`INSERT INTO claims … ON CONFLICT DO NOTHING` — rowcount 1 wins. A lapsed lease
is flipped to a durable `lapsed` state by the opportunistic `sweep` (the
attempt count survives requeue); a lapsed message is re-won by a guarded
`UPDATE` that increments `deliveries`, and at `max_deliveries` the sweep flips
the claim to `dead` instead. This replaces v1's "crashed consumer = message
stuck forever". (ADR-001.)

## v1→v2 migration

v2 is a breaking rewrite. The v1 `BusClient(session_id, role)` API survives
**one release** as `raven_bus.compat`, implemented on the v2 store (ADR-004).
It keeps the v1 examples runnable as the rewrite's regression net and is
deprecated from day one.

```python
from raven_bus.compat import BusClient     # v1 API on the v2 store

conductor = BusClient(session_id="v0-2", role="conductor")
conductor.send(to="architect:v0-2", type="plan", body={"step": 1})
for msg in conductor.inbox():
    conductor.ack(msg.id)
```

The shim maps v1 onto v2: a v1 address `"<role>:<session>"` becomes a 2-consumer
broadcast channel `compat/<session>/<role>`, and v1's `(role, session)` becomes
the v2 consumer `<role>@<session>`. Three narrowings are unavoidable because v2
has no per-message status column, no hash alias, and no session fence — they
are documented loudly in `compat.py` and the CHANGELOG:

1. **Case-folding.** v2's grammar is lowercase-only (ADR-002); the shim
   `.lower()`s role and session before mapping. (All v1 examples are
   lowercase, so they keep running unchanged.)
2. **Cursor-jump ack.** v1 acked a single message; `compat.ack(id)` jumps the
   cursor to `id`, acking everything up to it. Fine for in-order ackers.
3. **`task_id` in body.** v2 has no `task_id` column; the shim smuggles it
   under `body["__task_id__"]` on send and strips it on read.

### What was removed

- The **v1 import root** — renamed to `raven_bus` (ADR-004).
- The **per-message `status` column** — read-state now lives in cursors/claims (ADR-001).
- **Hash aliases** (`role + 6-hex-sha1`) — full `<role>@<run>` strings instead (ADR-002).
- **Session fences** on sends — channels are host-global; run-scoping is a naming convention (ADR-002).
- The **`raven init` / `session init`** commands and the v1 **HTTP bridge** (`GET /inbox`, `GET /message/{id}`) — the DB is created on first use; the v2 bridge is [ravend](#http-bridge-optional).

## Roadmap

v0.2.0 ships P1 (core store + CLI + compat) and P2 (the optional ravend HTTP
bridge). Later phases are described in the
design doc ([§8 Phasing](docs/design/raven2-architecture.md#8-phasing)); this
section points rather than restates:

- **P2 — ravend:** shipped — the optional loopback HTTP bridge, `raven serve`,
  read+write+SSE (see [HTTP bridge](#http-bridge-optional) and
  [ADR-005](docs/adr/ADR-005-ravend-http-contract.md)).
- **P3 — adapters:** `raven-acp` harness + Claude Code hook adapter; the shared
  injection-policy module (ADR-003 — injection policy lives in adapters, *not*
  the store; the bus never decides when a message enters an agent's context).
- **P4 — fleetflow:** `ff-spawn --acp`, heartbeat switch, `ff-clean` teardown, dashboard SSE.
- **P5 — bridges:** raven↔Buzz relay.

## Documentation

- [docs/QUICKSTART.md](docs/QUICKSTART.md) — 5-minute walkthrough
- [AGENTS.md](AGENTS.md) — developer guide (architecture, landmines, testing)
- [docs/design/raven2-architecture.md](docs/design/raven2-architecture.md) — the v2 design
- [docs/adr/](docs/adr/) — decisions of record (ADR-001…004)
- [CHANGELOG.md](CHANGELOG.md) — release notes

## Troubleshooting

- **`raven doctor`** — checks the DB is reachable, reports `schema_version`,
  WAL mode, and a dry sweep tally (`expired`/`requeued`/`dead_lettered`).
- **Bad address:** consumer ids must be `<role>@<run>` and channel names
  path-style, all lowercase atoms `[a-z0-9][a-z0-9._-]*` — a violation is a
  usage error (exit 2), not a traceback (ADR-002).
- **Wrong channel kind:** a queue op on a broadcast channel (or vice versa)
  raises `WrongChannelKindError`. Kind is immutable after creation.
- **`--db` override:** every CLI command accepts `--db PATH` to point at a
  different DB (tests use this); otherwise resolution is `RAVEN_DB` → `~/.raven/bus.db`.

## License

See [LICENSE](LICENSE).
