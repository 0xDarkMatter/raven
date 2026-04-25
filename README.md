# claude-bus

A SQLite-backed, role-addressable **message bus** for agent sessions.

## Mailbox vs bus — where claude-bus fits

| | [Pigeon](https://github.com/0xDarkMatter/pigeon) (mailbox) | **claude-bus** (bus) |
|---|---|---|
| Concurrency | Async, eventually consistent | Live, session-active |
| Recipient state | Probably **not running** when you write | **Running and waiting** for messages |
| Latency tolerated | Minutes to days | Sub-second to seconds |
| Mental model | Email / inbox | Slack / message bus |
| Address scheme | Per-project hash (one mailbox per repo) | `<role>:<session>` (many mailboxes per swarm) |
| Use cases | Handoff between waves, notes-to-self | Live role-to-role coordination during a swarm run |
| Shape | Go CLI binary | Python in-process + optional HTTP bridge |

**Use Pigeon** when one session needs to leave a note for another project, possibly across days.

**Use claude-bus** when agent roles in the same swarm need to coordinate while running.

Both exist in the same ecosystem because both shapes are needed.

## Install

```bash
pip install claude-bus              # core
pip install 'claude-bus[http]'      # + optional HTTP bridge
```

Requires Python 3.12+.

## Quickstart (5 lines of Python)

```python
from claude_bus import BusClient

a = BusClient(session_id="swarm-1", role="conductor", db_path="bus.db")
b = BusClient(session_id="swarm-1", role="architect", db_path="bus.db")

a.send(to=b.address, type="plan", body={"step": 1, "goal": "design auth"})

for msg in b.inbox():
    print(msg.body)        # {'step': 1, 'goal': 'design auth'}
    b.ack(msg.id)
```

## CLI quickstart

```bash
$ claude-bus init
wrote claude-bus.yaml
initialised .../claude-bus.db
ready. try: claude-bus doctor

$ claude-bus send --from conductor:swarm-1 --to architect:swarm-1 \
    --type plan --body '{"step": 1}'
sent #1 conductor:swarm-1 -> architect:swarm-1 type=plan

$ claude-bus inbox --role architect:swarm-1 --json
{
  "messages": [
    {"id": 1, "sender": "conductor:swarm-1", "body": {"step": 1}, ...}
  ]
}

$ claude-bus ack 1
acked #1
```

The full Phase-1 CLI surface is **8 commands**: `init`, `doctor`, `session init`, `send`, `inbox`, `read`, `ack`, `serve`.

## Async subscribe

```python
import asyncio
from claude_bus import BusClient

async def consume():
    b = BusClient(session_id="swarm-1", role="architect", db_path="bus.db")
    async for msg in b.subscribe(poll_interval_s=0.5):
        print(f"#{msg.id} {msg.type}: {msg.body}")
        # message is acked before yield (at-most-once)

asyncio.run(consume())
```

## Pluggable schemas (opt-in)

By default any JSON body is accepted. Register a Pydantic model to start enforcing a shape per message type:

```python
from pydantic import BaseModel
from claude_bus import SchemaRegistry

class PlanBody(BaseModel):
    step: int
    goal: str

SchemaRegistry.register("plan", PlanBody)

# Now send(type="plan", body=...) validates against PlanBody.
# SchemaRegistry.strict_mode(True) rejects unregistered types entirely.
```

## HTTP bridge (optional)

The `[http]` extra ships a small Starlette app for non-Python consumers (e.g. an agent inside a Docker container that can't share the host's filesystem):

```bash
$ pip install 'claude-bus[http]'
$ claude-bus serve --port 7713 &

$ curl http://127.0.0.1:7713/health
{"status": "ok", "db": "...", "version": "0.1.0"}

$ curl 'http://127.0.0.1:7713/inbox?role=architect:swarm-1'
{"messages": [...]}

$ curl http://127.0.0.1:7713/message/1
{"id": 1, "sender": "...", "body": {...}}
```

Phase 1 ships **read endpoints only** (`GET /health`, `GET /inbox`, `GET /message/{id}`).
The write path stays on the CLI / Python API in v0.1.x. `POST /send` and `POST /ack` are planned for v0.2.0.

The bridge binds to `127.0.0.1` by default — there is no built-in auth. For multi-host or untrusted-network deployments, terminate TLS + auth at a reverse proxy in front of `claude-bus serve`.

## Architecture (one-paragraph)

A single SQLite file holds an `aliases` table (deterministic identities per `(role, session)`) and a `messages` table (append-only, indexed for inbox reads). `BusClient` is a thin layer that auto-registers the sender + recipient identities and wraps `send`/`inbox`/`ack` calls. WAL mode lets multiple producers and consumers share the file with no broker process. The optional HTTP bridge is a Starlette app that exposes the same reads over loopback HTTP for consumers that can't share the filesystem.

```
┌──────────┐  send/inbox/ack  ┌────────────────┐
│ producer │ ────────────────►│  SQLite store  │◄─── HTTP bridge ──── docker agent
│  Python  │                  │  (WAL, single  │     (optional)        (curl etc.)
└──────────┘                  │   shared file) │
                              └────────────────┘
                                        ▲
                              consumer  │ subscribe / inbox / ack
                                        │
                                  ┌──────────┐
                                  │ consumer │
                                  │  Python  │
                                  └──────────┘
```

## Status

| | |
|---|---|
| Version | **0.1.0** (Phase 1 ship) |
| Python | 3.12+ |
| License | MIT |
| Status | Alpha — public surface stable for v0.1.x; see `CHANGELOG.md` for the v0.2 plan |

## Documentation

- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) — 5-minute walkthrough
- [`examples/01-hello-world/`](examples/01-hello-world/) — single-process round-trip
- [`examples/02-two-processes/`](examples/02-two-processes/) — live cross-process coordination
- [`CHANGELOG.md`](CHANGELOG.md) — release notes + v0.2 roadmap

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `claude-bus serve` exits with `failed to bind 127.0.0.1:7713` | Another process is on that port (often a stray previous run, or your real Axiom). | `claude-bus serve --port 7714`, or `lsof -i :7713` / `netstat -ano` to find and kill the holder. |
| `claude-bus serve` exits with `DB preflight failed: cannot write` | The directory holding `claude-bus.db` is read-only or doesn't exist. | Create the dir, fix permissions, or pass `--db /writable/path/bus.db`. |
| `claude-bus inbox` returns `(no messages)` but you just sent one | Address mismatch: producer used `--to alice:s1` but consumer asked for `--role Alice:s1` (case-sensitive) or a different session id. | Check casing and that both ends agree on `<role>:<session>`. |
| `SchemaValidationError: body for type='X' failed validation` | You registered a Pydantic model for type `X` and the body doesn't match it. | Either fix the body, drop the schema (`SchemaRegistry.unregister("X")`), or send with `validate=False` at the `_core.send` layer. |
| Send hangs for several seconds | Another writer holds a lock on the WAL. The default busy timeout is 5s; if a peer process has the file open in a long transaction it can stall. | Confirm peers commit promptly. As a workaround you can adjust `claude_bus.db.DEFAULT_BUSY_TIMEOUT_S`. |
| `pip install 'claude-bus[http]'` fine but `claude-bus serve` says `starlette + uvicorn are required` | The CLI is resolving a different Python (system `claude-bus`, not the venv one). | Activate the venv first, or invoke `python -m claude_bus.cli.main serve`. |
| Two subscribers see the same message | This shouldn't happen as of v0.1.1+ — `subscribe()` uses an atomic `UPDATE … WHERE status IN ('sent','delivered')` claim. If you see it on an older install, `pip install -U claude-bus`. | — |

## Acknowledgements

claude-bus is the messaging primitive extracted from [Axiom](https://github.com/0xDarkMatter/axiom) (internal codename: *Raven*). Axiom keeps its own role-aware adapter layer; this project is the lower-level reusable substrate, sized for any single-host multi-session agent system.

## License

[MIT](LICENSE).
