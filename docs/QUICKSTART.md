# Quickstart — claude-bus

Five-minute walkthrough. Assumes Python 3.12+ and a fresh venv.

## 1. Install

```bash
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install claude-bus              # add [http] for the bridge
```

## 2. Initialise a workspace

```bash
$ claude-bus init
wrote claude-bus.yaml
initialised /tmp/example/claude-bus.db
ready. try: claude-bus doctor
```

`init` writes a default config plus an empty SQLite DB at `./claude-bus.db`. Both are idempotent — re-run with `--force` to overwrite the config.

## 3. Send your first message

```bash
$ claude-bus send \
    --from conductor:demo-1 \
    --to architect:demo-1 \
    --type plan \
    --body '{"step": 1, "goal": "design login"}'
sent #1 conductor:demo-1 -> architect:demo-1 type=plan
```

Addresses are `"<role>:<session>"`. Both producer and consumer addresses are auto-registered on first use — no separate "register identity" step.

## 4. Read it from the recipient's inbox

```bash
$ claude-bus inbox --role architect:demo-1
#1  conductor:demo-1 -> architect:demo-1  type=plan  status=unread  created=...
  body: {"goal": "design login", "step": 1}

$ claude-bus inbox --role architect:demo-1 --json
{ "messages": [...] }
```

## 5. Ack to clear it

```bash
$ claude-bus ack 1
acked #1

$ claude-bus inbox --role architect:demo-1
(no messages)
```

`read` is similar to `inbox` but takes a single id and does **not** clear the message — useful if you want to look at a conversation history without consuming it.

## 6. Same flow from Python

```python
from claude_bus import BusClient

conductor = BusClient(session_id="demo-1", role="conductor", db_path="./claude-bus.db")
architect = BusClient(session_id="demo-1", role="architect", db_path="./claude-bus.db")

conductor.send(to=architect.address, type="plan", body={"step": 2})

for msg in architect.inbox():
    print(msg.id, msg.type, msg.body)
    architect.ack(msg.id)
```

## 7. Live subscribe (the bus shape)

```python
import asyncio
from claude_bus import BusClient

async def main():
    client = BusClient(session_id="demo-1", role="architect", db_path="./claude-bus.db")
    async for msg in client.subscribe(poll_interval_s=0.5):
        print("got:", msg.body)
        # msg is already acked at this point

asyncio.run(main())
```

This is what makes claude-bus a **bus** rather than a mailbox: the consumer is alive and waiting for messages to flow in.

## 8. Optional: turn on the HTTP bridge

```bash
$ pip install 'claude-bus[http]'
$ claude-bus serve --port 7713
```

In another terminal:

```bash
$ curl 'http://127.0.0.1:7713/inbox?role=architect:demo-1'
$ curl  http://127.0.0.1:7713/message/1
$ curl  http://127.0.0.1:7713/health
```

Useful when a consumer can't share the host filesystem — e.g. an agent running inside a Docker container.

## 9. Optional: enforce a body shape

```python
from pydantic import BaseModel
from claude_bus import SchemaRegistry

class PlanBody(BaseModel):
    step: int
    goal: str

SchemaRegistry.register("plan", PlanBody)

# Subsequent sends with type='plan' are validated against PlanBody.
```

## See also

- [`README.md`](../README.md) — overview + mailbox-vs-bus framing
- [`examples/01-hello-world/`](../examples/01-hello-world/) — runnable script
- [`CHANGELOG.md`](../CHANGELOG.md) — release notes + Phase 2 roadmap
