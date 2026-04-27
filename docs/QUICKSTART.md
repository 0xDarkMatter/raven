# Quickstart — raven

Five-minute walkthrough. Assumes Python 3.12+ and a fresh venv.

## 1. Install

```bash
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install raven              # add [http] for the bridge
```

## 2. Initialise a workspace

```bash
$ raven init
wrote raven.yaml
initialised /tmp/example/raven.db
ready. try: raven doctor
```

`init` writes a default config plus an empty SQLite DB at `./raven.db`. Both are idempotent — re-run with `--force` to overwrite the config.

## 3. Send your first message

```bash
$ raven send \
    --from conductor:demo-1 \
    --to architect:demo-1 \
    --type plan \
    --body '{"step": 1, "goal": "design login"}'
sent #1 conductor:demo-1 -> architect:demo-1 type=plan
```

Addresses are `"<role>:<session>"`. Both producer and consumer addresses are auto-registered on first use — no separate "register identity" step.

## 4. Read it from the recipient's inbox

```bash
$ raven inbox --role architect:demo-1
#1  conductor:demo-1 -> architect:demo-1  type=plan  status=unread  created=...
  body: {"goal": "design login", "step": 1}

$ raven inbox --role architect:demo-1 --json
{ "messages": [...] }
```

## 5. Ack to clear it

```bash
$ raven ack 1
acked #1

$ raven inbox --role architect:demo-1
(no messages)
```

`read` is similar to `inbox` but takes a single id and does **not** clear the message — useful if you want to look at a conversation history without consuming it.

## 6. Same flow from Python

```python
from claude_bus import BusClient

conductor = BusClient(session_id="demo-1", role="conductor", db_path="./raven.db")
architect = BusClient(session_id="demo-1", role="architect", db_path="./raven.db")

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
    client = BusClient(session_id="demo-1", role="architect", db_path="./raven.db")
    async for msg in client.subscribe(poll_interval_s=0.5):
        print("got:", msg.body)
        # msg is already acked at this point

asyncio.run(main())
```

This is what makes raven a **bus** rather than a mailbox: the consumer is alive and waiting for messages to flow in.

## 8. Watch live traffic with `tail`

`raven tail` is an identity-free observer — it streams every message that flows through the bus without consuming any of them:

```bash
$ raven tail                           # follow all traffic
$ raven tail --role writer:demo-1      # filter to one recipient
$ raven tail --no-follow               # print backlog and exit
$ raven tail --json                    # newline-delimited JSON
```

Useful for watching a multi-agent pipeline run in real time. Multiple tailers can run alongside active consumers with no interference.

## 9. Optional: turn on the HTTP bridge

```bash
$ pip install 'raven[http]'
$ raven serve --port 7713
```

In another terminal:

```bash
$ curl 'http://127.0.0.1:7713/inbox?role=architect:demo-1'
$ curl  http://127.0.0.1:7713/message/1
$ curl  http://127.0.0.1:7713/health
```

Useful when a consumer can't share the host filesystem — e.g. an agent running inside a Docker container.

## 10. Optional: enforce a body shape

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
