# Quickstart — raven v2

Five-minute walkthrough of the v2 store. Assumes Python 3.12+. The pip
distribution name is undecided — install from source (ADR-004):

```bash
pip install -e .
raven version
```

There is no `init` step. The DB is created on first use at `~/.raven/bus.db`
(override with `RAVEN_DB`, or pass `--db` to any command). One host DB holds
every run; runs are namespaced by channel prefix (ADR-002).

We'll use a run called `demo`. Two roles: `orchestrator@demo` and `lane-1@demo`.

## 1. Broadcast: send, read, ack

A `broadcast` channel is how a runner steers its lanes — every subscriber sees
every message; ack = advance your cursor.

```bash
$ raven send --channel run/demo/control --from orchestrator@demo \
    -t steer --body '{"note": "prefer the streaming parser"}'
sent #1 orchestrator@demo -> run/demo/control type=steer
```

`--channel` is auto-created as `broadcast` (the default `--kind`) on first send.

```bash
# Read the lane's unseen messages. Reading does NOT ack.
$ raven read --channel run/demo/control --as lane-1@demo
#1  orchestrator@demo -> run/demo/control  type=steer  urgency=prompt  created=2026-...
  body: {"note": "prefer the streaming parser"}

# Ack by jumping the cursor to the highest id you've handled.
$ raven ack --channel run/demo/control --as lane-1@demo --up-to 1
acked lane-1@demo on run/demo/control up_to=1

$ raven read --channel run/demo/control --as lane-1@demo
(no messages)
```

A second lane has its own independent cursor — it sees the same message until
it acks too. That's the fan-out property: `raven read --as lane-2@demo ...`
returns `#1` regardless of `lane-1`'s cursor.

> **Ack is a cursor jump, not per-message** (ADR-001). `ack --up-to 3` marks
> everything up to id 3 as seen. Backwards ack is a silent no-op.

## 2. Queue: claim, done

A `queue` channel hands out work packets — exactly one consumer wins each
message, holds a lease, and finishes it. Create it with `--kind queue`.

```bash
$ raven send --channel run/demo/queue --from orchestrator@demo \
    -t packet --body '{"file": "src/parser.py"}' --kind queue
sent #2 orchestrator@demo -> run/demo/queue type=packet

# Claim the oldest unclaimed message (leases it for 300s by default).
$ raven claim --channel run/demo/queue --as lane-1@demo
#2  orchestrator@demo -> run/demo/queue  type=packet  urgency=prompt  created=...

# If lane-1 crashes before `done`, the lease expires and sweep requeues it —
# it becomes claimable again, with deliveries incremented. Finish it instead:
$ raven done --id 2 --as lane-1@demo
done #2 as lane-1@demo
```

`release --id 2 --as lane-1@demo` gives a message back voluntarily (immediately
claimable, and it does **not** count toward dead-lettering). After
`max_deliveries` (default 3) lapsed leases, a message goes to `dead` instead of
requeueing (ADR-001).

## 3. Tail (the forensic surface)

`tail` streams raw log messages — identity-free, never acks, never steals from
consumers, and **includes expired** messages (it's for forensics, not liveness):

```bash
$ raven tail --channel run/demo/control        # follow new messages
$ raven tail --no-follow                        # drain the backlog and exit
$ raven tail --json                             # newline-delimited JSON
$ raven tail --from 2                           # resume from a known id
```

## 4. Teardown

`teardown --run` deletes every row belonging to a run — messages, cursors,
claims, channels, and consumers — the one sanctioned bulk delete (ADR-001/002):

```bash
$ raven teardown --run demo --yes
removed 3 rows for run 'demo'
```

(Drop `--yes` for a confirmation prompt.)

## 5. Health check

```bash
$ raven doctor
  [ok]    db       reachable at ~/.raven/bus.db (schema_version=2)
  [ok]    wal      journal_mode=wal
  [ok]    sweep    expired=0 requeued=0 dead_lettered=0
all checks passed
```

## 6. The same flow from Python

```python
from raven_bus import db, channels, log, cursors, claims

db.init_db()  # creates ~/.raven/bus.db if missing (idempotent)

# Broadcast: send + read + cursor-jump ack
with db.connection() as conn:
    channels.ensure_channel(conn, "run/demo/control", kind="broadcast")
    log.append(conn, channel="run/demo/control", sender="orchestrator@demo",
               type="steer", body={"note": "prefer the streaming parser"})

with db.connection() as conn:
    for msg in cursors.pending(conn, "lane-1@demo", "run/demo/control"):
        handle(msg)
        cursors.ack(conn, "lane-1@demo", "run/demo/control", up_to_id=msg.id)

# Queue: claim + complete
with db.connection() as conn:
    channels.ensure_channel(conn, "run/demo/queue", kind="queue")
    log.append(conn, channel="run/demo/queue", sender="orchestrator@demo",
               type="packet", body={"file": "src/parser.py"})

with db.connection() as conn:
    msg = claims.claim_next(conn, "lane-1@demo", "run/demo/queue")  # or None
    if msg is not None:
        do_work(msg)
        claims.complete(conn, msg.id, "lane-1@demo")
```

Connections are short-lived context managers (WAL, foreign keys, auto
commit/rollback). Cheap change-detection is `db.data_version(conn)` — poll it
before running a full query.

## See also

- [README.md](../README.md) — overview, the three channel kinds, delivery-semantics table
- [AGENTS.md](../AGENTS.md) — developer guide: architecture map, landmines, testing patterns
- [docs/design/raven2-architecture.md](design/raven2-architecture.md) — the v2 design
- [CHANGELOG.md](../CHANGELOG.md) — release notes
