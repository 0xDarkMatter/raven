# Two processes coordinating live

Demonstrates raven_bus v2's **queue** channel kind — a producer and two
competing consumers running as separate Python processes, talking
through the same SQLite file, with no broker daemon between them.

## Run

In two terminals:

```bash
python consumer.py --id consumer-a@demo
python consumer.py --id consumer-b@demo
```

In a third:

```bash
python producer.py
```

Each of the 5 tasks lands with exactly one consumer — `claims.claim_next`'s
`INSERT ... ON CONFLICT DO NOTHING` makes the claim atomic, so racing
consumers split the work with no double-delivery. Every claimed task is
finished with `claims.complete`.

## Try it

- Start two (or more) consumers at once, then run the producer. Watch
  the ids get split between them.
- Kill a consumer mid-batch and restart it — a task it claimed but
  never completed is reaped by the opportunistic sweep once its lease
  lapses, and becomes claimable again.
- Pass `--db PATH` to either script (or set `RAVEN_DB`) to point them
  at a non-default location.

## Files

- `producer.py` — creates the `run/demo/work` queue channel and sends
  5 typed tasks 200ms apart
- `consumer.py` — loops `claims.claim_next` / `claims.complete`; each
  instance needs a distinct `--id`
- the SQLite file is created on first run as `./bus.db`
