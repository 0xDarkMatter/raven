# Two processes coordinating live

Demonstrates the **bus** shape — a producer and a consumer running as
separate Python processes, talking to each other through the same
SQLite file, with no broker daemon between them.

## Run

In one terminal:

```bash
python consumer.py
```

In a second terminal:

```bash
python producer.py
```

You'll see the consumer print each message as it arrives, with
sub-second latency, while it sits in an `async for` loop. The
underlying mechanism is SQLite + WAL — both processes have the file
open, the consumer polls every 100ms, and `subscribe()`'s atomic
claim guarantees at-most-once delivery even if you start a *second*
consumer.

## Try it

- Start two consumers at once, then run the producer. Watch the
  messages get split between them — every id reaches exactly one
  consumer (atomic claim, no double-delivery).
- Kill the consumer mid-batch and restart it. The unread tail is
  still there waiting.
- Add `--db /some/path/bus.db` to both scripts (set `BUS_DB` env
  var) to point them at a non-default location.

## Files

- `producer.py` — sends 5 typed messages spaced 200ms apart
- `consumer.py` — `async for` over `subscribe()`; prints each message
- the SQLite file is created on first run as `./bus.db`
