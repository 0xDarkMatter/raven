# News-desk — five agents coordinating live

A small demo of the **bus shape**: five roles working a single
editorial pipeline through the same SQLite file, with no broker
process.

```
                                                      ┌──────────┐
                                  ┌──────draft────────►  editor  ├──approval─┐
                                  │                   └──────────┘           │
 ┌───────┐         ┌────────┐    │                                          ▼
 │ scout ├──lead──►│ writer ├────┤                                     ┌──────────┐
 └───────┘         └────────┘    │                                     │publisher │
                                  │                   ┌──────────────┐ └────┬─────┘
                                  └──────draft────────► fact_checker │      │
                                                      └──────┬───────┘      │
                                                             └─verification─┘
                                                             ▼
                                                          (publishes when
                                                           both approvals
                                                           for the same
                                                           correlation_id
                                                           arrive)
```

## What it demonstrates

- **Fan-out**: writer sends each draft to editor *and* fact_checker
  with the same `correlation_id`, so both can review independently.
- **Fan-in**: publisher waits for both an editor approval *and* a
  fact-checker verification for the same `correlation_id` before
  publishing.
- **Request/response with `reply_to`**: every downstream message
  references the upstream message id, so you can read the audit
  trail in either direction.
- **Live subscribe**: every agent runs `async for msg in subscribe()`.
  No polling code anywhere except inside `claude-bus` itself.
- **Schema validation**: each message type has a Pydantic body
  schema registered. Try editing one of the bodies in `agents.py`
  to see strict mode catch it.
- **Atomic claim**: spin up multiple writers (or copies of any role)
  and watch each message land at exactly one consumer.

## Run

```bash
python run.py
```

Default: 3 articles flow through the pipeline, then everyone exits
cleanly. The script prints a colour-free transcript so it's safe to
pipe into a file. Pass `--articles N` to push more through, or
`--db /path/to/bus.db` to use a non-default location.

Expected output (~3 seconds):

```
[scout    ] sent lead       #1  topic=batteries
[writer   ] drafted article #2  (in reply to lead #1)  -> editor + fact_checker
[editor   ] approved        #3  (correlation #2)
[fact_chk ] verified        #4  (correlation #2)
[publisher] PUBLISHED       #5  "Batteries: a deep dive"
... (×3)
[scout    ] done
[publisher] published 3/3 articles, exiting
```

## Files

- `agents.py` — the five role coroutines, plus message-body schemas
- `run.py` — orchestrates them via `asyncio.gather` against one DB
