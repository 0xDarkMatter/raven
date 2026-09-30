# News-desk — five agents coordinating live

A small demo of the **bus shape**: five roles working a single
editorial pipeline through the same SQLite file, with no broker
process.

> Runs on the deprecated v1 compat shim (`raven_bus.compat`, ADR-004).
> The shim is removed after one minor release; its v1 -> v2 narrowings
> are listed in `src/raven_bus/compat.py`'s module docstring.

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
  No polling code in the agents — the shim's `subscribe()` polls
  under the hood.
- **Typed bodies (documentation only)**: each message type has a
  Pydantic body model registered with `SchemaRegistry`, but under the
  compat shim registration is a no-op — nothing is validated and
  strict mode is ignored. The models document the wire shapes.

Not shown: **competing consumers**. Every copy of a role shares one
cursor with no claim, so two `writer` clients would not split the
leads — they could both handle the same one. Exactly-one-winner
delivery is the v2 `queue` channel kind; see `../02-two-processes`.

## Run

```bash
python run.py
```

Default: 3 articles flow through the pipeline, then everyone exits
cleanly. The script prints a colour-free transcript so it's safe to
pipe into a file. Pass `--articles N` to push more through, or
`--db /path/to/bus.db` to use a non-default location (default:
`newsdesk.db` next to `run.py`, wherever you run it from; deleted
before each run unless `--keep-db`).

Expected output (a real run; under a second or two — timings and the
order of interleaved lines vary run to run):

```
  +    0ms  [setup     ] db=.../examples/03-news-desk/newsdesk.db, articles=3, session=newsdesk
  +   78ms  [scout     ] sent lead       #1  topic=batteries
  +  110ms  [writer    ] drafted article #2  (in reply to lead #1)  -> editor + fact_checker
  +  141ms  [editor    ] approved        #4  (correlation #1)
  +  157ms  [fact_chk  ] verified        #5  (correlation #1)
  +  172ms  [publisher ] PUBLISHED       (correlation #1)  "Batteries: next-gen sodium-ion economics"  [editor #4, fact_chk #5]
  +  188ms  [scout     ] sent lead       #6  topic=agriculture
  ...                                    (same five steps for articles 2 and 3)
  +  360ms  [publisher ] PUBLISHED       (correlation #11)  "Space: private orbital servicing race"  [editor #14, fact_chk #15]
  +  360ms  [publisher ] published 3/3 articles, exiting
  +  360ms  [scout     ] done
  +  407ms  [bell      ] wrap broadcast
  +  422ms  [writer    ] wrap received -> exit
  +  422ms  [editor    ] wrap received -> exit
  +  438ms  [fact_checker] wrap received -> exit
```

The correlation id is the **lead's** id: the writer threads both
drafts (ids 2 and 3 — only the editor's copy is printed) under the lead
they answer, and editor/fact-checker carry it through to the publisher.

## Files

- `agents.py` — the five role coroutines, plus message-body schemas
- `run.py` — orchestrates them via `asyncio.gather` against one DB
