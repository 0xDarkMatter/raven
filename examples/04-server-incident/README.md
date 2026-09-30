# Server incident — five SRE agents diagnose & fix a flaky server

A simulated production server suffers a sequence of faults. Five
agents — each subscribed to its own inbox — collaborate to detect,
diagnose, fix, and verify each one.

> Runs on the deprecated v1 compat shim (`raven_bus.compat`, ADR-004).
> The shim is removed after one minor release; its v1 -> v2 narrowings
> are listed in `src/raven_bus/compat.py`'s module docstring.

## Roles

| Role | What it does |
|---|---|
| **monitor** | Polls `server.health()` every 50ms; sends `incident` to triager when a new symptom appears. Also injects scheduled faults so the demo has something to react to. |
| **triager** | Classifies the incident (`high`/`medium`/`low` severity by symptom) and forwards it as `investigate` to the diagnoser. |
| **diagnoser** | Calls `server.diagnose(symptom)` to get evidence + a prescribed fix; sends `prescription` to the fixer. |
| **fixer** | Calls `server.apply(fix)`; sends `fix_applied` to the verifier. |
| **verifier** | Re-checks `server.health()`; sends `resolved` (or in principle `escalate`) back to monitor and counts down to wrap-up. |

A small `bell` sentinel broadcasts `wrap` once all incidents are
resolved so every `subscribe()` loop exits cleanly.

## What it shows

| Pattern | Where you see it |
|---|---|
| **Pipeline routing** | Each role only knows its upstream/downstream addresses, not the whole graph. |
| **Correlation IDs** | The original incident's id flows through every downstream message as its `thread_id`, so `raven tail --db incident.db --no-follow --json` gives you the full audit trail per fault (one JSON object per line, grouped by channel; filter on `thread_id`). |
| **`reply_to` chain** | Each step references its parent, so you can walk backward from a fix to the symptom that prompted it. |
| **Shared mutable state outside the bus** | The `FlakyServer` instance is passed in-process to every agent; the bus carries *coordination*, not *state*. (For multi-process deployments you'd serialize state into messages or share a DB — both fine, but separate concerns from the messaging primitive.) |
| **Typed bodies (documentation only)** | Each message type has a Pydantic body model registered with `SchemaRegistry`, but under the compat shim registration is a no-op — nothing is validated and strict mode is ignored. The models document the wire shapes. |
| **Live subscribe** | Every consumer is a single `async for msg in subscribe()` loop. |

## Run

```bash
python run.py
```

Default: three faults injected in sequence (`db_disconnected`,
`cpu_saturated`, `errors_spiking`), each fully resolved before the
next one fires. Pass `--faults` to override the schedule, repeat
faults, or shorten the run. The DB defaults to `incident.db` next to
`run.py` (wherever you run it from), is deleted before each run unless
`--keep-db`, and is left in place afterwards for inspection:

```bash
raven tail --db incident.db --no-follow --json
```

Expected output (a real run; under a second — timings vary run to run):

```
  +    0ms  [setup     ] db=.../examples/04-server-incident/incident.db, faults=['db_disconnected', 'cpu_saturated', 'errors_spiking'], session=incident
  +    0ms  [setup     ] server initial health = {'db_connected': True, 'cpu_pct': 25, 'error_rate': 0.01, 'problems': [], 'ok': True}
  +   62ms  [monitor   ] (fault injected externally: db_disconnected)
  +  125ms  [monitor   ] INCIDENT #1  symptom=db_disconnected  health=['db_disconnected']
  +  203ms  [triager   ] investigate #2  symptom=db_disconnected  severity=high
  +  250ms  [diagnoser ] prescription #3  symptom=db_disconnected  fix=reconnect_db  evidence="TCP connect to db:5432 timed out"
  +  297ms  [fixer     ] applied        #4  fix=reconnect_db  success=True
  +  312ms  [verifier  ] RESOLVED       (correlation #1, duration=186ms, health=all clear)
  ...                                    (same six steps for cpu_saturated and errors_spiking)
  +  765ms  [verifier  ] RESOLVED       (correlation #11, duration=110ms, health=all clear)
  +  765ms  [verifier  ] resolved 3/3 incidents, exiting
  +  797ms  [bell      ] wrap broadcast
  +  812ms  [triager   ] wrap received -> exit
  +  812ms  [diagnoser ] wrap received -> exit
  +  828ms  [fixer     ] wrap received -> exit
  +  828ms  [setup     ] final server health = {'db_connected': True, 'cpu_pct': 25, 'error_rate': 0.01, 'problems': [], 'ok': True}
```

## Files

- `server.py` — the simulated `FlakyServer` (state, inspection, fix application, fault injection)
- `agents.py` — the five agent coroutines + Pydantic body schemas
- `run.py` — orchestrator that wires up the bus, injects faults, and reports
