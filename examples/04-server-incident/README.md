# Server incident — five SRE agents diagnose & fix a flaky server

A simulated production server suffers a sequence of faults. Five
agents — each subscribed to its own inbox — collaborate to detect,
diagnose, fix, and verify each one.

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
| **Correlation IDs** | The original incident's id flows through every downstream message, so `claude-bus inbox --json` gives you the full audit trail per fault. |
| **`reply_to` chain** | Each step references its parent, so you can walk backward from a fix to the symptom that prompted it. |
| **Shared mutable state outside the bus** | The `FlakyServer` instance is passed in-process to every agent; the bus carries *coordination*, not *state*. (For multi-process deployments you'd serialize state into messages or share a DB — both fine, but separate concerns from the messaging primitive.) |
| **Strict schemas** | Each message type has a Pydantic body schema registered. Try changing one of the `body=` dicts in `agents.py` to see strict validation reject it. |
| **Live subscribe** | Every consumer is a single `async for msg in subscribe()` loop. |

## Run

```bash
python run.py
```

Default: three faults injected in sequence (`db_disconnected`,
`cpu_saturated`, `errors_spiking`), each fully resolved before the
next one fires. Pass `--faults` to override the schedule, repeat
faults, or shorten the run.

Expected output (~400ms total):

```
  +    0ms  [setup     ] db=...incident.db, faults=[...], session=incident
  +    1ms  [setup     ] server initial health = {db_connected: True, cpu_pct: 25, ...}
  +   55ms  [monitor   ] (fault injected externally: db_disconnected)
  +   85ms  [monitor   ] INCIDENT #1  symptom=db_disconnected  health=['db_disconnected']
  +  100ms  [triager   ] investigate #2  symptom=db_disconnected  severity=high
  +  115ms  [diagnoser ] prescription #3  symptom=db_disconnected  fix=reconnect_db
  +  130ms  [fixer     ] applied        #4  fix=reconnect_db  success=True
  +  148ms  [verifier  ] RESOLVED       (correlation #1, duration=63ms, health=all clear)
... (×3 incidents)
  +  385ms  [verifier  ] resolved 3/3 incidents, exiting
  +  390ms  [bell      ] wrap broadcast
```

## Files

- `server.py` — the simulated `FlakyServer` (state, inspection, fix application, fault injection)
- `agents.py` — the five agent coroutines + Pydantic body schemas
- `run.py` — orchestrator that wires up the bus, injects faults, and reports
