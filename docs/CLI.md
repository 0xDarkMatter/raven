# CLI surface

The `raven` command surface, frozen: **flags may grow, commands may not**
(adding a command needs a decision). Moved out of AGENTS.md; `cli/main.py`'s
module docstring is the in-code copy — change both together.

```
raven send      --channel C --from R@RUN -t TYPE --body JSON
                [--urgency U] [--tag T]... [--reply-to ID] [--expires-in S]
                [--kind broadcast|queue|stream]
raven read      --channel C --as R@RUN [-m MAX≥1] [-j | --framed]
                                                            (broadcast pending; --framed = the
                                                             policy.render data frame)
raven ack       --channel C --as R@RUN --up-to ID          (cursor jump)
raven claim     --channel C --as R@RUN [--lease S] [-j]    (queue: claim next)
raven done      --id ID --as R@RUN                         (complete claim)
raven release   --id ID --as R@RUN
raven tail      [--channel C] [--from ID] [--no-follow] [--json] [--interval S]
raven channels  [--prefix P] [-j]
raven doctor    [--db P]
raven teardown  --run RUN [--yes]
raven version
raven serve    [--host 127.0.0.1] [--port 7713] [--db P] [--yes-expose]
                                                            (run ravend under uvicorn; `[http]` extra;
                                                             non-loopback --host needs --yes-expose)
raven acp      --as R@RUN --channel C [--channel C]... [--reply-to C]
               [--db P] [--poll-interval S] [--budget N] [--cwd .]
               [--mode M] [--initial-prompt-file F] [--timeout S] -- <agent cmd...>
                                                            (dumb-pipe ACP harness; ADR-006.
                                                             --mode = session/set_mode after
                                                             session/new; headless lanes need a
                                                             non-prompting permission mode.
                                                             --initial-prompt-file = the task
                                                             packet, VERBATIM boundary 0 —
                                                             trusted spawner input, never
                                                             data-framed; bus messages are)
```

Exit codes (`cli/_common.py`): `0` ok / `2` usage / `3` not-found / `10` error.
Failures render as one-line `error: …`; tracebacks never reach users. Consumer
ids are `<role>@<run>`; channels are path-style; atoms are lowercase
`[a-z0-9][a-z0-9._-]*` (ADR-002).
