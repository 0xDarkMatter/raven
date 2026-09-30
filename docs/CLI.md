# CLI surface

The `raven` command surface, frozen: **flags may grow, commands may not**
(adding a command needs a decision). Moved out of AGENTS.md; `cli/main.py`'s
module docstring is the in-code summary — change both together.

```
raven send      --channel C --from R@RUN -t TYPE --body JSON
                [--urgency blocking|prompt|fyi] [--tag T]... [--reply-to ID]
                [--expires-in S] [--kind broadcast|queue|stream]
raven read      --channel C --as R@RUN [-m MAX] [-j | --framed]   (broadcast pending)
raven ack       --channel C --as R@RUN --up-to ID                 (cursor jump)
raven claim     --channel C --as R@RUN [--lease S] [-j]           (queue: claim next)
raven done      --id ID --as R@RUN                                (complete claim)
raven release   --id ID --as R@RUN
raven tail      [--channel C] [--from ID] [--no-follow] [--json] [--interval S]
raven channels  [--prefix P] [-j]
raven doctor    [--db P]
raven teardown  --run RUN [--yes]
raven version
raven serve     [--host 127.0.0.1] [--port 7713] [--db P] [--yes-expose]
raven acp       --as R@RUN --channel C [--channel C]... [--reply-to C]
                [--db P] [--poll-interval S] [--budget N] [--cwd .]
                [--mode M] [--initial-prompt-file F] [--timeout S] -- <agent cmd...>
```

Every command also takes `--db P` (else `RAVEN_DB`, else `~/.raven/bus.db`).

## Bounds (out of range = usage error, exit 2)

| Flag | Range |
|---|---|
| `send -t/--type` | non-empty (not whitespace-only) |
| `send --expires-in`, `claim --lease` | 1 .. 2,592,000 s (30 days) |
| `send --reply-to`, `done/release --id` | 1 .. 2⁶³−1 |
| `ack --up-to`, `tail --from` | 0 .. 2⁶³−1 (an ack past the channel head is clamped to it) |
| `read -m/--max` | ≥ 1 |
| `tail --interval` | > 0 and ≤ 3600 (finite) |
| `serve --port` | 1 .. 65535 |
| `acp --poll-interval`, `acp --timeout` | > 0 (finite); `--timeout` omitted = no limit |
| `acp --budget` | ≥ 1 |

Bound and choice violations print Typer's usage box (exit 2); every other
failure is one line `error: …`.

## Behaviour worth knowing

- **`send --kind`** is optional. Omitted: sends to the channel whatever its
  kind, creating it as `broadcast` if absent. Given: creates the channel as
  that kind, or exits 10 if it already exists as another. A body nested too
  deep (> 64 levels, or past the JSON parser's limit) exits 2.
- **`read --framed`** prints the consumer's pending messages in the same
  `policy.render` data frame the ACP harness injects — the command the hook's
  notice gives agents. It can't be combined with `-j`.
- **`tail`** always prints in message-id order (across all channels when no
  `--channel` is given); `--no-follow` drains the whole backlog, not one
  batch. It is identity-free and includes expired messages (forensic view).
- **`doctor`** runs one real sweep (it requeues lapsed leases). If the DB
  path didn't exist it creates it but warns (`[warn] db did not exist -
  created it at <path>`) and ends `all checks passed (with warnings)`. A
  foreign DB is a `[fail]` line.
- **`teardown`** without `--yes` asks for confirmation on a terminal; with no
  terminal (or on EOF) it refuses: `error: refusing to tear down without
  confirmation (pass --yes)`, exit 2. If another run's message replies into
  this run, it refuses (exit 10) and names the blockers — nothing is deleted.
- **`serve`** checks the port before starting uvicorn: `error: cannot bind
  <host>:<port>: <reason>`, exit 10. A non-loopback `--host` needs
  `--yes-expose` (ravend has no auth — ADR-005).
- **`acp`** — see README "`raven acp` — the ACP harness" for exit codes,
  channel handling and the `RAVEN_ACP_*` environment it gives the agent.

## Exit codes (`cli/_common.py`)

`0` ok / `2` usage / `3` not-found / `10` error. Any unexpected exception is
rendered as `error: <Type>: <message>` with exit 10 — tracebacks never reach
users. Consumer ids are `<role>@<run>`; channels are path-style; atoms are
lowercase `[a-z0-9][a-z0-9._-]*` (ADR-002).
