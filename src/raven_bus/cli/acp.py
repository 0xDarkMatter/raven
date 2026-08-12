"""`raven acp` — run an agent under the ACP harness.  LANE: acp-harness.

    raven acp --as lane-3@v0-2 --channel run/v0-2/lane/3 \
              [--channel run/v0-2/control]... [--reply-to run/v0-2/telemetry] \
              [--db PATH] [--poll-interval 1.0] [--budget 2000] [--cwd .] \
              -- <agent command...>

Spawns the agent command (everything after ``--``) with piped stdio,
runs the harness loop, exits with the harness's code. Registration:
one `app.command` line in cli/main.py (the ONLY edit there). CLI error
conventions as the rest of the app (one-line error:, exit 2 usage /
10 error).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import typer

from raven_bus import db, models
from raven_bus.adapters.acp.harness import HarnessConfig, parse_channels, run_harness
from raven_bus.cli._common import EXIT_USAGE, die, handle_errors

# Registration note (mirrors cli/main.py's pattern for other commands):
#   app.command(
#       "acp",
#       help="Spawn an agent under the bus<->ACP harness.",
#       context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
#   )(acp_cmd.acp)
# The context_settings are load-bearing: they let everything after ``--``
# (including tokens that look like options, e.g. ``-y``) land in
# ``ctx.args`` unparsed, instead of raven trying to consume them itself.


def acp(
    ctx: typer.Context,
    as_: str = typer.Option(
        ..., "--as", help="Consumer id '<role>@<run>' driving the agent."
    ),
    channel: list[str] = typer.Option(  # noqa: B008
        ..., "--channel", help="Channel to watch (repeatable)."
    ),
    reply_to: str | None = typer.Option(
        None, "--reply-to", help="Channel to post agent replies/telemetry to."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
    poll_interval: float = typer.Option(
        1.0, "--poll-interval", help="Idle poll interval, in seconds."
    ),
    budget: int = typer.Option(
        2000, "--budget", help="Per-boundary injected-content token budget."
    ),
    cwd: str = typer.Option(
        ".", "--cwd", help="Working directory passed to session/new."
    ),
) -> None:
    """Spawn an agent (everything after ``--``) under the bus<->ACP
    harness: a dumb pipe (ADR-006) — no respawn, exit when the child
    exits."""
    agent_argv = list(ctx.args)
    if not agent_argv:
        die(
            "missing agent command: pass it after `--` "
            "(raven acp --as ... --channel ... -- <agent command...>)",
            EXIT_USAGE,
        )
        return  # pragma: no cover -- die always raises

    with handle_errors():
        models.parse_consumer_id(as_)
        channels = parse_channels(channel)
        if reply_to is not None:
            models.validate_channel_name(reply_to)
        db.init_db(db_path)

    config = HarnessConfig(
        consumer=as_,
        channels=channels,
        reply_channel=reply_to,
        db_path=db_path,
        poll_interval_s=poll_interval,
        token_budget=budget,
        cwd=cwd,
    )

    child = subprocess.Popen(
        agent_argv,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    try:
        exit_code = run_harness(config, child)
    finally:
        if child.poll() is None:
            child.terminate()

    raise typer.Exit(code=exit_code)


__all__ = ["acp"]
