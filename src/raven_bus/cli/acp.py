"""`raven acp` — run an agent under the ACP harness.  LANE: acp-harness.

    raven acp --as lane-3@v0-2 --channel run/v0-2/lane/3 \
              [--channel run/v0-2/control]... [--reply-to run/v0-2/telemetry] \
              [--db PATH] [--poll-interval 1.0] [--budget 2000] [--cwd .] \
              [--mode MODE] [--initial-prompt-file FILE] [--timeout S] \
              -- <agent command...>

Spawns the agent command (everything after ``--``) with piped stdio,
runs the harness loop, exits with the harness's code. Registration:
one `app.command` line in cli/main.py (the ONLY edit there). CLI error
conventions as the rest of the app (one-line error:, exit 2 usage /
10 error).
"""

from __future__ import annotations

import math
import os
import subprocess
from pathlib import Path

import typer

from raven_bus import channels as channels_mod
from raven_bus import db, models
from raven_bus.adapters.acp.harness import HarnessConfig, parse_channels, run_harness
from raven_bus.adapters.hooks.peek import ENV_ACP_CHANNELS, ENV_ACP_CONSUMER
from raven_bus.cli._common import EXIT_ERROR, EXIT_USAGE, die, handle_errors

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
    mode: str | None = typer.Option(
        None,
        "--mode",
        help=(
            "Session mode to select via session/set_mode after session/new "
            "(agent-defined, e.g. bypassPermissions, dontAsk, acceptEdits). "
            "Headless lanes need a non-prompting permission mode: the "
            "harness refuses session/request_permission, so an agent left "
            "prompting cannot use tools."
        ),
    ),
    initial_prompt_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--initial-prompt-file",
        help=(
            "File whose contents are sent VERBATIM as the session's first "
            "prompt (the lane's task packet) before the bus loop starts. "
            "Trusted spawner input — bus messages stay data-framed; a task "
            "delivered as a bus message reads as data and a well-behaved "
            "agent refuses it."
        ),
    ),
    timeout: float | None = typer.Option(
        None,
        "--timeout",
        help=(
            "Inactivity limit in seconds: exit 10 when the agent sends "
            "nothing for this long (every streamed update resets it). "
            "Default: none — a healthy agent is silent while a long tool "
            "call runs, and a dead agent is detected without it."
        ),
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
        if mode is not None and not mode.strip():
            die("--mode must be a non-empty mode id", EXIT_USAGE)
        if timeout is not None and not timeout > 0:
            die("--timeout must be a positive number of seconds", EXIT_USAGE)
        # Usage errors, not runtime ones (QA finding A13): a zero poll
        # interval busy-looped the idle path at full CPU, a negative one
        # raised ValueError from time.sleep (a traceback), and a budget
        # below 1 token defers everything but the escape-valve rescue.
        if not (math.isfinite(poll_interval) and poll_interval > 0):
            die("--poll-interval must be a positive number of seconds", EXIT_USAGE)
        if budget < 1:
            die("--budget must be at least 1 token", EXIT_USAGE)
        initial_prompt: str | None = None
        if initial_prompt_file is not None:
            try:
                initial_prompt = initial_prompt_file.read_text(encoding="utf-8")
            except OSError as exc:
                die(f"cannot read --initial-prompt-file: {exc}", EXIT_USAGE)
            except UnicodeDecodeError as exc:
                die(f"--initial-prompt-file is not valid UTF-8: {exc}", EXIT_USAGE)
            if initial_prompt is None or not initial_prompt.strip():
                die("--initial-prompt-file is empty", EXIT_USAGE)
        db.init_db(db_path)
        # Ensure the lane's channels exist BEFORE the loop: a lane must be
        # startable before its orchestrator has sent anything (the first
        # pending() poll on a never-used channel raised UnknownChannelError
        # and killed the harness — found by the P4b live run). Broadcast is
        # the only kind the cursor loop can serve; a kind mismatch on an
        # existing channel fails loudly here (WrongChannelKindError).
        with db.connection(db_path) as conn:
            for name in channels:
                channels_mod.ensure_channel(conn, name, kind="broadcast")
            if reply_to is not None:
                channels_mod.ensure_channel(conn, reply_to, kind="broadcast")

    config = HarnessConfig(
        consumer=as_,
        channels=channels,
        reply_channel=reply_to,
        db_path=db_path,
        poll_interval_s=poll_interval,
        token_budget=budget,
        cwd=cwd,
        mode=mode,
        initial_prompt=initial_prompt,
        timeout_s=timeout,
    )

    try:
        # Tell a raven hook inside the agent which consumer/channels this
        # harness already delivers, so it doesn't re-announce them during
        # the very turn that injects them (QA finding A11; peek skips
        # exactly these channels for exactly this consumer).
        child_env = {
            **os.environ,
            ENV_ACP_CONSUMER: as_,
            ENV_ACP_CHANNELS: ",".join(channels),
        }
        child = subprocess.Popen(
            agent_argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=child_env,
        )
    except OSError as exc:
        # A missing/unlaunchable agent must render as the CLI's one-line
        # error, not a traceback (found by the live P3 smoke).
        die(f"cannot launch agent {agent_argv[0]!r}: {exc}", EXIT_ERROR)
        return  # pragma: no cover -- die always raises
    try:
        exit_code = run_harness(config, child)
    finally:
        if child.poll() is None:
            child.terminate()

    raise typer.Exit(code=exit_code)


__all__ = ["acp"]
