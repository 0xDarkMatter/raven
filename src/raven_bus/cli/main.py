"""raven v2 CLI entry point.  LANE: cli (raven2-p1).

Commands (frozen surface — flags may grow, commands may not):

    raven send      --channel C --from R@RUN -t TYPE --body JSON
                    [--urgency U] [--tag T]... [--reply-to ID]
                    [--expires-in S] [--kind broadcast|queue|stream]
    raven read      --channel C --as R@RUN [-m MAX] [-j]     (broadcast pending)
    raven ack       --channel C --as R@RUN --up-to ID        (cursor jump)
    raven claim     --channel C --as R@RUN [--lease S] [-j]  (queue: claim next)
    raven done      --id ID --as R@RUN                       (complete claim)
    raven release   --id ID --as R@RUN
    raven tail      [--channel C] [--from ID] [--no-follow] [--json]
                    (identity-free, includes expired — forensic surface)
    raven channels  [--prefix P] [-j]
    raven doctor    (db reachable, schema version, WAL, sweep dry stats)
    raven teardown  --run RUN [--yes]
    raven version

Conventions carried from v1: one-line ``error: ...`` on failure, exit
codes 0 ok / 2 usage / 3 not-found / 10 error, ``-j/--json`` on read
surfaces, tracebacks never shown to users. Every read command runs the
opportunistic sweep first (ADR-001).
"""

from __future__ import annotations

import sys

import typer

from raven_bus import __version__
from raven_bus.cli import ack as ack_cmd
from raven_bus.cli import channels_cmd
from raven_bus.cli import claim as claim_cmd
from raven_bus.cli import doctor as doctor_cmd
from raven_bus.cli import done as done_cmd
from raven_bus.cli import read as read_cmd
from raven_bus.cli import release as release_cmd
from raven_bus.cli import send as send_cmd
from raven_bus.cli import tail as tail_cmd
from raven_bus.cli import teardown as teardown_cmd
from raven_bus.cli._common import EXIT_ERROR
from raven_bus.exceptions import RavenBusError

app = typer.Typer(
    name="raven",
    help="SQLite-backed role-addressable message bus for agent sessions (v2).",
    no_args_is_help=True,
    add_completion=False,
)

app.command("send", help="Append a message to a channel.")(send_cmd.cmd_send)
app.command("read", help="List a broadcast channel's unseen live messages.")(
    read_cmd.cmd_read
)
app.command("ack", help="Advance a broadcast cursor (jump-ack).")(ack_cmd.cmd_ack)
app.command("claim", help="Claim the next live message on a queue channel.")(
    claim_cmd.cmd_claim
)
app.command("done", help="Mark a held claim complete.")(done_cmd.cmd_done)
app.command("release", help="Voluntarily give back a held claim.")(
    release_cmd.cmd_release
)
app.command("tail", help="Stream raw log messages (identity-free, forensic).")(
    tail_cmd.cmd_tail
)
app.command("channels", help="List the channel registry.")(
    channels_cmd.cmd_channels
)
app.command("doctor", help="Run health checks against the local environment.")(
    doctor_cmd.cmd_doctor
)
app.command("teardown", help="Delete all data for a run.")(
    teardown_cmd.cmd_teardown
)


@app.command("version", help="Print raven version and exit.")
def cmd_version() -> None:
    typer.echo(f"raven {__version__}")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"raven {__version__}")
        raise typer.Exit()


@app.callback()
def _global(
    version: bool = typer.Option(
        None,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Print version and exit.",
    ),
) -> None:
    """raven — SQLite-backed role-addressable message bus (v2)."""


def cli_main() -> None:
    """Console entry point (wired as ``raven2`` during the build run;
    takes over the ``raven`` script name when claude_bus is removed at
    integrate — ADR-004).

    Individual commands map raven_bus's exception hierarchy to the
    frozen exit codes via ``cli._common.handle_errors``; this is a
    last-resort net for anything that slips through (Ctrl-C, an
    unhandled RavenBusError) so users never see a traceback.
    """
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)
    except RavenBusError as exc:
        typer.echo(f"error: {exc}", err=True)
        sys.exit(EXIT_ERROR)


__all__ = ["app", "cli_main"]


if __name__ == "__main__":  # pragma: no cover
    cli_main()
