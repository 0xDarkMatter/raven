"""``claude-bus`` CLI entry point.

The 8 Phase-1 commands wired here are::

    claude-bus init
    claude-bus doctor
    claude-bus session init <id>
    claude-bus send --from <r:s> --to <r:s> --type <t> --body <json>
    claude-bus inbox --role <r:s> [--max N] [--json]
    claude-bus read <id> [--json]
    claude-bus ack <id>
    claude-bus serve [--port 7713] [--host 127.0.0.1]
"""

from __future__ import annotations

import logging
import os
import sys

import structlog
import typer

from claude_bus import __version__

# Quiet structlog by default at the CLI surface — set CLAUDE_BUS_LOG_LEVEL
# to DEBUG/INFO to bring it back. Library callers configure structlog
# themselves and aren't affected.
_log_level = os.environ.get("CLAUDE_BUS_LOG_LEVEL", "WARNING").upper()
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, _log_level, logging.WARNING)
    ),
)
from claude_bus.cli import ack as ack_cmd
from claude_bus.cli import doctor as doctor_cmd
from claude_bus.cli import inbox as inbox_cmd
from claude_bus.cli import init as init_cmd
from claude_bus.cli import read as read_cmd
from claude_bus.cli import send as send_cmd
from claude_bus.cli import serve as serve_cmd
from claude_bus.cli import session as session_cmd
from claude_bus.cli import tail as tail_cmd

app = typer.Typer(
    name="claude-bus",
    help="SQLite-backed role-addressable message bus for agent sessions.",
    no_args_is_help=True,
    add_completion=False,
)

app.command("init", help="Scaffold claude-bus in the current directory.")(
    init_cmd.cmd_init
)
app.command("doctor", help="Run health checks against the local environment.")(
    doctor_cmd.cmd_doctor
)
app.command("send", help="Send a typed message between two roles.")(
    send_cmd.cmd_send
)
app.command("inbox", help="List unread messages for a role.")(
    inbox_cmd.cmd_inbox
)
app.command("read", help="Read a message by id without acking it.")(
    read_cmd.cmd_read
)
app.command("ack", help="Mark a message as read.")(ack_cmd.cmd_ack)
app.command("tail", help="Stream new messages live (identity-free observer).")(
    tail_cmd.cmd_tail
)
app.command("serve", help="Start the optional HTTP bridge.")(serve_cmd.cmd_serve)

app.add_typer(
    session_cmd.app,
    name="session",
    help="Session lifecycle helpers.",
)


@app.command("version", help="Print claude-bus version and exit.")
def cmd_version() -> None:
    typer.echo(f"claude-bus {__version__}")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"claude-bus {__version__}")
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
    """claude-bus — SQLite-backed role-addressable message bus."""


def cli_main() -> None:
    """Entry point referenced by ``[project.scripts]`` in pyproject.toml.

    Catches the broad classes of error users actually hit — bad config,
    DB issues, schema validation, missing messages — and renders them
    as one-line errors instead of a Python traceback. Unexpected
    exceptions still raise so they're visible during development.
    """
    from claude_bus.cli._common import (
        EXIT_CONFIG,
        EXIT_DB,
        EXIT_GENERIC,
        EXIT_MESSAGE_NOT_FOUND,
        EXIT_SCHEMA_VALIDATION,
    )
    from claude_bus.exceptions import (
        ClaudeBusError,
        SchemaValidationError,
        UnknownMessageError,
    )

    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)
    except SchemaValidationError as exc:
        typer.echo(f"error: schema validation failed: {exc}", err=True)
        sys.exit(EXIT_SCHEMA_VALIDATION)
    except UnknownMessageError as exc:
        typer.echo(f"error: {exc}", err=True)
        sys.exit(EXIT_MESSAGE_NOT_FOUND)
    except ClaudeBusError as exc:
        typer.echo(f"error: {exc}", err=True)
        sys.exit(EXIT_GENERIC)
    except FileNotFoundError as exc:
        typer.echo(f"error: file not found: {exc.filename or exc}", err=True)
        sys.exit(EXIT_CONFIG)
    except PermissionError as exc:
        typer.echo(f"error: permission denied: {exc.filename or exc}", err=True)
        sys.exit(EXIT_DB)


if __name__ == "__main__":  # pragma: no cover
    cli_main()
