"""``raven read`` — list a broadcast channel's unseen live messages.

Three output forms: human (default), ``--json``, and ``--framed``.
``--framed`` prints the messages inside ``policy.render``'s
sender-attributed DATA frame — the same frame ``raven acp`` injects — and
is the form the hook's pull notice tells an agent to run. Pulled content
that skipped the frame was the hole in ADR-003/006 the notice opened (QA
finding A3): the human form is for humans, the frame is for agents.
"""

from __future__ import annotations

import sys
from pathlib import Path

import typer

from raven_bus import cursors, db, models, policy
from raven_bus.cli._common import (
    EXIT_OK,
    EXIT_USAGE,
    die,
    echo_json,
    echo_messages_human,
    handle_errors,
    message_to_json,
)
from raven_bus.models import Message


def cmd_read(
    channel: str = typer.Option(..., "--channel", help="Channel to read."),
    as_: str = typer.Option(
        ..., "--as", help="Reader consumer id '<role>@<run>'."
    ),
    max_: int = typer.Option(100, "-m", "--max", help="Maximum messages to return."),
    json_out: bool = typer.Option(
        False, "-j", "--json", help="Emit JSON instead of text."
    ),
    framed: bool = typer.Option(
        False,
        "--framed",
        help=(
            "Print the messages in the sender-attributed DATA frame raven acp "
            "injects (content can't escape it). Use this when an agent reads."
        ),
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """List unseen live messages on a broadcast channel; does not ack."""
    if json_out and framed:
        die("--json and --framed are mutually exclusive", EXIT_USAGE)
    with handle_errors():
        models.parse_consumer_id(as_)
        models.validate_channel_name(channel)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            msgs = cursors.pending(conn, as_, channel, limit=max_)
    if json_out:
        echo_json([message_to_json(m) for m in msgs])
    elif framed:
        _echo_framed(msgs)
    else:
        echo_messages_human(msgs)
    raise typer.Exit(code=EXIT_OK)


def _echo_framed(msgs: list[Message]) -> None:
    """Print ``msgs`` through ``policy.render`` — never a frame of our own
    (AGENTS.md: only policy composes injection text). Blocking messages go
    in the interrupt section so the tier headings stay truthful; every
    block also carries its own ``urgency:`` line.

    The frame keeps non-ASCII readable (``ensure_ascii=False``), and a
    Windows pipe's stdout is cp1252: characters it can't encode are
    written as backslash escapes instead of raising UnicodeEncodeError
    (which would be a traceback, not a one-line error)."""
    if not msgs:
        typer.echo("(no messages)")
        return
    text = policy.render(
        policy.InjectionPlan(
            interrupt=[m for m in msgs if m.urgency == "blocking"],
            batch=[m for m in msgs if m.urgency != "blocking"],
        )
    )
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    typer.echo(text.encode(encoding, "backslashreplace").decode(encoding), nl=False)
