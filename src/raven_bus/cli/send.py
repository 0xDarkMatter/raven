"""``raven send`` — append a message to a channel.

``--kind`` is optional on purpose (QA cli #1). Omitted, the send goes
through ``log.append(ensure=True)``: get-or-create with no kind opinion,
so an existing queue/stream channel is appended to as-is and only an
absent channel is created (as broadcast). Given, the kind is ENFORCED:
``channels.ensure_channel(kind)`` creates the channel as that kind or
raises WrongChannelKindError on a mismatch, then ``append(ensure=False)``.
A ``"broadcast"`` default here made every plain send to a queue fail.
"""

from __future__ import annotations

from pathlib import Path
from typing import get_args

import click
import typer

from raven_bus import channels, db, log, models
from raven_bus.cli._common import EXIT_OK, handle_errors, parse_body

_KINDS = list(get_args(models.ChannelKind))


def cmd_send(
    channel: str = typer.Option(..., "--channel", help="Target channel name."),
    from_: str = typer.Option(
        ..., "--from", help="Sender consumer id '<role>@<run>'."
    ),
    type_: str = typer.Option(..., "--type", "-t", help="Message type."),
    body: str = typer.Option(..., "--body", help="JSON body object."),
    urgency: str = typer.Option(
        "prompt", "--urgency", help="blocking|prompt|fyi (default prompt)."
    ),
    tag: list[str] = typer.Option([], "--tag", help="Repeatable tag."),  # noqa: B008
    reply_to: int | None = typer.Option(
        None, "--reply-to", help="Id of the message this replies to."
    ),
    expires_in: int | None = typer.Option(
        None, "--expires-in", help="TTL in seconds."
    ),
    kind: str | None = typer.Option(
        None,
        "--kind",
        click_type=click.Choice(_KINDS),
        help="Require this channel kind: creates the channel as KIND if it is "
        "new, errors if it exists with another kind. Omit to send to an "
        "existing channel of any kind (a new one is created as broadcast).",
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Send a message; prints the new message id."""
    payload = parse_body(body)
    with handle_errors():
        models.parse_consumer_id(from_)
        models.validate_channel_name(channel)
        tags = models.validate_tags(tag)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            if kind is not None:
                channels.ensure_channel(conn, channel, kind)  # type: ignore[arg-type]
            msg = log.append(
                conn,
                channel=channel,
                sender=from_,
                type=type_,
                body=payload,
                urgency=urgency,  # type: ignore[arg-type]
                tags=tags,
                reply_to=reply_to,
                expires_in_s=expires_in,
                ensure=kind is None,
            )
    typer.echo(f"sent #{msg.id} {msg.sender} -> {msg.channel} type={msg.type}")
    raise typer.Exit(code=EXIT_OK)
