"""``raven claim`` — claim the next live message on a queue channel."""

from __future__ import annotations

from pathlib import Path

import typer

from raven_bus import claims, db, models
from raven_bus.claims import DEFAULT_LEASE_S
from raven_bus.cli._common import (
    EXIT_OK,
    MAX_DURATION_S,
    echo_json,
    echo_message_human,
    handle_errors,
    message_to_json,
)


def cmd_claim(
    channel: str = typer.Option(..., "--channel", help="Queue channel to claim from."),
    as_: str = typer.Option(
        ..., "--as", help="Claiming consumer id '<role>@<run>'."
    ),
    lease: int = typer.Option(
        DEFAULT_LEASE_S, "--lease", min=1, max=MAX_DURATION_S,
        help="Lease duration in seconds (1 to 2592000 = 30 days).",
    ),
    json_out: bool = typer.Option(
        False, "-j", "--json", help="Emit JSON instead of text."
    ),
    db_path: Path | None = typer.Option(None, "--db", help="DB path override."),  # noqa: B008
) -> None:
    """Claim the oldest unclaimed live message, or report there is none."""
    with handle_errors():
        models.parse_consumer_id(as_)
        models.validate_channel_name(channel)
        db.init_db(db_path)
        with db.connection(db_path) as conn:
            msg = claims.claim_next(conn, as_, channel, lease_s=lease)
    if msg is None:
        if json_out:
            echo_json(None)
        else:
            typer.echo("(no message)")
        raise typer.Exit(code=EXIT_OK)
    if json_out:
        echo_json(message_to_json(msg))
    else:
        echo_message_human(msg)
    raise typer.Exit(code=EXIT_OK)
