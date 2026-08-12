"""Shared CLI helpers: exit codes, error mapping, JSON/human rendering.

Exit codes (frozen — cli/main.py docstring): 0 ok / 2 usage / 3 not-found
/ 10 error. ``handle_errors`` is the single place that maps raven_bus's
exception hierarchy onto them so every command renders failures the
same way.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from pydantic import BaseModel

from raven_bus.exceptions import (
    InvalidAddressError,
    RavenBusError,
    UnknownChannelError,
    UnknownMessageError,
)
from raven_bus.models import Message

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_ERROR = 10


def die(message: str, code: int = EXIT_ERROR) -> None:
    """Echo ``error: <message>`` to stderr and exit with ``code``."""
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(code=code)


@contextmanager
def handle_errors() -> Iterator[None]:
    """Map the raven_bus exception hierarchy to the frozen exit codes.

    InvalidAddressError (bad role/run/channel/tag) is a usage error;
    unknown channel/message is a not-found error; every other
    RavenBusError is a generic store error. Wrap the store calls a
    command makes in ``with handle_errors():``.
    """
    try:
        yield
    except InvalidAddressError as exc:
        die(str(exc), EXIT_USAGE)
    except (UnknownChannelError, UnknownMessageError) as exc:
        die(str(exc), EXIT_NOT_FOUND)
    except RavenBusError as exc:
        die(str(exc), EXIT_ERROR)


def _serialise(value: Any) -> Any:
    """Recursive JSON-safe converter for objects we hand to the user."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {k: _serialise(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialise(v) for v in value]
    return value


def model_to_json(model: BaseModel) -> dict[str, Any]:
    return _serialise(model.model_dump(mode="json"))


def message_to_json(msg: Message) -> dict[str, Any]:
    return model_to_json(msg)


def echo_json(payload: Any) -> None:
    typer.echo(json.dumps(_serialise(payload), indent=2, sort_keys=True))


def echo_message_human(msg: Message) -> None:
    typer.echo(
        f"#{msg.id}  {msg.sender} -> {msg.channel}  "
        f"type={msg.type}  urgency={msg.urgency}  "
        f"created={msg.created_at.isoformat()}"
    )
    typer.echo(f"  body: {json.dumps(msg.body, sort_keys=True)}")
    if msg.tags:
        typer.echo(f"  tags: {', '.join(msg.tags)}")


def echo_messages_human(msgs: list[Message]) -> None:
    if not msgs:
        typer.echo("(no messages)")
        return
    for m in msgs:
        echo_message_human(m)


def parse_body(body: str) -> dict[str, Any]:
    """Parse ``--body`` as a JSON object, or exit with a usage error."""
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        die(f"body is not valid JSON: {exc}", EXIT_USAGE)
        return {}
    if not isinstance(parsed, dict):
        die("body must be a JSON object", EXIT_USAGE)
        return {}
    return parsed
