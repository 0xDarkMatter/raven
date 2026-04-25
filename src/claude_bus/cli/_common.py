"""Shared CLI helpers: exit codes, JSON output, body parsing."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import typer

from claude_bus.client import Message

# ----- exit codes ----------------------------------------------------

EXIT_OK = 0
EXIT_GENERIC = 1
EXIT_CONFIG = 2
EXIT_DB = 3
EXIT_MESSAGE_NOT_FOUND = 4
EXIT_SCHEMA_VALIDATION = 5
EXIT_SESSION_NOT_INIT = 6
EXIT_HTTP_BIND = 7

# ----- output helpers ------------------------------------------------


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


def message_to_json(msg: Message) -> dict[str, Any]:
    return _serialise(msg.model_dump(mode="json"))


def echo_json(payload: Any) -> None:
    typer.echo(json.dumps(_serialise(payload), indent=2, sort_keys=True))


def echo_message_human(msg: Message) -> None:
    typer.echo(
        f"#{msg.id}  {msg.sender} -> {msg.recipient}  "
        f"type={msg.type}  status={msg.status}  "
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


# ----- body parsing --------------------------------------------------


def parse_body(body: str | None, body_file: Path | None) -> dict[str, Any]:
    """Resolve the JSON body from either ``--body`` or ``--body-file``."""
    if body is not None and body_file is not None:
        typer.echo(
            "error: --body and --body-file are mutually exclusive",
            err=True,
        )
        raise typer.Exit(code=EXIT_CONFIG)
    if body is None and body_file is None:
        return {}
    raw = body if body is not None else body_file.read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        typer.echo(f"error: body is not valid JSON: {exc}", err=True)
        raise typer.Exit(code=EXIT_SCHEMA_VALIDATION) from exc
    if not isinstance(parsed, dict):
        typer.echo("error: body must be a JSON object", err=True)
        raise typer.Exit(code=EXIT_SCHEMA_VALIDATION)
    return parsed


def parse_address(addr: str) -> tuple[str, str]:
    """Validate and split an ``"<role>:<session>"`` address."""
    if ":" not in addr:
        typer.echo(
            f"error: address {addr!r} must be of the form '<role>:<session>'",
            err=True,
        )
        raise typer.Exit(code=EXIT_CONFIG)
    role, session = addr.split(":", 1)
    if not role or not session:
        typer.echo(
            f"error: address {addr!r} has empty role or session",
            err=True,
        )
        raise typer.Exit(code=EXIT_CONFIG)
    return role, session


def die(message: str, code: int = EXIT_GENERIC) -> None:
    """Echo to stderr and exit."""
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(code=code)


def fatal_unexpected(exc: BaseException) -> None:  # pragma: no cover
    typer.echo(f"unexpected error: {exc.__class__.__name__}: {exc}", err=True)
    sys.exit(EXIT_GENERIC)
