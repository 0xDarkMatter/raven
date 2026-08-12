"""ravend application factory — FROZEN wave-0 artifact (raven2-p2).

The route table below IS ADR-005's endpoint table; lanes implement the
handlers in read.py / write.py / sse.py and never edit this file. The
error envelope and the exception→status mapping live here so every
handler renders failures identically.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route
except ImportError as exc:  # pragma: no cover -- exercised only without the extra
    raise ImportError(
        "raven_bus.http requires the [http] extra: pip install -e '.[http]'"
    ) from exc

from raven_bus.exceptions import (
    ClaimDeniedError,
    InvalidAddressError,
    RavenBusError,
    UnknownChannelError,
    UnknownMessageError,
    WrongChannelKindError,
)
from raven_bus.paths import resolve_db_path

DEFAULT_HTTP_HOST = "127.0.0.1"
"""Loopback only — no auth in-process (ADR-005)."""

DEFAULT_HTTP_PORT = 7713
"""Unclaimed in the machine port registry as of 2026-08-12 (ADR-005)."""


def error_response(code: str, detail: str, status: int) -> JSONResponse:
    """The one error envelope (ADR-005): {"error": code, "detail": human}."""
    return JSONResponse({"error": code, "detail": detail}, status_code=status)


def map_exception(exc: Exception) -> JSONResponse:
    """Exception → envelope. 400 invalid input, 404 unknown, 409 denied/
    wrong-kind, 500 anything else raven-shaped."""
    if isinstance(exc, InvalidAddressError | ValueError):
        return error_response("bad_request", str(exc), 400)
    if isinstance(exc, UnknownChannelError | UnknownMessageError):
        return error_response("not_found", str(exc), 404)
    if isinstance(exc, ClaimDeniedError | WrongChannelKindError):
        return error_response("conflict", str(exc), 409)
    if isinstance(exc, RavenBusError):  # pragma: no cover -- future subclasses
        return error_response("error", str(exc), 500)
    raise exc


def create_app(db_path: str | Path | None = None) -> Starlette:
    """Build the ravend app bound to ``db_path``. Pure function — the
    resolved path is captured in ``app.state.db_path``; handlers read it
    from ``request.app.state`` so tests can point one process at many
    DBs."""
    from raven_bus.http import read, sse, write

    resolved = resolve_db_path(db_path)
    app = Starlette(
        debug=False,
        routes=[
            Route("/health", read.health, methods=["GET"]),
            Route("/channels", read.list_channels, methods=["GET"]),
            Route("/channels/{name:path}/messages", read.messages, methods=["GET"]),
            Route("/channels/{name:path}/pending", read.pending, methods=["GET"]),
            Route("/channels/{name:path}/cursor", read.cursor, methods=["GET"]),
            Route("/tail", sse.tail, methods=["GET"]),
            Route("/send", write.send, methods=["POST"]),
            Route("/claim", write.claim, methods=["POST"]),
            Route("/claims/{message_id:int}/renew", write.claim_renew, methods=["POST"]),
            Route("/claims/{message_id:int}/done", write.claim_done, methods=["POST"]),
            Route("/claims/{message_id:int}/release", write.claim_release, methods=["POST"]),
            Route("/ack", write.ack, methods=["POST"]),
            Route("/heartbeat", write.heartbeat, methods=["POST"]),
        ],
    )
    app.state.db_path = resolved
    return app


async def read_json_body(request: Request) -> dict[str, Any]:
    """Decode a JSON object body or raise ValueError (→ 400 envelope)."""
    try:
        body = await request.json()
    except Exception as exc:
        raise ValueError(f"request body is not valid JSON: {exc}") from exc
    if not isinstance(body, dict):
        raise ValueError("request body must be a JSON object")
    return body


__all__ = [
    "DEFAULT_HTTP_HOST",
    "DEFAULT_HTTP_PORT",
    "Request",
    "Response",
    "create_app",
    "error_response",
    "map_exception",
    "read_json_body",
]
