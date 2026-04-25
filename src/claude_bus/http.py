"""Optional read-only HTTP bridge for claude-bus.

Phase 1 surface (read-only)::

    GET /health
    GET /inbox?role=<role>:<session>&max=<n>
    GET /message/<id>

Write endpoints (``POST /send``, ``POST /ack``) are deferred to Phase 2
— for now the write path lives on the CLI/Python API only.

The bridge is opt-in: install with ``pip install 'claude-bus[http]'``
to pull in starlette + uvicorn. Without those extras, importing this
module raises immediately.
"""

from __future__ import annotations

from pathlib import Path

try:
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "claude_bus.http requires the [http] extra. "
        "Install with: pip install 'claude-bus[http]'"
    ) from exc

from claude_bus import __version__, _core
from claude_bus.client import BusClient, _to_public
from claude_bus.db import connection
from claude_bus.exceptions import UnknownMessageError
from claude_bus.paths import resolve_db_path


def create_app(db_path: str | Path | None = None) -> Starlette:
    """Build the read-only Starlette app bound to ``db_path``.

    Pure function — no global state. ``db_path`` is captured in
    closures so the same module can be imported into multiple servers
    pointing at different DBs in tests.
    """
    resolved = resolve_db_path(db_path)

    async def health(_request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "db": str(resolved),
                "version": __version__,
            }
        )

    async def get_inbox(request: Request) -> JSONResponse:
        role_param = request.query_params.get("role")
        if not role_param or ":" not in role_param:
            return JSONResponse(
                {
                    "error": "bad_request",
                    "detail": "missing or malformed 'role' query parameter; "
                    "expected '<role>:<session>'",
                },
                status_code=400,
            )
        role, session = role_param.split(":", 1)
        if not role or not session:
            return JSONResponse(
                {
                    "error": "bad_request",
                    "detail": f"'role' query parameter has empty role or session: "
                    f"{role_param!r}",
                },
                status_code=400,
            )
        try:
            max_n = int(request.query_params.get("max", "100"))
        except ValueError:
            return JSONResponse(
                {"error": "bad_request", "detail": "'max' must be an integer"},
                status_code=400,
            )
        if max_n < 1:
            return JSONResponse(
                {"error": "bad_request", "detail": "'max' must be a positive integer"},
                status_code=400,
            )

        client = BusClient(session_id=session, role=role, db_path=resolved)
        msgs = client.inbox(max=max_n)
        return JSONResponse(
            {"messages": [m.model_dump(mode="json") for m in msgs]}
        )

    async def get_message(request: Request) -> JSONResponse:
        try:
            message_id = int(request.path_params["message_id"])
        except (KeyError, ValueError):
            return JSONResponse(
                {"error": "bad_request", "detail": "message_id must be an integer"},
                status_code=400,
            )
        try:
            with connection(resolved) as conn:
                internal = _core.read_by_id(conn, message_id)
                msg = _to_public(internal, conn)
        except UnknownMessageError:
            return JSONResponse(
                {"error": "message_not_found", "id": message_id},
                status_code=404,
            )
        return JSONResponse(msg.model_dump(mode="json"))

    return Starlette(
        debug=False,
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/inbox", get_inbox, methods=["GET"]),
            Route("/message/{message_id}", get_message, methods=["GET"]),
        ],
    )


__all__ = ["create_app"]
