"""ravend application factory + the shared error/codec layer (ADR-005).

The ROUTE TABLE in :func:`create_app` IS ADR-005's endpoint table and is
frozen (append-only in spirit — breaking a route supersedes the ADR).
Everything else here is what every handler shares: the error envelope
and exception→status map (so failures render identically), the strict
integer codec, and :func:`render` (encode-inside-the-transaction for
write handlers). Handlers live in read.py / write.py / sse.py.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

try:
    from anyio import to_thread
    from starlette.applications import Starlette
    from starlette.exceptions import HTTPException
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route
except ImportError as exc:  # pragma: no cover -- exercised only without the extra
    raise ImportError(
        "raven_bus.http requires the [http] extra: pip install -e '.[http]'"
    ) from exc

from raven_bus import db
from raven_bus.exceptions import (
    ClaimDeniedError,
    InvalidAddressError,
    InvalidBodyError,
    RavenBusError,
    SchemaMismatchError,
    StoreUnavailableError,
    TeardownBlockedError,
    UnknownChannelError,
    UnknownMessageError,
    WrongChannelKindError,
)
from raven_bus.paths import resolve_db_path

_log = logging.getLogger("raven_bus.http")

DEFAULT_HTTP_HOST = "127.0.0.1"
"""Loopback only — no auth in-process (ADR-005)."""

DEFAULT_HTTP_PORT = 7713
"""Unclaimed in the machine port registry as of 2026-08-12 (ADR-005)."""


def error_response(code: str, detail: str, status: int) -> JSONResponse:
    """The one error envelope (ADR-005): {"error": code, "detail": human}."""
    return JSONResponse({"error": code, "detail": detail}, status_code=status)


class ResponseEncodingError(RuntimeError):
    """A result could not be rendered as JSON — e.g. a legacy body nested
    past pydantic's serialiser depth (rows written before
    ``log.MAX_BODY_DEPTH`` existed). A server-side fault: 500
    ``internal_error``, never the caller's 400 (pydantic raises a plain
    ValueError here, which the 400 branch would otherwise claim)."""


def render(build: Callable[[], Any], status_code: int = 200) -> JSONResponse:
    """Build + JSON-encode a success body; any encode failure becomes
    :class:`ResponseEncodingError`.

    Write handlers call this INSIDE their ``db.connection`` block, so a
    failure rolls the transaction back. WHY (QA http H2): /claim used to
    commit the lease and THEN fail to encode the message — the caller got
    an error while a lease it never saw ran out and dead-lettered the
    message unseen. Encoding inside the transaction means no committed
    state outlives a failed response."""
    try:
        return JSONResponse(build(), status_code=status_code)
    except (ValueError, RecursionError) as exc:
        raise ResponseEncodingError(f"response could not be encoded as JSON: {exc}") from exc


# sqlite's wording when a query names a table/column the file lacks, or
# the file is not SQLite at all: whatever sits at the path is not raven's
# v2 store (replaced, or re-created empty). Message-matched because sqlite
# gives these no distinct result code (SQLITE_ERROR / SQLITE_NOTADB).
_FOREIGN_STORE_MARKERS = ("no such table", "no such column", "file is not a database")


def _is_foreign_store(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.DatabaseError) and str(exc).startswith(
        _FOREIGN_STORE_MARKERS
    )


def map_exception(exc: Exception) -> JSONResponse:
    """Exception → envelope. Every handler's failure path ends here.

    - 400 ``bad_request``: invalid input (address grammar, body, ints).
    - 404 ``not_found``: unknown channel / message.
    - 409 ``conflict``: claim denied, wrong channel kind, teardown blocked.
    - 503 ``busy``: the SQLite lock outlasted the busy timeout — retry.
    - 503 ``unavailable``: the store file is missing/unopenable (ravend
      never re-creates it — ``db.connection(create=False)``).
    - 503 ``schema_mismatch``: the file is not a current raven v2 store.
    - 500 ``error``: any other raven-shaped error.
    - 500 ``internal_error``: anything unexpected. The traceback goes to
      the ``raven_bus.http`` logger, NEVER into ``detail``.

    Unknown exceptions used to be re-raised, which Starlette rendered as
    a plain-text 500 with no envelope (QA http H3)."""
    if isinstance(exc, ResponseEncodingError):
        return _internal(exc)
    if isinstance(exc, InvalidAddressError | InvalidBodyError | ValueError):
        return error_response("bad_request", str(exc), 400)
    if isinstance(exc, UnknownChannelError | UnknownMessageError):
        return error_response("not_found", str(exc), 404)
    if isinstance(exc, ClaimDeniedError | WrongChannelKindError | TeardownBlockedError):
        return error_response("conflict", str(exc), 409)
    if db.is_busy_error(exc):
        return error_response(
            "busy", "the store is locked by another writer; retry shortly", 503
        )
    if isinstance(exc, StoreUnavailableError):
        return error_response("unavailable", str(exc), 503)
    if isinstance(exc, SchemaMismatchError):
        return error_response("schema_mismatch", str(exc), 503)
    if _is_foreign_store(exc):
        return error_response(
            "schema_mismatch", f"the DB file is not a raven v2 store ({exc})", 503
        )
    if isinstance(exc, RavenBusError):  # pragma: no cover -- future subclasses
        return error_response("error", str(exc), 500)
    return _internal(exc)


def _internal(exc: Exception) -> JSONResponse:
    """500 ``internal_error``: log the traceback, return a fixed detail."""
    _log.error("ravend: unhandled %s", type(exc).__name__, exc_info=exc)
    return error_response(
        "internal_error", "internal server error (details in the ravend log)", 500
    )


# --------------------------------------------------------------------------- #
# Strict integer codec — query params and the SSE Last-Event-ID header.
# --------------------------------------------------------------------------- #
MAX_SQLITE_INT = 2**63 - 1

# ASCII digits with an optional leading '-' only. int() alone accepted
# '1_0' (as 10), '+1', ' 1' and non-ASCII decimal digits (QA http H13).
_INT_RE = re.compile(r"-?[0-9]+")


def parse_int(
    raw: str, what: str, *, minimum: int = 0, maximum: int = MAX_SQLITE_INT
) -> int:
    """Parse ``raw`` strictly as a bounded decimal int; ValueError (→ 400)
    naming ``what`` otherwise."""
    if not _INT_RE.fullmatch(raw):
        raise ValueError(f"{what!r} must be an integer")
    # More than 19 significant digits is beyond int64 whatever the sign;
    # checked before int(), which raises its own ValueError past 4300.
    if len(raw.lstrip("-").lstrip("0")) > 19 or not minimum <= int(raw) <= maximum:
        raise ValueError(f"{what!r} must be between {minimum} and {maximum}")
    return int(raw)


def query_int(
    request: Request, key: str, default: int, *,
    minimum: int = 0, maximum: int = MAX_SQLITE_INT,
) -> int:
    """Query param ``key`` via :func:`parse_int`; ``default`` when absent
    or empty — ``?after=`` means "the default" on every route (QA H13)."""
    raw = request.query_params.get(key)
    if raw is None or raw == "":
        return default
    return parse_int(raw, key, minimum=minimum, maximum=maximum)


async def run_db(fn):
    """Run a blocking store closure in a worker thread.

    Every handler's sqlite3 work goes through this: blocking I/O
    directly in an async handler serializes the WHOLE process behind
    one busy-timeout (a held write lock froze /channels for 5.46s in
    the verify round — including SSE streams)."""
    return await to_thread.run_sync(fn)


async def _http_exception(request: Request, exc: HTTPException) -> JSONResponse:
    """Router-level errors (404 no-route, 405 method) wear the same
    envelope as handler errors — plain-text bodies from the router were
    a verify finding. exc.headers pass through (405's Allow header —
    re-verify finding)."""
    code = "not_found" if exc.status_code == 404 else "error"
    if exc.status_code == 405:
        code = "method_not_allowed"
    response = error_response(code, exc.detail or "", exc.status_code)
    for key, value in (exc.headers or {}).items():
        response.headers[key] = value
    return response


def create_app(db_path: str | Path | None = None) -> Starlette:
    """Build the ravend app bound to ``db_path``. Pure function — the
    resolved path is captured in ``app.state.db_path``; handlers read it
    from ``request.app.state`` so tests can point one process at many
    DBs."""
    from raven_bus.http import read, sse, write

    resolved = resolve_db_path(db_path)
    app = Starlette(
        debug=False,
        exception_handlers={HTTPException: _http_exception},
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
        # ValueError, not TypeError: map_exception routes ValueError to the
        # 400 envelope — a TypeError would surface as a 500.
        raise ValueError("request body must be a JSON object")  # noqa: TRY004
    return body


__all__ = [
    "DEFAULT_HTTP_HOST",
    "DEFAULT_HTTP_PORT",
    "MAX_SQLITE_INT",
    "Request",
    "Response",
    "ResponseEncodingError",
    "create_app",
    "error_response",
    "map_exception",
    "parse_int",
    "query_int",
    "read_json_body",
    "render",
    "run_db",
]
