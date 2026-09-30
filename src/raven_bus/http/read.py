"""ravend read handlers.  LANE: http-read (raven2-p2).

Thin-bridge rule (ADR-005): each handler validates/decodes, opens ONE
``db.connection(request.app.state.db_path, create=False)``, calls ONE
module-contract function, encodes the result. **GET handlers perform NO
writes** — v1's GET-inbox-registers-aliases bug class is the reason this
line exists — and that includes creating the DB file: every connection
is ``create=False``, so a vanished store is a 503 ``unavailable``, never
a fresh empty file (QA http H4). Exceptions render through
``app.map_exception``.

Addresses are validated UP FRONT (pure grammar checks, no I/O) so the
same malformed channel name / consumer id is a 400 on every route: the
store's lookups turned a bad name into 404 on some routes and a bad
consumer into ``200 null`` on /cursor (QA http H10).

NOTE the one sanctioned nuance: ``pending`` maps to ``cursors.pending``,
which by module contract runs the opportunistic sweep and upserts the
consumer's presence row (ADR-005 Amendment). That is the module's write,
not the handler's — the handler still calls exactly one contract
function. ``messages`` / ``cursor`` / ``list_channels`` / ``health`` are
write-free all the way down.
"""

from __future__ import annotations

from raven_bus import __version__, channels, cursors, db, log
from raven_bus.http.app import (
    JSONResponse,
    Request,
    Response,
    map_exception,
    query_int,
    render,
    run_db,
)
from raven_bus.models import parse_consumer_id, validate_channel_name

_TRUE = {"true", "1"}
_FALSE = {"false", "0"}

# SQLite treats LIMIT -1 as "no limit" (a GET could dump a whole channel)
# — caller limits are bounded (opus verify round).
MAX_LIMIT = 1000


def _query_bool(request: Request, key: str, default: bool) -> bool:
    """Parse query param ``key`` as bool (``default`` when absent/empty).
    Accepts true/false/1/0 (case-insensitive); anything else raises
    ValueError (→ 400 envelope)."""
    raw = request.query_params.get(key)
    if raw is None or raw == "":
        return default
    lowered = raw.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ValueError(f"{key!r} must be true or false, got {raw!r}")


def _required_query(request: Request, key: str) -> str:
    """Return required query param ``key``; ValueError (→ 400) if absent/empty."""
    raw = request.query_params.get(key)
    if raw is None or raw == "":
        raise ValueError(f"missing required query parameter {key!r}")
    return raw


def _channel(request: Request) -> str:
    """The ``{name}`` path param, grammar-checked (InvalidAddressError → 400)."""
    return validate_channel_name(request.path_params["name"])


def _consumer(request: Request) -> str:
    """The required ``consumer`` query param, grammar-checked (→ 400)."""
    consumer = _required_query(request, "consumer")
    parse_consumer_id(consumer)
    return consumer


async def health(request: Request) -> Response:
    """GET /health → 200 {"status":"ok","db":str,"version":str,"schema":str}.

    A real read-only probe on every call via ``db.probe`` — never
    ``init_db``, whose process cache answered "ok" for a file deleted
    after startup and which would re-create a missing one (QA http H4).
    Missing/unopenable file → 503 ``unavailable``; not a current raven
    v2 store → 503 ``schema_mismatch``; neither creates anything."""
    try:
        path = request.app.state.db_path
        schema = await run_db(lambda: db.probe(path))
        return JSONResponse(
            {"status": "ok", "db": str(path), "version": __version__, "schema": schema}
        )
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def list_channels(request: Request) -> Response:
    """GET /channels?prefix= → 200 {"channels":[Channel…]} (model_dump
    mode='json'). Unknown prefix simply yields an empty list."""
    try:
        prefix = request.query_params.get("prefix") or None

        def work() -> JSONResponse:
            with db.connection(request.app.state.db_path, create=False) as conn:
                found = channels.list_channels(conn, prefix=prefix)
            return render(lambda: {"channels": [c.model_dump(mode="json") for c in found]})

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def messages(request: Request) -> Response:
    """GET /channels/{name}/messages?after=0&limit=100&include_expired=false
    → 200 {"messages":[Message…]} via log.read_after. `name` arrives
    percent-encoded (contains '/'). Bad name/ints/flags → 400 envelope;
    unknown channel → 404."""
    try:
        name = _channel(request)
        after = query_int(request, "after", 0)
        limit = query_int(request, "limit", 100, minimum=1, maximum=MAX_LIMIT)
        include_expired = _query_bool(request, "include_expired", False)

        def work() -> JSONResponse:
            with db.connection(request.app.state.db_path, create=False) as conn:
                msgs = log.read_after(
                    conn, name, after, limit=limit, include_expired=include_expired
                )
            return render(lambda: {"messages": [m.model_dump(mode="json") for m in msgs]})

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def pending(request: Request) -> Response:
    """GET /channels/{name}/pending?consumer=&limit=100
    → 200 {"messages":[Message…]} via cursors.pending. Missing/bad
    consumer or bad name → 400; unknown channel → 404; wrong kind → 409."""
    try:
        name = _channel(request)
        consumer = _consumer(request)
        limit = query_int(request, "limit", 100, minimum=1, maximum=MAX_LIMIT)

        def work() -> JSONResponse:
            with db.connection(request.app.state.db_path, create=False) as conn:
                msgs = cursors.pending(conn, consumer, name, limit=limit)
            return render(lambda: {"messages": [m.model_dump(mode="json") for m in msgs]})

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def cursor(request: Request) -> Response:
    """GET /channels/{name}/cursor?consumer= → 200 Cursor JSON, or
    200 with JSON null body when no cursor exists (ADR-005) — including
    for a well-formed but unknown channel (``cursors.get_cursor`` is a
    pure join). A malformed name or consumer is a 400, as everywhere."""
    try:
        name = _channel(request)
        consumer = _consumer(request)

        def work() -> JSONResponse:
            with db.connection(request.app.state.db_path, create=False) as conn:
                cur = cursors.get_cursor(conn, consumer, name)
            return render(lambda: None if cur is None else cur.model_dump(mode="json"))

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


__all__ = ["MAX_LIMIT", "cursor", "health", "list_channels", "messages", "pending"]
