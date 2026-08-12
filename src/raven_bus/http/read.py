"""ravend read handlers.  LANE: http-read (raven2-p2).

Thin-bridge rule (ADR-005): each handler validates/decodes, opens ONE
``db.connection(request.app.state.db_path)``, calls ONE module-contract
function, encodes the result. **GET handlers perform NO writes** —
v1's GET-inbox-registers-aliases bug class is the reason this line
exists. Exceptions render through ``app.map_exception``.

NOTE the one sanctioned nuance: ``pending`` maps to ``cursors.pending``,
which by module contract runs the opportunistic sweep and bumps the
consumer's last_seen_at. That is the module's write, not the handler's —
the handler still calls exactly one contract function. ``messages`` /
``cursor`` / ``list_channels`` / ``health`` must be write-free all the
way down (read_after / get_cursor / list_channels are).
"""

from __future__ import annotations

from raven_bus import __version__, channels, cursors, db, log
from raven_bus.http.app import JSONResponse, Request, Response, map_exception

_TRUE = {"true", "1"}
_FALSE = {"false", "0"}


def _query_int(request: Request, key: str, default: int) -> int:
    """Parse query param ``key`` as int (``default`` when absent/empty).
    A present-but-non-int value raises ValueError (→ 400 envelope)."""
    raw = request.query_params.get(key)
    if raw is None or raw == "":
        return default
    return int(raw)  # ValueError propagates → map_exception → 400


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


async def health(request: Request) -> Response:
    """GET /health → 200 {"status":"ok","db":str,"version":str,"schema":str}.

    Probes the DB (init if missing via db.init_db — the ONE deliberate
    exception to write-free reads, mirroring `raven doctor`; document in
    the response if the DB was just created is NOT required)."""
    try:
        db.init_db(request.app.state.db_path)
        return JSONResponse(
            {
                "status": "ok",
                "db": str(request.app.state.db_path),
                "version": __version__,
                "schema": db.SCHEMA_VERSION,
            }
        )
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def list_channels(request: Request) -> Response:
    """GET /channels?prefix= → 200 {"channels":[Channel…]} (model_dump
    mode='json'). Unknown prefix simply yields an empty list."""
    try:
        prefix = request.query_params.get("prefix") or None
        with db.connection(request.app.state.db_path) as conn:
            found = channels.list_channels(conn, prefix=prefix)
        return JSONResponse({"channels": [c.model_dump(mode="json") for c in found]})
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def messages(request: Request) -> Response:
    """GET /channels/{name}/messages?after=0&limit=100&include_expired=false
    → 200 {"messages":[Message…]} via log.read_after. `name` arrives
    percent-encoded (contains '/'). Bad ints/flags → 400 envelope;
    unknown channel → 404."""
    try:
        name = request.path_params["name"]
        after = _query_int(request, "after", 0)
        limit = _query_int(request, "limit", 100)
        include_expired = _query_bool(request, "include_expired", False)
        with db.connection(request.app.state.db_path) as conn:
            msgs = log.read_after(conn, name, after, limit=limit, include_expired=include_expired)
        return JSONResponse({"messages": [m.model_dump(mode="json") for m in msgs]})
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def pending(request: Request) -> Response:
    """GET /channels/{name}/pending?consumer=&limit=100
    → 200 {"messages":[Message…]} via cursors.pending. Missing consumer
    → 400; wrong kind → 409."""
    try:
        name = request.path_params["name"]
        consumer = _required_query(request, "consumer")
        limit = _query_int(request, "limit", 100)
        with db.connection(request.app.state.db_path) as conn:
            msgs = cursors.pending(conn, consumer, name, limit=limit)
        return JSONResponse({"messages": [m.model_dump(mode="json") for m in msgs]})
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def cursor(request: Request) -> Response:
    """GET /channels/{name}/cursor?consumer= → 200 Cursor JSON, or
    200 with JSON null body when no cursor exists (ADR-005)."""
    try:
        name = request.path_params["name"]
        consumer = _required_query(request, "consumer")
        with db.connection(request.app.state.db_path) as conn:
            cur = cursors.get_cursor(conn, consumer, name)
        return JSONResponse(cur.model_dump(mode="json") if cur is not None else None)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


__all__ = ["cursor", "health", "list_channels", "messages", "pending"]
