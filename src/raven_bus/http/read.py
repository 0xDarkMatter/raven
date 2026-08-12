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

from raven_bus.http.app import Request, Response


async def health(request: Request) -> Response:
    """GET /health → 200 {"status":"ok","db":str,"version":str,"schema":str}.

    Probes the DB (init if missing via db.init_db — the ONE deliberate
    exception to write-free reads, mirroring `raven doctor`; document in
    the response if the DB was just created is NOT required)."""
    raise NotImplementedError


async def list_channels(request: Request) -> Response:
    """GET /channels?prefix= → 200 {"channels":[Channel…]} (model_dump
    mode='json'). Unknown prefix simply yields an empty list."""
    raise NotImplementedError


async def messages(request: Request) -> Response:
    """GET /channels/{name}/messages?after=0&limit=100&include_expired=false
    → 200 {"messages":[Message…]} via log.read_after. `name` arrives
    percent-encoded (contains '/'). Bad ints/flags → 400 envelope;
    unknown channel → 404."""
    raise NotImplementedError


async def pending(request: Request) -> Response:
    """GET /channels/{name}/pending?consumer=&limit=100
    → 200 {"messages":[Message…]} via cursors.pending. Missing consumer
    → 400; wrong kind → 409."""
    raise NotImplementedError


async def cursor(request: Request) -> Response:
    """GET /channels/{name}/cursor?consumer= → 200 Cursor JSON, or
    200 with JSON null body when no cursor exists (ADR-005)."""
    raise NotImplementedError


__all__ = ["cursor", "health", "list_channels", "messages", "pending"]
