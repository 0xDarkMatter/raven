"""ravend write handlers.  LANE: http-write (raven2-p2).

Thin-bridge rule (ADR-005): decode via ``app.read_json_body``, open ONE
``db.connection(request.app.state.db_path)``, call ONE module-contract
function, encode. Exceptions render through ``app.map_exception`` —
handlers never hand-craft error JSON.
"""

from __future__ import annotations

from raven_bus.http.app import Request, Response


async def send(request: Request) -> Response:
    """POST /send → 201 Message. Body mirrors log.append kwargs:
    {channel, sender, type, body, urgency?, tags?, reply_to?,
    thread_id?, expires_in_s?, kind?} — `kind` (default 'broadcast')
    feeds ensure_channel semantics exactly like the CLI's --kind.
    Unknown body keys → 400 (strict: catches caller typos)."""
    raise NotImplementedError


async def claim(request: Request) -> Response:
    """POST /claim {channel, consumer, lease_s?} → 200 Message, or
    **204 empty body** when the queue has no claimable message
    (ADR-005 — a JSON null would force every poller to parse)."""
    raise NotImplementedError


async def claim_renew(request: Request) -> Response:
    """POST /claims/{message_id}/renew {consumer, lease_s?} → 200 Claim.
    ClaimDeniedError → 409 envelope."""
    raise NotImplementedError


async def claim_done(request: Request) -> Response:
    """POST /claims/{message_id}/done {consumer} → 200 Claim (idempotent
    for the owner, per claims.complete contract)."""
    raise NotImplementedError


async def claim_release(request: Request) -> Response:
    """POST /claims/{message_id}/release {consumer} → 204."""
    raise NotImplementedError


async def ack(request: Request) -> Response:
    """POST /ack {channel, consumer, up_to_id} → 200 Cursor (monotonic;
    backwards ack returns the unchanged cursor, still 200)."""
    raise NotImplementedError


async def heartbeat(request: Request) -> Response:
    """POST /heartbeat {consumer} → 204. Upserts the consumer row and
    bumps last_seen_at — the fleet live-signal endpoint. Validate the
    consumer id (400 on bad grammar)."""
    raise NotImplementedError


__all__ = [
    "ack",
    "claim",
    "claim_done",
    "claim_release",
    "claim_renew",
    "heartbeat",
    "send",
]
