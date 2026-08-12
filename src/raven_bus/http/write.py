"""ravend write handlers.  LANE: http-write (raven2-p2).

Thin-bridge rule (ADR-005): decode via ``app.read_json_body``, open ONE
``db.connection(request.app.state.db_path)``, call ONE module-contract
function, encode. Exceptions render through ``app.map_exception`` —
handlers never hand-craft error JSON.
"""

from __future__ import annotations

from starlette.responses import JSONResponse

from raven_bus import channels, claims, cursors, db, log, models
from raven_bus.http.app import Request, Response, map_exception, read_json_body

_NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def _require(payload: dict, *keys: str) -> None:
    missing = [k for k in keys if k not in payload]
    if missing:
        raise ValueError(f"missing required field(s): {sorted(missing)}")


async def send(request: Request) -> Response:
    """POST /send → 201 Message. Body mirrors log.append kwargs:
    {channel, sender, type, body, urgency?, tags?, reply_to?,
    thread_id?, expires_in_s?, kind?} — `kind` (default 'broadcast')
    feeds ensure_channel semantics exactly like the CLI's --kind.
    Unknown body keys → 400 (strict: catches caller typos)."""
    try:
        payload = await read_json_body(request)
        allowed = {
            "channel",
            "sender",
            "type",
            "body",
            "urgency",
            "tags",
            "reply_to",
            "thread_id",
            "expires_in_s",
            "kind",
        }
        unknown = set(payload) - allowed
        if unknown:
            raise ValueError(f"unknown field(s): {sorted(unknown)}")
        _require(payload, "channel", "sender", "type", "body")
        with db.connection(request.app.state.db_path) as conn:
            # Sanctioned pairing (mirrors `raven send`, cli/send.py):
            # ensure_channel establishes/validates the channel kind,
            # then append(ensure=False) writes the message — two store
            # contract calls, zero logic between them.
            channels.ensure_channel(conn, payload["channel"], payload.get("kind", "broadcast"))
            msg = log.append(
                conn,
                channel=payload["channel"],
                sender=payload["sender"],
                type=payload["type"],
                body=payload["body"],
                urgency=payload.get("urgency", "prompt"),
                tags=payload.get("tags"),
                reply_to=payload.get("reply_to"),
                thread_id=payload.get("thread_id"),
                expires_in_s=payload.get("expires_in_s"),
                ensure=False,
            )
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(msg.model_dump(mode="json"), status_code=201)


async def claim(request: Request) -> Response:
    """POST /claim {channel, consumer, lease_s?} → 200 Message, or
    **204 empty body** when the queue has no claimable message
    (ADR-005 — a JSON null would force every poller to parse)."""
    try:
        payload = await read_json_body(request)
        _require(payload, "channel", "consumer")
        kwargs = {}
        if "lease_s" in payload:
            kwargs["lease_s"] = payload["lease_s"]
        with db.connection(request.app.state.db_path) as conn:
            msg = claims.claim_next(conn, payload["consumer"], payload["channel"], **kwargs)
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    if msg is None:
        return Response(status_code=204)
    return JSONResponse(msg.model_dump(mode="json"), status_code=200)


async def claim_renew(request: Request) -> Response:
    """POST /claims/{message_id}/renew {consumer, lease_s?} → 200 Claim.
    ClaimDeniedError → 409 envelope."""
    try:
        payload = await read_json_body(request)
        _require(payload, "consumer")
        kwargs = {}
        if "lease_s" in payload:
            kwargs["lease_s"] = payload["lease_s"]
        message_id = request.path_params["message_id"]
        with db.connection(request.app.state.db_path) as conn:
            result = claims.renew(conn, message_id, payload["consumer"], **kwargs)
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(result.model_dump(mode="json"), status_code=200)


async def claim_done(request: Request) -> Response:
    """POST /claims/{message_id}/done {consumer} → 200 Claim (idempotent
    for the owner, per claims.complete contract)."""
    try:
        payload = await read_json_body(request)
        _require(payload, "consumer")
        message_id = request.path_params["message_id"]
        with db.connection(request.app.state.db_path) as conn:
            result = claims.complete(conn, message_id, payload["consumer"])
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(result.model_dump(mode="json"), status_code=200)


async def claim_release(request: Request) -> Response:
    """POST /claims/{message_id}/release {consumer} → 204."""
    try:
        payload = await read_json_body(request)
        _require(payload, "consumer")
        message_id = request.path_params["message_id"]
        with db.connection(request.app.state.db_path) as conn:
            claims.release(conn, message_id, payload["consumer"])
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return Response(status_code=204)


async def ack(request: Request) -> Response:
    """POST /ack {channel, consumer, up_to_id} → 200 Cursor (monotonic;
    backwards ack returns the unchanged cursor, still 200)."""
    try:
        payload = await read_json_body(request)
        _require(payload, "channel", "consumer", "up_to_id")
        with db.connection(request.app.state.db_path) as conn:
            result = cursors.ack(
                conn, payload["consumer"], payload["channel"], payload["up_to_id"]
            )
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(result.model_dump(mode="json"), status_code=200)


async def heartbeat(request: Request) -> Response:
    """POST /heartbeat {consumer} → 204. Upserts the consumer row and
    bumps last_seen_at — the fleet live-signal endpoint. Validate the
    consumer id (400 on bad grammar)."""
    try:
        payload = await read_json_body(request)
        _require(payload, "consumer")
        consumer = payload["consumer"]
        role, run = models.parse_consumer_id(consumer)
        with db.connection(request.app.state.db_path) as conn:
            # No store-contract function owns "upsert a consumer" in
            # isolation; mirrors cursors._upsert_consumer's raw-SQL
            # shape (module docstring sanctions this for heartbeat).
            conn.execute(
                f"""
                INSERT INTO consumers (id, role, run, last_seen_at)
                VALUES (?, ?, ?, {_NOW_SQL})
                ON CONFLICT(id) DO UPDATE SET last_seen_at = {_NOW_SQL}
                """,
                (consumer, role, run),
            )
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return Response(status_code=204)


__all__ = [
    "ack",
    "claim",
    "claim_done",
    "claim_release",
    "claim_renew",
    "heartbeat",
    "send",
]
