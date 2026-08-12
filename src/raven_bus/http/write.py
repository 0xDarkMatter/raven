"""ravend write handlers.  LANE: http-write (raven2-p2); hardened by the
orchestrator after the opus verify round (see docs/adr/ADR-005 amendment).

Thin-bridge rule (ADR-005): decode via ``app.read_json_body``, validate
STRICTLY (every handler rejects unknown body keys — a typo like
``leases_s`` silently applying a default lease was a verify finding),
run the store calls in a worker thread via ``app.run_db`` (blocking
sqlite3 on the event loop head-of-line-blocked the whole process), and
render every failure through ``map_exception``.
"""

from __future__ import annotations

from typing import Any

from starlette.responses import JSONResponse

from raven_bus import channels, claims, consumers, cursors, db, log
from raven_bus.http.app import (
    Request,
    Response,
    map_exception,
    read_json_body,
    run_db,
)

# Bounds for caller-supplied integers. MAX_LEASE_S caps at 30 days —
# far beyond any sane lease, small enough that datetime arithmetic can
# never overflow (10**30 seconds did, as a 500 — verify finding).
MAX_LEASE_S = 30 * 24 * 3600
MAX_SQLITE_INT = 2**63 - 1


def _strict(
    payload: dict, required: frozenset[str] | set[str],
    optional: frozenset[str] | set[str] = frozenset(),
) -> None:
    """Reject missing required and ANY unknown keys (400 envelope)."""
    missing = required - set(payload)
    if missing:
        raise ValueError(f"missing required field(s): {sorted(missing)}")
    unknown = set(payload) - required - optional
    if unknown:
        raise ValueError(f"unknown field(s): {sorted(unknown)}")


def _int_field(
    payload: dict, key: str, *, minimum: int, maximum: int, default: int | None = None
) -> int | None:
    """Bounded-int body field: bool/str/float/out-of-range → 400."""
    if key not in payload:
        return default
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key!r} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{key!r} must be between {minimum} and {maximum}")
    return value


def _tags_field(payload: dict) -> list[str] | None:
    """``tags`` must be a list of strings — a bare string would be
    iterated character-wise by the store (verify finding)."""
    if "tags" not in payload or payload["tags"] is None:
        return None
    tags = payload["tags"]
    if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
        raise ValueError("'tags' must be a list of strings")
    return tags


async def send(request: Request) -> Response:
    """POST /send → 201 Message. Body mirrors log.append kwargs + `kind`
    (default 'broadcast'). Unknown body keys → 400."""
    try:
        payload = await read_json_body(request)
        _strict(
            payload,
            {"channel", "sender", "type", "body"},
            {"urgency", "tags", "reply_to", "thread_id", "expires_in_s", "kind"},
        )
        if not isinstance(payload["body"], dict):
            raise ValueError("'body' must be a JSON object")
        tags = _tags_field(payload)
        reply_to = _int_field(payload, "reply_to", minimum=1, maximum=MAX_SQLITE_INT)
        thread_id = _int_field(payload, "thread_id", minimum=1, maximum=MAX_SQLITE_INT)
        expires_in_s = _int_field(
            payload, "expires_in_s", minimum=-MAX_LEASE_S, maximum=MAX_LEASE_S
        )

        def work() -> dict[str, Any]:
            with db.connection(request.app.state.db_path) as conn:
                # Sanctioned pairing (mirrors `raven send`, cli/send.py):
                # ensure_channel establishes/validates the channel kind,
                # then append(ensure=False) writes the message — two
                # store contract calls, zero logic between them.
                channels.ensure_channel(
                    conn, payload["channel"], payload.get("kind", "broadcast")
                )
                return log.append(
                    conn,
                    channel=payload["channel"],
                    sender=payload["sender"],
                    type=payload["type"],
                    body=payload["body"],
                    urgency=payload.get("urgency", "prompt"),
                    tags=tags,
                    reply_to=reply_to,
                    thread_id=thread_id,
                    expires_in_s=expires_in_s,
                    ensure=False,
                ).model_dump(mode="json")

        dumped = await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(dumped, status_code=201)


async def claim(request: Request) -> Response:
    """POST /claim {channel, consumer, lease_s?} → 200 Message, or
    **204 empty body** when the queue has no claimable message."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"channel", "consumer"}, {"lease_s"})
        lease_s = _int_field(
            payload, "lease_s", minimum=1, maximum=MAX_LEASE_S,
            default=claims.DEFAULT_LEASE_S,
        )

        def work() -> dict[str, Any] | None:
            with db.connection(request.app.state.db_path) as conn:
                msg = claims.claim_next(
                    conn, payload["consumer"], payload["channel"], lease_s=lease_s
                )
            return None if msg is None else msg.model_dump(mode="json")

        dumped = await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    if dumped is None:
        return Response(status_code=204)
    return JSONResponse(dumped, status_code=200)


def _claim_action(request: Request, payload: dict, fn):
    """Build the store closure for renew/done/release (run via run_db)."""
    message_id = request.path_params["message_id"]

    def work() -> dict[str, Any] | None:
        with db.connection(request.app.state.db_path) as conn:
            result = fn(conn, message_id, payload["consumer"])
        return None if result is None else result.model_dump(mode="json")

    return work


async def claim_renew(request: Request) -> Response:
    """POST /claims/{message_id}/renew {consumer, lease_s?} → 200 Claim."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"}, {"lease_s"})
        lease_s = _int_field(
            payload, "lease_s", minimum=1, maximum=MAX_LEASE_S,
            default=claims.DEFAULT_LEASE_S,
        )

        def renew_fn(conn, message_id, consumer):
            return claims.renew(conn, message_id, consumer, lease_s=lease_s)

        dumped = await run_db(_claim_action(request, payload, renew_fn))
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(dumped, status_code=200)


async def claim_done(request: Request) -> Response:
    """POST /claims/{message_id}/done {consumer} → 200 Claim (owner-idempotent)."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"})
        dumped = await run_db(_claim_action(request, payload, claims.complete))
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(dumped, status_code=200)


async def claim_release(request: Request) -> Response:
    """POST /claims/{message_id}/release {consumer} → 204."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"})
        await run_db(_claim_action(request, payload, claims.release))
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return Response(status_code=204)


async def ack(request: Request) -> Response:
    """POST /ack {channel, consumer, up_to_id} → 200 Cursor (monotonic;
    backwards ack returns the unchanged cursor, still 200)."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"channel", "consumer", "up_to_id"})
        up_to_id = _int_field(payload, "up_to_id", minimum=0, maximum=MAX_SQLITE_INT)

        def work() -> dict[str, Any]:
            with db.connection(request.app.state.db_path) as conn:
                return cursors.ack(
                    conn, payload["consumer"], payload["channel"], up_to_id
                ).model_dump(mode="json")

        dumped = await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return JSONResponse(dumped, status_code=200)


async def heartbeat(request: Request) -> Response:
    """POST /heartbeat {consumer} → 204 via consumers.touch — the fleet
    live-signal endpoint."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"})

        def work() -> None:
            with db.connection(request.app.state.db_path) as conn:
                consumers.touch(conn, payload["consumer"])

        await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- map_exception re-raises anything it does not recognize
        return map_exception(exc)
    return Response(status_code=204)


__all__ = [
    "MAX_LEASE_S",
    "MAX_SQLITE_INT",
    "ack",
    "claim",
    "claim_done",
    "claim_release",
    "claim_renew",
    "heartbeat",
    "send",
]
