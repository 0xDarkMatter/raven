"""ravend write handlers.  LANE: http-write (raven2-p2); hardened by the
orchestrator after the opus verify round (see docs/adr/ADR-005 amendment).

Thin-bridge rule (ADR-005): decode via ``app.read_json_body``, validate
STRICTLY (every handler rejects unknown body keys — a typo like
``leases_s`` silently applying a default lease was a verify finding),
run the store calls in a worker thread via ``app.run_db`` (blocking
sqlite3 on the event loop head-of-line-blocked the whole process), and
render every failure through ``map_exception``.

Two rules every handler here follows:

- **Encode inside the transaction.** The success response is built with
  ``app.render`` INSIDE the ``db.connection`` block, so an encode
  failure rolls the write back instead of committing state the caller
  never saw (QA http H2: a committed-but-unreturned /claim lease).
- **Never create the store.** Connections are ``create=False``: the
  ``raven serve`` preflight made the DB; if it vanishes, writes answer
  503 ``unavailable`` rather than writing into a fresh empty file.
"""

from __future__ import annotations

import math
from typing import Any, get_args

from raven_bus import channels, claims, consumers, cursors, db, log
from raven_bus.http.app import (
    MAX_SQLITE_INT,
    JSONResponse,
    Request,
    Response,
    map_exception,
    read_json_body,
    render,
    run_db,
)
from raven_bus.models import ChannelKind, parse_consumer_id, validate_channel_name

# Bounds for caller-supplied integers. MAX_LEASE_S caps at 30 days —
# far beyond any sane lease, small enough that datetime arithmetic can
# never overflow (10**30 seconds did, as a 500 — verify finding).
MAX_LEASE_S = 30 * 24 * 3600

# expires_in_s shares the 30-day ceiling and must be POSITIVE: zero or a
# negative value wrote a message born expired — invisible to every
# liveness read, i.e. a silent drop (QA http H13; `raven send
# --expires-in` applies the same 1..30d range).
MAX_EXPIRES_S = MAX_LEASE_S

_KINDS = get_args(ChannelKind)


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


def _str_fields(payload: dict, *keys: str) -> None:
    """Present keys among ``keys`` must be non-empty strings — JSON
    scalars like ``false`` were silently coerced downstream (re-verify
    finding: type:false stored as '0', kind:true hit an IntegrityError)."""
    for key in keys:
        if key in payload:
            value = payload[key]
            if not isinstance(value, str) or not value:
                raise ValueError(f"{key!r} must be a non-empty string")


def _finite_body(body: dict) -> None:
    """Reject NaN/Infinity anywhere in the body BEFORE the store commit —
    python json accepts them on ingest but the response encoder refuses,
    which used to commit the row and then 500 (re-verify finding).

    Iterative on purpose: the ``json.dumps(allow_nan=False)`` check this
    replaced recursed, so a body deep enough to parse could still blow
    the stack here — a RecursionError, i.e. a 500 instead of a 400 (QA
    http H2). Depth itself is the store's rule (``log.MAX_BODY_DEPTH`` →
    InvalidBodyError → 400), not re-implemented here."""
    stack: list[Any] = [body]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"'body' must contain only finite numbers, got {value!r}")


def _channel_field(payload: dict) -> str:
    """``channel`` grammar-checked up front (→ 400), like every route —
    the store's lookups made a malformed name a 404 on some (QA http H10)."""
    return validate_channel_name(payload["channel"])


def _consumer_field(payload: dict, key: str = "consumer") -> str:
    """``consumer`` (or ``sender``) grammar-checked up front (→ 400)."""
    parse_consumer_id(payload[key])
    return payload[key]


def _path_message_id(request: Request) -> int:
    """Path message ids must fit SQLite's int64 — the {message_id:int}
    converter happily matches larger digits (re-verify finding:
    OverflowError as a 500)."""
    message_id = request.path_params["message_id"]
    if not 0 <= message_id <= MAX_SQLITE_INT:
        raise ValueError(f"message_id must be between 0 and {MAX_SQLITE_INT}")
    return message_id


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
    """POST /send → 201 Message. Body mirrors log.append kwargs + an
    optional ``kind``. Unknown body keys → 400.

    ``kind`` ABSENT → ``log.append(ensure=True)``: append to the existing
    channel whatever its kind, creating an absent one as broadcast. It
    used to default to 'broadcast' and ENFORCE it, so a producer feeding
    an existing queue/stream without restating ``kind`` got 409 (QA http
    H9). ``kind`` PRESENT → validated (broadcast|queue|stream, else 400)
    and enforced: ``ensure_channel(kind)`` + ``append(ensure=False)`` —
    the one sanctioned two-call pairing (mirrors ``raven send --kind``);
    a mismatch with the stored kind → 409."""
    try:
        payload = await read_json_body(request)
        _strict(
            payload,
            {"channel", "sender", "type", "body"},
            {"urgency", "tags", "reply_to", "thread_id", "expires_in_s", "kind"},
        )
        _str_fields(payload, "channel", "sender", "type", "urgency", "kind")
        channel = _channel_field(payload)
        sender = _consumer_field(payload, "sender")
        kind = payload.get("kind")
        if kind is not None and kind not in _KINDS:
            raise ValueError(f"'kind' must be one of {list(_KINDS)}, got {kind!r}")
        if not isinstance(payload["body"], dict):
            raise ValueError("'body' must be a JSON object")
        _finite_body(payload["body"])
        tags = _tags_field(payload)
        reply_to = _int_field(payload, "reply_to", minimum=1, maximum=MAX_SQLITE_INT)
        thread_id = _int_field(payload, "thread_id", minimum=1, maximum=MAX_SQLITE_INT)
        expires_in_s = _int_field(payload, "expires_in_s", minimum=1, maximum=MAX_EXPIRES_S)

        def work() -> JSONResponse:
            with db.connection(request.app.state.db_path, create=False) as conn:
                if kind is not None:
                    channels.ensure_channel(conn, channel, kind)
                msg = log.append(
                    conn,
                    channel=channel,
                    sender=sender,
                    type=payload["type"],
                    body=payload["body"],
                    urgency=payload.get("urgency", "prompt"),
                    tags=tags,
                    reply_to=reply_to,
                    thread_id=thread_id,
                    expires_in_s=expires_in_s,
                    ensure=kind is None,
                )
                return render(lambda: msg.model_dump(mode="json"), status_code=201)

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def claim(request: Request) -> Response:
    """POST /claim {channel, consumer, lease_s?} → 200 Message, or
    **204 empty body** when the queue has no claimable message.

    The message is encoded INSIDE the claim transaction: if encoding
    fails the lease rolls back with it (500 ``internal_error``) instead
    of committing a delivery the caller never received (QA http H2)."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"channel", "consumer"}, {"lease_s"})
        _str_fields(payload, "channel", "consumer")
        channel = _channel_field(payload)
        consumer = _consumer_field(payload)
        lease_s = _int_field(
            payload, "lease_s", minimum=1, maximum=MAX_LEASE_S,
            default=claims.DEFAULT_LEASE_S,
        )

        def work() -> Response:
            with db.connection(request.app.state.db_path, create=False) as conn:
                msg = claims.claim_next(conn, consumer, channel, lease_s=lease_s)
                if msg is None:
                    return Response(status_code=204)
                return render(lambda: msg.model_dump(mode="json"))

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


def _claim_action(request: Request, payload: dict, fn):
    """Build the store closure for renew/done/release (run via run_db).
    Returns the encoded Claim (rendered inside the transaction), or None
    for release's empty result."""
    message_id = _path_message_id(request)
    consumer = _consumer_field(payload)

    def work() -> JSONResponse | None:
        with db.connection(request.app.state.db_path, create=False) as conn:
            result = fn(conn, message_id, consumer)
            return None if result is None else render(lambda: result.model_dump(mode="json"))

    return work


async def claim_renew(request: Request) -> Response:
    """POST /claims/{message_id}/renew {consumer, lease_s?} → 200 Claim."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"}, {"lease_s"})
        _str_fields(payload, "consumer")
        lease_s = _int_field(
            payload, "lease_s", minimum=1, maximum=MAX_LEASE_S,
            default=claims.DEFAULT_LEASE_S,
        )

        def renew_fn(conn, message_id, consumer):
            return claims.renew(conn, message_id, consumer, lease_s=lease_s)

        return await run_db(_claim_action(request, payload, renew_fn))
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def claim_done(request: Request) -> Response:
    """POST /claims/{message_id}/done {consumer} → 200 Claim (owner-idempotent)."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"})
        _str_fields(payload, "consumer")
        return await run_db(_claim_action(request, payload, claims.complete))
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def claim_release(request: Request) -> Response:
    """POST /claims/{message_id}/release {consumer} → 204."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"})
        _str_fields(payload, "consumer")
        await run_db(_claim_action(request, payload, claims.release))
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)
    return Response(status_code=204)


async def ack(request: Request) -> Response:
    """POST /ack {channel, consumer, up_to_id} → 200 Cursor (monotonic;
    backwards ack returns the unchanged cursor, still 200). Malformed
    channel/consumer → 400 (checked up front — QA http H10)."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"channel", "consumer", "up_to_id"})
        _str_fields(payload, "channel", "consumer")
        channel = _channel_field(payload)
        consumer = _consumer_field(payload)
        up_to_id = _int_field(payload, "up_to_id", minimum=0, maximum=MAX_SQLITE_INT)

        def work() -> JSONResponse:
            with db.connection(request.app.state.db_path, create=False) as conn:
                cur = cursors.ack(conn, consumer, channel, up_to_id)
                return render(lambda: cur.model_dump(mode="json"))

        return await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)


async def heartbeat(request: Request) -> Response:
    """POST /heartbeat {consumer} → 204 via consumers.touch — the fleet
    live-signal endpoint."""
    try:
        payload = await read_json_body(request)
        _strict(payload, {"consumer"})
        _str_fields(payload, "consumer")
        consumer = _consumer_field(payload)

        def work() -> None:
            with db.connection(request.app.state.db_path, create=False) as conn:
                consumers.touch(conn, consumer)

        await run_db(work)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure → map_exception
        return map_exception(exc)
    return Response(status_code=204)


__all__ = [
    "MAX_EXPIRES_S",
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
