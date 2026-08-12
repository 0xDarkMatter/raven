"""ravend SSE tail.  LANE: http-sse (raven2-p2).

GET /tail?channel=&after=0 → text/event-stream. An OBSERVER (like
`raven tail`): never consumes, never mutates, may serve expired
(include_expired=True — forensic surface). Each message renders as:

    event: message
    id: <message id>
    data: <Message JSON, one line>

While idle, emit ``: ping`` comment lines (~15s) so proxies keep the
stream open. Poll via db.data_version at ~250ms (design §9 Q2): run the
full read only when the version changed. `channel` given → one
log.read_after per poll (thin-bridge rule). `channel` absent → tail ALL
channels by iterating channels.list_channels and read_after per channel
each changed-poll, merging by message id — O(channels) per changed
poll; note the cost in a comment, add nothing to the log module.

Disconnect: starlette raises on write to a gone client — let the
generator exit cleanly (no error envelope mid-stream).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from anyio import to_thread

from raven_bus import channels, db, log
from raven_bus.http.app import Request, Response, map_exception

try:
    from starlette.responses import StreamingResponse
except ImportError as exc:  # pragma: no cover -- app.py provides the same guard
    raise ImportError(
        "raven_bus.http requires the [http] extra: pip install -e '.[http]'"
    ) from exc

POLL_INTERVAL_S = 0.25
PING_INTERVAL_S = 15.0

# Bounds (opus verify round): an `after` beyond int64 aborted MID-STREAM
# (headers already sent — no envelope possible), and a burst larger than
# one read_after batch stranded the remainder because delivery was gated
# on data_version CHANGING — hence the drain loop in _collect_batch.
_MAX_SQLITE_INT = 2**63 - 1
_READ_BATCH = 100


async def tail(request: Request) -> Response:
    """GET /tail?channel=&after=0 → StreamingResponse(text/event-stream).

    Bad `after` → 400 envelope (before the stream starts); unknown
    channel → 404 before streaming."""
    channel = request.query_params.get("channel") or None
    raw_after = request.query_params.get("after", "0")
    try:
        after = int(raw_after)
        if not 0 <= after <= _MAX_SQLITE_INT:
            raise ValueError(
                f"after must be between 0 and {_MAX_SQLITE_INT}"
            )
    except ValueError:
        detail = (
            f"after must be between 0 and {_MAX_SQLITE_INT}"
            if raw_after.lstrip("-").isdigit()
            else "after must be an integer"
        )
        return map_exception(ValueError(detail))

    db_path = request.app.state.db_path
    if channel is not None:
        try:
            with db.connection(db_path) as conn:
                channels.get_channel(conn, channel)
        except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure routes through map_exception
            return map_exception(exc)

    return StreamingResponse(
        _events(request, db_path=db_path, channel=channel, after=after),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _events(
    request: Request, *, db_path, channel: str | None, after: int
) -> AsyncIterator[str]:
    last_ids: dict[str, int] = {} if channel is None else {channel: after}
    last_version: int | None = None
    idle_s = 0.0

    def _collect_batch(conn) -> list:
        """Blocking read pass (runs in a worker thread): DRAINS each
        channel — read_after loops until a short batch, so a burst
        larger than one batch committed in a single txn cannot strand
        messages behind an unchanged data_version (verify finding)."""
        if channel is None:
            # Deliberately O(channels) per changed poll: the HTTP bridge
            # composes public store contracts instead of adding log APIs.
            names = [item.name for item in channels.list_channels(conn)]
        else:
            names = [channel]

        collected = []
        for name in names:
            while True:
                channel_after = last_ids.setdefault(name, after)
                batch = log.read_after(
                    conn, name, channel_after,
                    limit=_READ_BATCH, include_expired=True,
                )
                collected.extend(batch)
                if batch:
                    last_ids[name] = batch[-1].id
                if len(batch) < _READ_BATCH:
                    break
        return sorted(collected, key=lambda item: item.id)

    try:
        # cross_thread: the connection lives on this task but every
        # blocking call runs via to_thread; calls are awaited serially,
        # so no two threads ever touch it at once (db.connection docs).
        with db.connection(db_path, cross_thread=True) as conn:
            while not await request.is_disconnected():
                version = await to_thread.run_sync(db.data_version, conn)
                if last_version is None or version != last_version:
                    last_version = version
                    messages = await to_thread.run_sync(_collect_batch, conn)

                    for message in messages:
                        payload = json.dumps(
                            message.model_dump(mode="json"),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        yield (
                            f"event: message\n"
                            f"id: {message.id}\n"
                            f"data: {payload}\n\n"
                        )
                        idle_s = 0.0

                await asyncio.sleep(POLL_INTERVAL_S)
                idle_s += POLL_INTERVAL_S
                if idle_s >= PING_INTERVAL_S:
                    yield ": ping\n\n"
                    idle_s = 0.0
    except asyncio.CancelledError:
        return


__all__ = ["PING_INTERVAL_S", "POLL_INTERVAL_S", "tail"]
