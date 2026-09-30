"""ravend SSE tail.  LANE: http-sse (raven2-p2).

GET /tail?channel=&after=0 → text/event-stream. An OBSERVER (like
`raven tail`): never consumes, never mutates, may serve expired
(include_expired=True — forensic surface). Each message renders as:

    event: message
    id: <message id>
    data: <Message JSON, one line>

While idle, emit ``: ping`` comment lines (~15s) so proxies keep the
stream open. Poll via db.data_version at ~250ms (design §9 Q2): run the
full read only when the version changed. One store read per poll batch:
``channel`` given → ``log.read_after``; absent → ``log.read_all_after``.

WHY one global read for all-channel mode (QA http H5): it used to merge
per-channel ``read_after`` windows here. When one channel hit its
per-poll cap, a higher id on another channel was emitted first, so ids
went BACKWARDS across polls — and a client resuming from the max id it
had seen skipped the capped channel's remainder forever. Message ids
are global, so one ``ORDER BY id`` window is gap-free; resume state is a
single ``last_id`` in both modes.

Resume: ``Last-Event-ID`` (what a standard EventSource sends on
auto-reconnect) wins over ``?after=`` — otherwise a reconnect replayed
from the ORIGINAL ``after`` (QA http H11). Both are validated before the
stream starts (400 envelope).

Stream end: a client disconnect ends the generator quietly. So does the
tailed channel disappearing mid-stream (``raven teardown``): the events
already read are flushed, then the body closes cleanly — it used to die
with an unhandled UnknownChannelError, a truncated chunked body and a
server traceback (QA http H6). A reconnect then gets 404.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import anyio
from anyio import to_thread

from raven_bus import channels, db, log
from raven_bus.exceptions import UnknownChannelError
from raven_bus.http.app import Request, Response, map_exception, parse_int, query_int
from raven_bus.models import validate_channel_name

try:
    from starlette.responses import StreamingResponse
except ImportError as exc:  # pragma: no cover -- app.py provides the same guard
    raise ImportError(
        "raven_bus.http requires the [http] extra: pip install -e '.[http]'"
    ) from exc

POLL_INTERVAL_S = 0.25
PING_INTERVAL_S = 15.0

# One read batch. A burst larger than a batch must still drain without
# waiting for data_version to CHANGE again (verify finding) — hence the
# drain loop in _collect.
_READ_BATCH = 100

# Drain bound per poll: without it, a producer keeping >= 1 full batch
# unread livelocks the collect loop (nothing ever yields — re-verify
# finding). Hitting the bound forces an immediate re-read next tick
# instead of waiting on data_version.
_MAX_BATCHES_PER_POLL = 10


def _resume_after(request: Request) -> int:
    """Where to start: ``Last-Event-ID`` if the client sent one (an
    EventSource reconnect), else ``?after=`` (default 0; empty = default,
    as on /messages). Either must be a strict int in 0..int64 → else
    ValueError (→ 400 before streaming)."""
    last_event_id = request.headers.get("last-event-id")
    if last_event_id:
        return parse_int(last_event_id, "Last-Event-ID")
    return query_int(request, "after", 0)


async def tail(request: Request) -> Response:
    """GET /tail?channel=&after=0 → StreamingResponse(text/event-stream).

    Everything that can fail is checked BEFORE the stream starts, where
    an envelope is still possible: bad ``after``/``Last-Event-ID`` or a
    malformed channel name → 400; unknown channel → 404; missing store
    → 503 ``unavailable``; foreign store → 503 ``schema_mismatch``."""
    try:
        channel = request.query_params.get("channel") or None
        if channel is not None:
            validate_channel_name(channel)
        after = _resume_after(request)
        db_path = request.app.state.db_path

        def _preflight() -> None:
            if channel is None:
                db.probe(db_path)
                return
            with db.connection(db_path, create=False) as conn:
                channels.get_channel(conn, channel)

        # Off-loop like every other store call (re-verify finding: this
        # preflight could freeze the process behind a busy timeout).
        await to_thread.run_sync(_preflight)
    except Exception as exc:  # noqa: BLE001 -- ADR-005: every failure routes through map_exception
        return map_exception(exc)

    return StreamingResponse(
        _events(request, db_path=db_path, channel=channel, after=after),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _render(message) -> str:
    payload = json.dumps(
        message.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")
    )
    return f"event: message\nid: {message.id}\ndata: {payload}\n\n"


async def _events(
    request: Request, *, db_path, channel: str | None, after: int
) -> AsyncIterator[str]:
    last_id = after
    last_version: int | None = None
    idle_s = 0.0

    def _collect(conn, start: int) -> tuple[list, bool, bool]:
        """Blocking read pass (runs in a worker thread) from ``start``:
        up to ``_MAX_BATCHES_PER_POLL`` batches, globally id-ordered.
        Returns ``(messages, drained, gone)``. ``drained=False`` means
        more rows are already known to exist, so the caller re-reads
        WITHOUT waiting for data_version; ``gone=True`` means the tailed
        channel no longer exists (the messages read before that are
        still returned, to be flushed)."""
        collected: list = []
        cursor = start
        for _round in range(_MAX_BATCHES_PER_POLL):
            try:
                if channel is None:
                    batch = log.read_all_after(
                        conn, cursor, limit=_READ_BATCH, include_expired=True
                    )
                else:
                    batch = log.read_after(
                        conn, channel, cursor, limit=_READ_BATCH, include_expired=True
                    )
            except UnknownChannelError:
                return collected, True, True
            collected.extend(batch)
            if len(batch) < _READ_BATCH:
                return collected, True, False
            cursor = batch[-1].id
        return collected, False, False

    conn_cm = db.connection(db_path, cross_thread=True, create=False)
    try:
        # cross_thread: the connection lives on this task but every
        # blocking call (INCLUDING open/close — re-verify finding) runs
        # via to_thread; calls are awaited serially, so no two threads
        # ever touch it at once (db.connection docs).
        conn = await to_thread.run_sync(conn_cm.__enter__)
        try:
            while not await request.is_disconnected():
                version = await to_thread.run_sync(db.data_version, conn)
                if last_version is None or version != last_version:
                    last_version = version
                    messages, drained, gone = await to_thread.run_sync(
                        _collect, conn, last_id
                    )
                    if not drained:
                        # Force an immediate re-read next tick — rows we
                        # already know about must not wait for another
                        # commit to move data_version.
                        last_version = None
                    for message in messages:
                        yield _render(message)
                        last_id = message.id
                        idle_s = 0.0
                    if gone:
                        return

                await asyncio.sleep(POLL_INTERVAL_S)
                idle_s += POLL_INTERVAL_S
                if idle_s >= PING_INTERVAL_S:
                    yield ": ping\n\n"
                    idle_s = 0.0
        finally:
            # Close off-loop too; shielded so a disconnect-cancellation
            # arriving mid-close cannot leak the connection.
            with anyio.CancelScope(shield=True):
                await to_thread.run_sync(conn_cm.__exit__, None, None, None)
    except asyncio.CancelledError:
        return


__all__ = ["PING_INTERVAL_S", "POLL_INTERVAL_S", "tail"]
