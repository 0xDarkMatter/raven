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

from raven_bus.http.app import Request, Response


async def tail(request: Request) -> Response:
    """GET /tail?channel=&after=0 → StreamingResponse(text/event-stream).

    Bad `after` → 400 envelope (before the stream starts); unknown
    channel → 404 before streaming."""
    raise NotImplementedError


__all__ = ["tail"]
