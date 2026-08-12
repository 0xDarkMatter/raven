"""Channel registry.  LANE: log (raven2-p1).

Channels are created explicitly or on first send (``ensure_channel``).
``kind`` is immutable after creation — re-ensuring with a different kind
raises :class:`WrongChannelKindError` rather than silently mutating
semantics other consumers rely on.
"""

from __future__ import annotations

import sqlite3

from raven_bus.models import Channel, ChannelKind


def ensure_channel(
    conn: sqlite3.Connection,
    name: str,
    kind: ChannelKind = "broadcast",
    *,
    retention_s: int | None = None,
    max_deliveries: int = 3,
) -> Channel:
    """Get-or-create ``name``. Validates the name (models grammar).
    Existing channel: returns it unchanged; raises
    :class:`WrongChannelKindError` if ``kind`` differs from the stored
    kind. Never updates retention/max_deliveries on an existing row."""
    raise NotImplementedError


def get_channel(conn: sqlite3.Connection, name: str) -> Channel:
    """Return the channel or raise :class:`UnknownChannelError`."""
    raise NotImplementedError


def list_channels(
    conn: sqlite3.Connection, *, prefix: str | None = None
) -> list[Channel]:
    """All channels, name-ordered; ``prefix`` filters by name prefix
    (e.g. ``run/v0-2/``)."""
    raise NotImplementedError


__all__ = ["ensure_channel", "get_channel", "list_channels"]
