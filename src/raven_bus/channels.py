"""Channel registry.  LANE: log (raven2-p1).

Channels are created explicitly or on first send (``ensure_channel``).
``kind`` is immutable after creation — re-ensuring with a different kind
raises :class:`WrongChannelKindError` rather than silently mutating
semantics other consumers rely on.
"""

from __future__ import annotations

import sqlite3

from raven_bus.exceptions import UnknownChannelError, WrongChannelKindError
from raven_bus.models import Channel, ChannelKind, validate_channel_name

_SELECT = (
    "SELECT id, name, kind, retention_s, max_deliveries, created_at FROM channels"
)


def _row_to_channel(row: sqlite3.Row) -> Channel:
    return Channel(
        id=row["id"],
        name=row["name"],
        kind=row["kind"],
        retention_s=row["retention_s"],
        max_deliveries=row["max_deliveries"],
        created_at=row["created_at"],
    )


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
    validate_channel_name(name)
    row = conn.execute(f"{_SELECT} WHERE name = ?", (name,)).fetchone()
    if row is not None:
        if row["kind"] != kind:
            raise WrongChannelKindError(
                f"channel {name!r} is kind {row['kind']!r}, requested {kind!r}"
            )
        return _row_to_channel(row)
    cur = conn.execute(
        "INSERT INTO channels (name, kind, retention_s, max_deliveries) "
        "VALUES (?, ?, ?, ?)",
        (name, kind, retention_s, max_deliveries),
    )
    row = conn.execute(f"{_SELECT} WHERE id = ?", (cur.lastrowid,)).fetchone()
    return _row_to_channel(row)


def get_channel(conn: sqlite3.Connection, name: str) -> Channel:
    """Return the channel or raise :class:`UnknownChannelError`."""
    row = conn.execute(f"{_SELECT} WHERE name = ?", (name,)).fetchone()
    if row is None:
        raise UnknownChannelError(f"channel {name!r} does not exist")
    return _row_to_channel(row)


def list_channels(
    conn: sqlite3.Connection, *, prefix: str | None = None
) -> list[Channel]:
    """All channels, name-ordered; ``prefix`` filters by name prefix
    (e.g. ``run/v0-2/``)."""
    if prefix:
        # Escape LIKE wildcards so a literal '%' or '_' in a channel
        # segment can't widen the prefix match.
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        rows = conn.execute(
            f"{_SELECT} WHERE name LIKE ? ESCAPE '\\' ORDER BY name",
            (escaped + "%",),
        ).fetchall()
    else:
        rows = conn.execute(f"{_SELECT} ORDER BY name").fetchall()
    return [_row_to_channel(row) for row in rows]


__all__ = ["ensure_channel", "get_channel", "list_channels"]
