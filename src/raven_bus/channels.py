"""Channel registry.  LANE: log (raven2-p1).

Channels are created explicitly or on first send (``ensure_channel``).
``kind`` is immutable after creation — re-ensuring with a different kind
raises :class:`WrongChannelKindError` rather than silently mutating
semantics other consumers rely on.

Concurrency: ``ensure_channel`` is safe under concurrent first use of a
new name (many processes' first `raven send` / `raven acp` at once) —
see the insert-if-absent note inside it.
"""

from __future__ import annotations

import sqlite3
from typing import get_args

from raven_bus.exceptions import (
    InvalidAddressError,
    UnknownChannelError,
    WrongChannelKindError,
)
from raven_bus.models import Channel, ChannelKind, validate_channel_name

_SELECT = (
    "SELECT id, name, kind, retention_s, max_deliveries, created_at FROM channels"
)

# Mirrors the schema's CHECK (kind IN ('broadcast','queue','stream')) so a
# bad kind is an input error, not a raw IntegrityError from the CHECK.
_KINDS: frozenset[str] = frozenset(get_args(ChannelKind))


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
    """Get-or-create ``name`` as ``kind``. Validates the name (models
    grammar) and ``kind`` (:class:`InvalidAddressError` if not one of
    broadcast/queue/stream). Existing channel: returns it unchanged;
    raises :class:`WrongChannelKindError` if ``kind`` differs from the
    stored kind. Never updates retention/max_deliveries on an existing
    row — including a row a concurrent peer created first, whose
    settings win over this call's."""
    validate_channel_name(name)
    if kind not in _KINDS:
        raise InvalidAddressError(
            f"channel kind {kind!r} must be one of {sorted(_KINDS)}"
        )
    row = conn.execute(f"{_SELECT} WHERE name = ?", (name,)).fetchone()
    if row is None:
        # Insert-if-absent, then re-read BY NAME: a plain INSERT after the
        # SELECT miss raced — a peer committing the same name in between
        # surfaced a raw UNIQUE IntegrityError (QA store #1). ON CONFLICT
        # DO NOTHING makes the loser adopt the winner's row. The SELECT
        # stays in front because a conflicting INSERT still advances the
        # AUTOINCREMENT counter: insert-first would burn a channel id on
        # every send to an existing channel.
        conn.execute(
            "INSERT INTO channels (name, kind, retention_s, max_deliveries) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(name) DO NOTHING",
            (name, kind, retention_s, max_deliveries),
        )
        row = conn.execute(f"{_SELECT} WHERE name = ?", (name,)).fetchone()
    if row["kind"] != kind:
        raise WrongChannelKindError(
            f"channel {name!r} is kind {row['kind']!r}, requested {kind!r}"
        )
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
