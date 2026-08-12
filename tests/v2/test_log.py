"""Tests for raven_bus.channels + raven_bus.log.  LANE: log (raven2-p1).

Uses a private connection fixture rather than ``raven_bus.db`` — the
store lane owns db.py and may still be a stub in this worktree. This
file only depends on the frozen migration SQL + models grammar, never
on db.py internals.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from raven_bus import channels, log
from raven_bus.exceptions import (
    InvalidAddressError,
    UnknownChannelError,
    UnknownMessageError,
    WrongChannelKindError,
)

_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "raven_bus"
    / "migrations"
    / "0002_v2_schema.sql"
)


@pytest.fixture()
def conn() -> Iterator[sqlite3.Connection]:
    """Private fixture: raw sqlite3 connection with the v2 schema
    applied directly, matching db.py's documented contract (WAL,
    foreign_keys ON, Row factory) without depending on db.py itself."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(_MIGRATION.read_text(encoding="utf-8"))
    c.execute("PRAGMA foreign_keys = ON")
    yield c
    c.close()


SENDER = "agent@run-1"


# --- channels ---------------------------------------------------------


def test_ensure_channel_creates(conn: sqlite3.Connection) -> None:
    chan = channels.ensure_channel(conn, "run/v0-2/lane/1")
    assert chan.name == "run/v0-2/lane/1"
    assert chan.kind == "broadcast"
    assert chan.max_deliveries == 3
    assert chan.retention_s is None


def test_ensure_channel_get_or_create_returns_unchanged(
    conn: sqlite3.Connection,
) -> None:
    first = channels.ensure_channel(conn, "chan-a", "queue", max_deliveries=5)
    again = channels.ensure_channel(conn, "chan-a", "queue", max_deliveries=99)
    assert again.id == first.id
    assert again.max_deliveries == 5  # never updated on existing row


def test_ensure_channel_kind_mismatch_raises(conn: sqlite3.Connection) -> None:
    channels.ensure_channel(conn, "chan-b", "broadcast")
    with pytest.raises(WrongChannelKindError):
        channels.ensure_channel(conn, "chan-b", "queue")


def test_ensure_channel_bad_name_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(InvalidAddressError):
        channels.ensure_channel(conn, "Bad Name")
    with pytest.raises(InvalidAddressError):
        channels.ensure_channel(conn, "/leading-slash")


def test_get_channel_unknown_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(UnknownChannelError):
        channels.get_channel(conn, "nope")


def test_get_channel_returns_existing(conn: sqlite3.Connection) -> None:
    created = channels.ensure_channel(conn, "chan-c")
    fetched = channels.get_channel(conn, "chan-c")
    assert fetched == created


def test_list_channels_name_ordered(conn: sqlite3.Connection) -> None:
    channels.ensure_channel(conn, "b")
    channels.ensure_channel(conn, "a")
    channels.ensure_channel(conn, "c")
    names = [c.name for c in channels.list_channels(conn)]
    assert names == ["a", "b", "c"]


def test_list_channels_prefix_filter(conn: sqlite3.Connection) -> None:
    channels.ensure_channel(conn, "run/v0-2/lane/1")
    channels.ensure_channel(conn, "run/v0-2/lane/2")
    channels.ensure_channel(conn, "run/v0-3/lane/1")
    names = [c.name for c in channels.list_channels(conn, prefix="run/v0-2/")]
    assert names == ["run/v0-2/lane/1", "run/v0-2/lane/2"]


# --- log.append ---------------------------------------------------------


def test_append_round_trip(conn: sqlite3.Connection) -> None:
    msg = log.append(
        conn,
        channel="chan-1",
        sender=SENDER,
        type="ping",
        body={"b": 2, "a": 1},
        urgency="blocking",
        tags=["x", "y"],
    )
    assert msg.channel == "chan-1"
    assert msg.sender == SENDER
    assert msg.urgency == "blocking"
    assert msg.body == {"a": 1, "b": 2}
    assert msg.tags == ["x", "y"]
    assert msg.reply_to is None
    assert msg.thread_id is None

    fetched = log.read_by_id(conn, msg.id)
    assert fetched == msg


def test_append_ensures_channel(conn: sqlite3.Connection) -> None:
    log.append(conn, channel="auto-chan", sender=SENDER, type="t", body={})
    chan = channels.get_channel(conn, "auto-chan")
    assert chan.kind == "broadcast"


def test_append_without_ensure_requires_existing_channel(
    conn: sqlite3.Connection,
) -> None:
    with pytest.raises(UnknownChannelError):
        log.append(
            conn,
            channel="missing",
            sender=SENDER,
            type="t",
            body={},
            ensure=False,
        )


def test_append_bad_sender_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(InvalidAddressError):
        log.append(conn, channel="c", sender="not-a-consumer-id", type="t", body={})


def test_append_bad_tag_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(InvalidAddressError):
        log.append(
            conn,
            channel="c",
            sender=SENDER,
            type="t",
            body={},
            tags=["ok", "bad tag!"],
        )


def test_append_bad_urgency_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(InvalidAddressError):
        log.append(conn, channel="c", sender=SENDER, type="t", body={}, urgency="urgent")  # type: ignore[arg-type]


def test_append_upserts_consumer(conn: sqlite3.Connection) -> None:
    log.append(conn, channel="c", sender=SENDER, type="t", body={})
    row1 = conn.execute(
        "SELECT last_seen_at FROM consumers WHERE id = ?", (SENDER,)
    ).fetchone()
    time.sleep(0.01)
    log.append(conn, channel="c", sender=SENDER, type="t", body={})
    row2 = conn.execute(
        "SELECT last_seen_at FROM consumers WHERE id = ?", (SENDER,)
    ).fetchone()
    count = conn.execute("SELECT COUNT(*) AS n FROM consumers").fetchone()["n"]
    assert count == 1
    assert row2["last_seen_at"] >= row1["last_seen_at"]


def test_append_thread_inheritance_three_deep(conn: sqlite3.Connection) -> None:
    root = log.append(conn, channel="c", sender=SENDER, type="t", body={})
    assert root.thread_id is None

    reply1 = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, reply_to=root.id
    )
    assert reply1.thread_id == root.id

    reply2 = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, reply_to=reply1.id
    )
    assert reply2.thread_id == root.id  # inherits root's thread, not reply1.id

    reply3 = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, reply_to=reply2.id
    )
    assert reply3.thread_id == root.id


def test_append_expires_in_s_sets_expires_at(conn: sqlite3.Connection) -> None:
    msg = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, expires_in_s=3600
    )
    assert msg.expires_at is not None
    assert msg.expires_at > msg.created_at


# --- log.read_after -------------------------------------------------------


def test_read_after_ordering(conn: sqlite3.Connection) -> None:
    ids = [
        log.append(conn, channel="c", sender=SENDER, type="t", body={"i": i}).id
        for i in range(5)
    ]
    msgs = log.read_after(conn, "c", ids[1])
    assert [m.id for m in msgs] == ids[2:]


def test_read_after_limit(conn: sqlite3.Connection) -> None:
    for i in range(5):
        log.append(conn, channel="c", sender=SENDER, type="t", body={"i": i})
    msgs = log.read_after(conn, "c", 0, limit=2)
    assert len(msgs) == 2


def test_read_after_sender_filter(conn: sqlite3.Connection) -> None:
    other = "other@run-1"
    log.append(conn, channel="c", sender=SENDER, type="t", body={})
    log.append(conn, channel="c", sender=other, type="t", body={})
    msgs = log.read_after(conn, "c", 0, sender=other)
    assert len(msgs) == 1
    assert msgs[0].sender == other


def test_read_after_filters_expired_by_default(conn: sqlite3.Connection) -> None:
    expired = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, expires_in_s=-10
    )
    live = log.append(conn, channel="c", sender=SENDER, type="t", body={})

    live_only = log.read_after(conn, "c", 0)
    assert [m.id for m in live_only] == [live.id]

    with_expired = log.read_after(conn, "c", 0, include_expired=True)
    assert {m.id for m in with_expired} == {expired.id, live.id}


def test_read_after_unknown_channel_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(UnknownChannelError):
        log.read_after(conn, "nope", 0)


# --- log.read_by_id / read_thread -----------------------------------------


def test_read_by_id_missing_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(UnknownMessageError):
        log.read_by_id(conn, 99999)


def test_read_by_id_no_liveness_filter(conn: sqlite3.Connection) -> None:
    expired = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, expires_in_s=-10
    )
    fetched = log.read_by_id(conn, expired.id)
    assert fetched.id == expired.id


def test_read_thread_ordering(conn: sqlite3.Connection) -> None:
    root = log.append(conn, channel="c", sender=SENDER, type="t", body={})
    reply1 = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, reply_to=root.id
    )
    reply2 = log.append(
        conn, channel="c", sender=SENDER, type="t", body={}, reply_to=reply1.id
    )
    # unrelated message must not appear
    log.append(conn, channel="c", sender=SENDER, type="t", body={})

    thread = log.read_thread(conn, root.id)
    assert [m.id for m in thread] == [root.id, reply1.id, reply2.id]


def test_read_thread_includes_expired(conn: sqlite3.Connection) -> None:
    root = log.append(conn, channel="c", sender=SENDER, type="t", body={})
    reply = log.append(
        conn,
        channel="c",
        sender=SENDER,
        type="t",
        body={},
        reply_to=root.id,
        expires_in_s=-10,
    )
    thread = log.read_thread(conn, root.id)
    assert reply.id in {m.id for m in thread}
