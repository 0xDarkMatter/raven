"""Tests for raven_bus.cursors (broadcast read-state).  LANE: cursors.

The sibling lanes (db, channels, log) ship as stubs in this worktree,
so per the run plan we stand them in here with faithful in-place
contract implementations + raw-SQL data setup. Production code under
test calls ONLY the public contracts (db.sweep, channels.get_channel,
log.read_after, models helpers).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

import raven_bus.cursors as cursors_mod
from raven_bus import cursors
from raven_bus.exceptions import (
    InvalidAddressError,
    UnknownChannelError,
    WrongChannelKindError,
)

_TS_NOW = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
MIGRATION = (
    Path(__file__).resolve().parents[2] / "src" / "raven_bus" / "migrations" / "0002_v2_schema.sql"
)


# --------------------------------------------------------------------------- #
# Sibling-lane contract stand-ins (db / channels / log) + raw-SQL data setup.
# --------------------------------------------------------------------------- #
def _ensure_channel_row(conn: sqlite3.Connection, name: str, kind: str = "broadcast") -> int:
    """Get-or-create a channel row by name, returning its id."""
    row = conn.execute("SELECT id FROM channels WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return int(row["id"])
    cur = conn.execute("INSERT INTO channels (name, kind) VALUES (?, ?)", (name, kind))
    return int(cur.lastrowid)


def _insert_message(
    conn: sqlite3.Connection,
    channel_id: int,
    *,
    sender: str = "sender@run-x",
    msg_type: str = "note",
    expires_at: str | None = None,
) -> int:
    """Append a messages row and return its id (log.append contract: the
    only writer into messages)."""
    cur = conn.execute(
        """
        INSERT INTO messages (channel_id, sender, type, body, expires_at)
        VALUES (?, ?, ?, '{}', ?)
        """,
        (channel_id, sender, msg_type, expires_at),
    )
    return int(cur.lastrowid)


def _install_sibling_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch the sibling-lane entry points that cursors calls.

    These mirror the frozen docstring contracts so the code under test
    is exercised against real v2 semantics, not the lane stubs.
    MUST go through pytest's monkeypatch: cursors imports the REAL
    raven_bus.db/channels/log modules, so a bare assignment here would
    replace them for every later test file in the process (it did —
    8 cross-file failures at wave-1 landing).
    """
    # db.sweep — no-op for broadcast cursors (no leases to reap; expiry
    # is enforced by read_after's filter, matching ADR-001).
    monkeypatch.setattr(cursors_mod.db, "sweep", lambda _conn: None)

    def get_channel(_conn: sqlite3.Connection, name: str):
        from raven_bus.models import Channel

        row = _conn.execute(
            "SELECT id, name, kind, created_at FROM channels WHERE name = ?",
            (name,),
        ).fetchone()
        if row is None:
            raise UnknownChannelError(f"no channel named {name!r}")
        return Channel(
            id=int(row["id"]),
            name=row["name"],
            kind=row["kind"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    monkeypatch.setattr(cursors_mod.channels, "get_channel", get_channel)

    def read_after(
        _conn: sqlite3.Connection,
        channel: str,
        after_id: int,
        *,
        limit: int = 100,
        sender: str | None = None,
        include_expired: bool = False,
    ):
        from raven_bus.models import Message

        # Resolve channel_id the same way the log lane will, then read
        # id > after_id with the expiry filter (ADR-001 liveness rule).
        ch = _conn.execute("SELECT id FROM channels WHERE name = ?", (channel,)).fetchone()
        if ch is None:
            return []
        clause = "channel_id = ? AND id > ?"
        params: list[object] = [int(ch["id"]), after_id]
        if not include_expired:
            clause += (
                " AND (expires_at IS NULL OR expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))"
            )
        if sender is not None:
            clause += " AND sender = ?"
            params.append(sender)
        clause += " ORDER BY id ASC LIMIT ?"
        params.append(limit)
        rows = _conn.execute(f"SELECT * FROM messages WHERE {clause}", params).fetchall()
        return [
            Message(
                id=int(r["id"]),
                channel=channel,
                sender=r["sender"],
                type=r["type"],
                body={},
                tags=[],
                created_at=datetime.fromisoformat(r["created_at"]),
            )
            for r in rows
        ]

    monkeypatch.setattr(cursors_mod.log, "read_after", read_after)


# --------------------------------------------------------------------------- #
# Fixture: a raw sqlite3 connection on a freshly-migrated in-memory DB.
# --------------------------------------------------------------------------- #
@pytest.fixture()
def conn(monkeypatch: pytest.MonkeyPatch):
    """A migrated sqlite3 connection with sibling-lane stubs installed.

    Each test owns its own private DB; we commit at handoff so the next
    operation sees durable rows. Production code never commits (the
    db.connection context manager owns that); our private fixture does
    the commit harness the real connection would. Sibling stubs are
    scoped to the test via monkeypatch — they auto-restore on teardown.
    """
    cx = sqlite3.connect(":memory:")
    cx.row_factory = sqlite3.Row
    cx.execute("PRAGMA foreign_keys = ON")
    cx.executescript(MIGRATION.read_text(encoding="utf-8"))
    _install_sibling_stubs(monkeypatch)
    yield cx
    cx.close()


def _commit(conn: sqlite3.Connection) -> None:
    """Stand in for the db.connection context manager's clean-exit commit."""
    conn.commit()


# --------------------------------------------------------------------------- #
# pending()
# --------------------------------------------------------------------------- #
def test_pending_no_cursor_sees_all_live_messages(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for i in range(3):
        _insert_message(conn, cid, msg_type=f"m{i}")
    _commit(conn)

    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")

    assert [m.id for m in got] == [1, 2, 3]
    assert [m.type for m in got] == ["m0", "m1", "m2"]


def test_pending_no_cursor_is_empty_when_no_messages(conn):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)

    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")

    assert got == []


def test_pending_mid_cursor_returns_only_unseen(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for i in range(5):
        _insert_message(conn, cid, msg_type=f"m{i}")
    conn.execute(
        f"""
        INSERT INTO cursors (consumer, channel_id, last_ack_id, updated_at)
        VALUES ('worker@run-a', {cid}, 3, {_TS_NOW})
        """
    )
    _commit(conn)

    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")

    # cursor at 3 -> strictly-greater ids are 4, 5
    assert [m.id for m in got] == [4, 5]


def test_pending_filters_expired_messages(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    past = "1999-01-01T00:00:00.000Z"
    _insert_message(conn, cid, msg_type="expired", expires_at=past)
    _insert_message(conn, cid, msg_type="live")
    _commit(conn)

    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")

    assert [m.type for m in got] == ["live"]


def test_pending_respects_limit_and_orders_ascending(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for i in range(5):
        _insert_message(conn, cid, msg_type=f"m{i}")
    _commit(conn)

    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast", limit=2)

    assert [m.id for m in got] == [1, 2]


def test_pending_does_not_move_cursor(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for _ in range(3):
        _insert_message(conn, cid)
    _commit(conn)

    cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")
    # Reading must NOT have created or advanced a cursor row.
    row = conn.execute("SELECT last_ack_id FROM cursors WHERE consumer = 'worker@run-a'").fetchone()
    assert row is None


def test_pending_registers_consumer_last_seen(conn):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)

    cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")

    row = conn.execute(
        "SELECT id, role, run, last_seen_at FROM consumers WHERE id = 'worker@run-a'"
    ).fetchone()
    assert row is not None
    assert row["role"] == "worker"
    assert row["run"] == "run-a"
    assert row["last_seen_at"] is not None


# --------------------------------------------------------------------------- #
# Fan-out: two consumers see the same messages independently.
# --------------------------------------------------------------------------- #
def test_pending_fanout_two_consumers_independent(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for i in range(4):
        _insert_message(conn, cid, msg_type=f"m{i}")
    _commit(conn)

    # Consumer A acks through 2; consumer B acks nothing.
    conn.execute(
        f"""
        INSERT INTO cursors (consumer, channel_id, last_ack_id, updated_at)
        VALUES ('a@run', {cid}, 2, {_TS_NOW})
        """
    )
    _commit(conn)

    a_got = cursors.pending(conn, "a@run", "run/v0-2/broadcast")
    b_got = cursors.pending(conn, "b@run", "run/v0-2/broadcast")

    assert [m.id for m in a_got] == [3, 4]  # cursor at 2
    assert [m.id for m in b_got] == [1, 2, 3, 4]  # no cursor -> all live


# --------------------------------------------------------------------------- #
# ack()
# --------------------------------------------------------------------------- #
def test_ack_creates_row_when_absent(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for _ in range(5):
        _insert_message(conn, cid)
    _commit(conn)

    cur = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 5)

    assert cur.consumer == "worker@run-a"
    assert cur.channel == "run/v0-2/broadcast"
    assert cur.last_ack_id == 5
    row = conn.execute(
        "SELECT last_ack_id FROM cursors WHERE consumer='worker@run-a' AND channel_id=?",
        (cid,),
    ).fetchone()
    assert int(row["last_ack_id"]) == 5


def test_ack_is_monotonic_forwards(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for _ in range(8):
        _insert_message(conn, cid)
    _commit(conn)

    cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 5)
    cur = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 8)

    assert cur.last_ack_id == 8


def test_ack_backwards_is_silent_noop(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    conn.execute(
        f"""
        INSERT INTO cursors (consumer, channel_id, last_ack_id, updated_at)
        VALUES ('worker@run-a', {cid}, 10, '2020-01-01T00:00:00.000Z')
        """
    )
    _commit(conn)

    cur = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 3)

    # Monotonic: acking backwards neither errors nor retreats the cursor.
    assert cur.last_ack_id == 10


def test_ack_backwards_does_not_touch_updated_at(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    frozen = "2020-01-01T00:00:00.000Z"
    conn.execute(
        """
        INSERT INTO cursors (consumer, channel_id, last_ack_id, updated_at)
        VALUES ('worker@run-a', ?, 10, ?)
        """,
        (cid, frozen),
    )
    _commit(conn)

    cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 3)

    row = conn.execute("SELECT updated_at FROM cursors WHERE consumer='worker@run-a'").fetchone()
    assert row["updated_at"] == frozen


def test_ack_then_pending_only_returns_new_messages(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    for _ in range(4):
        _insert_message(conn, cid)
    _commit(conn)

    cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 2)
    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")

    assert [m.id for m in got] == [3, 4]


def test_ack_clamps_to_channel_head(conn):
    """QA store #3: message ids are global, so an ack past THIS channel's
    head (a typo, or an id from another channel) parked the monotonic
    cursor beyond every future message here — hidden forever, with no
    rewind. The cursor now stops at the channel's newest message."""
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    other = _ensure_channel_row(conn, "run/v0-2/other")
    head = _insert_message(conn, cid)
    foreign = [_insert_message(conn, other) for _ in range(3)][-1]
    _commit(conn)

    cur = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", foreign)
    assert cur.last_ack_id == head

    fresh = _insert_message(conn, cid)
    _commit(conn)
    got = cursors.pending(conn, "worker@run-a", "run/v0-2/broadcast")
    assert [m.id for m in got] == [fresh]


def test_ack_on_empty_channel_stays_at_zero(conn):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)

    cur = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 5)

    assert cur.last_ack_id == 0


@pytest.mark.parametrize("huge", [2**63, 10**30])
def test_ack_beyond_sqlite_integer_range_clamps_not_overflows(conn, huge):
    """The clamp happens in Python BEFORE binding, so an id past SQLite's
    INTEGER range can't raise OverflowError on the way in."""
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    head = _insert_message(conn, cid)
    _commit(conn)

    cur = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", huge)

    assert cur.last_ack_id == head


@pytest.mark.parametrize("low", [0, -5, -(2**70)])
def test_ack_zero_or_negative_is_a_noop(conn, low):
    """Zero/negative acks are backwards acks: no-op on an existing cursor,
    and a fresh cursor row starts at 0 (never a negative position)."""
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    first = _insert_message(conn, cid)
    _commit(conn)

    fresh = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", low)
    assert fresh.last_ack_id == 0

    cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", first)
    again = cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", low)
    assert again.last_ack_id == first


def test_ack_registers_consumer(conn):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)

    cursors.ack(conn, "worker@run-a", "run/v0-2/broadcast", 1)

    row = conn.execute("SELECT role, run FROM consumers WHERE id='worker@run-a'").fetchone()
    assert row is not None
    assert row["role"] == "worker"


# --------------------------------------------------------------------------- #
# get_cursor()
# --------------------------------------------------------------------------- #
def test_get_cursor_returns_none_when_absent(conn):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)

    assert cursors.get_cursor(conn, "worker@run-a", "run/v0-2/broadcast") is None


def test_get_cursor_returns_row_without_side_effects(conn):
    cid = _ensure_channel_row(conn, "run/v0-2/broadcast")
    conn.execute(
        """
        INSERT INTO cursors (consumer, channel_id, last_ack_id, updated_at)
        VALUES ('worker@run-a', ?, 7, '2020-01-01T00:00:00.000Z')
        """,
        (cid,),
    )
    _commit(conn)

    cur = cursors.get_cursor(conn, "worker@run-a", "run/v0-2/broadcast")

    assert cur is not None
    assert cur.last_ack_id == 7
    assert cur.consumer == "worker@run-a"
    assert cur.channel == "run/v0-2/broadcast"
    # Pure lookup: no consumer registration happened.
    row = conn.execute("SELECT id FROM consumers WHERE id='worker@run-a'").fetchone()
    assert row is None


def test_get_cursor_returns_none_for_unknown_channel(conn):
    _commit(conn)
    assert cursors.get_cursor(conn, "worker@run-a", "no/such/channel") is None


# --------------------------------------------------------------------------- #
# Channel-kind + address validation (error paths).
# --------------------------------------------------------------------------- #
def test_pending_raises_on_unknown_channel(conn):
    _commit(conn)
    with pytest.raises(UnknownChannelError):
        cursors.pending(conn, "worker@run-a", "no/such/channel")


def test_ack_raises_on_unknown_channel(conn):
    _commit(conn)
    with pytest.raises(UnknownChannelError):
        cursors.ack(conn, "worker@run-a", "no/such/channel", 1)


def test_pending_raises_wrong_kind_for_queue(conn):
    _ensure_channel_row(conn, "run/v0-2/work", kind="queue")
    _commit(conn)
    with pytest.raises(WrongChannelKindError):
        cursors.pending(conn, "worker@run-a", "run/v0-2/work")


def test_ack_raises_wrong_kind_for_stream(conn):
    _ensure_channel_row(conn, "run/v0-2/firehose", kind="stream")
    _commit(conn)
    with pytest.raises(WrongChannelKindError):
        cursors.ack(conn, "worker@run-a", "run/v0-2/firehose", 1)


@pytest.mark.parametrize("bad", ["no-at-sign", "a@b@c", "Role@run", "role@Run", "@run", "role@"])
def test_pending_rejects_bad_consumer_id(conn, bad):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)
    with pytest.raises(InvalidAddressError):
        cursors.pending(conn, bad, "run/v0-2/broadcast")


def test_ack_rejects_bad_consumer_id(conn):
    _ensure_channel_row(conn, "run/v0-2/broadcast")
    _commit(conn)
    with pytest.raises(InvalidAddressError):
        cursors.ack(conn, "no-at-sign", "run/v0-2/broadcast", 1)
