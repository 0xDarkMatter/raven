"""Tests for raven_bus.channels + raven_bus.log.  LANE: log (raven2-p1).

Uses a private connection fixture rather than ``raven_bus.db`` — the
store lane owns db.py and may still be a stub in this worktree. This
file only depends on the frozen migration SQL + models grammar, never
on db.py internals.
"""

from __future__ import annotations

import sqlite3
import threading
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


def test_append_empty_channel_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(InvalidAddressError):
        log.append(conn, channel="", sender=SENDER, type="t", body={})


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


# --- QA store-lane regressions: channel get-or-create ---------------------


def _is_channel_insert(sql: str) -> bool:
    return sql.lstrip().upper().startswith("INSERT INTO CHANNELS")


def test_ensure_channel_survives_concurrent_first_use(db: Path) -> None:
    """QA store #1: SELECT-then-INSERT raced. A peer committing the same
    new channel between our SELECT miss and our INSERT surfaced a raw
    ``IntegrityError: UNIQUE constraint failed: channels.name`` (8
    concurrent `raven send` to a fresh channel: up to 57/96 failed).
    Deterministic interleave: a holder owns the write lock with the row
    uncommitted; the contender misses on SELECT and blocks in INSERT;
    the holder commits; the contender must adopt the holder's row."""
    from raven_bus import db as bus_db
    from raven_bus.models import Channel

    holder = sqlite3.connect(db, timeout=5.0)
    holder.execute("BEGIN IMMEDIATE")
    holder.execute(
        "INSERT INTO channels (name, kind) VALUES ('run/r/fresh', 'broadcast')"
    )

    insert_started = threading.Event()
    outcome: list[object] = []

    def contender() -> None:
        try:
            with bus_db.connection(db) as cx:
                cx.set_trace_callback(
                    lambda sql: insert_started.set() if _is_channel_insert(sql) else None
                )
                outcome.append(channels.ensure_channel(cx, "run/r/fresh"))
        except BaseException as exc:  # noqa: BLE001 -- surfaced by the assert
            outcome.append(exc)

    thread = threading.Thread(target=contender)
    thread.start()
    try:
        assert insert_started.wait(5), "contender never reached its INSERT"
        time.sleep(0.2)  # let the contender park in the busy-wait
    finally:
        holder.commit()
        holder.close()
    thread.join(10)

    assert len(outcome) == 1
    assert isinstance(outcome[0], Channel), outcome[0]
    assert outcome[0].name == "run/r/fresh"


def test_ensure_channel_concurrent_first_use_stress(db: Path) -> None:
    """QA store #1, the reviewer's shape: many connections race the first
    use of one channel; every one must get the same row, none may crash."""
    from raven_bus import db as bus_db

    workers = 8
    barrier = threading.Barrier(workers)
    ids: list[int] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            with bus_db.connection(db) as cx:
                barrier.wait(5)
                ids.append(channels.ensure_channel(cx, "run/r/stress", "queue").id)
        except BaseException as exc:  # noqa: BLE001 -- surfaced by the assert
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)

    assert errors == []
    assert len(ids) == workers
    assert len(set(ids)) == 1


def test_ensure_channel_existing_hit_does_not_burn_autoincrement_ids(
    conn: sqlite3.Connection,
) -> None:
    """``INSERT ... ON CONFLICT DO NOTHING`` still advances AUTOINCREMENT
    on a conflict, so the insert must only run on a SELECT miss —
    otherwise every send to an existing channel would burn a channel id."""
    first = channels.ensure_channel(conn, "chan-a")
    for _ in range(5):
        channels.ensure_channel(conn, "chan-a")
    second = channels.ensure_channel(conn, "chan-b")
    assert second.id == first.id + 1


def test_ensure_channel_rejects_unknown_kind(conn: sqlite3.Connection) -> None:
    """QA store #2: a bogus kind hit the schema's raw CHECK
    IntegrityError; it is an input error (same class as a bad urgency)."""
    with pytest.raises(InvalidAddressError, match="kind"):
        channels.ensure_channel(conn, "chan-k", "bogus")  # type: ignore[arg-type]
    assert conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 0


@pytest.mark.parametrize("kind", ["queue", "stream"])
def test_append_ensure_uses_existing_channel_of_any_kind(
    conn: sqlite3.Connection, kind: str
) -> None:
    """QA store #2: ``append(ensure=True)`` re-ensured with the default
    kind 'broadcast', so appending to an EXISTING queue/stream raised
    WrongChannelKindError (broke the Python queue snippet, `raven send`
    to a queue, and harness telemetry to a stream reply channel).
    ensure=True is get-or-create: an existing channel is used as-is."""
    channels.ensure_channel(conn, "run/r/typed", kind)  # type: ignore[arg-type]
    msg = log.append(conn, channel="run/r/typed", sender=SENDER, type="t", body={})
    assert msg.channel == "run/r/typed"
    assert channels.get_channel(conn, "run/r/typed").kind == kind


def test_append_ensure_validates_absent_channel_name(conn: sqlite3.Connection) -> None:
    with pytest.raises(InvalidAddressError):
        log.append(conn, channel="Bad Name", sender=SENDER, type="t", body={})


# --- QA store-lane regressions: reply_to / thread_id references ----------


def _message_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]


@pytest.mark.parametrize("missing", [999, 0, -1, 2**63])
def test_append_unknown_reply_to_raises_unknown_message(
    conn: sqlite3.Connection, missing: int
) -> None:
    """QA store #4: a nonexistent reply_to fell through to a raw FK
    IntegrityError (or OverflowError past SQLite's INTEGER range)."""
    with pytest.raises(UnknownMessageError, match="reply_to"):
        log.append(conn, channel="c", sender=SENDER, type="t", body={}, reply_to=missing)
    assert _message_count(conn) == 0


def test_append_unknown_thread_id_raises_unknown_message(conn: sqlite3.Connection) -> None:
    with pytest.raises(UnknownMessageError, match="thread_id"):
        log.append(conn, channel="c", sender=SENDER, type="t", body={}, thread_id=999)
    assert _message_count(conn) == 0


def test_append_explicit_thread_id_is_checked_even_with_valid_reply_to(
    conn: sqlite3.Connection,
) -> None:
    root = log.append(conn, channel="c", sender=SENDER, type="t", body={})
    with pytest.raises(UnknownMessageError, match="thread_id"):
        log.append(
            conn, channel="c", sender=SENDER, type="t", body={},
            reply_to=root.id, thread_id=999,
        )
    assert _message_count(conn) == 1


def test_append_explicit_thread_id_is_kept(conn: sqlite3.Connection) -> None:
    root = log.append(conn, channel="c", sender=SENDER, type="t", body={})
    other = log.append(conn, channel="c", sender=SENDER, type="t", body={})
    reply = log.append(
        conn, channel="c", sender=SENDER, type="t", body={},
        reply_to=other.id, thread_id=root.id,
    )
    assert (reply.reply_to, reply.thread_id) == (other.id, root.id)


# --- QA store-lane regressions: body nesting cap --------------------------


def _nested_dicts(depth: int) -> dict:
    """``depth`` dicts nested inside each other (the body itself = 1)."""
    body: dict = {}
    inner = body
    for _ in range(depth - 1):
        inner["x"] = {}
        inner = inner["x"]
    return body


def _nested_lists(depth: int) -> dict:
    """A body dict holding ``depth - 1`` nested lists."""
    inner: list = []
    for _ in range(depth - 2):
        inner = [inner]
    return {"x": inner}


def test_append_accepts_body_at_the_depth_cap_and_readers_can_encode_it(
    conn: sqlite3.Connection,
) -> None:
    """The cap must sit below what the read side can re-serialise:
    pydantic's JSON serializer (HTTP readers) gives up at ~100 levels."""
    msg = log.append(
        conn, channel="c", sender=SENDER, type="t", body=_nested_dicts(log.MAX_BODY_DEPTH)
    )
    fetched = log.read_by_id(conn, msg.id)
    fetched.model_dump(mode="json")
    fetched.model_dump_json()


@pytest.mark.parametrize("builder", [_nested_dicts, _nested_lists])
def test_append_rejects_body_nested_past_the_cap(
    conn: sqlite3.Connection, builder
) -> None:
    """QA store #12: json.dumps accepted ~1000 levels, so a body nested
    ≥~100 deep was stored, then broke every HTTP reader (and an HTTP
    claim leased a message it could not return). The single writer now
    refuses it with a typed error — nothing is written."""
    from raven_bus.exceptions import InvalidBodyError

    with pytest.raises(InvalidBodyError, match="nested"):
        log.append(
            conn, channel="c", sender=SENDER, type="t",
            body=builder(log.MAX_BODY_DEPTH + 1),
        )
    assert _message_count(conn) == 0
    assert issubclass(InvalidBodyError, ValueError)


def test_append_depth_check_is_iterative(conn: sqlite3.Connection) -> None:
    """A body deeper than Python's recursion limit gets the typed error,
    not a RecursionError from a recursive walker or json.dumps."""
    from raven_bus.exceptions import InvalidBodyError

    with pytest.raises(InvalidBodyError):
        log.append(conn, channel="c", sender=SENDER, type="t", body=_nested_dicts(5000))
