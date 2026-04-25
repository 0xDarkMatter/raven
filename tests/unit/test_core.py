"""Direct tests for the low-level _core primitives.

These exercise the lifted-from-Raven functions that the public
BusClient doesn't surface in v0.1 (reply, conversation threading,
task grouping, expiry sweep, urgency filters). Without these tests
the primitives are technically reachable but never validated, and
later refactors could quietly break them.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from claude_bus import _core, aliases as alias_mod
from claude_bus.db import connection
from claude_bus.exceptions import (
    InvalidTagError,
    SchemaValidationError,
    UnknownMessageError,
    UnknownRoleError,
)


@pytest.fixture
def conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    with connection(db_path) as c:
        yield c


# ---------- reply + conversation threading ---------------------------


def test_reply_inherits_conversation_id(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    first = _core.send(
        conn, sender=a, recipient=b, type="ping", body={"q": "?"}, validate=False
    )
    second = _core.reply(
        conn, to_id=first.id, sender=b, type="pong", body={"a": "."},
        validate=False,
    )
    row = conn.execute(
        "SELECT in_reply_to, conversation_id FROM messages WHERE id = ?",
        (second.id,),
    ).fetchone()
    assert row["in_reply_to"] == first.id
    assert row["conversation_id"] == first.id


def test_reply_resolves_parent_by_default(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    first = _core.send(
        conn, sender=a, recipient=b, type="ping", body={}, validate=False
    )
    _core.reply(conn, to_id=first.id, sender=b, type="ack", body={}, validate=False)
    parent = conn.execute(
        "SELECT status, resolved_at FROM messages WHERE id = ?", (first.id,)
    ).fetchone()
    assert parent["status"] == "resolved"
    assert parent["resolved_at"] is not None


def test_reply_keeps_parent_when_resolve_false(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    first = _core.send(
        conn, sender=a, recipient=b, type="ping", body={}, validate=False
    )
    _core.reply(
        conn, to_id=first.id, sender=b, type="ack", body={},
        validate=False, resolve_parent=False,
    )
    status = conn.execute(
        "SELECT status FROM messages WHERE id = ?", (first.id,)
    ).fetchone()[0]
    assert status == "sent"


def test_reply_unknown_parent_raises(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    with pytest.raises(UnknownRoleError):
        _core.reply(
            conn, to_id=99999, sender=a, type="ping", body={}, validate=False
        )


def test_list_conversation_returns_root_and_replies(
    conn: sqlite3.Connection,
) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    first = _core.send(
        conn, sender=a, recipient=b, type="ping", body={}, validate=False
    )
    time.sleep(0.005)
    second = _core.reply(
        conn, to_id=first.id, sender=b, type="pong", body={}, validate=False
    )
    thread = _core.list_conversation(conn, first.id)
    assert [m.id for m in thread] == [first.id, second.id]


# ---------- task grouping --------------------------------------------


def test_list_by_task_filters_correctly(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, task_id="T-1",
               validate=False)
    _core.send(conn, sender=a, recipient=b, type="x", body={}, task_id="T-2",
               validate=False)
    matches = _core.list_by_task(conn, "T-1")
    assert len(matches) == 1
    assert matches[0].task_id == "T-1"


# ---------- urgency filters -----------------------------------------


def test_read_next_urgency_min_filters_out_lower_urgency(
    conn: sqlite3.Connection,
) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, urgency="fyi",
               validate=False)
    # Only blocking allowed → fyi-only inbox returns None.
    assert _core.read_next(conn, b, urgency_min="blocking") is None


def test_read_next_returns_blocking_first(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={"o": "fyi"},
               urgency="fyi", validate=False)
    time.sleep(0.005)
    _core.send(conn, sender=a, recipient=b, type="x", body={"o": "blocking"},
               urgency="blocking", validate=False)
    msg = _core.read_next(conn, b)
    assert msg is not None
    assert msg.urgency == "blocking"


def test_read_next_urgency_min_invalid_raises(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, validate=False)
    with pytest.raises(SchemaValidationError, match="urgency"):
        _core.read_next(conn, b, urgency_min="spicy")


def test_unread_count_with_urgency_filter(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, urgency="prompt",
               validate=False)
    _core.send(conn, sender=a, recipient=b, type="x", body={}, urgency="fyi",
               validate=False)
    assert _core.unread_count(conn, b) == 2
    assert _core.unread_count(conn, b, urgency="prompt") == 1
    assert _core.unread_count(conn, b, urgency="fyi") == 1


def test_list_unread_with_urgency_exact(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, urgency="fyi",
               validate=False)
    _core.send(conn, sender=a, recipient=b, type="x", body={}, urgency="prompt",
               validate=False)
    only_fyi = _core.list_unread(conn, b, urgency="fyi")
    assert len(only_fyi) == 1
    assert only_fyi[0].urgency == "fyi"


def test_read_next_mark_delivered_false_keeps_status(
    conn: sqlite3.Connection,
) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, validate=False)
    msg = _core.read_next(conn, b, mark_delivered=False)
    assert msg is not None
    assert msg.status == "sent"
    row = conn.execute(
        "SELECT status FROM messages WHERE id = ?", (msg.id,)
    ).fetchone()
    assert row["status"] == "sent"


def test_read_next_mark_delivered_true_transitions(
    conn: sqlite3.Connection,
) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    _core.send(conn, sender=a, recipient=b, type="x", body={}, validate=False)
    msg = _core.read_next(conn, b)  # default mark_delivered=True
    assert msg is not None
    assert msg.status == "delivered"
    assert msg.delivered_at is not None


def test_list_unread_mark_delivered_batch(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    for _ in range(3):
        _core.send(conn, sender=a, recipient=b, type="x", body={}, validate=False)
    got = _core.list_unread(conn, b, mark_delivered=True)
    assert all(m.status == "delivered" for m in got)


# ---------- expiry sweep --------------------------------------------


def test_sweep_expired_marks_only_stale_unresolved(
    conn: sqlite3.Connection,
) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    stale = _core.send(conn, sender=a, recipient=b, type="x", body={},
                       expires_in_s=0, validate=False)
    fresh = _core.send(conn, sender=a, recipient=b, type="x", body={},
                       expires_in_s=3600, validate=False)
    resolved = _core.send(conn, sender=a, recipient=b, type="x", body={},
                          expires_in_s=0, validate=False)
    _core.resolve(conn, resolved.id)

    time.sleep(0.02)
    swept = _core.sweep_expired(conn)
    assert swept == 1
    statuses = dict(conn.execute(
        "SELECT id, status FROM messages WHERE id IN (?, ?, ?)",
        (stale.id, fresh.id, resolved.id),
    ).fetchall())
    assert statuses[stale.id] == "expired"
    assert statuses[fresh.id] == "sent"
    assert statuses[resolved.id] == "resolved"


def test_resolve_then_resolve_is_noop(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    sent = _core.send(conn, sender=a, recipient=b, type="x", body={},
                      validate=False)
    _core.resolve(conn, sent.id)
    _core.resolve(conn, sent.id)  # no-op
    row = conn.execute(
        "SELECT status FROM messages WHERE id=?", (sent.id,)
    ).fetchone()
    assert row["status"] == "resolved"


# ---------- recipient wildcards (internal-only feature) -------------


def test_send_role_wildcard_expands(conn: sqlite3.Connection) -> None:
    """`recipient='role:*'` fans out to every alias of that role in the
    sender's session."""
    a = alias_mod.register(conn, "alice", "s")
    alias_mod.register(conn, "consumer", "s")
    result = _core.send(
        conn, sender=a, recipient="consumer:*", type="x", body={},
        validate=False,
    )
    assert len(result.message_ids) == 1


def test_send_all_wildcard_fans_out_to_session(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    alias_mod.register(conn, "bob", "s")
    alias_mod.register(conn, "charlie", "s")
    alias_mod.register(conn, "bob", "other")  # not in sender's session
    result = _core.send(
        conn, sender=a, recipient="all:*", type="x", body={}, validate=False,
    )
    # alice + bob + charlie in session "s" — 3 deliveries.
    assert len(result.message_ids) == 3


def test_send_role_wildcard_unknown_role_raises(
    conn: sqlite3.Connection,
) -> None:
    a = alias_mod.register(conn, "alice", "s")
    with pytest.raises(UnknownRoleError):
        _core.send(
            conn, sender=a, recipient="ghost:*", type="x", body={},
            validate=False,
        )


def test_send_unknown_recipient_raises(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    with pytest.raises(UnknownRoleError):
        _core.send(
            conn, sender=a, recipient="phantom-deadbeef", type="x", body={},
            validate=False,
        )


def test_send_unknown_sender_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(UnknownRoleError, match="sender"):
        _core.send(
            conn, sender="never-registered-XX", recipient="x", type="x",
            body={}, validate=False,
        )


# ---------- tag validation ------------------------------------------


def test_send_invalid_tag_raises(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    with pytest.raises(InvalidTagError):
        _core.send(
            conn, sender=a, recipient=b, type="x", body={},
            tags=["bad tag with space"], validate=False,
        )


def test_send_tags_round_trip(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    sent = _core.send(
        conn, sender=a, recipient=b, type="x", body={},
        tags=["alpha", "beta-1.0", "x_y"], validate=False,
    )
    msg = _core.read_by_id(conn, sent.id)
    assert msg.tags == ["alpha", "beta-1.0", "x_y"]


def test_send_invalid_urgency_raises(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    with pytest.raises(SchemaValidationError, match="urgency"):
        _core.send(
            conn, sender=a, recipient=b, type="x", body={},
            urgency="spicy", validate=False,
        )


# ---------- read_by_id ---------------------------------------------


def test_read_by_id_unknown_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(UnknownMessageError):
        _core.read_by_id(conn, 999)


# ---------- try_claim race-safe primitive --------------------------


def test_try_claim_first_wins(conn: sqlite3.Connection) -> None:
    a = alias_mod.register(conn, "alice", "s")
    b = alias_mod.register(conn, "bob", "s")
    sent = _core.send(conn, sender=a, recipient=b, type="x", body={},
                      validate=False)
    assert _core.try_claim(conn, sent.id) is True
    assert _core.try_claim(conn, sent.id) is False  # already resolved
    assert _core.try_claim(conn, 99999) is False  # nonexistent
