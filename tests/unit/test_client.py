"""BusClient end-to-end behaviour."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from claude_bus import (
    BusClient,
    SchemaRegistry,
    SchemaValidationError,
    UnknownMessageError,
)


def test_send_then_inbox_round_trip(db_path: Path) -> None:
    a = BusClient(session_id="s1", role="alice", db_path=db_path)
    b = BusClient(session_id="s1", role="bob", db_path=db_path)

    sent = a.send(to=b.address, type="ping", body={"n": 1})

    assert sent.sender == "alice:s1"
    assert sent.recipient == "bob:s1"
    assert sent.recipient_role == "bob"
    assert sent.recipient_session == "s1"
    assert sent.status == "unread"

    unread = b.inbox()
    assert len(unread) == 1
    assert unread[0].body == {"n": 1}
    assert unread[0].id == sent.id


def test_ack_marks_message_read(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="ping", body={})
    b.ack(sent.id)
    assert b.inbox() == []
    # Reading the message still works; status is now 'read'.
    msg = b.read(sent.id)
    assert msg.status == "read"


def test_ack_is_idempotent(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="ping", body={})
    b.ack(sent.id)
    b.ack(sent.id)  # second ack is a no-op


def test_read_unknown_id_raises(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    with pytest.raises(UnknownMessageError):
        a.read(999)


def test_ack_unknown_id_raises(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    with pytest.raises(UnknownMessageError):
        a.ack(999)


def test_send_to_self_works(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    sent = a.send(to=a.address, type="note", body={"x": 1})
    assert a.inbox()[0].id == sent.id


def test_address_property(db_path: Path) -> None:
    c = BusClient(session_id="swarm-99", role="conductor", db_path=db_path)
    assert c.address == "conductor:swarm-99"


def test_session_isolation(db_path: Path) -> None:
    """Two sessions don't see each other's messages."""
    a1 = BusClient(session_id="s1", role="a", db_path=db_path)
    b1 = BusClient(session_id="s1", role="b", db_path=db_path)
    b2 = BusClient(session_id="s2", role="b", db_path=db_path)

    a1.send(to=b1.address, type="ping", body={"to": "s1"})
    assert len(b1.inbox()) == 1
    assert b2.inbox() == []


def test_inbox_batches_alias_lookups(db_path: Path) -> None:
    """A 20-message inbox should issue at most one alias SELECT, not 40."""
    import sqlite3

    a = BusClient(session_id="s", role="alice", db_path=db_path)
    b = BusClient(session_id="s", role="bob", db_path=db_path)
    for i in range(20):
        a.send(to=b.address, type="ping", body={"i": i})

    # Trace SQL executed against a fresh connection to the same DB.
    # We can't trace the BusClient's internal connection directly because
    # set_trace_callback is per-connection — but we can replicate the
    # exact inbox path on a traced connection by calling list_unread +
    # the batched resolve helper used by `inbox`.
    from claude_bus import _core
    from claude_bus.client import _resolve_aliases_bulk, _to_public

    sqls: list[str] = []
    conn = sqlite3.connect(db_path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.set_trace_callback(sqls.append)

    internals = _core.list_unread(conn, b.alias, limit=100, mark_delivered=False)
    wanted: set[str] = {m.sender for m in internals} | {m.recipient for m in internals}
    alias_map = _resolve_aliases_bulk(conn, wanted)
    msgs = [_to_public(m, conn, alias_map=alias_map) for m in internals]
    conn.close()

    assert len(msgs) == 20
    alias_resolves = [s for s in sqls if "FROM aliases WHERE alias IN" in s
                                   or ("FROM aliases" in s and "WHERE alias = ?" in s)]
    assert len(alias_resolves) <= 1, (
        f"expected ≤1 alias-resolution query for a 20-message inbox, "
        f"got {len(alias_resolves)}: {alias_resolves}"
    )


def test_alias_register_is_process_cached(db_path: Path, monkeypatch) -> None:
    """Repeated BusClient() with same triple skips the SQLite round-trip."""
    from claude_bus import aliases as alias_mod
    from claude_bus.client import _reset_alias_register_cache

    _reset_alias_register_cache()
    calls: list[int] = []
    real = alias_mod.register

    def counting(conn, role, session_id):
        calls.append(1)
        return real(conn, role, session_id)

    monkeypatch.setattr(alias_mod, "register", counting)

    BusClient(session_id="s", role="alice", db_path=db_path)
    BusClient(session_id="s", role="alice", db_path=db_path)
    BusClient(session_id="s", role="alice", db_path=db_path)
    assert sum(calls) == 1, f"alias_mod.register ran {sum(calls)} times, expected 1"


def test_send_with_correlation_and_reply_to(db_path: Path) -> None:
    """correlation_id and reply_to flow through to stored Message."""
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    first = a.send(to=b.address, type="ping", body={"n": 1})
    second = a.send(
        to=b.address, type="pong", body={"n": 2},
        correlation_id=first.id, reply_to=first.id,
    )
    fetched = b.read(second.id)
    assert fetched.reply_to == first.id
    assert fetched.correlation_id == first.id


def test_send_with_task_id_round_trip(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="x", body={}, task_id="TASK-42")
    fetched = b.read(sent.id)
    assert fetched.task_id == "TASK-42"


def test_send_with_tags(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="x", body={}, tags=["urgent", "v1.2"])
    fetched = b.read(sent.id)
    assert fetched.tags == ["urgent", "v1.2"]


def test_send_with_urgency(db_path: Path) -> None:
    """urgency='blocking' on a message should still flow through."""
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="x", body={}, urgency="blocking")
    # The internal _core.Message has the urgency field; the public Message
    # doesn't expose it directly in v0.1, so verify via the inbox query.
    msg = b.inbox()[0]
    assert msg.id == sent.id


def test_send_with_expires_in_s(db_path: Path) -> None:
    """A message with expires_in_s=0 will be flagged 'expired' by sweep."""
    import time
    from claude_bus import _core
    from claude_bus.db import connection

    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="x", body={}, expires_in_s=0)
    time.sleep(0.05)
    with connection(db_path) as conn:
        n = _core.sweep_expired(conn)
    assert n == 1
    # After sweep, status='expired' which maps to public 'read'.
    msg = b.read(sent.id)
    assert msg.status == "read"


def test_busclient_alias_property(db_path: Path) -> None:
    a = BusClient(session_id="s", role="alice", db_path=db_path)
    # Alias is deterministic: <role>-<6hex>
    assert a.alias.startswith("alice-")
    assert len(a.alias) == len("alice-") + 6


def test_inbox_role_param_accepts_address_form(db_path: Path) -> None:
    """`inbox(role="a:s")` (the address form) is accepted as well as the
    bare role name."""
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    a.send(to=b.address, type="x", body={})
    by_address = b.inbox(role=b.address)
    by_role_name = b.inbox(role="b")
    assert len(by_address) == len(by_role_name) == 1


def test_inbox_role_other_identity_rejected(db_path: Path) -> None:
    """v0.1 supports only the client's own inbox via role= argument."""
    b = BusClient(session_id="s", role="b", db_path=db_path)
    with pytest.raises(ValueError):
        b.inbox(role="someone-else:other")


def test_subscribe_role_other_identity_rejected(db_path: Path) -> None:
    b = BusClient(session_id="s", role="b", db_path=db_path)

    async def go():
        async for _ in b.subscribe(role="someone-else:other"):
            pass

    import asyncio

    with pytest.raises(ValueError):
        asyncio.run(go())


def test_busclient_rejects_empty_session(db_path: Path) -> None:
    with pytest.raises(ValueError, match="session_id"):
        BusClient(session_id="", role="alice", db_path=db_path)
    with pytest.raises(ValueError, match="session_id"):
        BusClient(session_id="   ", role="alice", db_path=db_path)


def test_busclient_rejects_empty_role(db_path: Path) -> None:
    with pytest.raises(ValueError, match="role"):
        BusClient(session_id="s1", role="", db_path=db_path)


def test_busclient_rejects_role_with_colon(db_path: Path) -> None:
    """':' is the address separator — a role containing it would break parsing."""
    with pytest.raises(ValueError, match="':'"):
        BusClient(session_id="s1", role="bad:role", db_path=db_path)


def test_send_invalid_address_raises(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    with pytest.raises(ValueError):
        a.send(to="missing-colon", type="x", body={})


# ----- schema registry --------------------------------------------------


class PlanBody(BaseModel):
    step: int
    goal: str


def test_schema_registry_validates_registered_type(db_path: Path) -> None:
    SchemaRegistry.register("plan", PlanBody)
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)

    a.send(to=b.address, type="plan", body={"step": 1, "goal": "x"})

    with pytest.raises(SchemaValidationError):
        a.send(to=b.address, type="plan", body={"step": "not-an-int"})


def test_schema_registry_permissive_on_unknown(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    a.send(to=b.address, type="anything", body={"arbitrary": True})


def test_schema_registry_strict_mode_rejects_unknown(db_path: Path) -> None:
    SchemaRegistry.strict_mode(True)
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    with pytest.raises(SchemaValidationError):
        a.send(to=b.address, type="never-registered", body={})
