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
