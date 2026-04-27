"""raven — SQLite-backed role-addressable message bus.

Public surface (Phase 1, v0.1.x)::

    from claude_bus import (
        BusClient,
        Message,
        SchemaRegistry,
        ClaudeBusError,
        SchemaValidationError,
        UnknownRoleError,
        UnknownMessageError,
        InvalidTagError,
        init_db,
        register_role_alias,
    )

Quickstart::

    from claude_bus import BusClient

    alice = BusClient(session_id="swarm-1", role="alice", db_path="bus.db")
    bob = BusClient(session_id="swarm-1", role="bob", db_path="bus.db")

    alice.send(to=bob.address, type="plan", body={"step": 1})

    for msg in bob.inbox():
        print(msg.body)
        bob.ack(msg.id)
"""

from __future__ import annotations

__version__ = "0.1.1"

from claude_bus.client import BusClient, Message
from claude_bus.db import init_db
from claude_bus.exceptions import (
    ClaudeBusError,
    InvalidTagError,
    SchemaValidationError,
    UnknownMessageError,
    UnknownRoleError,
)
from claude_bus.schemas import SchemaRegistry
from claude_bus.session import register_role_alias

__all__ = [
    "BusClient",
    "ClaudeBusError",
    "InvalidTagError",
    "Message",
    "SchemaRegistry",
    "SchemaValidationError",
    "UnknownMessageError",
    "UnknownRoleError",
    "__version__",
    "init_db",
    "register_role_alias",
]
