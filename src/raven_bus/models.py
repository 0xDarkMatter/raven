"""Pydantic models + address grammar for raven_bus.

FROZEN INTERFACES (wave-0, run raven2-p1) — lanes import, never edit.

ADR-002 grammar (owned there; restated here as the enforcement site):

- name atom:      ``[a-z0-9][a-z0-9._-]*``  (roles, runs, channel segments)
- consumer id:    ``<role>@<run>``          (full string, no derived hash)
- channel name:   atoms joined by ``/``     (e.g. ``run/v0-2/lane/3``)
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ChannelKind = Literal["broadcast", "queue", "stream"]
Urgency = Literal["blocking", "prompt", "fyi"]

URGENCY_RANK: dict[str, int] = {"blocking": 0, "prompt": 1, "fyi": 2}

_ATOM_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_TAG_RE = re.compile(r"^[a-zA-Z0-9._-]{1,32}$")


def validate_atom(value: str, *, what: str) -> str:
    """Return ``value`` if it matches the ADR-002 atom grammar.

    Raises :class:`raven_bus.exceptions.InvalidAddressError` naming
    ``what`` (e.g. "role", "run", "channel segment") otherwise.
    """
    from raven_bus.exceptions import InvalidAddressError

    if not isinstance(value, str) or not _ATOM_RE.match(value):
        raise InvalidAddressError(
            f"{what} {value!r} must match [a-z0-9][a-z0-9._-]* (ADR-002)"
        )
    return value


def parse_consumer_id(consumer: str) -> tuple[str, str]:
    """Split ``<role>@<run>`` into ``(role, run)``, validating both atoms.

    Raises :class:`InvalidAddressError` on missing/extra ``@`` or bad atoms.
    """
    from raven_bus.exceptions import InvalidAddressError

    if not isinstance(consumer, str) or consumer.count("@") != 1:
        raise InvalidAddressError(
            f"consumer id {consumer!r} must be '<role>@<run>' with exactly one '@'"
        )
    role, run = consumer.split("@", 1)
    return validate_atom(role, what="role"), validate_atom(run, what="run")


def format_consumer_id(role: str, run: str) -> str:
    """Inverse of :func:`parse_consumer_id`; validates both atoms."""
    return f"{validate_atom(role, what='role')}@{validate_atom(run, what='run')}"


def validate_channel_name(name: str) -> str:
    """Return ``name`` if every ``/``-separated segment is a valid atom.

    Raises :class:`InvalidAddressError` otherwise. Empty segments
    (leading/trailing/double slash) are invalid.
    """
    from raven_bus.exceptions import InvalidAddressError

    if not isinstance(name, str) or not name:
        raise InvalidAddressError(f"channel name {name!r} must be a non-empty string")
    for segment in name.split("/"):
        validate_atom(segment, what="channel segment")
    return name


def validate_tags(tags: list[str] | None) -> list[str]:
    """Return cleaned tag list; raise :class:`InvalidAddressError` on a
    tag not matching ``^[a-zA-Z0-9._-]{1,32}$``. ``None`` → ``[]``."""
    from raven_bus.exceptions import InvalidAddressError

    if not tags:
        return []
    for tag in tags:
        if not _TAG_RE.match(tag):
            raise InvalidAddressError(
                f"tag {tag!r} does not match ^[a-zA-Z0-9._-]{{1,32}}$"
            )
    return list(tags)


class Channel(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    name: str
    kind: ChannelKind
    retention_s: int | None = None
    max_deliveries: int = 3
    created_at: datetime


class Message(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    channel: str                     # channel NAME (resolved), not id
    sender: str                      # consumer id '<role>@<run>'
    type: str
    urgency: Urgency = "prompt"
    body: dict[str, Any]
    tags: list[str] = Field(default_factory=list)
    reply_to: int | None = None
    thread_id: int | None = None
    expires_at: datetime | None = None
    created_at: datetime


class Cursor(BaseModel):
    model_config = ConfigDict(frozen=True)

    consumer: str
    channel: str
    last_ack_id: int
    updated_at: datetime


class Claim(BaseModel):
    model_config = ConfigDict(frozen=True)

    message_id: int
    consumer: str
    state: Literal["leased", "lapsed", "done", "dead"]
    deliveries: int
    lease_until: datetime
    updated_at: datetime


class Consumer(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str                          # '<role>@<run>'
    role: str
    run: str
    kind: Literal["agent", "orchestrator", "observer"] = "agent"
    last_seen_at: datetime | None = None


class SweepResult(BaseModel):
    """Counts returned by db.sweep() — expiry + lease reaping (ADR-001)."""

    model_config = ConfigDict(frozen=True)

    expired: int = 0
    requeued: int = 0
    dead_lettered: int = 0


__all__ = [
    "URGENCY_RANK",
    "Channel",
    "ChannelKind",
    "Claim",
    "Consumer",
    "Cursor",
    "Message",
    "SweepResult",
    "Urgency",
    "format_consumer_id",
    "parse_consumer_id",
    "validate_atom",
    "validate_channel_name",
    "validate_tags",
]
