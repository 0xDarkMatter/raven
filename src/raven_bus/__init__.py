"""raven_bus — local agent-coordination substrate (v2).

Append-only message log partitioned into channels; read-state lives per
channel kind (ADR-001): broadcast → cursors, queue → claims+leases,
stream → none. Addressing is full-string ``<role>@<run>`` on one host DB
(ADR-002). This package replaces the retired v1 import root (ADR-004); the v1 API
survives temporarily in :mod:`raven_bus.compat`.

Wave-0 skeleton note (raven2-p1 run): signatures in this package are
FROZEN for the duration of the build run. Lanes implement bodies; a lane
needing a signature change reports it in its FINAL REPLY instead of
editing another lane's file.
"""

from __future__ import annotations

__version__ = "0.2.0.dev0"

from raven_bus.exceptions import (
    ClaimDeniedError,
    InvalidAddressError,
    InvalidBodyError,
    RavenBusError,
    SchemaMismatchError,
    StoreUnavailableError,
    TeardownBlockedError,
    UnknownChannelError,
    UnknownMessageError,
    WrongChannelKindError,
)
from raven_bus.models import (
    Channel,
    ChannelKind,
    Claim,
    Consumer,
    Cursor,
    Message,
    Urgency,
)

__all__ = [
    "Channel",
    "ChannelKind",
    "Claim",
    "ClaimDeniedError",
    "Consumer",
    "Cursor",
    "InvalidAddressError",
    "InvalidBodyError",
    "Message",
    "RavenBusError",
    "SchemaMismatchError",
    "StoreUnavailableError",
    "TeardownBlockedError",
    "UnknownChannelError",
    "UnknownMessageError",
    "Urgency",
    "WrongChannelKindError",
    "__version__",
]
