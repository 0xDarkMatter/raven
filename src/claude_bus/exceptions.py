"""Exception hierarchy for claude-bus.

All claude-bus errors inherit from :class:`ClaudeBusError`. Validation
errors also subclass :class:`ValueError`, lookup errors subclass
:class:`KeyError`, so callers that don't want to import claude-bus
errors can catch the stdlib parent.
"""

from __future__ import annotations


class ClaudeBusError(Exception):
    """Root of all claude-bus-raised exceptions."""


class SchemaValidationError(ClaudeBusError, ValueError):
    """A message body does not match the registered schema for its type."""


class InvalidTagError(ClaudeBusError, ValueError):
    """One or more tags fail the validation regex."""


class UnknownRoleError(ClaudeBusError, KeyError):
    """Addressed recipient (alias or role:*) has no registered identity."""


class UnknownMessageError(ClaudeBusError, KeyError):
    """Message id does not exist."""


__all__ = [
    "ClaudeBusError",
    "InvalidTagError",
    "SchemaValidationError",
    "UnknownMessageError",
    "UnknownRoleError",
]
