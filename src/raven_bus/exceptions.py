"""Exception hierarchy for raven_bus.

Flat and small on purpose: every error a caller can act on gets its own
class; everything else raises the base. CLI maps these to one-line
``error: ...`` messages with semantic exit codes (see cli/_common.py).
"""

from __future__ import annotations


class RavenBusError(Exception):
    """Base class for all raven_bus errors."""


class InvalidAddressError(RavenBusError, ValueError):
    """A consumer id or channel name violates the ADR-002 grammar."""


class UnknownChannelError(RavenBusError):
    """Channel name does not exist (and auto-create was not requested)."""


class UnknownMessageError(RavenBusError):
    """Message id does not exist."""


class ClaimDeniedError(RavenBusError):
    """Claim/renew/complete attempted by a consumer that does not hold
    the lease (or the message is not claimable)."""


class WrongChannelKindError(RavenBusError):
    """Operation not valid for this channel kind (e.g. claim on a
    broadcast channel, cursor-ack on a queue)."""


class TeardownBlockedError(RavenBusError):
    """``db.teardown_run`` refused: messages OUTSIDE the run reference
    (reply_to/thread_id) messages inside it, and the schema's foreign
    keys forbid orphaning them. Nothing was deleted.

    ``blockers`` holds the first few ``(message_id, channel_name)``
    referencing messages (id order); ``total`` is the full count."""

    def __init__(
        self, message: str, *, blockers: list[tuple[int, str]], total: int
    ) -> None:
        super().__init__(message)
        self.blockers = blockers
        self.total = total


class InvalidBodyError(RavenBusError, ValueError):
    """A message body the store refuses to write — today, one nested
    deeper than ``log.MAX_BODY_DEPTH`` (readers could not re-serialise
    it). A ``ValueError`` like :class:`InvalidAddressError`: bad input."""
