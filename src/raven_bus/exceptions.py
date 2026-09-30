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


class SchemaMismatchError(RavenBusError, RuntimeError):
    """The file at the DB path is not a raven v2 store this code can use:
    stamped with another ``schema_version``, or a SQLite file holding
    tables raven did not create and no stamp. raven never migrates or
    adopts such files. Also a ``RuntimeError`` because ``init_db`` raised
    bare RuntimeError for the foreign-version case before this class
    existed — callers catching that keep working."""


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


class StoreUnavailableError(RavenBusError):
    """The DB file a caller said must ALREADY exist is missing or cannot
    be opened (``db.connection(create=False)`` / ``db.probe``). Those
    callers — ravend, whose ``raven serve`` preflight created the store —
    must never silently create an empty, schema-less file in its place."""


class InvalidDbPathError(RavenBusError, ValueError):
    """``RAVEN_DB`` is not an absolute path. The variable is inherited by
    every child process, so a relative value resolved against each one's
    cwd and split one run across several DBs - the v1 per-cwd bug ADR-002
    removed (QA S13). An explicit ``db_path`` / ``--db`` stays
    cwd-relative: it is a per-invocation file argument, not inherited."""


class InvalidBodyError(RavenBusError, ValueError):
    """A message body the store refuses to write — today, one nested
    deeper than ``log.MAX_BODY_DEPTH`` (readers could not re-serialise
    it). A ``ValueError`` like :class:`InvalidAddressError`: bad input."""
