"""Tests for the v1→v2 compat shim (lane: compat).

The v2 siblings (``log``, ``cursors``, ``db``, ``channels``) may be
stubs in this worktree, so these tests are **mapping-dominant**: they
mock the sibling entry points and assert the v1→v2 translation (channel
names, consumer ids, kwarg renaming, status derivation) rather than
store behaviour. ``raven_bus.models`` is frozen + implemented, so v2
``Message`` rows are built for real and fed through the mapper.

A single end-to-end test at the bottom is guarded by a real-store probe
(``pytest.importorskip``-style): it runs only if the siblings happen to
be implemented in this worktree, and is skipped otherwise.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from raven_bus.compat import (
    BusClient,
    Message,
    SchemaRegistry,
)
from raven_bus.exceptions import InvalidAddressError, UnknownMessageError
from raven_bus.models import Cursor
from raven_bus.models import Message as V2Message

# Module paths for patching the v2 siblings the shim calls into.
_LOG = "raven_bus.compat.log"
_CURSORS = "raven_bus.compat.cursors"
_DB = "raven_bus.compat.db"
_CHANNELS = "raven_bus.compat.channels"

_NOW = datetime(2026, 8, 12, 12, 0, 0, tzinfo=UTC)

# Every BusClient construction warns (ADR-004: deprecated from day one).
# That's asserted once, explicitly, in TestDeprecation; everywhere else it
# is expected noise, so filter exactly that message (other warnings still
# surface in the summary).
pytestmark = pytest.mark.filterwarnings(
    "ignore:raven_bus.compat.BusClient is deprecated:DeprecationWarning"
)


def _v2msg(
    *,
    mid: int = 1,
    channel: str = "compat/s1/alice",
    sender: str = "bob@s1",
    type_: str = "ping",
    body: dict[str, Any] | None = None,
    thread_id: int | None = None,
    reply_to: int | None = None,
    tags: list[str] | None = None,
    created_at: datetime = _NOW,
) -> V2Message:
    """Build a real v2 Message row for feeding through _to_public / mocks."""
    return V2Message(
        id=mid,
        channel=channel,
        sender=sender,
        type=type_,
        body={} if body is None else body,
        thread_id=thread_id,
        reply_to=reply_to,
        tags=tags or [],
        created_at=created_at,
    )


@pytest.fixture()
def stub_store() -> Any:
    """Neutralise the v2 store so BusClient.__init__ never hits a stub.

    Patches db.init_db, db.connection, and channels.ensure_channel to
    no-op fakes for the duration of a test. Yields the patchers' targets
    are already active via the context stack."""
    with (
        patch(f"{_DB}.init_db") as init_db,
        patch(f"{_DB}.connection") as connection,
        patch(_CHANNELS) as channels_mod,
    ):
        # db.connection is used as a context manager.
        connection.return_value.__enter__ = MagicMock(return_value=MagicMock())
        connection.return_value.__exit__ = MagicMock(return_value=False)
        yield {"init_db": init_db, "connection": connection, "channels": channels_mod}


# =====================================================================
# Constructor validation matrix (v1 parity + case-folding tightening)
# =====================================================================


class TestConstructorValidation:
    """v1 raised ValueError on bad (session_id, role); the shim keeps that
    and additionally lowercases both fields (v2 grammar is lowercase-only)."""

    @pytest.mark.usefixtures("stub_store")
    def test_valid_lowercase_identity_maps_to_v2_atoms(self) -> None:
        c = BusClient(session_id="S1", role="Alice")
        # Case folded before mapping (tightening vs v1).
        assert c.role == "alice"
        assert c.session_id == "s1"
        assert c.address == "alice:s1"
        assert c.consumer_id == "alice@s1"
        assert c.channel == "compat/s1/alice"

    @pytest.mark.usefixtures("stub_store")
    def test_empty_session_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="session_id"):
            BusClient(session_id="", role="alice")

    @pytest.mark.usefixtures("stub_store")
    def test_whitespace_session_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="session_id"):
            BusClient(session_id="   ", role="alice")

    @pytest.mark.usefixtures("stub_store")
    def test_non_str_session_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="session_id"):
            BusClient(session_id=123, role="alice")  # type: ignore[arg-type]

    @pytest.mark.usefixtures("stub_store")
    def test_empty_role_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="role"):
            BusClient(session_id="s1", role="")

    @pytest.mark.usefixtures("stub_store")
    def test_non_str_role_raises_valueerror(self) -> None:
        with pytest.raises(ValueError, match="role"):
            BusClient(session_id="s1", role=None)  # type: ignore[arg-type]

    @pytest.mark.usefixtures("stub_store")
    def test_role_with_colon_raises_valueerror(self) -> None:
        # ':' is the v1 address separator — v1 rejected it; so do we.
        with pytest.raises(ValueError, match="':'"):
            BusClient(session_id="s1", role="a:b")

    @pytest.mark.usefixtures("stub_store")
    def test_bad_atom_raises_invalidaddress_which_is_valueerror(self) -> None:
        # After case-folding, an atom still violating the grammar (e.g.
        # uppercase survived because it's non-ASCII, or illegal chars)
        # surfaces as InvalidAddressError — a ValueError subclass, so v1
        # callers' ``except ValueError`` still catches it.
        with pytest.raises(InvalidAddressError) as excinfo:
            BusClient(session_id="s1", role="bad role")
        assert isinstance(excinfo.value, ValueError)

    @pytest.mark.usefixtures("stub_store")
    def test_init_db_and_ensure_channel_invoked(self, stub_store: dict) -> None:
        # Constructor must provision the store + own broadcast channel.
        BusClient(session_id="s1", role="alice")
        stub_store["init_db"].assert_called_once()
        call = stub_store["channels"].ensure_channel.call_args
        # (conn, name) positional, kind kwarg — broadcast for v1 inboxes.
        assert call.args[1] == "compat/s1/alice"
        assert call.kwargs["kind"] == "broadcast"


# =====================================================================
# send(): v1 address → v2 channel/consumer + kwarg translation
# =====================================================================


class TestSendMapping:
    """send() must parse the v1 `to`, target the recipient compat channel,
    translate kwargs, smuggle task_id, and return a v1-shaped Message."""

    @pytest.mark.usefixtures("stub_store")
    def test_send_targets_recipient_compat_channel_and_sender_consumer(
        self,
    ) -> None:
        c = BusClient(session_id="s1", role="alice")
        with patch(_LOG) as log_mod:
            log_mod.append.return_value = _v2msg(
                channel="compat/s1/bob", sender="alice@s1"
            )
            c.send(to="bob:s1", type="greeting", body={"text": "hi"})

        kwargs = log_mod.append.call_args.kwargs
        assert kwargs["channel"] == "compat/s1/bob"      # recipient channel
        assert kwargs["sender"] == "alice@s1"            # own consumer id
        assert kwargs["type"] == "greeting"
        assert kwargs["body"] == {"text": "hi"}
        assert kwargs["urgency"] == "prompt"             # default passes through

    @pytest.mark.usefixtures("stub_store")
    def test_send_correlation_id_maps_to_thread_id(self) -> None:
        c = BusClient(session_id="s1", role="alice")
        with patch(_LOG) as log_mod:
            log_mod.append.return_value = _v2msg(channel="compat/s1/bob")
            c.send(
                to="bob:s1", type="t", body={}, correlation_id=42, reply_to=7
            )
        kwargs = log_mod.append.call_args.kwargs
        assert kwargs["thread_id"] == 42                  # correlation→thread
        assert kwargs["reply_to"] == 7                    # reply_to verbatim

    @pytest.mark.usefixtures("stub_store")
    def test_send_task_id_smuggled_into_body(self) -> None:
        # v2 has no task_id column → carried under __task_id__ in body.
        c = BusClient(session_id="s1", role="alice")
        with patch(_LOG) as log_mod:
            log_mod.append.return_value = _v2msg(channel="compat/s1/bob")
            c.send(to="bob:s1", type="t", body={"x": 1}, task_id="job-9")
        kwargs = log_mod.append.call_args.kwargs
        assert kwargs["body"] == {"x": 1, "__task_id__": "job-9"}

    @pytest.mark.usefixtures("stub_store")
    def test_send_task_id_absent_leaves_body_clean(self) -> None:
        c = BusClient(session_id="s1", role="alice")
        with patch(_LOG) as log_mod:
            log_mod.append.return_value = _v2msg(channel="compat/s1/bob")
            c.send(to="bob:s1", type="t", body={"x": 1})
        assert log_mod.append.call_args.kwargs["body"] == {"x": 1}

    @pytest.mark.usefixtures("stub_store")
    def test_send_normalises_case_in_to_address(self) -> None:
        # v1 `to` also gets the lowercase tightening before channel build.
        c = BusClient(session_id="s1", role="alice")
        with patch(_LOG) as log_mod:
            log_mod.append.return_value = _v2msg(channel="compat/s1/bob")
            c.send(to="BOB:S1", type="t", body={})
        assert log_mod.append.call_args.kwargs["channel"] == "compat/s1/bob"

    @pytest.mark.usefixtures("stub_store")
    def test_send_returns_v1_shape_unread(self) -> None:
        c = BusClient(session_id="s1", role="alice")
        with patch(_LOG) as log_mod:
            log_mod.append.return_value = _v2msg(
                mid=5,
                channel="compat/s1/bob",
                sender="alice@s1",
                type_="greeting",
                body={"text": "hi"},
                thread_id=3,
                reply_to=2,
            )
            msg = c.send(to="bob:s1", type="greeting", body={"text": "hi"})
        assert msg.id == 5
        assert msg.status == "unread"                     # just-sent → unread
        assert msg.sender == "alice:s1"                   # consumer → v1 addr
        assert msg.recipient == "bob:s1"                  # from channel name
        assert msg.recipient_role == "bob"
        assert msg.recipient_session == "s1"
        assert msg.session_id == "s1"                     # sender's run
        assert msg.correlation_id == 3                    # thread→correlation
        assert msg.reply_to == 2
        assert msg.type == "greeting"
        assert msg.body == {"text": "hi"}

    @pytest.mark.usefixtures("stub_store")
    @pytest.mark.parametrize(
        "bad_to",
        ["bobsession", "bob:", ":s1", ":", ""],
    )
    def test_send_bad_to_raises_valueerror(self, bad_to: str) -> None:
        c = BusClient(session_id="s1", role="alice")
        with pytest.raises(ValueError):
            c.send(to=bad_to, type="t", body={})


# =====================================================================
# inbox / read / ack status transitions
# =====================================================================


class TestInboxReadAck:
    """inbox = pending (unread); read derives status from the cursor; ack
    advances the cursor (cursor-jump narrowing)."""

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_calls_pending_on_own_channel_consumer(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = [
                _v2msg(mid=1, channel="compat/s1/bob", sender="alice@s1"),
                _v2msg(mid=2, channel="compat/s1/bob", sender="alice@s1"),
            ]
            msgs = c.inbox()
        call = cur_mod.pending.call_args
        # (conn, consumer, channel) positional, limit kwarg — consumer +
        # channel are the OWN identity.
        assert call.args[1] == "bob@s1"
        assert call.args[2] == "compat/s1/bob"
        assert call.kwargs["limit"] == 100
        assert len(msgs) == 2
        # pending → all unread, ids preserved, sender reconstructed.
        assert all(m.status == "unread" for m in msgs)
        assert [m.id for m in msgs] == [1, 2]
        assert msgs[0].sender == "alice:s1"
        assert msgs[0].recipient == "bob:s1"

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_foreign_sender_falls_back_to_raw_string(self) -> None:
        """A row whose sender isn't a valid '<role>@<run>' consumer id (a
        foreign producer, not sent via BusClient) still surfaces — with
        the raw string preserved as ``sender`` — instead of raising."""
        c = BusClient(session_id="s1", role="bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = [
                _v2msg(mid=1, channel="compat/s1/bob", sender="not-a-consumer-id"),
            ]
            msgs = c.inbox()
        assert msgs[0].sender == "not-a-consumer-id"

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_non_compat_channel_falls_back_to_own_identity(self) -> None:
        """A row on a channel that isn't ``compat/<session>/<role>`` shaped
        (defensive: only compat channels are ever read here in practice)
        falls back to the reader's own identity as the recipient."""
        c = BusClient(session_id="s1", role="bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = [
                _v2msg(mid=1, channel="not/compat/shaped", sender="alice@s1"),
            ]
            msgs = c.inbox()
        assert msgs[0].recipient == "bob:s1"
        assert msgs[0].recipient_role == "bob"
        assert msgs[0].recipient_session == "s1"

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_max_passes_through_as_limit(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = []
            c.inbox(max=7)
        assert cur_mod.pending.call_args.kwargs["limit"] == 7

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_role_mismatch_raises(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with pytest.raises(ValueError, match="own role"):
            c.inbox(role="eve")

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_strips_task_id_from_body(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = [
                _v2msg(
                    mid=1,
                    channel="compat/s1/bob",
                    body={"x": 1, "__task_id__": "j1"},
                )
            ]
            msgs = c.inbox()
        assert msgs[0].body == {"x": 1}                  # task_id stripped out
        assert msgs[0].task_id == "j1"                   # surfaced on the msg

    @pytest.mark.usefixtures("stub_store")
    def test_read_unread_when_id_above_cursor(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(
                mid=5, channel="compat/s1/bob", sender="alice@s1"
            )
            cur_mod.get_cursor.return_value = Cursor(
                consumer="bob@s1",
                channel="compat/s1/bob",
                last_ack_id=2,
                updated_at=_NOW,
            )
            msg = c.read(5)
        assert msg.id == 5
        assert msg.status == "unread"                     # 5 > 2
        assert msg.read_at is None

    @pytest.mark.usefixtures("stub_store")
    def test_read_read_when_id_at_or_below_cursor(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(
                mid=2, channel="compat/s1/bob", sender="alice@s1"
            )
            cur_mod.get_cursor.return_value = Cursor(
                consumer="bob@s1",
                channel="compat/s1/bob",
                last_ack_id=5,
                updated_at=_NOW,
            )
            msg = c.read(2)
        assert msg.status == "read"                       # 2 <= 5
        assert msg.read_at == _NOW

    @pytest.mark.usefixtures("stub_store")
    def test_read_no_cursor_treated_as_unread(self) -> None:
        # Never acked → cursor None → last_ack_id 0 → id always unread.
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(
                mid=1, channel="compat/s1/bob", sender="alice@s1"
            )
            cur_mod.get_cursor.return_value = None
            msg = c.read(1)
        assert msg.status == "unread"

    @pytest.mark.usefixtures("stub_store")
    def test_read_propagates_unknown_message_error(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.side_effect = UnknownMessageError("nope")
            cur_mod.get_cursor.return_value = None
            with pytest.raises(UnknownMessageError):
                c.read(99)

    @pytest.mark.usefixtures("stub_store")
    def test_ack_advances_cursor_up_to_id(self) -> None:
        # NARROWING: ack is cursor-jump (acks everything ≤ id).
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(mid=7, channel="compat/s1/bob")
            c.ack(7)
        cur_mod.ack.assert_called_once()
        call = cur_mod.ack.call_args
        # (conn, consumer, channel) positional, up_to_id kwarg.
        assert call.args[1] == "bob@s1"
        assert call.args[2] == "compat/s1/bob"
        assert call.kwargs["up_to_id"] == 7

    @pytest.mark.usefixtures("stub_store")
    def test_ack_unknown_message_raises_before_cursor_move(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.side_effect = UnknownMessageError("missing")
            with pytest.raises(UnknownMessageError):
                c.ack(404)
        cur_mod.ack.assert_not_called()                  # cursor untouched

    @pytest.mark.usefixtures("stub_store")
    def test_ack_foreign_channel_id_is_noop(self) -> None:
        # Ids are global: acking a message on ANOTHER client's channel
        # (here one bob sent to alice) must not jump bob's own cursor.
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(
                mid=6, channel="compat/s1/alice", sender="bob@s1"
            )
            assert c.ack(6) is None                      # no error (v1 parity)
        cur_mod.ack.assert_not_called()

    @pytest.mark.usefixtures("stub_store")
    def test_read_foreign_message_uses_recipient_cursor(self) -> None:
        # A message bob SENT lives on alice's channel: its status is
        # alice's read-state there, never bob's own-channel cursor.
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(
                mid=6, channel="compat/s1/alice", sender="bob@s1"
            )
            cur_mod.get_cursor.return_value = None
            msg = c.read(6)
        assert cur_mod.get_cursor.call_args.args[1:] == ("alice@s1", "compat/s1/alice")
        assert msg.status == "unread"

    @pytest.mark.usefixtures("stub_store")
    def test_read_non_compat_channel_uses_own_consumer(self) -> None:
        # A native v2 channel has no v1 recipient: fall back to this
        # client's consumer on THAT channel.
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_LOG) as log_mod,
            patch(_CURSORS) as cur_mod,
        ):
            log_mod.read_by_id.return_value = _v2msg(mid=3, channel="run/s1/control")
            cur_mod.get_cursor.return_value = None
            c.read(3)
        assert cur_mod.get_cursor.call_args.args[1:] == ("bob@s1", "run/s1/control")

    @pytest.mark.usefixtures("stub_store")
    @pytest.mark.parametrize("role", ["Bob", "BOB", "bob", "Bob:S1", "bob:s1"])
    def test_inbox_role_is_case_folded_like_constructor(self, role: str) -> None:
        # BusClient("S1", "Bob") folds to bob:s1; inbox(role=...) must
        # accept the same spellings the constructor did.
        c = BusClient(session_id="S1", role="Bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = []
            assert c.inbox(role=role) == []

    @pytest.mark.usefixtures("stub_store")
    def test_inbox_non_str_role_raises(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with pytest.raises(ValueError, match="own role"):
            c.inbox(role=7)  # type: ignore[arg-type]


# =====================================================================
# subscribe: ack-before-yield + clean cancellation
# =====================================================================


class TestSubscribe:
    @pytest.mark.usefixtures("stub_store")
    def test_subscribe_acks_before_yield(self) -> None:
        # Each message is acked (cursor advanced) BEFORE it is yielded —
        # v1's at-most-once crash semantics.
        c = BusClient(session_id="s1", role="bob")
        msgs = [
            _v2msg(mid=1, channel="compat/s1/bob", sender="alice@s1"),
            _v2msg(mid=2, channel="compat/s1/bob", sender="alice@s1"),
        ]
        with (
            patch(_CURSORS) as cur_mod,
            patch.object(c, "ack") as ack_mock,
        ):
            cur_mod.pending.return_value = msgs
            # Stop the loop after one poll by cancelling via a sentinel:
            # make the second poll raise to break out.
            calls = {"n": 0}

            async def drive() -> list[Message]:
                out: list[Message] = []
                async for m in c.subscribe(poll_interval_s=0):
                    out.append(m)
                    calls["n"] += 1
                    if calls["n"] == 2:
                        raise _StopLoop
                return out

            with pytest.raises(_StopLoop):
                import asyncio

                asyncio.run(drive())

        # ack called once per yielded message, in id order, before yield.
        assert ack_mock.call_count == 2
        assert [c.args[0] for c in ack_mock.call_args_list] == [1, 2]

    @pytest.mark.usefixtures("stub_store")
    def test_subscribe_yields_messages_as_read(self) -> None:
        c = BusClient(session_id="s1", role="bob")
        with (
            patch(_CURSORS) as cur_mod,
            patch.object(c, "ack"),
        ):
            cur_mod.pending.return_value = [
                _v2msg(mid=1, channel="compat/s1/bob", sender="alice@s1"),
            ]

            async def drive() -> Message | None:
                async for m in c.subscribe(poll_interval_s=0):
                    return m
                return None

            import asyncio

            out = asyncio.run(drive())
        assert out is not None
        assert out.status == "read"                       # yielded as read

    @pytest.mark.usefixtures("stub_store")
    def test_subscribe_role_mismatch_raises(self) -> None:
        c = BusClient(session_id="s1", role="bob")

        async def drive() -> None:
            async for _ in c.subscribe(role="eve", poll_interval_s=0):
                pass

        import asyncio

        with pytest.raises(ValueError, match="own role"):
            asyncio.run(drive())

    @pytest.mark.usefixtures("stub_store")
    def test_subscribe_role_is_case_folded(self) -> None:
        c = BusClient(session_id="Demo", role="Carol")
        with (
            patch(_CURSORS) as cur_mod,
            patch.object(c, "ack"),
        ):
            cur_mod.pending.return_value = [
                _v2msg(mid=1, channel="compat/demo/carol", sender="alice@demo"),
            ]

            async def drive() -> Message:
                async for m in c.subscribe(role="Carol:Demo", poll_interval_s=0):
                    return m
                raise AssertionError("subscribe yielded nothing")

            import asyncio

            assert asyncio.run(drive()).id == 1

    @pytest.mark.usefixtures("stub_store")
    def test_subscribe_cancellation_propagates(self) -> None:
        # An empty inbox + sleep that gets cancelled must surface
        # CancelledError, not swallow it.
        c = BusClient(session_id="s1", role="bob")
        with patch(_CURSORS) as cur_mod:
            cur_mod.pending.return_value = []  # always empty → sleeps forever

            import asyncio

            async def drive() -> None:
                task = asyncio.ensure_future(
                    anext(c.subscribe(poll_interval_s=10))
                )
                await asyncio.sleep(0)  # let it enter the sleep
                task.cancel()
                await task

            with pytest.raises(asyncio.CancelledError):
                asyncio.run(drive())


class _StopLoop(Exception):
    """Sentinel to break out of the infinite subscribe loop in tests."""


# =====================================================================
# Deprecation: every construction warns (ADR-004)
# =====================================================================


class TestDeprecation:
    @pytest.mark.usefixtures("stub_store")
    def test_construction_emits_deprecation_warning_at_caller(self) -> None:
        with pytest.warns(DeprecationWarning, match="raven_bus.compat.BusClient") as rec:
            BusClient(session_id="s1", role="alice")
        assert len(rec) == 1                             # once per construction
        # stacklevel=2 -> attributed to THIS file (the caller), which is
        # what keeps it hidden by default outside __main__.
        assert rec[0].filename == __file__


# =====================================================================
# SchemaRegistry: permissive no-op surface
# =====================================================================


class TestSchemaRegistry:
    def test_validate_returns_body_unchanged(self) -> None:
        body = {"a": 1, "b": [2, 3]}
        assert SchemaRegistry.validate("anything", body) == body
        # Returns the same value (permissive — no enforcement).
        assert SchemaRegistry.validate("unregistered", {}) == {}

    def test_register_unregister_strict_mode_are_noops(self) -> None:
        # All three accept any input, store nothing, return None.
        from pydantic import BaseModel

        class M(BaseModel):
            x: int

        assert SchemaRegistry.register("t", M) is None
        assert SchemaRegistry.unregister("t") is None
        assert SchemaRegistry.strict_mode(True) is None
        # Strict mode does NOT flip behaviour — validate still permissive.
        assert SchemaRegistry.validate("t", {"x": 1}) == {"x": 1}
        # And an unregistered type under "strict" still passes.
        assert SchemaRegistry.validate("never_registered", {"y": 2}) == {"y": 2}


# =====================================================================
# End-to-end: only when the v2 store siblings are actually implemented
# =====================================================================


def _store_implemented() -> bool:
    """True if the v2 sibling bodies are no longer stubs in this worktree."""
    from raven_bus import cursors, log

    try:
        import inspect

        src = inspect.getsource(log.append)
        if "NotImplementedError" in src:
            return False
        src = inspect.getsource(cursors.pending)
        return "NotImplementedError" not in src
    except (OSError, TypeError):
        return False


@pytest.mark.skipif(
    not _store_implemented(),
    reason="v2 store siblings are stubs in this worktree; mapping is covered by mocks above",
)
def test_e2e_send_inbox_ack_read(tmp_path: Path) -> None:
    """Real-store round-trip: send → inbox → ack → read, v1-shaped end to end.
    Skipped unless log/cursors are implemented here."""
    db_path = tmp_path / "bus.db"
    alice = BusClient(session_id="s1", role="alice", db_path=db_path)
    bob = BusClient(session_id="s1", role="bob", db_path=db_path)

    sent = alice.send(
        to=bob.address,
        type="greeting",
        body={"text": "hello, bob"},
        correlation_id=None,
    )
    assert sent.sender == "alice:s1"
    assert sent.recipient == "bob:s1"
    assert sent.status == "unread"

    inbox = bob.inbox()
    assert len(inbox) == 1
    assert inbox[0].body == {"text": "hello, bob"}

    bob.ack(inbox[0].id)
    assert bob.inbox() == []                               # acked → no longer pending

    reread = bob.read(sent.id)
    assert reread.status == "read"                         # cursor now past id


@pytest.mark.skipif(
    not _store_implemented(),
    reason="v2 store siblings are stubs in this worktree; mapping is covered by mocks above",
)
def test_e2e_ack_of_foreign_id_keeps_own_unread(tmp_path: Path) -> None:
    """QA regression: bob has m5 unread; bob sends m6 to alice; bob.ack(m6)
    used to jump bob's cursor to 6 and silently lose m5."""
    db_path = tmp_path / "bus.db"
    alice = BusClient(session_id="s1", role="alice", db_path=db_path)
    bob = BusClient(session_id="s1", role="bob", db_path=db_path)

    m5 = alice.send(to=bob.address, type="t", body={"n": 5})
    m6 = bob.send(to=alice.address, type="t", body={"n": 6})
    assert m6.id > m5.id

    bob.ack(m6.id)                                          # foreign id: no-op
    assert [m.id for m in bob.inbox()] == [m5.id]           # m5 NOT lost
    assert [m.id for m in alice.inbox()] == [m6.id]         # alice untouched
    assert bob.read(m6.id).status == "unread"               # alice hasn't read it

    alice.ack(m6.id)
    assert bob.read(m6.id).status == "read"                 # recipient's state
