"""Tests for ravend write handlers.  LANE: http-write (raven2-p2).

Every test asserts both the HTTP response AND the raw store (a write
endpoint's test isn't done until the row is proven).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from raven_bus import channels, cursors
from raven_bus import db as db_module
from raven_bus.http import write as write_mod
from raven_bus.http.app import create_app


@pytest.fixture()
def client(db: Path) -> TestClient:
    app = create_app(db)
    return TestClient(app)


def _raw(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# --------------------------------------------------------------------------
# /send
# --------------------------------------------------------------------------


def test_send_happy_path(client: TestClient, db: Path) -> None:
    resp = client.post(
        "/send",
        json={
            "channel": "run/v0/lane",
            "sender": "worker@v0",
            "type": "status",
            "body": {"ok": True},
        },
    )
    assert resp.status_code == 201
    payload = resp.json()
    assert payload["channel"] == "run/v0/lane"
    assert payload["sender"] == "worker@v0"
    assert payload["type"] == "status"
    assert payload["body"] == {"ok": True}
    assert payload["urgency"] == "prompt"

    conn = _raw(db)
    row = conn.execute("SELECT * FROM messages WHERE id = ?", (payload["id"],)).fetchone()
    assert row is not None
    assert row["sender"] == "worker@v0"
    conn.close()


def test_send_with_all_optional_kwargs(client: TestClient, db: Path) -> None:
    first = client.post(
        "/send",
        json={
            "channel": "run/v0/thread",
            "sender": "worker@v0",
            "type": "start",
            "body": {"n": 1},
        },
    ).json()

    resp = client.post(
        "/send",
        json={
            "channel": "run/v0/thread",
            "sender": "worker@v0",
            "type": "reply",
            "body": {"n": 2},
            "urgency": "blocking",
            "tags": ["a", "b"],
            "reply_to": first["id"],
            "expires_in_s": 3600,
        },
    )
    assert resp.status_code == 201
    payload = resp.json()
    assert payload["urgency"] == "blocking"
    assert payload["tags"] == ["a", "b"]
    assert payload["reply_to"] == first["id"]
    assert payload["thread_id"] == first["id"]
    assert payload["expires_at"] is not None

    conn = _raw(db)
    row = conn.execute("SELECT * FROM messages WHERE id = ?", (payload["id"],)).fetchone()
    assert row["tags"] == "a,b"
    assert row["reply_to"] == first["id"]
    conn.close()


def test_send_with_explicit_thread_id(client: TestClient, db: Path) -> None:
    root = client.post(
        "/send",
        json={
            "channel": "run/v0/thread2",
            "sender": "worker@v0",
            "type": "note",
            "body": {},
        },
    ).json()

    resp = client.post(
        "/send",
        json={
            "channel": "run/v0/thread2",
            "sender": "worker@v0",
            "type": "note",
            "body": {},
            "thread_id": root["id"],
        },
    )
    assert resp.status_code == 201
    assert resp.json()["thread_id"] == root["id"]


def test_send_kind_queue_creates_queue_channel(client: TestClient, db: Path) -> None:
    resp = client.post(
        "/send",
        json={
            "channel": "run/v0/q",
            "sender": "worker@v0",
            "type": "task",
            "body": {},
            "kind": "queue",
        },
    )
    assert resp.status_code == 201

    with db_module.connection(db) as c:
        chan = channels.get_channel(c, "run/v0/q")
    assert chan.kind == "queue"


def test_send_unknown_key_400(client: TestClient) -> None:
    resp = client.post(
        "/send",
        json={
            "channel": "run/v0/lane",
            "sender": "worker@v0",
            "type": "status",
            "body": {},
            "bogus": 1,
        },
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_send_missing_required_400(client: TestClient) -> None:
    resp = client.post("/send", json={"channel": "run/v0/lane"})
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_send_malformed_json_400(client: TestClient) -> None:
    resp = client.post(
        "/send",
        content=b"{not json",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_send_body_not_object_400(client: TestClient) -> None:
    resp = client.post("/send", json=[1, 2, 3])
    assert resp.status_code == 400


# --------------------------------------------------------------------------
# /claim
# --------------------------------------------------------------------------


def test_claim_empty_queue_204(client: TestClient, db: Path) -> None:
    conn = db_module.connection(db)
    with conn as c:
        channels.ensure_channel(c, "run/v0/queue", "queue")

    resp = client.post(
        "/claim", json={"channel": "run/v0/queue", "consumer": "worker@v0"}
    )
    assert resp.status_code == 204
    assert resp.content == b""


def test_claim_happy_path_200(client: TestClient, db: Path) -> None:
    client.post(
        "/send",
        json={
            "channel": "run/v0/queue2",
            "sender": "worker@v0",
            "type": "task",
            "body": {"n": 1},
            "kind": "queue",
        },
    )

    resp = client.post(
        "/claim", json={"channel": "run/v0/queue2", "consumer": "consumer@v0"}
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["body"] == {"n": 1}

    conn = _raw(db)
    row = conn.execute(
        "SELECT * FROM claims WHERE message_id = ?", (payload["id"],)
    ).fetchone()
    assert row["consumer"] == "consumer@v0"
    assert row["state"] == "leased"
    conn.close()


def test_claim_two_consumers_yield_different_messages(client: TestClient, db: Path) -> None:
    client.post(
        "/send",
        json={
            "channel": "run/v0/queue3",
            "sender": "worker@v0",
            "type": "task",
            "body": {"n": 1},
            "kind": "queue",
        },
    )
    client.post(
        "/send",
        json={
            "channel": "run/v0/queue3",
            "sender": "worker@v0",
            "type": "task",
            "body": {"n": 2},
            "kind": "queue",
        },
    )

    first = client.post(
        "/claim", json={"channel": "run/v0/queue3", "consumer": "a@v0"}
    )
    second = client.post(
        "/claim", json={"channel": "run/v0/queue3", "consumer": "b@v0"}
    )
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] != second.json()["id"]


def test_claim_lease_s_kwarg(client: TestClient, db: Path) -> None:
    client.post(
        "/send",
        json={
            "channel": "run/v0/queue4",
            "sender": "worker@v0",
            "type": "task",
            "body": {},
            "kind": "queue",
        },
    )
    resp = client.post(
        "/claim",
        json={"channel": "run/v0/queue4", "consumer": "a@v0", "lease_s": 10},
    )
    assert resp.status_code == 200


def test_claim_missing_consumer_400(client: TestClient) -> None:
    resp = client.post("/claim", json={"channel": "run/v0/queue"})
    assert resp.status_code == 400


def test_claim_wrong_kind_409(client: TestClient, db: Path) -> None:
    conn = db_module.connection(db)
    with conn as c:
        channels.ensure_channel(c, "run/v0/broadcast1", "broadcast")

    resp = client.post(
        "/claim", json={"channel": "run/v0/broadcast1", "consumer": "a@v0"}
    )
    assert resp.status_code == 409


def test_claim_unknown_channel_404(client: TestClient) -> None:
    resp = client.post(
        "/claim", json={"channel": "run/v0/nope", "consumer": "a@v0"}
    )
    assert resp.status_code == 404


# --------------------------------------------------------------------------
# /claims/{id}/renew, /done, /release
# --------------------------------------------------------------------------


def _send_and_claim(client: TestClient, channel: str, consumer: str = "a@v0") -> int:
    client.post(
        "/send",
        json={
            "channel": channel,
            "sender": "worker@v0",
            "type": "task",
            "body": {},
            "kind": "queue",
        },
    )
    resp = client.post("/claim", json={"channel": channel, "consumer": consumer})
    return int(resp.json()["id"])


def test_renew_happy_path(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/renew1")
    resp = client.post(f"/claims/{mid}/renew", json={"consumer": "a@v0"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["message_id"] == mid
    assert payload["consumer"] == "a@v0"

    conn = _raw(db)
    row = conn.execute("SELECT * FROM claims WHERE message_id = ?", (mid,)).fetchone()
    assert row["state"] == "leased"
    conn.close()


def test_renew_with_lease_s(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/renew2")
    resp = client.post(f"/claims/{mid}/renew", json={"consumer": "a@v0", "lease_s": 60})
    assert resp.status_code == 200


def test_renew_wrong_consumer_409(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/renew3")
    resp = client.post(f"/claims/{mid}/renew", json={"consumer": "other@v0"})
    assert resp.status_code == 409
    assert resp.json()["error"] == "conflict"


def test_renew_missing_consumer_400(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/renew4")
    resp = client.post(f"/claims/{mid}/renew", json={})
    assert resp.status_code == 400


def test_done_happy_path(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/done1")
    resp = client.post(f"/claims/{mid}/done", json={"consumer": "a@v0"})
    assert resp.status_code == 200
    assert resp.json()["state"] == "done"

    conn = _raw(db)
    row = conn.execute("SELECT * FROM claims WHERE message_id = ?", (mid,)).fetchone()
    assert row["state"] == "done"
    conn.close()


def test_done_idempotent_for_owner(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/done2")
    first = client.post(f"/claims/{mid}/done", json={"consumer": "a@v0"})
    second = client.post(f"/claims/{mid}/done", json={"consumer": "a@v0"})
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["state"] == "done"


def test_done_wrong_consumer_409(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/done3")
    resp = client.post(f"/claims/{mid}/done", json={"consumer": "other@v0"})
    assert resp.status_code == 409


def test_release_happy_path(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/release1")
    resp = client.post(f"/claims/{mid}/release", json={"consumer": "a@v0"})
    assert resp.status_code == 204
    assert resp.content == b""

    conn = _raw(db)
    row = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?", (mid,)
    ).fetchone()
    # Release flips to lapsed/0 rather than deleting: a deleted row became
    # a never-claimed candidate below every process's claim frontier and
    # was permanently hidden (raven2-p2 refute-frontier finding).
    assert row is not None
    assert (row["state"], row["deliveries"]) == ("lapsed", 0)
    conn.close()


def test_release_wrong_consumer_409(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/release2")
    resp = client.post(f"/claims/{mid}/release", json={"consumer": "other@v0"})
    assert resp.status_code == 409


def test_release_missing_consumer_400(client: TestClient, db: Path) -> None:
    mid = _send_and_claim(client, "run/v0/release3")
    resp = client.post(f"/claims/{mid}/release", json={})
    assert resp.status_code == 400


# --------------------------------------------------------------------------
# /ack
# --------------------------------------------------------------------------


def test_ack_happy_path(client: TestClient, db: Path) -> None:
    sent = client.post(
        "/send",
        json={
            "channel": "run/v0/broadcast_ack",
            "sender": "worker@v0",
            "type": "note",
            "body": {},
        },
    ).json()

    resp = client.post(
        "/ack",
        json={
            "channel": "run/v0/broadcast_ack",
            "consumer": "reader@v0",
            "up_to_id": sent["id"],
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["last_ack_id"] == sent["id"]
    assert payload["consumer"] == "reader@v0"

    conn = _raw(db)
    row = conn.execute(
        "SELECT * FROM cursors WHERE consumer = ?", ("reader@v0",)
    ).fetchone()
    assert row["last_ack_id"] == sent["id"]
    conn.close()


def test_ack_monotonic_backwards_is_noop(client: TestClient, db: Path) -> None:
    first = client.post(
        "/send",
        json={
            "channel": "run/v0/broadcast_ack2",
            "sender": "worker@v0",
            "type": "note",
            "body": {},
        },
    ).json()
    second = client.post(
        "/send",
        json={
            "channel": "run/v0/broadcast_ack2",
            "sender": "worker@v0",
            "type": "note",
            "body": {},
        },
    ).json()

    client.post(
        "/ack",
        json={
            "channel": "run/v0/broadcast_ack2",
            "consumer": "reader@v0",
            "up_to_id": second["id"],
        },
    )
    resp = client.post(
        "/ack",
        json={
            "channel": "run/v0/broadcast_ack2",
            "consumer": "reader@v0",
            "up_to_id": first["id"],
        },
    )
    assert resp.status_code == 200
    assert resp.json()["last_ack_id"] == second["id"]


def test_ack_missing_field_400(client: TestClient) -> None:
    resp = client.post("/ack", json={"channel": "run/v0/x", "consumer": "a@v0"})
    assert resp.status_code == 400


def test_ack_wrong_channel_kind_409(client: TestClient, db: Path) -> None:
    conn = db_module.connection(db)
    with conn as c:
        channels.ensure_channel(c, "run/v0/queue_ack", "queue")

    resp = client.post(
        "/ack",
        json={"channel": "run/v0/queue_ack", "consumer": "a@v0", "up_to_id": 1},
    )
    assert resp.status_code == 409


def test_ack_unknown_channel_404(client: TestClient) -> None:
    resp = client.post(
        "/ack",
        json={"channel": "run/v0/nope", "consumer": "a@v0", "up_to_id": 1},
    )
    assert resp.status_code == 404


# --------------------------------------------------------------------------
# /heartbeat
# --------------------------------------------------------------------------


def test_heartbeat_creates_consumer(client: TestClient, db: Path) -> None:
    resp = client.post("/heartbeat", json={"consumer": "worker@v0"})
    assert resp.status_code == 204
    assert resp.content == b""

    conn = _raw(db)
    row = conn.execute(
        "SELECT * FROM consumers WHERE id = ?", ("worker@v0",)
    ).fetchone()
    assert row is not None
    assert row["role"] == "worker"
    assert row["run"] == "v0"
    assert row["last_seen_at"] is not None
    conn.close()


def test_heartbeat_bumps_last_seen_at(client: TestClient, db: Path) -> None:
    client.post("/heartbeat", json={"consumer": "worker@v0"})
    conn = _raw(db)
    first_seen = conn.execute(
        "SELECT last_seen_at FROM consumers WHERE id = ?", ("worker@v0",)
    ).fetchone()["last_seen_at"]
    conn.close()

    client.post("/heartbeat", json={"consumer": "worker@v0"})
    conn = _raw(db)
    second_seen = conn.execute(
        "SELECT last_seen_at FROM consumers WHERE id = ?", ("worker@v0",)
    ).fetchone()["last_seen_at"]
    conn.close()

    assert second_seen >= first_seen


def test_heartbeat_missing_consumer_400(client: TestClient) -> None:
    resp = client.post("/heartbeat", json={})
    assert resp.status_code == 400


def test_heartbeat_bad_grammar_400(client: TestClient) -> None:
    resp = client.post("/heartbeat", json={"consumer": "not-a-valid-consumer"})
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_unused_import_guard() -> None:
    # cursors module is used indirectly via /ack; import kept for the
    # _raw()-style direct-store assertions pattern other tests follow.
    assert cursors.get_cursor is not None


# --------------------------------------------------------------------------
# QA http H9: /send's `kind` is optional and NOT defaulted.
# --------------------------------------------------------------------------


def _send(client: TestClient, channel: str, **extra) -> object:
    return client.post(
        "/send",
        json={"channel": channel, "sender": "p@v0", "type": "job", "body": {}, **extra},
    )


@pytest.mark.parametrize("kind", ["queue", "stream"])
def test_send_without_kind_appends_to_an_existing_non_broadcast_channel(
    client: TestClient, db: Path, kind: str
) -> None:
    """A producer feeding an existing queue/stream without restating
    `kind` got 409: the handler defaulted kind to 'broadcast' AND
    enforced it."""
    with db_module.connection(db) as c:
        channels.ensure_channel(c, "run/v0/work", kind)

    resp = _send(client, "run/v0/work")

    assert resp.status_code == 201
    with db_module.connection(db) as c:
        assert channels.get_channel(c, "run/v0/work").kind == kind
        count = c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    assert count == 1


def test_send_without_kind_creates_an_absent_channel_as_broadcast(
    client: TestClient, db: Path
) -> None:
    assert _send(client, "run/v0/fresh").status_code == 201
    with db_module.connection(db) as c:
        assert channels.get_channel(c, "run/v0/fresh").kind == "broadcast"


def test_send_with_matching_kind_appends(client: TestClient, db: Path) -> None:
    with db_module.connection(db) as c:
        channels.ensure_channel(c, "run/v0/q", "queue")

    assert _send(client, "run/v0/q", kind="queue").status_code == 201


def test_send_with_mismatched_kind_is_409_and_writes_nothing(
    client: TestClient, db: Path
) -> None:
    with db_module.connection(db) as c:
        channels.ensure_channel(c, "run/v0/q", "queue")

    resp = _send(client, "run/v0/q", kind="broadcast")

    assert resp.status_code == 409
    conn = _raw(db)
    assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
    conn.close()


def test_send_with_unknown_kind_is_400_and_creates_nothing(
    client: TestClient, db: Path
) -> None:
    resp = _send(client, "run/v0/k", kind="bogus")

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"
    conn = _raw(db)
    assert conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 0
    conn.close()


# --------------------------------------------------------------------------
# QA http H2: encode inside the transaction — no phantom lease.
# --------------------------------------------------------------------------


def _nest(depth: int) -> list:
    value: list = []
    for _ in range(depth):
        value = [value]
    return value


def test_claim_that_cannot_be_encoded_rolls_the_lease_back(
    client: TestClient, db: Path
) -> None:
    """A legacy row nested past pydantic's serialiser depth (written
    before log.MAX_BODY_DEPTH existed — so raw SQL here) used to be
    LEASED and committed, then fail to encode: the worker got a 400 and
    the lease it never saw dead-lettered the message unseen."""
    with db_module.connection(db) as c:
        chan = channels.ensure_channel(c, "run/v0/legacy", "queue")
        c.execute(
            "INSERT INTO messages (channel_id, sender, type, body) VALUES (?, ?, ?, ?)",
            (chan.id, "p@v0", "job", json.dumps({"tree": _nest(150)})),
        )

    resp = client.post("/claim", json={"channel": "run/v0/legacy", "consumer": "w@v0"})

    assert resp.status_code == 500
    assert resp.json()["error"] == "internal_error"
    conn = _raw(db)
    assert conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
    conn.close()


def test_finite_body_check_does_not_recurse() -> None:
    """The json.dumps-based check raised RecursionError (→ 500) on a
    body deep enough to parse; the iterative walk can't blow the stack."""
    write_mod._finite_body({"x": _nest(100_000)})
    with pytest.raises(ValueError, match="finite"):
        write_mod._finite_body({"x": [*_nest(5), {"y": float("inf")}]})


def test_send_of_a_very_deep_body_is_a_400_envelope(client: TestClient) -> None:
    raw = '{"channel":"run/v0/d","sender":"p@v0","type":"t","body":{"x":%s}}' % (
        "[" * 900 + "]" * 900
    )
    resp = client.post(
        "/send", content=raw.encode(), headers={"content-type": "application/json"}
    )

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


# --------------------------------------------------------------------------
# QA http H3: every failure wears the envelope.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["reply_to", "thread_id"])
def test_send_referencing_a_missing_message_is_404(
    client: TestClient, field: str
) -> None:
    resp = _send(client, "run/v0/r", **{field: 999})

    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"


def test_held_write_lock_is_503_busy(
    client: TestClient, db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lock outlasting the busy timeout was a plain-text 500."""
    monkeypatch.setattr(db_module, "DEFAULT_BUSY_TIMEOUT_S", 0.05)
    holder = sqlite3.connect(str(db), timeout=0)
    holder.execute("BEGIN IMMEDIATE")
    holder.execute("INSERT INTO bus_meta (key, value) VALUES ('qa-lock', '1')")
    try:
        send = _send(client, "run/v0/b")
        beat = client.post("/heartbeat", json={"consumer": "w@v0"})
    finally:
        holder.rollback()
        holder.close()

    for resp in (send, beat):
        assert resp.status_code == 503
        assert resp.json()["error"] == "busy"


def test_write_to_a_foreign_file_is_503_schema_mismatch(tmp_path: Path) -> None:
    foreign = tmp_path / "foreign.db"
    raw = sqlite3.connect(foreign)
    raw.execute("CREATE TABLE unrelated (x)")
    raw.close()

    resp = _send(TestClient(create_app(foreign)), "run/v0/x")

    assert resp.status_code == 503
    assert resp.json()["error"] == "schema_mismatch"


def test_write_to_a_vanished_db_is_503_and_recreates_nothing(
    client: TestClient, db: Path
) -> None:
    for suffix in ("", "-wal", "-shm"):
        Path(f"{db}{suffix}").unlink(missing_ok=True)

    resp = _send(client, "run/v0/x")

    assert resp.status_code == 503
    assert resp.json()["error"] == "unavailable"
    assert not db.exists()


def test_unexpected_store_failure_is_500_internal_error_without_leaking(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    def _boom(*_args, **_kwargs):
        raise KeyError("secret-internal-state")

    monkeypatch.setattr(write_mod.log, "append", _boom)
    with caplog.at_level("ERROR", logger="raven_bus.http"):
        resp = _send(client, "run/v0/x")

    assert resp.status_code == 500
    assert resp.json()["error"] == "internal_error"
    assert "secret-internal-state" not in resp.text
    assert "secret-internal-state" in caplog.text


# --------------------------------------------------------------------------
# QA http H10 / H13: up-front address checks; expires_in_s range.
# --------------------------------------------------------------------------


def test_ack_with_malformed_channel_is_400_not_404(client: TestClient) -> None:
    resp = client.post(
        "/ack", json={"channel": "BAD NAME", "consumer": "a@v0", "up_to_id": 1}
    )

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/claim", {"channel": "run/v0/q", "consumer": "NOPE"}),
        ("/claims/1/done", {"consumer": "NOPE"}),
        ("/send", {"channel": "run/v0/q", "sender": "NOPE", "type": "t", "body": {}}),
    ],
)
def test_malformed_consumer_is_400(client: TestClient, path: str, body: dict) -> None:
    resp = client.post(path, json=body)

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


@pytest.mark.parametrize("seconds", [0, -5, write_mod.MAX_EXPIRES_S + 1])
def test_send_expires_in_s_outside_1_to_30_days_is_400(
    client: TestClient, db: Path, seconds: int
) -> None:
    """Zero/negative wrote a message born expired — invisible to every
    liveness read, i.e. a silent drop."""
    resp = _send(client, "run/v0/e", expires_in_s=seconds)

    assert resp.status_code == 400
    conn = _raw(db)
    assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
    conn.close()


@pytest.mark.parametrize("seconds", [1, write_mod.MAX_EXPIRES_S])
def test_send_expires_in_s_range_bounds_are_inclusive(
    client: TestClient, seconds: int
) -> None:
    resp = _send(client, "run/v0/e", expires_in_s=seconds)

    assert resp.status_code == 201
    assert resp.json()["expires_at"] is not None
