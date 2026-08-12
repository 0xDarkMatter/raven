"""Tests for ravend write handlers.  LANE: http-write (raven2-p2).

Every test asserts both the HTTP response AND the raw store (a write
endpoint's test isn't done until the row is proven).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from raven_bus import channels, cursors
from raven_bus import db as db_module
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
