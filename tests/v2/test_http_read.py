"""Tests for ravend GET handlers.  LANE: http-read (raven2-p2).

Drives the real app (``create_app``) via Starlette's TestClient against
an isolated tmp DB, seeding data through the REAL store modules
(``log.append`` / ``channels.ensure_channel`` / ``cursors.ack``) — the
store is fully implemented, so no module stubs are needed. Covers every
endpoint's happy path, each documented error status, percent-encoded
channel names (ADR-002), and the GET write-free guarantee.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from starlette.testclient import TestClient

import raven_bus
from raven_bus import channels, cursors, db, log
from raven_bus.http.app import create_app

# A channel name containing '/' (ADR-002): percent-encoded on the wire,
# delivered decoded by starlette's {name:path} converter.
SLASHY = "run/x/lane/3"


@pytest.fixture()
def client(db: Path) -> TestClient:
    """App + client bound to the isolated, already-initialised tmp DB."""
    return TestClient(create_app(db))


# --------------------------------------------------------------------------- #
# helpers — seed through the real store modules inside one connection.
# --------------------------------------------------------------------------- #
def _append(
    db_path: Path,
    channel: str,
    *,
    type_: str = "note",
    body: dict | None = None,
    sender: str = "sender@run-x",
    expires_in_s: int | None = None,
) -> int:
    with db.connection(db_path) as conn:
        msg = log.append(
            conn,
            channel=channel,
            sender=sender,
            type=type_,
            body=body or {},
            expires_in_s=expires_in_s,
        )
    return msg.id


def _ensure(db_path: Path, name: str, kind: str = "broadcast") -> None:
    with db.connection(db_path) as conn:
        channels.ensure_channel(conn, name, kind=kind)


def _ack(db_path: Path, consumer: str, channel: str, up_to_id: int) -> None:
    with db.connection(db_path) as conn:
        cursors.ack(conn, consumer, channel, up_to_id)


def _count_rows(db_path: Path, table: str, where: str = "") -> int:
    with db.connection(db_path) as conn:
        q = f"SELECT COUNT(*) FROM {table}"
        if where:
            q += f" WHERE {where}"
        return int(conn.execute(q).fetchone()[0])


# --------------------------------------------------------------------------- #
# /health
# --------------------------------------------------------------------------- #
def test_health_ok(client: TestClient, db: Path):
    resp = client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["version"] == raven_bus.__version__
    assert body["schema"] == raven_bus.db.SCHEMA_VERSION
    assert body["db"] == str(db)


def test_health_inits_missing_db(tmp_path: Path):
    """The one sanctioned read-time write: init_db creates a missing DB
    (mirrors `raven doctor`)."""
    db._reset_init_cache()
    missing = tmp_path / "fresh.db"
    assert not missing.exists()
    with TestClient(create_app(missing)) as c:
        resp = c.get("/health")

    assert resp.status_code == 200
    assert missing.exists()  # init_db created it


# --------------------------------------------------------------------------- #
# /channels
# --------------------------------------------------------------------------- #
def test_list_channels_empty(client: TestClient):
    resp = client.get("/channels")

    assert resp.status_code == 200
    assert resp.json() == {"channels": []}


def test_list_channels_returns_all_json_mode(client: TestClient, db: Path):
    _ensure(db, "run/a")
    _ensure(db, "run/b")

    resp = client.get("/channels")

    assert resp.status_code == 200
    names = [c["name"] for c in resp.json()["channels"]]
    assert names == ["run/a", "run/b"]
    # model_dump(mode="json") renders datetimes as ISO strings, not objects.
    assert isinstance(resp.json()["channels"][0]["created_at"], str)


def test_list_channels_prefix_filter(client: TestClient, db: Path):
    _ensure(db, "run/a")
    _ensure(db, "run/b")
    _ensure(db, "other/c")

    resp = client.get("/channels", params={"prefix": "run/"})

    names = [c["name"] for c in resp.json()["channels"]]
    assert names == ["run/a", "run/b"]


def test_list_channels_unknown_prefix_empty(client: TestClient, db: Path):
    _ensure(db, "run/a")

    resp = client.get("/channels", params={"prefix": "nope/"})

    assert resp.status_code == 200
    assert resp.json() == {"channels": []}


def test_list_channels_is_write_free(client: TestClient, db: Path):
    before = _count_rows(db, "channels")
    client.get("/channels")
    assert _count_rows(db, "channels") == before  # GET created no rows


# --------------------------------------------------------------------------- #
# /channels/{name}/messages
# --------------------------------------------------------------------------- #
def test_messages_happy_path(client: TestClient, db: Path):
    ids = [_append(db, "run/a", type_=f"m{i}") for i in range(3)]

    resp = client.get("/channels/run/a/messages")

    assert resp.status_code == 200
    got = resp.json()["messages"]
    assert [m["id"] for m in got] == ids
    assert [m["type"] for m in got] == ["m0", "m1", "m2"]


def test_messages_after_filters(client: TestClient, db: Path):
    _append(db, "run/a", type_="m0")
    second = _append(db, "run/a", type_="m1")
    _append(db, "run/a", type_="m2")

    resp = client.get("/channels/run/a/messages", params={"after": second - 1})

    assert [m["type"] for m in resp.json()["messages"]] == ["m1", "m2"]


def test_messages_limit(client: TestClient, db: Path):
    for i in range(5):
        _append(db, "run/a", type_=f"m{i}")

    resp = client.get("/channels/run/a/messages", params={"limit": 2})

    assert [m["id"] for m in resp.json()["messages"]] == [1, 2]


def test_messages_default_excludes_expired(client: TestClient, db: Path):
    # Real store module: a negative expires_in_s yields a genuinely
    # expired row (expires_at in the past) through log.append itself.
    _append(db, "run/a", type_="expired", expires_in_s=-3600)
    _append(db, "run/a", type_="live")

    resp = client.get("/channels/run/a/messages")

    assert [m["type"] for m in resp.json()["messages"]] == ["live"]


def test_messages_include_expired_true(client: TestClient, db: Path):
    _append(db, "run/a", type_="expired", expires_in_s=-3600)
    _append(db, "run/a", type_="live")

    resp = client.get("/channels/run/a/messages", params={"include_expired": "true"})

    assert sorted(m["type"] for m in resp.json()["messages"]) == ["expired", "live"]


@pytest.mark.parametrize("param", ["after", "limit"])
def test_messages_bad_int_is_400(client: TestClient, param: str):
    resp = client.get("/channels/run/a/messages", params={param: "abc"})

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_messages_bad_flag_is_400(client: TestClient):
    resp = client.get("/channels/run/a/messages", params={"include_expired": "maybe"})

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_messages_unknown_channel_is_404(client: TestClient):
    resp = client.get("/channels/no/such/channel/messages")

    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"


def test_messages_defaults_apply(client: TestClient, db: Path):
    """Omitting after/limit/include_expired uses 0/100/false."""
    _append(db, "run/a", type_="only")

    resp = client.get("/channels/run/a/messages")

    assert resp.status_code == 200
    assert len(resp.json()["messages"]) == 1


def test_messages_get_is_write_free(client: TestClient, db: Path):
    _append(db, "run/a")  # append registers its sender consumer
    consumers_before = _count_rows(db, "consumers")
    cursors_before = _count_rows(db, "cursors")

    client.get("/channels/run/a/messages")

    # GET /messages takes no consumer and must not mutate read-state.
    assert _count_rows(db, "consumers") == consumers_before
    assert _count_rows(db, "cursors") == cursors_before


# --------------------------------------------------------------------------- #
# /channels/{name}/pending
# --------------------------------------------------------------------------- #
def test_pending_happy_path(client: TestClient, db: Path):
    _ensure(db, "run/b")
    for i in range(3):
        _append(db, "run/b", type_=f"m{i}")

    resp = client.get("/channels/run/b/pending", params={"consumer": "worker@run-a"})

    assert resp.status_code == 200
    assert [m["type"] for m in resp.json()["messages"]] == ["m0", "m1", "m2"]


def test_pending_missing_consumer_is_400(client: TestClient, db: Path):
    _ensure(db, "run/b")

    resp = client.get("/channels/run/b/pending")

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_pending_bad_consumer_grammar_is_400(client: TestClient, db: Path):
    _ensure(db, "run/b")

    resp = client.get("/channels/run/b/pending", params={"consumer": "no-at-sign"})

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_pending_wrong_kind_is_409(client: TestClient, db: Path):
    _ensure(db, "run/q", kind="queue")

    resp = client.get("/channels/run/q/pending", params={"consumer": "worker@run-a"})

    assert resp.status_code == 409
    assert resp.json()["error"] == "conflict"


def test_pending_unknown_channel_is_404(client: TestClient):
    resp = client.get("/channels/no/such/channel/pending", params={"consumer": "worker@run-a"})

    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"


def test_pending_limit(client: TestClient, db: Path):
    _ensure(db, "run/b")
    for i in range(5):
        _append(db, "run/b", type_=f"m{i}")

    resp = client.get("/channels/run/b/pending", params={"consumer": "worker@run-a", "limit": 2})

    assert [m["id"] for m in resp.json()["messages"]] == [1, 2]


def test_pending_registers_consumer(client: TestClient, db: Path):
    """Sanctioned write (ADR-005 nuance): cursors.pending bumps the
    consumer's last_seen_at — the MODULE's write, surfaced positively."""
    _ensure(db, "run/b")
    _append(db, "run/b")

    assert _count_rows(db, "consumers", "id = 'worker@run-a'") == 0
    client.get("/channels/run/b/pending", params={"consumer": "worker@run-a"})

    assert _count_rows(db, "consumers", "id = 'worker@run-a'") == 1


def test_pending_does_not_advance_cursor(client: TestClient, db: Path):
    """Reading is not acking: pending leaves no cursor row behind."""
    _ensure(db, "run/b")
    _append(db, "run/b")

    client.get("/channels/run/b/pending", params={"consumer": "worker@run-a"})

    assert _count_rows(db, "cursors", "consumer = 'worker@run-a'") == 0


# --------------------------------------------------------------------------- #
# /channels/{name}/cursor
# --------------------------------------------------------------------------- #
def test_cursor_returns_null_when_absent(client: TestClient, db: Path):
    _ensure(db, "run/b")

    resp = client.get("/channels/run/b/cursor", params={"consumer": "worker@run-a"})

    assert resp.status_code == 200
    assert resp.json() is None


def test_cursor_returns_cursor_after_ack(client: TestClient, db: Path):
    _ensure(db, "run/b")
    _append(db, "run/b")
    second = _append(db, "run/b")
    _ack(db, "worker@run-a", "run/b", second)

    resp = client.get("/channels/run/b/cursor", params={"consumer": "worker@run-a"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["consumer"] == "worker@run-a"
    assert body["channel"] == "run/b"
    assert body["last_ack_id"] == second


def test_cursor_missing_consumer_is_400(client: TestClient, db: Path):
    _ensure(db, "run/b")

    resp = client.get("/channels/run/b/cursor")

    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_cursor_unknown_channel_returns_null_not_404(client: TestClient):
    """cursors.get_cursor is a pure join → no row → None → 200 null.
    The handler faithfully reflects that contract (unlike /messages,
    whose read_after resolves the channel and raises → 404)."""
    resp = client.get("/channels/no/such/channel/cursor", params={"consumer": "worker@run-a"})

    assert resp.status_code == 200
    assert resp.json() is None


def test_cursor_get_is_write_free(client: TestClient, db: Path):
    """THE write-free guarantee: a GET with an unregistered consumer
    creates neither a consumers row nor a cursors row (v1 bug class)."""
    _ensure(db, "run/b")
    _append(db, "run/b")
    assert _count_rows(db, "consumers", "id = 'ghost@run-z'") == 0
    assert _count_rows(db, "cursors", "consumer = 'ghost@run-z'") == 0

    client.get("/channels/run/b/cursor", params={"consumer": "ghost@run-z"})

    assert _count_rows(db, "consumers", "id = 'ghost@run-z'") == 0
    assert _count_rows(db, "cursors", "consumer = 'ghost@run-z'") == 0


# --------------------------------------------------------------------------- #
# ADR-002: percent-encoded channel names containing '/'.
# --------------------------------------------------------------------------- #
def test_messages_slashy_channel_literal(client: TestClient, db: Path):
    """{name:path} captures a slash-containing channel name verbatim."""
    _append(db, SLASHY, type_="m0")
    _append(db, SLASHY, type_="m1")

    resp = client.get(f"/channels/{SLASHY}/messages")

    assert resp.status_code == 200
    assert [m["type"] for m in resp.json()["messages"]] == ["m0", "m1"]


def test_messages_slashy_channel_percent_encoded(client: TestClient, db: Path):
    """Channel name percent-encoded on the wire (slashes as %2F) is
    delivered decoded to the handler (ADR-002)."""
    _append(db, SLASHY, type_="enc")

    encoded = quote(SLASHY, safe="")  # run%2Fx%2Flane%2F3
    resp = client.get(f"/channels/{encoded}/messages")

    assert resp.status_code == 200
    assert [m["channel"] for m in resp.json()["messages"]] == [SLASHY]


def test_pending_slashy_channel(client: TestClient, db: Path):
    _ensure(db, SLASHY)
    _append(db, SLASHY, type_="p0")

    resp = client.get(f"/channels/{SLASHY}/pending", params={"consumer": "worker@run-a"})

    assert resp.status_code == 200
    assert [m["type"] for m in resp.json()["messages"]] == ["p0"]


def test_cursor_slashy_channel(client: TestClient, db: Path):
    _ensure(db, SLASHY)

    resp = client.get(f"/channels/{SLASHY}/cursor", params={"consumer": "worker@run-a"})

    assert resp.status_code == 200
    assert resp.json() is None
