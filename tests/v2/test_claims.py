from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path

import pytest

from raven_bus.claims import claim_next, complete, get_claim, release, renew
from raven_bus.exceptions import (
    ClaimDeniedError,
    InvalidAddressError,
    UnknownChannelError,
    WrongChannelKindError,
)

MIGRATION = (
    Path(__file__).resolve().parents[2] / "src" / "raven_bus" / "migrations" / "0002_v2_schema.sql"
)
QUEUE = "run/test/queue"
CONSUMER = "worker@test"
OTHER_CONSUMER = "other@test"


@pytest.fixture()
def raw_db(tmp_path: Path) -> Path:
    path = tmp_path / "claims.db"
    conn = sqlite3.connect(path)
    conn.executescript(MIGRATION.read_text(encoding="utf-8"))
    conn.close()
    return path


def _connect(path: Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=5.0, check_same_thread=check_same_thread)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _insert_channel(
    conn: sqlite3.Connection,
    *,
    name: str = QUEUE,
    kind: str = "queue",
    max_deliveries: int = 3,
) -> int:
    cursor = conn.execute(
        "INSERT INTO channels(name, kind, max_deliveries) VALUES (?, ?, ?)",
        (name, kind, max_deliveries),
    )
    return int(cursor.lastrowid)


def _insert_message(
    conn: sqlite3.Connection,
    channel_id: int,
    *,
    expires_at: str | None = None,
    body: str = '{"work": 1}',
    tags: str = "one,two",
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO messages(channel_id, sender, type, body, tags, expires_at)
        VALUES (?, 'producer@test', 'work', ?, ?, ?)
        """,
        (channel_id, body, tags, expires_at),
    )
    return int(cursor.lastrowid)


def _insert_claim(
    conn: sqlite3.Connection,
    message_id: int,
    *,
    consumer: str = CONSUMER,
    state: str = "leased",
    deliveries: int = 1,
    lease_until: str = "2999-01-01T00:00:00.000Z",
) -> None:
    conn.execute(
        """
        INSERT INTO claims(message_id, consumer, state, deliveries, lease_until)
        VALUES (?, ?, ?, ?, ?)
        """,
        (message_id, consumer, state, deliveries, lease_until),
    )


def _expire_claim(conn: sqlite3.Connection, message_id: int) -> None:
    conn.execute(
        "UPDATE claims SET lease_until = '2000-01-01T00:00:00.000Z' "
        "WHERE message_id = ?",
        (message_id,),
    )
    conn.commit()


def test_two_connections_have_exactly_one_winner(raw_db: Path) -> None:
    setup = _connect(raw_db)
    channel_id = _insert_channel(setup)
    message_id = _insert_message(setup, channel_id)
    setup.commit()
    setup.close()

    barrier = threading.Barrier(2)
    results: list[tuple[str, int | None]] = []
    failures: list[BaseException] = []

    def contender(consumer: str) -> None:
        conn = _connect(raw_db, check_same_thread=False)
        try:
            barrier.wait()
            message = claim_next(conn, consumer, QUEUE)
            conn.commit()
            results.append((consumer, None if message is None else message.id))
        except BaseException as exc:  # noqa: BLE001 -- race-harness thread re-raises in the main test
            failures.append(exc)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=contender, args=(CONSUMER,)),
        threading.Thread(target=contender, args=(OTHER_CONSUMER,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not failures
    assert all(not thread.is_alive() for thread in threads)
    assert sorted(message for _, message in results if message is not None) == [
        message_id
    ]
    assert sum(message is None for _, message in results) == 1

    check = _connect(raw_db)
    row = check.execute(
        "SELECT message_id, consumer FROM claims WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    assert row is not None
    assert row["consumer"] in {CONSUMER, OTHER_CONSUMER}
    check.close()


def test_claim_skips_expired_and_already_claimed_messages(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    expired_id = _insert_message(
        conn, channel_id, expires_at="2000-01-01T00:00:00.000Z"
    )
    claimed_id = _insert_message(conn, channel_id)
    expected_id = _insert_message(conn, channel_id)
    _insert_claim(conn, claimed_id, consumer=OTHER_CONSUMER, state="done")
    conn.commit()

    message = claim_next(conn, CONSUMER, QUEUE)

    assert message is not None
    assert message.id == expected_id
    assert message.channel == QUEUE
    assert message.body == {"work": 1}
    assert message.tags == ["one", "two"]
    assert conn.execute(
        "SELECT 1 FROM claims WHERE message_id = ?", (expired_id,)
    ).fetchone() is None
    conn.close()


def test_lost_insert_race_falls_through_to_next_candidate(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    first_id = _insert_message(conn, channel_id)
    second_id = _insert_message(conn, channel_id)
    # The trigger inserts a competing claim between candidate selection and
    # the outer guarded insert, reproducing the only lost-race outcome.
    conn.execute(
        f"""
        CREATE TRIGGER rival_claims_first
        BEFORE INSERT ON claims
        WHEN NEW.message_id = {first_id} AND NEW.consumer = '{CONSUMER}'
        BEGIN
            INSERT OR IGNORE INTO claims(
                message_id, consumer, state, deliveries, lease_until
            ) VALUES (
                {first_id}, '{OTHER_CONSUMER}', 'leased', 1,
                '2999-01-01T00:00:00.000Z'
            );
        END
        """
    )
    conn.commit()

    message = claim_next(conn, CONSUMER, QUEUE)

    assert message is not None
    assert message.id == second_id
    rows = conn.execute(
        "SELECT message_id, consumer FROM claims ORDER BY message_id"
    ).fetchall()
    assert [(row["message_id"], row["consumer"]) for row in rows] == [
        (first_id, OTHER_CONSUMER),
        (second_id, CONSUMER),
    ]
    conn.close()


def test_claim_returns_none_for_empty_queue(raw_db: Path) -> None:
    conn = _connect(raw_db)
    _insert_channel(conn)
    conn.commit()

    assert claim_next(conn, CONSUMER, QUEUE) is None
    conn.close()


def test_claim_rejects_unknown_channel(raw_db: Path) -> None:
    conn = _connect(raw_db)

    with pytest.raises(UnknownChannelError):
        claim_next(conn, CONSUMER, QUEUE)
    conn.close()


def test_claim_rejects_non_queue_channel(raw_db: Path) -> None:
    conn = _connect(raw_db)
    _insert_channel(conn, kind="broadcast")
    conn.commit()

    with pytest.raises(WrongChannelKindError):
        claim_next(conn, CONSUMER, QUEUE)
    conn.close()


def test_consumer_is_validated_and_upserted(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    _insert_message(conn, channel_id)
    conn.commit()

    claim_next(conn, CONSUMER, QUEUE)

    row = conn.execute(
        "SELECT id, role, run, last_seen_at FROM consumers WHERE id = ?",
        (CONSUMER,),
    ).fetchone()
    assert row is not None
    assert (row["id"], row["role"], row["run"]) == (
        CONSUMER,
        "worker",
        "test",
    )
    assert row["last_seen_at"] is not None
    conn.close()


def test_claim_validates_consumer_id(raw_db: Path) -> None:
    conn = _connect(raw_db)
    _insert_channel(conn)
    conn.commit()

    with pytest.raises(InvalidAddressError):
        claim_next(conn, "missing-run", QUEUE)
    assert conn.execute("SELECT 1 FROM consumers").fetchone() is None
    conn.close()


def test_renew_extends_owned_lease(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    message_id = _insert_message(conn, channel_id)
    conn.commit()
    claim_next(conn, CONSUMER, QUEUE, lease_s=60)
    before = get_claim(conn, message_id)
    assert before is not None

    renewed = renew(conn, message_id, CONSUMER, lease_s=600)

    assert renewed.state == "leased"
    assert renewed.consumer == CONSUMER
    assert renewed.lease_until > before.lease_until
    conn.close()


def test_complete_is_idempotent_for_same_consumer(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    message_id = _insert_message(conn, channel_id)
    conn.commit()
    claim_next(conn, CONSUMER, QUEUE)

    first = complete(conn, message_id, CONSUMER)
    second = complete(conn, message_id, CONSUMER)

    assert first.state == "done"
    assert second == first
    conn.close()


def test_release_makes_message_claimable_without_counting_delivery(
    raw_db: Path,
) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    message_id = _insert_message(conn, channel_id)
    conn.commit()
    claim_next(conn, CONSUMER, QUEUE)

    release(conn, message_id, CONSUMER)
    reclaimed = claim_next(conn, OTHER_CONSUMER, QUEUE)
    claim = get_claim(conn, message_id)

    assert reclaimed is not None
    assert reclaimed.id == message_id
    assert claim is not None
    assert claim.consumer == OTHER_CONSUMER
    assert claim.deliveries == 1
    conn.close()


def test_release_keeps_earlier_lapses_so_a_poison_message_dead_letters(
    raw_db: Path,
) -> None:
    """QA store #7: release reset deliveries to 0, erasing every earlier
    INVOLUNTARY lapse — so a poison message never dead-lettered as long
    as someone released it in between. A release now undoes only the
    one delivery its own claim added."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn, max_deliveries=3)
    message_id = _insert_message(conn, channel_id)
    conn.commit()

    for _ in range(2):  # two claims that crash (lease lapses)
        assert claim_next(conn, CONSUMER, QUEUE).id == message_id
        _expire_claim(conn, message_id)
    assert claim_next(conn, CONSUMER, QUEUE).id == message_id  # deliveries=3
    release(conn, message_id, CONSUMER)
    row = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?", (message_id,)
    ).fetchone()
    assert (row["state"], row["deliveries"]) == ("lapsed", 2)

    assert claim_next(conn, OTHER_CONSUMER, QUEUE).id == message_id  # deliveries=3
    _expire_claim(conn, message_id)
    assert claim_next(conn, CONSUMER, QUEUE) is None  # the sweep dead-letters it
    row = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?", (message_id,)
    ).fetchone()
    assert (row["state"], row["deliveries"]) == ("dead", 3)
    conn.close()


DENIAL_CASES = [
    ("renew", "absent"),
    ("renew", "wrong_consumer"),
    ("renew", "done"),
    ("renew", "dead"),
    ("complete", "absent"),
    ("complete", "wrong_consumer"),
    ("complete", "dead"),
    ("release", "absent"),
    ("release", "wrong_consumer"),
    ("release", "done"),
    ("release", "dead"),
]


@pytest.mark.parametrize(("operation", "condition"), DENIAL_CASES)
def test_claim_operation_denial_matrix(
    raw_db: Path,
    operation: str,
    condition: str,
) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    message_id = _insert_message(conn, channel_id)
    if condition != "absent":
        owner = OTHER_CONSUMER if condition == "wrong_consumer" else CONSUMER
        state = "leased" if condition == "wrong_consumer" else condition
        _insert_claim(conn, message_id, consumer=owner, state=state)
    conn.commit()

    operations: dict[str, Callable[[], object]] = {
        "renew": lambda: renew(conn, message_id, CONSUMER),
        "complete": lambda: complete(conn, message_id, CONSUMER),
        "release": lambda: release(conn, message_id, CONSUMER),
    }
    with pytest.raises(ClaimDeniedError):
        operations[operation]()
    conn.close()


def test_lapsed_lease_is_reclaimable_with_incremented_delivery(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn, max_deliveries=3)
    message_id = _insert_message(conn, channel_id)
    conn.commit()
    first = claim_next(conn, CONSUMER, QUEUE)
    assert first is not None
    _expire_claim(conn, message_id)

    second = claim_next(conn, OTHER_CONSUMER, QUEUE)
    claim = get_claim(conn, message_id)

    assert second is not None
    assert second.id == message_id
    assert claim is not None
    assert claim.consumer == OTHER_CONSUMER
    assert claim.deliveries == 2
    conn.close()


def test_dead_letters_exactly_at_max_deliveries(raw_db: Path) -> None:
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn, max_deliveries=2)
    message_id = _insert_message(conn, channel_id)
    conn.commit()

    assert claim_next(conn, CONSUMER, QUEUE) is not None
    _expire_claim(conn, message_id)
    assert claim_next(conn, OTHER_CONSUMER, QUEUE) is not None
    second = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    assert second is not None
    assert (second["state"], second["deliveries"]) == ("leased", 2)

    _expire_claim(conn, message_id)
    assert claim_next(conn, CONSUMER, QUEUE) is None
    terminal = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    assert terminal is not None
    assert (terminal["state"], terminal["deliveries"]) == ("dead", 2)
    conn.close()

def test_delivery_count_survives_foreign_sweep(raw_db: Path) -> None:
    """verify-001 regression: a sweep triggered ANYWHERE (another
    channel's claimant, cursors.pending, raven doctor) between lapse and
    re-claim must not reset the attempt count. The count is durable in
    the 'lapsed' claim row, so dead-letter fires at max_deliveries even
    when this channel's claimant never sweeps its own lapsed lease."""
    conn = _connect(raw_db)
    _insert_channel(conn, name="run/t/qa", kind="queue", max_deliveries=2)
    qb_id = _insert_channel(conn, name="run/t/qb", kind="queue", max_deliveries=2)
    mid = _insert_message(conn, qb_id)
    conn.commit()

    # Attempt 1 on qb lapses; a FOREIGN sweep (qa claimant) reaps it.
    assert claim_next(conn, "w1@t", "run/t/qb").id == mid
    _expire_claim(conn, mid)
    assert claim_next(conn, "other@t", "run/t/qa") is None  # sweeps

    row = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?", (mid,)
    ).fetchone()
    assert (row["state"], row["deliveries"]) == ("lapsed", 1)

    # Attempt 2 re-claims WITH the incremented count...
    msg = claim_next(conn, "w2@t", "run/t/qb")
    assert msg is not None and msg.id == mid
    row = conn.execute(
        "SELECT deliveries FROM claims WHERE message_id = ?", (mid,)
    ).fetchone()
    assert row["deliveries"] == 2

    # ...so the next lapse dead-letters at max_deliveries=2, despite the
    # sweep again coming from the foreign channel.
    _expire_claim(conn, mid)
    assert claim_next(conn, "other@t", "run/t/qa") is None  # sweeps
    row = conn.execute(
        "SELECT state, deliveries FROM claims WHERE message_id = ?", (mid,)
    ).fetchone()
    assert (row["state"], row["deliveries"]) == ("dead", 2)
    conn.close()


def test_sweep_never_touches_terminal_claims_with_stale_leases(raw_db: Path) -> None:
    """verify-000 regression (predicate form): sweep's writes re-check
    state at write time, so a claim that reached 'done' keeps its state
    even when its lease_until is long past."""
    from raven_bus import db as bus_db

    conn = _connect(raw_db)
    qid = _insert_channel(conn, name="run/t/q", kind="queue")
    mid = _insert_message(conn, qid)
    _insert_claim(conn, mid, state="done", lease_until="2000-01-01T00:00:00.000Z")
    conn.commit()

    result = bus_db.sweep(conn)
    assert result.requeued == 0
    assert result.dead_lettered == 0
    row = conn.execute(
        "SELECT state FROM claims WHERE message_id = ?", (mid,)
    ).fetchone()
    assert row["state"] == "done"
    conn.close()
