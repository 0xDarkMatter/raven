"""Frontier optimisation for ``claims.claim_next`` (verify-002 waived P1).

Companion to ``test_claims.py`` (unchanged, the semantics regression net).
These tests target the ``_FRONTIER`` watermark specifically: that it
actually bounds the fresh-candidate scan (measured via SQLite's VDBE
progress handler as a step-count proxy for work done), that lapsed rows
below the frontier are still found, and that a cold process (empty
cache) is still correct.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from raven_bus import claims
from raven_bus.claims import claim_next, complete, get_claim

MIGRATION = Path("src/raven_bus/migrations/0002_v2_schema.sql")
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


@pytest.fixture(autouse=True)
def _clean_frontier_cache():
    claims._FRONTIER.clear()
    yield
    claims._FRONTIER.clear()


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
    tags: str = "",
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO messages(channel_id, sender, type, body, tags, expires_at)
        VALUES (?, 'producer@test', 'work', ?, ?, ?)
        """,
        (channel_id, body, tags, expires_at),
    )
    return int(cursor.lastrowid)


def _expire_claim(conn: sqlite3.Connection, message_id: int) -> None:
    conn.execute(
        "UPDATE claims SET lease_until = '2000-01-01T00:00:00.000Z' "
        "WHERE message_id = ?",
        (message_id,),
    )
    conn.commit()


def _drain_and_complete(conn: sqlite3.Connection, channel: str, count: int) -> None:
    """Claim and complete ``count`` messages on ``channel`` so they
    become a terminal ('done') backlog."""
    for _ in range(count):
        msg = claim_next(conn, CONSUMER, channel)
        assert msg is not None
        complete(conn, msg.id, CONSUMER)
    conn.commit()


def _vdbe_steps(conn: sqlite3.Connection, fn) -> int:
    """VDBE-instruction tick count during ``fn()`` -- a proxy for row-
    scan work done, independent of how many rows the call returns.
    Statement *count* does not distinguish the fix: a single ``SELECT
    ... WHERE cl.message_id IS NULL LIMIT 32`` against a terminal
    backlog is still one client-visible statement whether SQLite has
    to internally touch 1 row or 100,000 to satisfy it -- the bug (and
    the fix) live inside that one statement's row-scan cost, which
    only a step/row proxy like this one can see."""
    count = 0

    def handler() -> int:
        nonlocal count
        count += 1
        return 0

    conn.set_progress_handler(handler, 1)
    try:
        fn()
    finally:
        conn.set_progress_handler(None, 0)
    return count


BACKLOG = 200


def test_frontier_reduces_scan_cost_on_repeat_polls(raw_db: Path) -> None:
    """Second ``claim_next`` after N completed messages does not
    re-examine them: the first call over a purely-terminal backlog has
    to scan it once to prove nothing is claimable (warming the
    frontier); a second, otherwise-identical call over the *same*
    static backlog is measurably cheaper because the fresh half of the
    scan starts at the frontier instead of message id 0. (The lapsed
    half of the scan is NOT frontier-bounded by design -- see the
    module docstring -- so it contributes an equal fixed cost to both
    calls; the reduction below comes entirely from the fresh half.)"""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    for _ in range(BACKLOG):
        _insert_message(conn, channel_id)
    conn.commit()
    _drain_and_complete(conn, QUEUE, BACKLOG)

    cold_steps = _vdbe_steps(conn, lambda: claim_next(conn, CONSUMER, QUEUE))
    assert (claims._frontier_db_key(conn), channel_id) in claims._FRONTIER

    warm_steps = _vdbe_steps(conn, lambda: claim_next(conn, CONSUMER, QUEUE))

    # A comfortable margin below "no improvement at all" (1.0), well
    # short of claiming the lapsed-side cost also vanishes.
    assert warm_steps < cold_steps * 0.75
    conn.close()


def test_lapsed_row_below_frontier_is_still_claimed(raw_db: Path) -> None:
    """A message claimed early (small id), whose lease later lapses
    AFTER the frontier has already advanced past its id, must still be
    reclaimed -- the frontier only ever says 'no NEVER-claimed message
    below here', never 'nothing left to do below here'."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn, max_deliveries=5)
    conn.commit()

    early_id = _insert_message(conn, channel_id)
    conn.commit()
    first = claim_next(conn, CONSUMER, QUEUE)
    assert first is not None and first.id == early_id

    for _ in range(BACKLOG):
        _insert_message(conn, channel_id)
    conn.commit()
    _drain_and_complete(conn, QUEUE, BACKLOG)

    # This call finds nothing fresh (early_id is 'leased', the rest
    # are 'done') and advances the frontier well past early_id.
    assert claim_next(conn, CONSUMER, QUEUE) is None
    frontier_key = (claims._frontier_db_key(conn), channel_id)
    assert claims._FRONTIER[frontier_key] > early_id

    _expire_claim(conn, early_id)
    reclaimed = claim_next(conn, OTHER_CONSUMER, QUEUE)
    claim = get_claim(conn, early_id)

    assert reclaimed is not None
    assert reclaimed.id == early_id
    assert claim is not None
    assert claim.consumer == OTHER_CONSUMER
    assert claim.deliveries == 2
    conn.close()


def test_cold_process_fresh_dict_still_correct(raw_db: Path) -> None:
    """A restarted process (empty ``_FRONTIER``) must still return the
    right message -- the cache is pure optimisation, never a
    correctness dependency. Build a backlog large enough that a prior
    call is guaranteed to have warmed the cache, then clear it and
    confirm the next call is still correct."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    for _ in range(BACKLOG):
        _insert_message(conn, channel_id)
    conn.commit()
    _drain_and_complete(conn, QUEUE, BACKLOG)

    # Empty queue -- the for-loop finds nothing to win, so this call
    # reaches the no-win branch and warms the cache.
    assert claim_next(conn, OTHER_CONSUMER, QUEUE) is None
    assert (claims._frontier_db_key(conn), channel_id) in claims._FRONTIER

    claims._FRONTIER.clear()  # simulate a fresh process
    expected_id = _insert_message(conn, channel_id)
    conn.commit()

    message = claim_next(conn, CONSUMER, QUEUE)

    assert message is not None
    assert message.id == expected_id
    conn.close()


def test_two_claimant_race_still_exactly_one_winner(raw_db: Path) -> None:
    """The frontier-aware two-part scan must not weaken the atomic
    winner guarantee: with a terminal backlog ahead of it, exactly one
    of two racing connections wins the sole live message."""
    setup = _connect(raw_db)
    channel_id = _insert_channel(setup)
    for _ in range(50):
        _insert_message(setup, channel_id)
    setup.commit()
    _drain_and_complete(setup, QUEUE, 50)
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
        except BaseException as exc:  # noqa: BLE001
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
    assert sorted(mid for _, mid in results if mid is not None) == [message_id]
    assert sum(mid is None for _, mid in results) == 1
    check = _connect(raw_db)
    row = check.execute(
        "SELECT consumer FROM claims WHERE message_id = ?", (message_id,)
    ).fetchone()
    assert row is not None and row["consumer"] in {CONSUMER, OTHER_CONSUMER}
    check.close()


def test_empty_queue_polls_stay_flat_once_frontier_is_warm(raw_db: Path) -> None:
    """Once the frontier is warm, repeated polls of a permanently
    empty queue stay at a steady, backlog-independent cost -- the
    second warm poll is not measurably more expensive than the first,
    even though the terminal backlog never shrinks."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    conn.commit()
    for _ in range(BACKLOG):
        _insert_message(conn, channel_id)
    conn.commit()
    _drain_and_complete(conn, QUEUE, BACKLOG)

    assert claim_next(conn, CONSUMER, QUEUE) is None  # cold warm-up call
    assert (claims._frontier_db_key(conn), channel_id) in claims._FRONTIER

    first_warm = _vdbe_steps(conn, lambda: claim_next(conn, CONSUMER, QUEUE))
    second_warm = _vdbe_steps(conn, lambda: claim_next(conn, CONSUMER, QUEUE))

    # Both stay in the same small ballpark -- neither creeps back up
    # toward the cold, whole-backlog scan cost.
    assert abs(first_warm - second_warm) < max(first_warm, second_warm) * 0.5
    conn.close()


def test_released_message_below_frontier_is_reclaimable(raw_db: Path) -> None:
    """raven2-p2 refute-frontier regression, finding 1 (the release
    hide): append m1/m2, claim both, advance the frontier past them,
    release m1 — m1 must still be claimable by ANY consumer/process
    (it travels the lapsed scan, which ignores the frontier)."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    m1 = _insert_message(conn, channel_id)
    m2 = _insert_message(conn, channel_id)
    conn.commit()

    assert claim_next(conn, "a@r", QUEUE).id == m1
    assert claim_next(conn, "a@r", QUEUE).id == m2
    # A poll on the drained queue advances the frontier past both ids.
    assert claim_next(conn, "a@r", QUEUE) is None

    claims.release(conn, m1, "a@r")
    reclaimed = claim_next(conn, "b@r", QUEUE)
    assert reclaimed is not None and reclaimed.id == m1
    # Voluntary release never counts toward dead-letter: fresh count.
    row = conn.execute(
        "SELECT deliveries FROM claims WHERE message_id = ?", (m1,)
    ).fetchone()
    assert row["deliveries"] == 1
    conn.close()


def test_frontier_goes_cold_when_db_file_is_replaced(tmp_path: Path) -> None:
    """raven2-p2 refute-frontier regression, finding 2: a warm frontier
    must not survive the FILE being replaced at the same path — the
    cache key carries (st_dev, st_ino), so a new file is a new key."""
    from raven_bus import db as bus_db

    target = tmp_path / "swap.db"

    def _fresh_db_with_one_message() -> None:
        bus_db._reset_init_cache()
        bus_db.init_db(target, force=True)
        conn = _connect(target)
        channel_id = _insert_channel(conn)
        _insert_message(conn, channel_id)
        conn.commit()
        conn.close()

    _fresh_db_with_one_message()
    conn = _connect(target)
    assert claim_next(conn, "a@r", QUEUE) is not None
    assert claim_next(conn, "a@r", QUEUE) is None  # frontier warms
    conn.close()

    target.unlink()  # replace the file wholesale at the same path
    _fresh_db_with_one_message()
    conn = _connect(target)
    # Stale frontier would hide message id 1 in the NEW file.
    assert claim_next(conn, "b@r", QUEUE) is not None
    conn.close()


def test_frontier_key_degrades_to_path_when_stat_fails(
    raw_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Filesystems that cannot identify files degrade to the bare-path
    key (worst case: the original staleness), never a crash."""
    conn = _connect(raw_db)

    def _no_stat(_path):
        raise OSError("stat unavailable")

    monkeypatch.setattr(claims.os, "stat", _no_stat)
    key = claims._frontier_db_key(conn)
    assert key.endswith("claims.db")
    assert "|" not in key
    conn.close()
