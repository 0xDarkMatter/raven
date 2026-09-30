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


def test_frontier_key_drops_the_inode_when_stat_fails(
    raw_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A filesystem that can't stat the file only loses the inode
    discriminator; the in-file instance id still names the file (QA
    store #11) — never a crash, never a bare-path key."""
    conn = _connect(raw_db)
    identity = claims.db.instance_id(conn)

    def _no_stat(_path, **_kwargs):
        raise OSError("stat unavailable")

    monkeypatch.setattr(claims.os, "stat", _no_stat)
    key = claims._frontier_db_key(conn)
    # Restore os.stat before asserting: it is the GLOBAL os module, and
    # pytest's own failure rendering stats files.
    monkeypatch.undo()
    assert identity is not None
    assert key is not None and key.endswith(f"claims.db|{identity}")
    conn.close()


# --------------------------------------------------------------------------- #
# QA store-lane regressions (#10 ceiling order, #11 file identity)
# --------------------------------------------------------------------------- #
class _Rows:
    def __init__(self, rows: list[sqlite3.Row]) -> None:
        self._rows = rows

    def fetchall(self) -> list[sqlite3.Row]:
        return self._rows


class _PeerCommitsAfterFreshScan:
    """Connection proxy: right after claim_next's fresh-candidate scan, a
    PEER commits a new message on the channel — the window an autocommit
    connection leaves open before a post-scan ceiling read."""

    def __init__(self, conn: sqlite3.Connection, inject) -> None:
        self._conn = conn
        self._inject = inject
        self.injected: list[int] = []

    def execute(self, sql: str, params=()):
        cursor = self._conn.execute(sql, params)
        if sql == claims._FRESH_CANDIDATES_SQL and not self.injected:
            rows = cursor.fetchall()
            self.injected.append(self._inject())
            return _Rows(rows)
        return cursor


def test_frontier_ceiling_is_read_before_the_fresh_scan(raw_db: Path) -> None:
    """QA store #10: the MAX(id) ceiling was read AFTER the fresh scan. On
    an autocommit connection (no write transaction pinning a snapshot) a
    message committed in between lies above everything the scan saw yet
    at/below the ceiling — the frontier advanced past a never-claimed
    message, which that process then never claimed."""
    setup = _connect(raw_db)
    channel_id = _insert_channel(setup)
    first = _insert_message(setup, channel_id)
    setup.commit()
    assert claim_next(setup, CONSUMER, QUEUE).id == first
    setup.commit()

    def peer_append() -> int:
        peer = _connect(raw_db)
        try:
            message_id = _insert_message(peer, channel_id)
            peer.commit()
            return message_id
        finally:
            peer.close()

    auto = sqlite3.connect(raw_db, timeout=5.0, isolation_level=None)
    auto.row_factory = sqlite3.Row
    auto.execute("PRAGMA foreign_keys = ON")
    proxy = _PeerCommitsAfterFreshScan(auto, peer_append)
    assert claim_next(proxy, CONSUMER, QUEUE) is None  # scan saw nothing fresh
    assert len(proxy.injected) == 1

    late = claim_next(auto, OTHER_CONSUMER, QUEUE)
    assert late is not None and late.id == proxy.injected[0]
    auto.close()
    setup.close()


def test_frontier_not_reused_for_a_different_file_at_the_same_inode(
    raw_db: Path,
) -> None:
    """QA store #11: the cache key was (path, st_dev, st_ino), but ext4
    hands a replaced file the SAME inode deterministically, so a
    long-lived process (ravend) kept the dead file's frontier and hid
    every message at or below it in the new file. The key now carries a
    durable identity stored IN the file. Simulated here (NTFS never
    reuses inodes): same path + inode, different instance id, and the
    early message unclaimed in the "new" file."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    first = _insert_message(conn, channel_id)
    conn.commit()
    assert claim_next(conn, "a@r", QUEUE).id == first
    assert claim_next(conn, "a@r", QUEUE) is None  # frontier warms past it
    conn.commit()

    conn.execute("DELETE FROM claims")
    conn.execute("UPDATE bus_meta SET value = 'replaced' WHERE key = 'instance_id'")
    conn.commit()

    reborn = claim_next(conn, "b@r", QUEUE)
    assert reborn is not None and reborn.id == first
    conn.close()


def test_instance_id_is_backfilled_once_and_stable(raw_db: Path) -> None:
    """Pre-existing DBs have no instance id: claim_next backfills it
    inside its own transaction, and it never changes afterwards."""
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    _insert_message(conn, channel_id)
    conn.commit()
    assert conn.execute(
        "SELECT 1 FROM bus_meta WHERE key = 'instance_id'"
    ).fetchone() is None

    claim_next(conn, CONSUMER, QUEUE)
    conn.commit()
    first = conn.execute(
        "SELECT value FROM bus_meta WHERE key = 'instance_id'"
    ).fetchone()["value"]
    claim_next(conn, CONSUMER, QUEUE)
    conn.commit()
    again = conn.execute(
        "SELECT value FROM bus_meta WHERE key = 'instance_id'"
    ).fetchone()["value"]

    assert first == again
    assert len(first) == 32
    conn.close()


def test_frontier_cache_skipped_without_an_instance_id(
    raw_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No readable identity = no cache: never fall back to trusting the
    inode. Claiming stays correct (the cache was only ever a shortcut)."""
    monkeypatch.setattr(claims.db, "instance_id", lambda _conn: None)
    conn = _connect(raw_db)
    channel_id = _insert_channel(conn)
    first = _insert_message(conn, channel_id)
    conn.commit()

    assert claim_next(conn, CONSUMER, QUEUE).id == first
    assert claim_next(conn, CONSUMER, QUEUE) is None
    second = _insert_message(conn, channel_id)
    conn.commit()
    assert claim_next(conn, CONSUMER, QUEUE).id == second

    assert claims._FRONTIER == {}
    conn.close()


class _PeerWinsFirstRound:
    """Connection proxy: during claim_next's FIRST round a peer claims
    each candidate just before this connection's attempt, so every race
    in that round is lost; later rounds run untouched."""

    def __init__(self, conn: sqlite3.Connection, peer: sqlite3.Connection) -> None:
        self._conn = conn
        self._peer = peer
        self._scans = 0

    def execute(self, sql: str, params=()):
        if sql == claims._FRESH_CANDIDATES_SQL:
            self._scans += 1
        elif self._scans == 1 and "INSERT INTO claims" in sql:
            self._peer.execute(
                "INSERT INTO claims(message_id, consumer, state, deliveries, lease_until) "
                "VALUES (?, 'peer@test', 'leased', 1, '2999-01-01T00:00:00.000Z')",
                (params[0],),
            )
            self._peer.commit()
        elif self._scans == 1 and "state = 'lapsed'" in sql and sql.lstrip().startswith(
            "UPDATE"
        ):
            self._peer.execute(
                "UPDATE claims SET state = 'leased', consumer = 'peer@test', "
                "lease_until = '2999-01-01T00:00:00.000Z' WHERE message_id = ?",
                (params[2],),
            )
            self._peer.commit()
        return self._conn.execute(sql, params)


def test_lost_round_does_not_skip_fresh_rows_below_a_lapsed_candidate(
    raw_db: Path,
) -> None:
    """Found while fixing QA store #10: both scans resumed from ONE
    ``after_id`` — the last candidate of the merged batch. A full fresh
    batch (ids up to X) plus a lapsed candidate at Y > X, all lost to
    racers, restarted the fresh scan at Y: fresh rows in (X, Y) were
    never examined, and the next short scan advanced the frontier past
    them — hidden from this process for good."""
    setup = _connect(raw_db)
    channel_id = _insert_channel(setup, max_deliveries=9)
    fresh = [_insert_message(setup, channel_id) for _ in range(claims._CANDIDATE_BATCH + 1)]
    lapsed = _insert_message(setup, channel_id)
    _insert_claim_row(setup, lapsed, state="lapsed")
    setup.commit()

    auto = sqlite3.connect(raw_db, timeout=5.0, isolation_level=None)
    auto.row_factory = sqlite3.Row
    auto.execute("PRAGMA foreign_keys = ON")
    peer = _connect(raw_db)
    proxy = _PeerWinsFirstRound(auto, peer)

    won = claim_next(proxy, CONSUMER, QUEUE)

    assert won is not None and won.id == fresh[-1]
    peer.close()
    auto.close()
    setup.close()


def _insert_claim_row(conn: sqlite3.Connection, message_id: int, *, state: str) -> None:
    conn.execute(
        "INSERT INTO claims(message_id, consumer, state, deliveries, lease_until) "
        "VALUES (?, 'old@test', ?, 1, '2000-01-01T00:00:00.000Z')",
        (message_id, state),
    )
