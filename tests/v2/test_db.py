"""Tests for raven_bus.db — lifecycle + sweep. LANE: store (raven2-p1)."""

from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from raven_bus import db as bus_db
from raven_bus.exceptions import (
    InvalidAddressError,
    SchemaMismatchError,
    TeardownBlockedError,
)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _insert_channel(
    conn: sqlite3.Connection,
    name: str,
    kind: str = "queue",
    max_deliveries: int = 3,
) -> int:
    cur = conn.execute(
        "INSERT INTO channels (name, kind, max_deliveries) VALUES (?, ?, ?)",
        (name, kind, max_deliveries),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_message(
    conn: sqlite3.Connection,
    channel_id: int,
    *,
    sender: str = "role@run-a",
    expires_at: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO messages (channel_id, sender, type, body, expires_at) "
        "VALUES (?, ?, 'test', '{}', ?)",
        (channel_id, sender, expires_at),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_claim(
    conn: sqlite3.Connection,
    message_id: int,
    *,
    consumer: str = "role@run-a",
    state: str = "leased",
    deliveries: int = 1,
    lease_until: str,
) -> None:
    conn.execute(
        "INSERT INTO claims (message_id, consumer, state, deliveries, lease_until) "
        "VALUES (?, ?, ?, ?, ?)",
        (message_id, consumer, state, deliveries, lease_until),
    )


# --- init_db -----------------------------------------------------------


def test_init_db_creates_parent_dirs_and_schema(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "nested" / "dir" / "bus.db"
    resolved = bus_db.init_db(target)

    assert resolved == target.resolve()
    assert resolved.exists()

    with bus_db.connection(resolved) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {"channels", "messages", "cursors", "claims", "consumers", "bus_meta"} <= tables


def test_init_db_records_schema_version(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)

    with bus_db.connection(target) as conn:
        row = conn.execute(
            "SELECT value FROM bus_meta WHERE key = 'schema_version'"
        ).fetchone()
    assert row[0] == bus_db.SCHEMA_VERSION


def test_init_db_is_idempotent(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)
    # calling again must not raise (CREATE TABLE IF NOT EXISTS + upsert)
    resolved = bus_db.init_db(target)
    assert resolved == target.resolve()


def test_init_db_process_cache_skips_reinit(tmp_path: Path, monkeypatch) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)

    calls: list[str] = []
    original = sqlite3.connect

    def spy_connect(*args, **kwargs):
        calls.append("connect")
        return original(*args, **kwargs)

    monkeypatch.setattr(bus_db.sqlite3, "connect", spy_connect)
    bus_db.init_db(target)  # cached — should not touch sqlite3.connect
    assert calls == []


def test_init_db_force_bypasses_cache(tmp_path: Path, monkeypatch) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)

    calls: list[str] = []
    original = sqlite3.connect

    def spy_connect(*args, **kwargs):
        calls.append("connect")
        return original(*args, **kwargs)

    monkeypatch.setattr(bus_db.sqlite3, "connect", spy_connect)
    bus_db.init_db(target, force=True)
    # force runs the version probe AND the schema apply — two connects,
    # where the cached path makes none.
    assert calls == ["connect", "connect"]


def test_reset_init_cache_clears(tmp_path: Path) -> None:
    bus_db._reset_init_cache()
    target = tmp_path / "bus.db"
    bus_db.init_db(target)
    assert target.resolve() in bus_db._init_cache
    bus_db._reset_init_cache()
    assert bus_db._init_cache == set()


# --- connection ----------------------------------------------------------


def test_connection_sets_wal_and_foreign_keys(db: Path) -> None:
    with bus_db.connection(db) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_connection_row_factory(db: Path) -> None:
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/c")
    with bus_db.connection(db) as conn:
        row = conn.execute("SELECT id, name FROM channels WHERE id = ?", (cid,)).fetchone()
        assert row["name"] == "run/t/c"


def test_connection_commits_on_clean_exit(db: Path) -> None:
    with bus_db.connection(db) as conn:
        _insert_channel(conn, "run/t/committed")

    with bus_db.connection(db) as conn:
        row = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/t/committed'"
        ).fetchone()
    assert row is not None


def test_connection_rolls_back_on_exception(db: Path) -> None:
    class Boom(Exception):
        pass

    with pytest.raises(Boom), bus_db.connection(db) as conn:
        _insert_channel(conn, "run/t/rolledback")
        raise Boom("fail")

    with bus_db.connection(db) as conn:
        row = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/t/rolledback'"
        ).fetchone()
    assert row is None


# --- data_version --------------------------------------------------------


def test_data_version_changes_across_connections(db: Path) -> None:
    # PRAGMA data_version snapshots at the start of a connection's read
    # transaction (sqlite docs: "the value returned only changes after
    # the connection has opened a new transaction") — so the poller
    # must end its transaction (rollback, since it did no writes) before
    # re-checking to observe a commit made by another connection.
    poller = sqlite3.connect(str(db), isolation_level="DEFERRED")
    try:
        before = bus_db.data_version(poller)
        poller.rollback()

        with bus_db.connection(db) as conn:
            _insert_channel(conn, "run/t/dv")

        poller.rollback()
        after = bus_db.data_version(poller)
    finally:
        poller.close()

    assert after != before


def test_data_version_returns_int(db: Path) -> None:
    with bus_db.connection(db) as conn:
        assert isinstance(bus_db.data_version(conn), int)


# --- sweep ----------------------------------------------------------------


def test_sweep_noop_on_empty_db(db: Path) -> None:
    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
    assert result.expired == 0
    assert result.requeued == 0
    assert result.dead_lettered == 0


def test_sweep_requeues_lease_below_max_deliveries(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid)
        _insert_claim(conn, mid, deliveries=2, lease_until=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        remaining = conn.execute(
            "SELECT state, deliveries FROM claims WHERE message_id = ?", (mid,)
        ).fetchone()

    assert result.requeued == 1
    assert result.dead_lettered == 0
    # Requeue keeps the row as 'lapsed' with its attempt count intact —
    # deleting it was how delivery counts reset and dead-lettering became
    # evadable (verify-000/001 fix round).
    assert remaining is not None
    assert remaining["state"] == "lapsed"
    assert remaining["deliveries"] == 2


def test_sweep_dead_letters_at_max_deliveries_boundary(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid)
        _insert_claim(conn, mid, deliveries=3, lease_until=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        row = conn.execute(
            "SELECT state FROM claims WHERE message_id = ?", (mid,)
        ).fetchone()

    assert result.requeued == 0
    assert result.dead_lettered == 1
    assert row["state"] == "dead"


def test_sweep_ignores_non_expired_leases(db: Path) -> None:
    future = _iso(datetime.now(UTC) + timedelta(hours=1))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid)
        _insert_claim(conn, mid, deliveries=1, lease_until=future)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn)
        row = conn.execute(
            "SELECT * FROM claims WHERE message_id = ?", (mid,)
        ).fetchone()

    assert result.requeued == 0
    assert result.dead_lettered == 0
    assert row is not None
    assert row["state"] == "leased"


def test_sweep_never_touches_messages_table(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=1)
        mid = _insert_message(conn, cid, expires_at=past)

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn, count_expired=True)
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone()

    assert row is not None  # message never deleted by sweep
    assert result.expired == 1


def test_sweep_expired_excludes_terminal_claims(db: Path) -> None:
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        mid = _insert_message(conn, cid, expires_at=past)
        _insert_claim(conn, mid, state="done", deliveries=1, lease_until=past)
        _insert_message(conn, cid, expires_at=past)  # counted: no claim

    with bus_db.connection(db) as conn:
        result = bus_db.sweep(conn, count_expired=True)

    assert result.expired == 1


def test_sweep_skips_the_expired_count_unless_asked(db: Path) -> None:
    """QA store #9: sweep runs on every pending/claim — every hook peek —
    inside the write transaction, and its expired COUNT scanned every
    expired message in the DB (18ms -> 119ms as they piled up). The
    count is observability only, so it is opt-in (raven doctor)."""
    past = _iso(datetime.now(UTC) - timedelta(seconds=10))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q")
        _insert_message(conn, cid, expires_at=past)

    statements: list[str] = []
    with bus_db.connection(db) as conn:
        conn.set_trace_callback(statements.append)
        result = bus_db.sweep(conn)

    assert result.expired == 0
    assert not [sql for sql in statements if "COUNT(" in sql.upper()]


def test_sweep_cheap_when_nothing_stale(db: Path) -> None:
    # populate a modest number of non-stale rows and ensure sweep doesn't
    # take anything close to O(seconds) — the partial indexes should make
    # this a near-instant no-op even with a few hundred rows present.
    future = _iso(datetime.now(UTC) + timedelta(hours=1))
    with bus_db.connection(db) as conn:
        cid = _insert_channel(conn, "run/t/q", max_deliveries=3)
        for _ in range(200):
            mid = _insert_message(conn, cid)
            _insert_claim(conn, mid, deliveries=1, lease_until=future)

    with bus_db.connection(db) as conn:
        start = time.perf_counter()
        result = bus_db.sweep(conn)
        elapsed = time.perf_counter() - start

    assert result.requeued == 0
    assert result.dead_lettered == 0
    assert elapsed < 0.5


# --- teardown_run ----------------------------------------------------------


def test_teardown_run_deletes_scoped_rows(db: Path) -> None:
    with bus_db.connection(db) as conn:
        cid_exact = _insert_channel(conn, "run/target")
        cid_nested = _insert_channel(conn, "run/target/lane/1")
        mid1 = _insert_message(conn, cid_exact)
        mid2 = _insert_message(conn, cid_nested)
        _insert_claim(
            conn,
            mid2,
            deliveries=1,
            lease_until=_iso(datetime.now(UTC) + timedelta(hours=1)),
        )
        conn.execute(
            "INSERT INTO cursors (consumer, channel_id, last_ack_id) VALUES (?, ?, 0)",
            ("role@target", cid_exact),
        )
        conn.execute(
            "INSERT INTO consumers (id, role, run) VALUES (?, ?, ?)",
            ("role@target", "role", "target"),
        )

    with bus_db.connection(db) as conn:
        deleted = bus_db.teardown_run(conn, "target")

    assert deleted > 0

    with bus_db.connection(db) as conn:
        remaining_channels = conn.execute(
            "SELECT COUNT(*) FROM channels WHERE name LIKE 'run/target%'"
        ).fetchone()[0]
        remaining_messages = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE id IN (?, ?)", (mid1, mid2)
        ).fetchone()[0]
        remaining_consumers = conn.execute(
            "SELECT COUNT(*) FROM consumers WHERE run = 'target'"
        ).fetchone()[0]

    assert remaining_channels == 0
    assert remaining_messages == 0
    assert remaining_consumers == 0


def test_teardown_run_leaves_other_runs_untouched(db: Path) -> None:
    with bus_db.connection(db) as conn:
        cid_target = _insert_channel(conn, "run/target")
        cid_other = _insert_channel(conn, "run/other")
        _insert_message(conn, cid_target)
        mid_other = _insert_message(conn, cid_other)
        conn.execute(
            "INSERT INTO consumers (id, role, run) VALUES (?, ?, ?)",
            ("role@other", "role", "other"),
        )

    with bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    with bus_db.connection(db) as conn:
        other_channel = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/other'"
        ).fetchone()
        other_message = conn.execute(
            "SELECT 1 FROM messages WHERE id = ?", (mid_other,)
        ).fetchone()
        other_consumer = conn.execute(
            "SELECT 1 FROM consumers WHERE run = 'other'"
        ).fetchone()

    assert other_channel is not None
    assert other_message is not None
    assert other_consumer is not None


def test_teardown_run_does_not_match_prefix_lookalikes(db: Path) -> None:
    # "run/targetx" must not be swept up by teardown_run("target")
    with bus_db.connection(db) as conn:
        _insert_channel(conn, "run/targetx")

    with bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    with bus_db.connection(db) as conn:
        row = conn.execute(
            "SELECT 1 FROM channels WHERE name = 'run/targetx'"
        ).fetchone()
    assert row is not None


def test_teardown_run_rejects_invalid_atom(db: Path) -> None:
    with bus_db.connection(db) as conn, pytest.raises(InvalidAddressError):
        bus_db.teardown_run(conn, "Bad Run!")


def test_teardown_run_returns_zero_for_unknown_run(db: Path) -> None:
    with bus_db.connection(db) as conn:
        deleted = bus_db.teardown_run(conn, "nonexistent")
    assert deleted == 0


def _insert_reply(
    conn: sqlite3.Connection,
    channel_id: int,
    *,
    reply_to: int | None = None,
    thread_id: int | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO messages (channel_id, sender, type, body, reply_to, thread_id) "
        "VALUES (?, 'role@run-a', 'test', '{}', ?, ?)",
        (channel_id, reply_to, thread_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _row_counts(db: Path) -> tuple[int, int]:
    with bus_db.connection(db) as conn:
        return (
            conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0],
        )


@pytest.mark.parametrize("ref", ["reply_to", "thread_id"])
def test_teardown_run_blocked_by_outside_reference_is_typed_and_atomic(
    db: Path, ref: str
) -> None:
    """QA store #6: a message OUTSIDE the run referencing one inside it
    made teardown die on a raw FOREIGN KEY IntegrityError (a traceback in
    the CLI), so the run could never be torn down. Existing DBs keep the
    FK (no migration chain), so teardown pre-checks and refuses with a
    typed error naming the blockers — and deletes nothing."""
    with bus_db.connection(db) as conn:
        inside_cid = _insert_channel(conn, "run/target/c", kind="broadcast")
        outside_cid = _insert_channel(conn, "run/other/c", kind="broadcast")
        inside = _insert_message(conn, inside_cid)
        outside = _insert_reply(conn, outside_cid, **{ref: inside})
    before = _row_counts(db)

    with pytest.raises(TeardownBlockedError) as info, bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    err = info.value
    assert err.blockers == [(outside, "run/other/c")]
    assert err.total == 1
    assert f"#{outside}" in str(err)
    assert "run/other/c" in str(err)
    assert _row_counts(db) == before


def test_teardown_run_blocker_list_is_capped(db: Path) -> None:
    with bus_db.connection(db) as conn:
        inside_cid = _insert_channel(conn, "run/target/c", kind="broadcast")
        outside_cid = _insert_channel(conn, "ops/log", kind="broadcast")
        inside = _insert_message(conn, inside_cid)
        outside = [_insert_reply(conn, outside_cid, reply_to=inside) for _ in range(12)]

    with pytest.raises(TeardownBlockedError) as info, bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    err = info.value
    assert err.total == 12
    assert [mid for mid, _ in err.blockers] == outside[:10]
    assert "and 2 more" in str(err)
    assert f"#{outside[10]}" not in str(err)


def _blocked_fixture(db: Path) -> None:
    """A run message with a claim, and an outside reply referencing it."""
    with bus_db.connection(db) as conn:
        inside_cid = _insert_channel(conn, "run/target/q")
        outside_cid = _insert_channel(conn, "run/other/c", kind="broadcast")
        inside = _insert_message(conn, inside_cid)
        _insert_claim(
            conn, inside, lease_until=_iso(datetime.now(UTC) + timedelta(hours=1))
        )
        _insert_reply(conn, outside_cid, reply_to=inside)


def _claim_count(db: Path) -> int:
    with bus_db.connection(db) as conn:
        return conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]


def test_teardown_run_race_backstop_is_typed_and_rolled_back(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pre-check runs before the transaction holds the write lock, so
    a peer can commit a referencing message in between. Simulated by a
    pre-check that sees nothing: the DELETE's FK failure must still come
    out as the typed refusal, and db.connection's rollback must restore
    the claims/cursors rows deleted before it."""
    _blocked_fixture(db)
    before = (_row_counts(db), _claim_count(db))
    real = bus_db._teardown_blockers
    calls: list[int] = []

    def blind_first(conn: sqlite3.Connection, ids: list[int]):
        calls.append(1)
        return ([], 0) if len(calls) == 1 else real(conn, ids)

    monkeypatch.setattr(bus_db, "_teardown_blockers", blind_first)
    with pytest.raises(TeardownBlockedError), bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")

    assert len(calls) == 2
    assert (_row_counts(db), _claim_count(db)) == before


def test_teardown_run_reraises_an_unexplained_integrity_error(
    db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _blocked_fixture(db)
    monkeypatch.setattr(bus_db, "_teardown_blockers", lambda _c, _ids: ([], 0))
    with pytest.raises(sqlite3.IntegrityError), bus_db.connection(db) as conn:
        bus_db.teardown_run(conn, "target")


def test_teardown_run_allows_references_within_the_run(db: Path) -> None:
    """Replies inside the run (even across its channels) go with it."""
    with bus_db.connection(db) as conn:
        a = _insert_channel(conn, "run/target/a", kind="broadcast")
        b = _insert_channel(conn, "run/target/b", kind="broadcast")
        root = _insert_message(conn, a)
        _insert_reply(conn, b, reply_to=root, thread_id=root)
        # A run-internal message replying OUTWARD doesn't block either.
        outside_cid = _insert_channel(conn, "run/other/c", kind="broadcast")
        foreign = _insert_message(conn, outside_cid)
        _insert_reply(conn, a, reply_to=foreign)

    with bus_db.connection(db) as conn:
        removed = bus_db.teardown_run(conn, "target")

    assert removed > 0
    assert _row_counts(db) == (1, 1)


# --------------------------------------------------------------------------- #
# init_db concurrency hardening (verify-004 fix round)
# --------------------------------------------------------------------------- #


def test_init_db_fast_path_skips_migration_read(db: Path, monkeypatch) -> None:
    """An already-migrated DB must not re-run (or even read) the script —
    re-applying it on a live file was the 'database is locked' race."""
    bus_db._reset_init_cache()
    monkeypatch.setattr(
        bus_db, "_V2_MIGRATION", Path("does/not/exist/0002.sql")
    )
    resolved = bus_db.init_db(db)  # would raise if the script were read
    assert resolved == db.resolve()


class _LockedConn:
    """Stand-in for sqlite3.connect whose executescript always hits the
    lock — sqlite3.Connection is an immutable C type, so the lock is
    simulated at the connect seam instead of by patching the method."""

    def executescript(self, _sql: str) -> None:
        raise sqlite3.OperationalError("database is locked")

    def close(self) -> None:  # pragma: no cover -- trivial stub
        pass


def test_init_db_accepts_peer_initialisation_after_lock(
    tmp_path: Path, monkeypatch
) -> None:
    """A locked first-create retries, and succeeds by ACCEPTING a peer
    process's completed initialisation rather than winning the lock."""
    bus_db._reset_init_cache()
    target = tmp_path / "peer.db"
    probes = iter([False, True])
    monkeypatch.setattr(bus_db, "_schema_current", lambda _p: next(probes))
    monkeypatch.setattr(bus_db.time, "sleep", lambda _s: None)
    monkeypatch.setattr(bus_db.sqlite3, "connect", lambda *_a, **_k: _LockedConn())
    assert bus_db.init_db(target) == target.resolve()


def test_init_db_raises_after_retries_exhausted(tmp_path: Path, monkeypatch) -> None:
    bus_db._reset_init_cache()
    monkeypatch.setattr(bus_db, "_schema_current", lambda _p: False)
    monkeypatch.setattr(bus_db.time, "sleep", lambda _s: None)
    monkeypatch.setattr(bus_db.sqlite3, "connect", lambda *_a, **_k: _LockedConn())
    with pytest.raises(sqlite3.OperationalError):
        bus_db.init_db(tmp_path / "never.db")


def test_schema_current_probe_edges(tmp_path: Path, db: Path) -> None:
    # Missing file: not initialised (and must not be created by probing).
    ghost = tmp_path / "ghost.db"
    assert bus_db._schema_current(ghost) is False
    assert not ghost.exists()
    # Garbage bytes: sqlite error -> not initialised.
    garbage = tmp_path / "garbage.db"
    garbage.write_bytes(b"definitely not a sqlite file")
    assert bus_db._schema_current(garbage) is False
    # Valid empty DB without bus_meta: not initialised.
    bare = tmp_path / "bare.db"
    sqlite3.connect(bare).close()
    assert bus_db._schema_current(bare) is False
    # Initialised DB with a stale version string: not current.
    with bus_db.connection(db) as conn:
        conn.execute("UPDATE bus_meta SET value = '0' WHERE key = 'schema_version'")
    assert bus_db._schema_current(db) is False
    # Restore and confirm the positive probe.
    with bus_db.connection(db) as conn:
        conn.execute(
            "UPDATE bus_meta SET value = ? WHERE key = 'schema_version'",
            (bus_db.SCHEMA_VERSION,),
        )
    assert bus_db._schema_current(db) is True


def test_init_db_refuses_foreign_schema_version(db: Path) -> None:
    """re-verify wave regression: a DB stamped by another schema version
    must be refused, never re-stamped — CREATE IF NOT EXISTS would keep
    the old tables while marking the file current."""
    with bus_db.connection(db) as conn:
        conn.execute("UPDATE bus_meta SET value = '1' WHERE key = 'schema_version'")
    bus_db._reset_init_cache()
    with pytest.raises(SchemaMismatchError, match="schema version '1'") as info:
        bus_db.init_db(db)
    # Still a RuntimeError: that is what init_db raised here before the
    # typed error existed (QA store #8), so old callers keep working.
    assert isinstance(info.value, RuntimeError)
    # The stamp must be untouched by the refusal.
    assert bus_db._stored_schema_version(db) == "1"


def _tables(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        conn.close()


def _foreign_file(path: Path, *ddl: str) -> Path:
    conn = sqlite3.connect(path)
    for statement in ddl:
        conn.execute(statement)
    conn.commit()
    conn.close()
    return path


@pytest.mark.parametrize(
    "ddl",
    [
        ("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",),
        # v1-style: some raven-named tables plus foreign ones.
        (
            "CREATE TABLE messages (id INTEGER PRIMARY KEY, text TEXT)",
            "CREATE TABLE aliases (name TEXT)",
        ),
    ],
    ids=["user-table", "v1-like"],
)
def test_init_db_refuses_unstamped_file_with_foreign_tables(
    tmp_path: Path, ddl: tuple[str, ...]
) -> None:
    """QA store #8: an unstamped SQLite file holding tables raven did not
    create was silently adopted and stamped '2' (or half-migrated before
    a raw OperationalError). It is refused untouched, with a typed error."""
    bus_db._reset_init_cache()
    target = _foreign_file(tmp_path / "foreign.db", *ddl)
    before = _tables(target)

    with pytest.raises(SchemaMismatchError, match="aliases|notes"):
        bus_db.init_db(target)

    assert _tables(target) == before
    assert bus_db._stored_schema_version(target) is None


def test_init_db_adopts_unstamped_raven_tables(tmp_path: Path) -> None:
    """Only raven's own tables and no stamp = a peer's first-create in
    flight (or one that crashed before stamping): finish it, as before."""
    bus_db._reset_init_cache()
    target = tmp_path / "half.db"
    conn = sqlite3.connect(target)
    conn.executescript(bus_db._V2_MIGRATION.read_text(encoding="utf-8"))
    conn.close()

    assert bus_db.init_db(target) == target.resolve()
    assert bus_db._stored_schema_version(target) == bus_db.SCHEMA_VERSION


def test_init_db_refuses_raven_named_tables_of_the_wrong_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raven-named tables the v2 script cannot apply over fail
    deterministically ('no such column'): a SchemaMismatchError at once,
    never five busy-retries ending in a raw OperationalError."""
    bus_db._reset_init_cache()
    sleeps: list[float] = []
    monkeypatch.setattr(bus_db.time, "sleep", sleeps.append)
    target = _foreign_file(
        tmp_path / "shape.db", "CREATE TABLE messages (id INTEGER PRIMARY KEY, text TEXT)"
    )

    with pytest.raises(SchemaMismatchError, match="no such column"):
        bus_db.init_db(target)
    assert sleeps == []


class _BrokenConn:
    """connect() stand-in whose script apply fails deterministically."""

    def executescript(self, _sql: str) -> None:
        raise sqlite3.OperationalError("disk I/O error")

    def close(self) -> None:  # pragma: no cover -- trivial stub
        pass


def test_init_db_does_not_retry_non_busy_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only busy/locked errors are worth a retry; anything else on a
    fresh file surfaces immediately, unwrapped (it is not a schema
    problem)."""
    bus_db._reset_init_cache()
    sleeps: list[float] = []
    monkeypatch.setattr(bus_db.time, "sleep", sleeps.append)
    monkeypatch.setattr(bus_db.sqlite3, "connect", lambda *_a, **_k: _BrokenConn())

    with pytest.raises(sqlite3.OperationalError, match="disk I/O") as info:
        bus_db.init_db(tmp_path / "fresh.db")
    assert not isinstance(info.value, SchemaMismatchError)
    assert sleeps == []


def _coded(message: str, code: int | None) -> sqlite3.OperationalError:
    exc = sqlite3.OperationalError(message)
    if code is not None:
        exc.sqlite_errorcode = code  # set by sqlite3 on real errors (3.11+)
    return exc


@pytest.mark.parametrize(
    ("exc", "busy"),
    [
        (_coded("database is locked", sqlite3.SQLITE_BUSY), True),
        (_coded("database is locked", 517), True),  # SQLITE_BUSY_SNAPSHOT
        (_coded("database table is locked", sqlite3.SQLITE_LOCKED), True),
        (_coded("no such column: channel_id", 1), False),
        (_coded("database is locked", None), True),  # hand-built: by message
        (_coded("disk I/O error", None), False),
    ],
)
def test_is_busy_classifies_retryable_errors(
    exc: sqlite3.OperationalError, busy: bool
) -> None:
    assert bus_db._is_busy(exc) is busy
