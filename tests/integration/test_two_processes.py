"""End-to-end test that examples/02-two-processes really enforces
exactly-one-winner delivery on a native v2 queue channel.

Spawns two ``consumer.py`` processes racing for the same queue channel,
runs ``producer.py`` to feed it 5 tasks, then asserts (from both
processes' stdout AND the raw ``claims`` table) that every task id was
claimed by exactly one consumer and none were double-delivered.
"""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent.parent / "examples" / "02-two-processes"
)

_GOT_RE = re.compile(r"got #(\d+)")


@pytest.mark.skipif(
    not EXAMPLE_DIR.exists(),
    reason="examples/02-two-processes/ not present (probably built without examples)",
)
def test_two_consumers_split_five_tasks_with_no_double_delivery(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    env = {**os.environ, "RAVEN_DB": str(db), "PYTHONUNBUFFERED": "1"}

    consumers = [
        subprocess.Popen(
            [
                sys.executable, "-u", "consumer.py",
                "--id", f"consumer-{i}@demo",
                "--poll-interval", "0.02",
            ],
            cwd=str(EXAMPLE_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for i in range(2)
    ]

    try:
        producer = subprocess.run(
            [sys.executable, "-u", "producer.py"],
            cwd=str(EXAMPLE_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        assert producer.returncode == 0, producer.stderr
        assert producer.stdout.count("[producer] sent") == 5

        # Poll the DB until all 5 tasks are completed (or a safety-fuse
        # deadline). The fuse is generous: two consumer processes + this
        # poller all share one WAL file, and on Windows a claimant can sit
        # out several seconds behind the 5s busy timeout — a tight fuse
        # here flaked ~1 in 4 runs at wave-2 gating. Each poll connection
        # is explicitly closed so the reader never lingers as a lock peer.
        deadline = time.time() + 15.0
        done = 0
        while time.time() < deadline:
            conn = sqlite3.connect(db, timeout=5.0)
            try:
                done = conn.execute(
                    "SELECT count(*) FROM claims WHERE state = 'done'"
                ).fetchone()[0]
            finally:
                conn.close()
            if done >= 5:
                break
            time.sleep(0.05)
    finally:
        outputs = []
        errs = []
        for c in consumers:
            c.terminate()
            try:
                stdout, stderr = c.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                c.kill()
                stdout, stderr = c.communicate()
            outputs.append(stdout)
            errs.append(stderr)

    assert done == 5, (
        f"expected 5 completed claims, got {done}. Consumer stdout:\n"
        + "\n---\n".join(outputs)
        + "\nConsumer stderr:\n"
        + "\n---\n".join(errs)
    )

    claimed_ids_by_consumer = [
        {int(m) for m in _GOT_RE.findall(out)} for out in outputs
    ]
    all_claimed = set().union(*claimed_ids_by_consumer)
    assert all_claimed == {1, 2, 3, 4, 5}, (
        f"expected ids 1-5 claimed exactly once total, got {all_claimed}"
    )
    overlap = claimed_ids_by_consumer[0] & claimed_ids_by_consumer[1]
    assert not overlap, f"task(s) {overlap} were claimed by both consumers"

    # The consumers were just terminate()d; on Windows their -shm/-wal
    # handles release asynchronously, and a read that triggers WAL
    # recovery in that window intermittently raises 'disk I/O error'.
    # Retry briefly — the DB itself is intact.
    conn = None
    deadline = time.time() + 5.0
    while True:
        try:
            conn = sqlite3.connect(db, timeout=5.0)
            conn.execute("SELECT count(*) FROM messages").fetchone()
            break
        except sqlite3.OperationalError:
            if conn is not None:
                conn.close()
                conn = None
            if time.time() >= deadline:
                raise
            time.sleep(0.1)
    try:
        assert conn.execute(
            "SELECT count(*) FROM messages WHERE type = 'task'"
        ).fetchone()[0] == 5
        state_counts = dict(
            conn.execute("SELECT state, count(*) FROM claims GROUP BY state").fetchall()
        )
        assert state_counts == {"done": 5}
        # One claim row per message: the schema's message_id PK already
        # forbids double-claiming, but assert the count explicitly too.
        assert conn.execute("SELECT count(*) FROM claims").fetchone()[0] == 5
    finally:
        conn.close()
