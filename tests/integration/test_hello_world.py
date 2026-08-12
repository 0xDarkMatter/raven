"""Integration test for examples/01-hello-world (native raven_bus v2).

Runs ``hello.py`` as a subprocess against a tmp-path DB, then verifies
both the terminal transcript and the raw SQLite state it left behind.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

EXAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "examples" / "01-hello-world"


def test_hello_world_round_trip(tmp_path: Path) -> None:
    db = tmp_path / "hello.db"

    result = subprocess.run(
        [sys.executable, "hello.py", "--db", str(db)],
        cwd=str(EXAMPLE_DIR),
        capture_output=True,
        text=True,
        timeout=15.0,
    )
    assert result.returncode == 0, (
        f"hello.py exited {result.returncode}.\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    assert "sent #1 alice@hello -> run/hello/lobby type=greeting" in result.stdout
    assert "bob inbox: 1 message" in result.stdout
    assert "body: {'text': 'hello, bob'}" in result.stdout
    assert "acked. inbox now empty: True" in result.stdout

    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT sender, type, body FROM messages").fetchall()
        assert len(row) == 1, "hello.py should append exactly one message"
        sender, msg_type, body = row[0]
        assert sender == "alice@hello"
        assert msg_type == "greeting"
        assert body == '{"text": "hello, bob"}'

        channel_kind = conn.execute(
            "SELECT kind FROM channels WHERE name = 'run/hello/lobby'"
        ).fetchone()
        assert channel_kind == ("broadcast",)

        cursor_row = conn.execute(
            "SELECT consumer, last_ack_id FROM cursors"
        ).fetchall()
        assert cursor_row == [("bob@hello", 1)], "bob's cursor should be acked to id 1"
