"""Integration tests for examples/03-news-desk and examples/04-server-incident.

Each test runs the example's ``run.py`` as a subprocess — exactly as a human
would — then checks terminal output for completion markers and spot-checks
the SQLite DB for correct message counts and correlation-ID threading.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent.parent / "examples"
NEWS_DESK_DIR = EXAMPLES_DIR / "03-news-desk"
INCIDENT_DIR = EXAMPLES_DIR / "04-server-incident"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _count(conn: sqlite3.Connection, msg_type: str) -> int:
    return conn.execute(
        "SELECT count(*) FROM messages WHERE type=?", (msg_type,)
    ).fetchone()[0]


def _fail(result: subprocess.CompletedProcess) -> str:
    return (
        f"run.py exited {result.returncode}.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


# ---------------------------------------------------------------------------
# 03-news-desk
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not NEWS_DESK_DIR.exists(),
    reason="examples/03-news-desk/ not present",
)
def test_news_desk_pipeline_publishes_all_articles(tmp_path: Path) -> None:
    """5-agent fan-out/fan-in editorial pipeline runs to completion.

    Pipeline shape::

        scout → writer → editor       → publisher
                       ↘ fact_checker ↗

    Verifies:
    - All N articles are published (stdout marker + DB count).
    - Writer fans each lead out to both editor AND fact_checker (draft count = N×2).
    - Publisher receives one approval + one verification per article.
    - Every draft/approval/verification carries a correlation_id.
    - Every correlation_id traces back to a scout lead.
    """
    db = tmp_path / "newsdesk.db"
    n = 3

    result = subprocess.run(
        [sys.executable, "run.py", "--articles", str(n), "--db", str(db)],
        cwd=str(NEWS_DESK_DIR),
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    assert result.returncode == 0, _fail(result)

    # Completion markers.
    assert result.stdout.count("PUBLISHED") == n, (
        f"expected {n} PUBLISHED lines; got {result.stdout.count('PUBLISHED')}"
    )
    assert f"published {n}/{n} articles" in result.stdout
    assert "wrap broadcast" in result.stdout

    with sqlite3.connect(db) as conn:
        # Message counts.
        assert _count(conn, "lead") == n
        assert _count(conn, "draft") == n * 2, (
            "writer should fan each lead to editor AND fact_checker"
        )
        assert _count(conn, "approval") == n
        assert _count(conn, "verification") == n

        # All pipeline messages must carry a conversation_id (correlation) so the
        # publisher can pair approvals with their matching verifications.
        missing_corr = conn.execute(
            "SELECT count(*) FROM messages "
            "WHERE type IN ('draft', 'approval', 'verification') "
            "AND conversation_id IS NULL"
        ).fetchone()[0]
        assert missing_corr == 0, f"{missing_corr} pipeline messages have no conversation_id"

        # Every conversation_id must resolve to a lead (the originating article).
        lead_ids = {r[0] for r in conn.execute("SELECT id FROM messages WHERE type='lead'")}
        corr_ids = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT conversation_id FROM messages "
                "WHERE type IN ('draft', 'approval', 'verification')"
            )
        }
        assert corr_ids == lead_ids, (
            f"conversation_ids {corr_ids} don't match lead ids {lead_ids}"
        )


# ---------------------------------------------------------------------------
# 04-server-incident
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not INCIDENT_DIR.exists(),
    reason="examples/04-server-incident/ not present",
)
def test_incident_pipeline_resolves_all_faults(tmp_path: Path) -> None:
    """5-agent SRE pipeline detects, diagnoses, fixes, and verifies all faults.

    Pipeline shape::

        monitor → triager → diagnoser → fixer → verifier → (monitor)

    Verifies:
    - All N faults are resolved (stdout marker + DB count).
    - Each fault produces exactly one complete pipeline chain (incident →
      investigate → prescription → fix_applied → resolved).
    - Every downstream message carries the originating incident's id as its
      correlation_id, enabling a full audit trail per fault.
    - Server is healthy at the end (``'problems': []`` in stdout).
    """
    db = tmp_path / "incident.db"
    faults = ["db_disconnected", "cpu_saturated", "errors_spiking"]
    n = len(faults)

    result = subprocess.run(
        [sys.executable, "run.py", "--faults", *faults, "--db", str(db)],
        cwd=str(INCIDENT_DIR),
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    assert result.returncode == 0, _fail(result)

    # Completion markers.
    assert result.stdout.count("RESOLVED") == n, (
        f"expected {n} RESOLVED lines; got {result.stdout.count('RESOLVED')}"
    )
    assert f"resolved {n}/{n} incidents" in result.stdout
    # Server health dict is printed at the end of the run.
    assert "'problems': []" in result.stdout, (
        "server should have no remaining problems after all fixes"
    )

    with sqlite3.connect(db) as conn:
        # One complete pipeline chain per fault.
        for msg_type in ("incident", "investigate", "prescription", "fix_applied", "resolved"):
            actual = _count(conn, msg_type)
            assert actual == n, f"type={msg_type!r}: expected {n}, got {actual}"

        # All downstream messages must carry a conversation_id (correlation).
        missing_corr = conn.execute(
            "SELECT count(*) FROM messages "
            "WHERE type IN ('investigate', 'prescription', 'fix_applied', 'resolved') "
            "AND conversation_id IS NULL"
        ).fetchone()[0]
        assert missing_corr == 0, f"{missing_corr} downstream messages have no conversation_id"

        # The set of conversation_ids used downstream must equal the set of
        # incident message ids — one complete chain per fault, no leakage.
        incident_ids = {
            r[0] for r in conn.execute("SELECT id FROM messages WHERE type='incident'")
        }
        downstream_corr_ids = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT conversation_id FROM messages "
                "WHERE type IN ('investigate', 'prescription', 'fix_applied', 'resolved')"
            )
        }
        assert downstream_corr_ids == incident_ids, (
            f"pipeline chains {downstream_corr_ids} don't match incident ids {incident_ids}"
        )
