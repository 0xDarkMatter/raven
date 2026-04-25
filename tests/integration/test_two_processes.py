"""End-to-end test that the two-process example actually coordinates live.

Spawns ``examples/02-two-processes/consumer.py`` as a real subprocess,
runs ``producer.py`` against the same SQLite file, and asserts the
consumer's stdout records every message the producer sent.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent.parent / "examples" / "02-two-processes"
)


@pytest.mark.skipif(
    not EXAMPLE_DIR.exists(),
    reason="examples/02-two-processes/ not present (probably built without examples)",
)
def test_producer_consumer_coordinate_live(tmp_path: Path) -> None:
    db = tmp_path / "bus.db"
    env = {**os.environ, "BUS_DB": str(db), "PYTHONUNBUFFERED": "1"}

    consumer = subprocess.Popen(
        [sys.executable, "-u", str(EXAMPLE_DIR / "consumer.py")],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Wait for consumer to register and start polling.
        deadline = time.time() + 5.0
        consumer_lines: list[str] = []
        while time.time() < deadline:
            line = consumer.stdout.readline()
            if line:
                consumer_lines.append(line)
                if "subscribing" in line:
                    break
            else:
                time.sleep(0.05)
        assert any("subscribing" in line for line in consumer_lines), (
            f"consumer never reported subscribing within 5s: {consumer_lines}"
        )

        # Run the producer to completion.
        producer = subprocess.run(
            [sys.executable, "-u", str(EXAMPLE_DIR / "producer.py")],
            env=env,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        assert producer.returncode == 0, f"producer failed: {producer.stderr}"
        producer_sent = sum(1 for line in producer.stdout.splitlines()
                            if "[producer] sent" in line)
        assert producer_sent == 5

        # Drain consumer output for up to 5s, looking for the 5 received lines.
        received = 0
        deadline = time.time() + 5.0
        while time.time() < deadline and received < 5:
            line = consumer.stdout.readline()
            if not line:
                time.sleep(0.05)
                continue
            consumer_lines.append(line)
            if "[consumer] got" in line:
                received += 1
        assert received == 5, (
            f"consumer received {received}/5 messages. Output:\n"
            + "".join(consumer_lines)
        )
    finally:
        consumer.terminate()
        try:
            consumer.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            consumer.kill()
            consumer.wait()
