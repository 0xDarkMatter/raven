"""Async subscribe iterator behaviour."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from claude_bus import BusClient


@pytest.mark.asyncio
async def test_subscribe_yields_existing_message(db_path: Path) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)
    sent = a.send(to=b.address, type="ping", body={"n": 1})

    received: list[int] = []

    async def consume() -> None:
        async for msg in b.subscribe(poll_interval_s=0.05):
            received.append(msg.id)
            assert msg.status == "read"
            break

    await asyncio.wait_for(consume(), timeout=2.0)
    assert received == [sent.id]


@pytest.mark.asyncio
async def test_subscribe_yields_messages_arriving_during_loop(
    db_path: Path,
) -> None:
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b = BusClient(session_id="s", role="b", db_path=db_path)

    received: list[int] = []

    async def consume() -> None:
        async for msg in b.subscribe(poll_interval_s=0.05):
            received.append(msg.id)
            if len(received) == 3:
                break

    async def produce() -> None:
        for i in range(3):
            a.send(to=b.address, type="ping", body={"i": i})
            await asyncio.sleep(0.05)

    await asyncio.gather(consume(), produce())
    assert len(received) == 3


@pytest.mark.asyncio
async def test_subscribe_cancellation_propagates(db_path: Path) -> None:
    b = BusClient(session_id="s", role="b", db_path=db_path)

    async def consume() -> None:
        async for _ in b.subscribe(poll_interval_s=0.05):
            pass  # never reached — empty inbox

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_subscribe_at_most_once_between_two_subscribers(
    db_path: Path,
) -> None:
    """When two consumers subscribe to the same role, each message is acked
    before yield, so a given message reaches at most one consumer."""
    a = BusClient(session_id="s", role="a", db_path=db_path)
    b1 = BusClient(session_id="s", role="b", db_path=db_path)
    b2 = BusClient(session_id="s", role="b", db_path=db_path)

    sent = a.send(to=b1.address, type="ping", body={})

    seen: list[tuple[str, int]] = []

    async def consume(name: str, client: BusClient) -> None:
        try:
            async for msg in client.subscribe(poll_interval_s=0.02):
                seen.append((name, msg.id))
                break
        except asyncio.CancelledError:
            pass

    t1 = asyncio.create_task(consume("b1", b1))
    t2 = asyncio.create_task(consume("b2", b2))

    # Give one a chance to pick it up.
    await asyncio.sleep(0.2)
    t1.cancel()
    t2.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)

    matching = [s for s in seen if s[1] == sent.id]
    assert len(matching) == 1, f"expected exactly one consumer to see #{sent.id}, got {seen}"
