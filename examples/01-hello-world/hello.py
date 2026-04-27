"""Smallest possible raven round-trip — see ./README.md."""

from __future__ import annotations

from pathlib import Path

from claude_bus import BusClient

DB = Path(__file__).with_name("hello.db")


def main() -> None:
    if DB.exists():
        DB.unlink()

    alice = BusClient(session_id="s1", role="alice", db_path=DB)
    bob = BusClient(session_id="s1", role="bob", db_path=DB)

    sent = alice.send(to=bob.address, type="greeting", body={"text": "hello, bob"})
    print(f"sent #{sent.id} {sent.sender} -> {sent.recipient} type={sent.type}")

    inbox = bob.inbox()
    print(f"bob inbox: {len(inbox)} message{'s' if len(inbox) != 1 else ''}")
    for msg in inbox:
        print(f"  body: {msg.body}")
        bob.ack(msg.id)

    print(f"acked. inbox now empty: {bob.inbox() == []}")


if __name__ == "__main__":
    main()
