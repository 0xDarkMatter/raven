# Hello world

Smallest possible round-trip on native raven_bus v2 primitives — no CLI,
no compat shim. Alice appends one message to a broadcast channel; bob
reads it via a cursor and acks it.

```bash
python hello.py
```

Expected output:

```
sent #1 alice@hello -> run/hello/lobby type=greeting
bob inbox: 1 message
  body: {'text': 'hello, bob'}
acked. inbox now empty: True
```

Pass `--db PATH` to point at a specific SQLite file (default
`./hello.db`, deleted before each run).
