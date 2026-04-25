# Hello world

Smallest possible round-trip — a producer sends one message, a consumer reads and acks it.

```bash
python hello.py
```

Expected output:

```
sent #1 alice:s1 -> bob:s1 type=greeting
bob inbox: 1 message
  body: {'text': 'hello, bob'}
acked. inbox now empty: True
```
