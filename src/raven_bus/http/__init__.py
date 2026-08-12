"""ravend — optional loopback HTTP bridge (ADR-005).

Requires the ``[http]`` extra (starlette + uvicorn). Thin-bridge rule:
handlers map 1:1 onto module contracts; no business logic here.

Wave-0 skeleton note (raven2-p2 run): ``app.create_app`` and every
handler signature are FROZEN; lanes implement handler bodies only.
"""

from raven_bus.http.app import create_app

__all__ = ["create_app"]
