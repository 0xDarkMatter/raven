"""Tests for the ``raven_bus`` package surface (``__init__`` exports)."""

from __future__ import annotations

import raven_bus
from raven_bus import exceptions


def test_every_exception_class_is_exported_from_the_package() -> None:
    """The README names ``from raven_bus import WrongChannelKindError``,
    which failed: the package exported only some of the hierarchy. Every
    public exception class must be importable from the package root."""
    public = {
        name
        for name, value in vars(exceptions).items()
        if isinstance(value, type) and issubclass(value, raven_bus.RavenBusError)
    }
    assert public <= set(raven_bus.__all__)
    for name in public:
        assert getattr(raven_bus, name) is getattr(exceptions, name)
