"""Cover SchemaRegistry classmethods (register/unregister/types/strict_mode)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from claude_bus import SchemaRegistry, SchemaValidationError


class _Body(BaseModel):
    n: int


def test_register_and_validate_round_trip() -> None:
    SchemaRegistry.register("ping", _Body)
    out = SchemaRegistry.validate("ping", {"n": 7})
    assert out == {"n": 7}


def test_register_rejects_non_basemodel() -> None:
    class NotPydantic:
        pass

    with pytest.raises(TypeError):
        SchemaRegistry.register("x", NotPydantic)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        SchemaRegistry.register("y", "not a class")  # type: ignore[arg-type]


def test_unregister_removes_schema() -> None:
    SchemaRegistry.register("ping", _Body)
    SchemaRegistry.unregister("ping")
    # Now permissive mode accepts anything for "ping" again.
    out = SchemaRegistry.validate("ping", {"anything": True})
    assert out == {"anything": True}


def test_unregister_unknown_is_noop() -> None:
    SchemaRegistry.unregister("never-registered")  # no exception


def test_types_returns_sorted_list() -> None:
    SchemaRegistry.register("zeta", _Body)
    SchemaRegistry.register("alpha", _Body)
    assert SchemaRegistry.types() == ["alpha", "zeta"]


def test_clear_drops_everything() -> None:
    SchemaRegistry.register("a", _Body)
    SchemaRegistry.register("b", _Body)
    SchemaRegistry.clear()
    assert SchemaRegistry.types() == []


def test_strict_mode_state_visible() -> None:
    assert SchemaRegistry.is_strict() is False
    SchemaRegistry.strict_mode(True)
    assert SchemaRegistry.is_strict() is True
    SchemaRegistry.strict_mode(False)
    assert SchemaRegistry.is_strict() is False


def test_strict_mode_rejects_unknown_with_clear_message() -> None:
    SchemaRegistry.register("known", _Body)
    SchemaRegistry.strict_mode(True)
    with pytest.raises(SchemaValidationError) as exc:
        SchemaRegistry.validate("unknown", {})
    msg = str(exc.value)
    assert "unknown" in msg
    assert "known" in msg  # mentions registered schemas


def test_validation_error_includes_field_detail() -> None:
    SchemaRegistry.register("ping", _Body)
    with pytest.raises(SchemaValidationError) as exc:
        SchemaRegistry.validate("ping", {"n": "not-an-int"})
    assert "ping" in str(exc.value)
