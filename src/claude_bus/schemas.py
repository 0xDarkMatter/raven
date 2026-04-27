"""Pluggable schema registry for raven message bodies.

By default the registry is empty and validation is permissive — you
can send messages of any string-typed kind with any JSON-serialisable
body. Register a Pydantic model to start enforcing a body shape per
type::

    from pydantic import BaseModel
    from claude_bus import SchemaRegistry

    class PlanBody(BaseModel):
        step: int
        goal: str

    SchemaRegistry.register("plan", PlanBody)

In strict mode, sending a message of an unregistered type raises
:class:`SchemaValidationError`.
"""

from __future__ import annotations

from typing import Any, ClassVar

import structlog
from pydantic import BaseModel, ValidationError

from claude_bus.exceptions import SchemaValidationError

log = structlog.get_logger(__name__)


class SchemaRegistry:
    """Process-global registry of message-type → Pydantic body schema."""

    _schemas: ClassVar[dict[str, type[BaseModel]]] = {}
    _strict: ClassVar[bool] = False

    @classmethod
    def register(cls, message_type: str, schema: type[BaseModel]) -> None:
        """Register a Pydantic schema for messages of this type."""
        if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
            raise TypeError(
                f"schema for type={message_type!r} must subclass pydantic.BaseModel"
            )
        cls._schemas[message_type] = schema

    @classmethod
    def unregister(cls, message_type: str) -> None:
        """Remove the registered schema for ``message_type`` (no-op if absent)."""
        cls._schemas.pop(message_type, None)

    @classmethod
    def clear(cls) -> None:
        """Wipe all registered schemas. Mostly useful in tests."""
        cls._schemas.clear()

    @classmethod
    def types(cls) -> list[str]:
        """Return the sorted list of currently registered message types."""
        return sorted(cls._schemas)

    @classmethod
    def strict_mode(cls, enabled: bool) -> None:
        """Toggle strict validation.

        When strict, sending a message whose type has no registered
        schema raises :class:`SchemaValidationError`.
        """
        cls._strict = bool(enabled)

    @classmethod
    def is_strict(cls) -> bool:
        return cls._strict

    @classmethod
    def validate(cls, message_type: str, body: dict[str, Any]) -> dict[str, Any]:
        """Validate ``body`` against the registered schema for ``message_type``.

        Returns the JSON-safe ``dict`` form of the validated model.
        For unregistered types: in permissive mode (default) returns
        ``body`` unchanged and logs a warning; in strict mode raises
        :class:`SchemaValidationError`.
        """
        schema_cls = cls._schemas.get(message_type)
        if schema_cls is None:
            if cls._strict:
                raise SchemaValidationError(
                    f"unknown message type {message_type!r}; "
                    f"strict mode requires a registered schema. "
                    f"Registered: {cls.types()}"
                )
            # Permissive mode is the *default* — accepting unregistered
            # types is expected behaviour, not a problem worth a WARNING.
            # Surface it at DEBUG for users who explicitly opt in.
            log.debug(
                "claude_bus.schema.unregistered_type",
                message_type=message_type,
            )
            return dict(body)

        try:
            model = schema_cls.model_validate(body)
        except ValidationError as exc:
            log.warning(
                "claude_bus.schema.failed",
                message_type=message_type,
                error=str(exc),
            )
            raise SchemaValidationError(
                f"body for type={message_type!r} failed validation: {exc}"
            ) from exc
        return model.model_dump(mode="json", exclude_none=False)


__all__ = ["SchemaRegistry"]
