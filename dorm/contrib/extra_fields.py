"""Extra field types — opt-in domain-specific columns.

The dorm core ships the field family Django users expect
(``CharField``, ``IntegerField``, etc.). This module collects the
domain-specific subclasses that aren't on the canonical list yet but
crop up frequently enough to keep DRY:

- :class:`MoneyField` — ``DecimalField`` + currency-aware ``Money``
  value object.
- :class:`SemverField` — ``CharField`` validated against the SemVer
  grammar.
- :class:`PhoneField` — E.164-normalised phone number.
- :class:`ColorField` — ``#RRGGBB`` / ``#RRGGBBAA`` hex strings.
- :class:`JSONSchemaField` — :class:`JSONField` with per-assignment
  JSON Schema validation. Requires the optional ``jsonschema``
  dependency.

Import explicitly; nothing here is re-exported under ``dorm.<Name>``
to keep the top-level namespace lean.
"""
from __future__ import annotations

import dataclasses
import decimal
import re
from typing import Any

from .. import fields as _fields
from ..exceptions import ValidationError


# ── MoneyField ──────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class Money:
    """Value object for currency-aware amounts.

    Stored as ``Decimal`` in the database column with a currency code
    on the field — multi-currency rows need a sibling column for the
    currency (or use ``MoneyField`` per known currency)."""

    amount: decimal.Decimal
    currency: str

    def __post_init__(self) -> None:
        # Normalise after construction so equality / repr work.
        object.__setattr__(self, "amount", decimal.Decimal(self.amount))

    def __str__(self) -> str:
        return f"{self.amount} {self.currency}"


class MoneyField(_fields.DecimalField):
    """Currency-aware decimal column.

    ``currency`` is fixed at field declaration time; rows that need a
    runtime currency should pair this with a sibling
    :class:`CharField` and roll the conversion themselves.

    Assigning either a :class:`Money` instance or a bare
    ``Decimal`` / ``int`` / ``str`` accepted — strings parse via
    ``Decimal``; the currency on a :class:`Money` instance must match
    the field's currency."""

    def __init__(
        self,
        *,
        currency: str = "USD",
        max_digits: int = 20,
        decimal_places: int = 2,
        **kwargs: Any,
    ) -> None:
        if not currency or not currency.isalpha() or len(currency) != 3:
            raise ValueError(
                f"MoneyField(currency={currency!r}) must be an ISO-4217 "
                "3-letter code."
            )
        self.currency = currency.upper()
        super().__init__(
            max_digits=max_digits, decimal_places=decimal_places, **kwargs
        )

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Money):
            if value.currency != self.currency:
                raise ValidationError(
                    f"MoneyField currency mismatch: field={self.currency!r}, "
                    f"value={value.currency!r}"
                )
            return value
        if isinstance(value, decimal.Decimal):
            return Money(amount=value, currency=self.currency)
        if isinstance(value, (int, float)):
            return Money(amount=decimal.Decimal(str(value)), currency=self.currency)
        if isinstance(value, str):
            try:
                return Money(amount=decimal.Decimal(value), currency=self.currency)
            except decimal.InvalidOperation as e:
                raise ValidationError(
                    f"MoneyField could not parse {value!r} as a decimal."
                ) from e
        raise ValidationError(
            f"MoneyField rejects values of type {type(value).__name__}"
        )

    def get_db_prep_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Money):
            return value.amount
        return decimal.Decimal(value)

    def from_db_value(self, value: Any, *_args: Any) -> Any:
        if value is None:
            return None
        return Money(amount=decimal.Decimal(value), currency=self.currency)

    def deconstruct(self) -> tuple[Any, str, list, dict]:
        name, path, args, kwargs = super().deconstruct()
        kwargs["currency"] = self.currency
        return name, path, args, kwargs


# ── SemverField ─────────────────────────────────────────────────────────────


_SEMVER_RE = re.compile(
    r"^(?P<major>0|[1-9]\d*)\."
    r"(?P<minor>0|[1-9]\d*)\."
    r"(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<pre>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+(?P<build>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)


class SemverField(_fields.CharField):
    """Field that stores a `SemVer 2.0.0 <https://semver.org>`_
    version string. Validates the grammar on assignment so a typo
    fails immediately instead of at the next ``ORDER BY`` against the
    column.

    Default ``max_length`` is 64, comfortably above the longest real-
    world SemVer string."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 64)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        if not _SEMVER_RE.match(value):
            raise ValidationError(f"SemverField: {value!r} is not a valid SemVer.")
        return value


# ── PhoneField ──────────────────────────────────────────────────────────────


# Loose E.164 matcher: optional ``+``, 7–15 digits. Strict regional
# validation is out of scope; users that need it should wire
# ``phonenumbers`` and a custom validator.
_PHONE_RE = re.compile(r"^\+?[1-9]\d{6,14}$")


class PhoneField(_fields.CharField):
    """E.164-shaped phone number. Strips whitespace + dashes +
    parentheses before validating so user-pasted strings round-trip
    cleanly."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 20)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        cleaned = re.sub(r"[\s\-()\.]", "", value)
        if not _PHONE_RE.match(cleaned):
            raise ValidationError(
                f"PhoneField: {value!r} is not a valid E.164 number."
            )
        return cleaned


# ── ColorField ──────────────────────────────────────────────────────────────


_COLOR_RE = re.compile(r"^#(?:[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})$")


class ColorField(_fields.CharField):
    """``#RRGGBB`` or ``#RRGGBBAA`` hex color string. Case is
    normalised to upper at storage so equality comparisons aren't
    surprising."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 9)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        if not _COLOR_RE.match(value):
            raise ValidationError(
                f"ColorField: {value!r} is not a #RRGGBB / #RRGGBBAA string."
            )
        return value.upper()


# ── JSONSchemaField ─────────────────────────────────────────────────────────


class JSONSchemaField(_fields.JSONField):
    """``JSONField`` that validates every assignment against a JSON
    Schema. Requires the optional ``jsonschema`` dependency.

    The schema is stored on the field instance and re-used across
    every assignment. Errors raise :class:`ValidationError` so
    callers can catch them alongside other field-level rejections."""

    def __init__(self, *, schema: dict[str, Any], **kwargs: Any) -> None:
        if not isinstance(schema, dict):
            raise TypeError("JSONSchemaField(schema=...) must be a dict.")
        self.schema = schema
        try:
            import jsonschema  # type: ignore[import-not-found]  # ty:ignore[unresolved-import]
        except ImportError as exc:
            raise ImportError(
                "JSONSchemaField requires the 'jsonschema' package. "
                "Install with: pip install jsonschema"
            ) from exc
        self._validator_cls = jsonschema.Draft202012Validator
        # Validate the schema itself at construction time — a typo in
        # ``properties`` would otherwise surface only on the first
        # assignment.
        self._validator_cls.check_schema(schema)
        self._validator = self._validator_cls(schema)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        try:
            self._validator.validate(value)
        except Exception as exc:
            raise ValidationError(f"JSONSchemaField rejection: {exc}") from exc
        return value

    def deconstruct(self) -> tuple[Any, str, list, dict]:
        name, path, args, kwargs = super().deconstruct()
        kwargs["schema"] = self.schema
        return name, path, args, kwargs


__all__ = [
    "Money",
    "MoneyField",
    "SemverField",
    "PhoneField",
    "ColorField",
    "JSONSchemaField",
]
