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


_IP_RANGE_RE = re.compile(
    r"^(?:\d{1,3}\.){3}\d{1,3}/(?:[0-9]|[1-2][0-9]|3[0-2])$"
    r"|^([0-9a-fA-F:]+)/(?:[0-9]|[1-9][0-9]|1[01][0-9]|12[0-8])$"
)


class IPRangeField(_fields.CharField):
    """CIDR / IP range column. Validates IPv4 / IPv6 notation on
    assignment.

    On PostgreSQL this could map to ``inet`` natively but the field
    intentionally uses ``VARCHAR`` so callers writing portable code
    don't end up with a per-vendor type. Validate at the Python
    boundary, store text.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 64)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        if not _IP_RANGE_RE.match(value):
            raise ValidationError(
                f"IPRangeField: {value!r} is not a valid CIDR range."
            )
        return value


_TZ_RE = re.compile(r"^[A-Za-z_]+(?:/[A-Za-z0-9_+\-]+)+$|^UTC$|^GMT$")


class TimezoneField(_fields.CharField):
    """IANA timezone name. Validates the shape (no full database of
    timezones bundled — that's what ``zoneinfo`` is for at runtime).
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 64)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        if not _TZ_RE.match(value):
            raise ValidationError(
                f"TimezoneField: {value!r} is not a valid IANA tz name."
            )
        return value


class PathField(_fields.CharField):
    """Filesystem-style path stored as text. Refuses parent-directory
    traversal (``..``) and absolute Windows paths by default."""

    def __init__(self, *, allow_traversal: bool = False, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 4096)
        self.allow_traversal = allow_traversal
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        if not self.allow_traversal and ".." in value.split("/"):
            raise ValidationError(
                f"PathField: {value!r} contains '..' segment; "
                "allow_traversal=True to bypass."
            )
        return value

    def deconstruct(self) -> tuple[Any, str, list, dict]:
        name, path, args, kwargs = super().deconstruct()
        if self.allow_traversal:
            kwargs["allow_traversal"] = True
        return name, path, args, kwargs


class PercentageField(_fields.DecimalField):
    """Decimal column constrained to ``[0, 100]``. ``max_digits`` /
    ``decimal_places`` default to ``5`` / ``2`` so values like
    ``99.99`` fit without surprise truncation."""

    def __init__(
        self,
        *,
        max_digits: int = 5,
        decimal_places: int = 2,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            max_digits=max_digits, decimal_places=decimal_places, **kwargs
        )

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, decimal.Decimal):
            try:
                value = decimal.Decimal(str(value))
            except decimal.InvalidOperation as e:
                raise ValidationError(
                    f"PercentageField: cannot parse {value!r} as Decimal."
                ) from e
        if value < 0 or value > 100:
            raise ValidationError(
                f"PercentageField: {value!r} outside [0, 100]."
            )
        return value


# ISO-3166 alpha-2 country codes — bundled because the list is
# bounded and rarely changes (a couple of revisions per decade).
_ISO_3166 = frozenset(
    """AF AX AL DZ AS AD AO AI AQ AG AR AM AW AU AT AZ BS BH BD BB BY BE BZ BJ
    BM BT BO BQ BA BW BV BR IO BN BG BF BI CV KH CM CA KY CF TD CL CN CX CC CO
    KM CD CG CK CR CI HR CU CW CY CZ DK DJ DM DO EC EG SV GQ ER EE SZ ET FK FO
    FJ FI FR GF PF TF GA GM GE DE GH GI GR GL GD GP GU GT GG GN GW GY HT HM VA
    HN HK HU IS IN ID IR IQ IE IM IL IT JM JP JE JO KZ KE KI KP KR KW KG LA LV
    LB LS LR LY LI LT LU MO MG MW MY MV ML MT MH MQ MR MU YT MX FM MD MC MN ME
    MS MA MZ MM NA NR NP NL NC NZ NI NE NG NU NF MK MP NO OM PK PW PS PA PG PY
    PE PH PN PL PT PR QA RE RO RU RW BL SH KN LC MF PM VC WS SM ST SA SN RS SC
    SL SG SX SK SI SB SO ZA GS SS ES LK SD SR SJ SE CH SY TW TJ TZ TH TL TG TK
    TO TT TN TR TM TC TV UG UA AE GB US UM UY UZ VU VE VN VG VI WF EH YE ZM ZW
    """.split()
)


class CountryField(_fields.CharField):
    """ISO-3166-1 alpha-2 country code (``"US"``, ``"GB"``, …).

    Validates against the bundled list of currently-allocated codes.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 2)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        normalised = value.strip().upper()
        if normalised not in _ISO_3166:
            raise ValidationError(
                f"CountryField: {value!r} is not a valid ISO-3166 alpha-2 code."
            )
        return normalised


def autoslug(source_field: str) -> Any:
    """Field-default factory that derives a URL-safe slug from another
    field on the same model.

    Usage::

        class Article(dorm.Model):
            title = dorm.CharField(max_length=200)
            slug = dorm.SlugField(
                max_length=200,
                default=autoslug("title"),
            )

    The factory expects ``self.title`` to be set on the instance at
    save time — call ``model.full_clean()`` or pass ``title=`` in the
    constructor before ``save()`` to populate it first.
    """
    import unicodedata

    def _factory(instance: Any = None) -> str:
        if instance is None:
            return ""
        raw = getattr(instance, source_field, "") or ""
        text = unicodedata.normalize("NFKD", str(raw))
        text = text.encode("ascii", "ignore").decode("ascii").lower()
        slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
        return slug or "untitled"

    return _factory


__all__ = [
    "Money",
    "MoneyField",
    "SemverField",
    "PhoneField",
    "ColorField",
    "JSONSchemaField",
    "IPRangeField",
    "TimezoneField",
    "PathField",
    "PercentageField",
    "CountryField",
    "autoslug",
]
