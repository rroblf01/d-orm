from __future__ import annotations

from collections.abc import Callable
from typing import Any

LOOKUP_SEP = "__"

# Maps lookup name → (sql_template, value_transform)
# %s is the column reference; value_transform applied to the value before binding
LOOKUPS: dict[str, tuple[str, Callable[..., Any] | None]] = {
    "exact": ("{col} = %s", lambda v: v),
    "iexact": ("LOWER({col}) = LOWER(%s)", lambda v: v),
    "contains": ("{col} LIKE %s", lambda v: f"%{v}%"),
    "icontains": ("LOWER({col}) LIKE LOWER(%s)", lambda v: f"%{v}%"),
    "startswith": ("{col} LIKE %s", lambda v: f"{v}%"),
    "istartswith": ("LOWER({col}) LIKE LOWER(%s)", lambda v: f"{v}%"),
    "endswith": ("{col} LIKE %s", lambda v: f"%{v}"),
    "iendswith": ("LOWER({col}) LIKE LOWER(%s)", lambda v: f"%{v}"),
    "gt": ("{col} > %s", lambda v: v),
    "gte": ("{col} >= %s", lambda v: v),
    "lt": ("{col} < %s", lambda v: v),
    "lte": ("{col} <= %s", lambda v: v),
    "in": ("{col} IN %s", lambda v: v),  # special handling below
    "range": ("{col} BETWEEN %s AND %s", lambda v: v),  # tuple
    "isnull": ("{col} IS NULL", None),  # value ignored
    "isnotnull": ("{col} IS NOT NULL", None),
    "regex": ("{col} REGEXP %s", lambda v: v),
    "iregex": ("LOWER({col}) REGEXP LOWER(%s)", lambda v: v),
    "date": ("DATE({col}) = %s", lambda v: v),
    "year": ("STRFTIME('%Y', {col}) = %s", lambda v: str(v)),
    "month": ("STRFTIME('%m', {col}) = %s", lambda v: str(v).zfill(2)),
    "day": ("STRFTIME('%d', {col}) = %s", lambda v: str(v).zfill(2)),
    "hour": ("STRFTIME('%H', {col}) = %s", lambda v: str(v).zfill(2)),
    "minute": ("STRFTIME('%M', {col}) = %s", lambda v: str(v).zfill(2)),
    "second": ("STRFTIME('%S', {col}) = %s", lambda v: str(v).zfill(2)),
    "week_day": ("STRFTIME('%w', {col}) = %s", lambda v: str(v)),
}

VALID_LOOKUPS = set(LOOKUPS.keys())


def parse_lookup_key(key: str) -> tuple[list[str], str]:
    """Split 'field__related__lookup' into (['field', 'related'], 'lookup')."""
    parts = key.split(LOOKUP_SEP)
    if len(parts) > 1 and parts[-1] in VALID_LOOKUPS:
        return parts[:-1], parts[-1]
    return parts, "exact"


def build_lookup_sql(col: str, lookup: str, value) -> tuple[str, list]:
    """Return (sql_fragment, params) for a single lookup condition."""
    if lookup not in LOOKUPS:
        raise ValueError(f"Unsupported lookup: '{lookup}'")

    template, transform = LOOKUPS[lookup]

    if lookup == "isnull":
        if value:
            return template.format(col=col), []
        else:
            return f"{col} IS NOT NULL", []

    if lookup == "isnotnull":
        return template.format(col=col), []

    if lookup == "in":
        if not value:
            return "1=0", []  # empty IN → always false
        placeholders = ", ".join(["%s"] * len(value))
        return f"{col} IN ({placeholders})", list(value)

    if lookup == "range":
        lo, hi = value
        return template.format(col=col), [lo, hi]

    transformed = transform(value) if transform else value
    return template.format(col=col), [transformed]
