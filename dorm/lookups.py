from __future__ import annotations

from collections.abc import Callable
from typing import Any

LOOKUP_SEP = "__"


def _escape_like(value: str) -> str:
    """Escape LIKE special characters so user values are treated as literals."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Maps lookup name → (sql_template, value_transform)
# %s is the column reference; value_transform applied to the value before binding
LOOKUPS: dict[str, tuple[str, Callable[..., Any] | None]] = {
    "exact": ("{col} = %s", lambda v: v),
    "iexact": ("LOWER({col}) = LOWER(%s)", lambda v: v),
    "contains": ("{col} LIKE %s ESCAPE '\\'", lambda v: f"%{_escape_like(v)}%"),
    "icontains": ("LOWER({col}) LIKE LOWER(%s) ESCAPE '\\'", lambda v: f"%{_escape_like(v)}%"),
    "startswith": ("{col} LIKE %s ESCAPE '\\'", lambda v: f"{_escape_like(v)}%"),
    "istartswith": ("LOWER({col}) LIKE LOWER(%s) ESCAPE '\\'", lambda v: f"{_escape_like(v)}%"),
    "endswith": ("{col} LIKE %s ESCAPE '\\'", lambda v: f"%{_escape_like(v)}"),
    "iendswith": ("LOWER({col}) LIKE LOWER(%s) ESCAPE '\\'", lambda v: f"%{_escape_like(v)}"),
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
    # ── PG array / JSON lookups ───────────────────────────────────────────
    # These generate native PG operators and will fail on SQLite — call out
    # vendor-specific code explicitly with these names instead of relying
    # on the generic ``__contains`` (which is LIKE-based, wrong for arrays).
    "array_contains": ("{col} @> %s", lambda v: v),    # ARRAY, JSONB
    "array_overlap": ("{col} && %s", lambda v: v),     # ARRAY only
    "json_has_key": ("{col} ? %s", lambda v: v),       # JSONB
    "json_has_any": ("{col} ?| %s", lambda v: v),      # JSONB, list of keys
    "json_has_all": ("{col} ?& %s", lambda v: v),      # JSONB, list of keys
}

VALID_LOOKUPS = set(LOOKUPS.keys())


def parse_lookup_key(key: str) -> tuple[list[str], str]:
    """Split 'field__related__lookup' into (['field', 'related'], 'lookup')."""
    parts = key.split(LOOKUP_SEP)
    if len(parts) > 1 and parts[-1] in VALID_LOOKUPS:
        return parts[:-1], parts[-1]
    return parts, "exact"


def build_lookup_sql(
    col: str, lookup: str, value, vendor: str = "sqlite"
) -> tuple[str, list]:
    """Return (sql_fragment, params) for a single lookup condition.

    *vendor* is ``"postgresql"`` or ``"sqlite"`` and currently only
    influences the ``__in`` lookup: PostgreSQL emits ``col = ANY(%s)``
    (one prepared-statement shape regardless of list length, so PG's
    plan cache hits across calls with different list sizes), while
    SQLite stays on the classic ``col IN (?, ?, ...)``.
    """
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
        if vendor == "postgresql":
            # ANY(array) bound as a single parameter — same SQL shape for
            # any list size, so PG's prepared-statement cache hits across
            # calls with different lengths. psycopg adapts a Python list
            # to a Postgres array automatically.
            return f"{col} = ANY(%s)", [list(value)]
        placeholders = ", ".join(["%s"] * len(value))
        return f"{col} IN ({placeholders})", list(value)

    if lookup == "range":
        lo, hi = value
        return template.format(col=col), [lo, hi]

    transformed = transform(value) if transform else value
    return template.format(col=col), [transformed]
