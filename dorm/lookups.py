from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

LOOKUP_SEP = "__"

# Identifier-shape validator for the FTS dictionary name spliced
# into ``to_tsvector('<cfg>', col)``. Bound parameters can't be
# used for the regconfig argument, so we whitelist instead.
_SAFE_CONFIG = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    # Date-part lookups are vendor-aware — the templates here are the
    # SQLite (``STRFTIME``) form. The PG branch in ``build_lookup_sql``
    # rewrites them to ``EXTRACT(unit FROM col) = %s``. Without that
    # rewrite, ``filter(created_at__year=2026)`` would fail on PG with
    # ``function strftime(unknown, timestamp) does not exist``.
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
    # ── Django-name aliases (3.1+) ────────────────────────────────────────
    # Django's contrib.postgres uses these spellings on
    # ``JSONField`` / ``ArrayField``. The ``contains`` lookup is
    # field-type ambiguous — for strings the LIKE version above is
    # right; for JSON/array, callers should use ``contained_by`` /
    # ``has_*`` explicitly. We don't shadow ``contains`` here to
    # avoid breaking string filters; users wanting JSON containment
    # use ``array_contains`` (covers JSONB too) or the new aliases
    # below for the singular operators.
    "contained_by": ("{col} <@ %s", lambda v: v),       # ARRAY, JSONB
    "has_key": ("{col} ? %s", lambda v: v),             # JSONB
    "has_keys": ("{col} ?& %s", lambda v: v),           # JSONB, list
    "has_any_keys": ("{col} ?| %s", lambda v: v),       # JSONB, list
    "overlap": ("{col} && %s", lambda v: v),            # ARRAY
    "len": ("array_length({col}, 1) = %s", lambda v: v),  # ARRAY (PG)
    # ── Full-text search (PostgreSQL only) ────────────────────────────────
    # ``to_tsvector(<config>, col) @@ plainto_tsquery(<config>, %s)`` —
    # the canonical "match this column against the search string"
    # idiom. SQLite is not supported here (use FTS5 virtual tables).
    # The lookup name ``search`` matches Django's contrib.postgres.
    # The ``<config>`` placeholder is filled at compile time from
    # ``settings.SEARCH_CONFIG`` (default ``'english'``); see
    # ``build_lookup_sql`` below. Hardcoding ``'english'`` here used
    # to silently break Spanish / multi-lingual apps.
    "search": (
        "to_tsvector('english', {col}) @@ plainto_tsquery('english', %s)",
        lambda v: v,
    ),
}

VALID_LOOKUPS = set(LOOKUPS.keys())


def parse_lookup_key(key: str) -> tuple[list[str], str]:
    """Split 'field__related__lookup' into (['field', 'related'], 'lookup')."""
    parts = key.split(LOOKUP_SEP)
    if len(parts) > 1 and parts[-1] in VALID_LOOKUPS:
        return parts[:-1], parts[-1]
    return parts, "exact"


# Maps the date-part lookup name to the SQL ``EXTRACT`` unit. PG uses
# integers for these (``EXTRACT(YEAR FROM ts)`` returns a numeric), so
# the value transform is to ``int`` rather than the zero-padded string
# the SQLite ``STRFTIME`` form produces. ``date`` is special-cased
# because PG has a ``DATE()`` cast (which works there too) and a
# different unit name for week-day (``ISODOW`` is 1-7 instead of 0-6;
# we expose ``DOW`` to match SQLite's 0=Sunday convention).
_PG_DATE_UNITS: dict[str, str] = {
    "year": "YEAR",
    "month": "MONTH",
    "day": "DAY",
    "hour": "HOUR",
    "minute": "MINUTE",
    "second": "SECOND",
    "week_day": "DOW",
}


def build_lookup_sql(
    col: str, lookup: str, value, vendor: str = "sqlite"
) -> tuple[str, list]:
    """Return (sql_fragment, params) for a single lookup condition.

    *vendor* (``"postgresql"`` or ``"sqlite"``) influences:
    - ``__in``: PG emits ``col = ANY(%s)`` (one prepared-statement
      shape regardless of list length); SQLite stays on classic
      ``col IN (?, ?, ...)``.
    - **Date-part lookups** (``__year``, ``__month``, ``__day``,
      ``__hour``, ``__minute``, ``__second``, ``__week_day``,
      ``__date``): PG emits ``EXTRACT(unit FROM col) = %s`` /
      ``DATE(col) = %s`` (server-side date arithmetic); SQLite
      stays on ``STRFTIME('%Y', col) = %s`` because it has no
      ``EXTRACT``. Before this fix, ``Order.objects.filter(created__year=2026)``
      crashed on PG with ``function strftime(unknown, timestamp)
      does not exist``.
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
        # ``value`` may be a generator / set / dict_values / queryset
        # subquery handled elsewhere — anything iterable. Materialise
        # once so we can both check emptiness AND know the length for
        # the SQLite placeholder list. Generators previously crashed
        # with ``object of type 'generator' has no len()`` because
        # ``len(value)`` was called on the un-materialised iterable.
        materialised = list(value)
        if not materialised:
            return "1=0", []  # empty IN → always false
        if vendor == "postgresql":
            # ANY(array) bound as a single parameter — same SQL shape for
            # any list size, so PG's prepared-statement cache hits across
            # calls with different lengths. psycopg adapts a Python list
            # to a Postgres array automatically.
            return f"{col} = ANY(%s)", [materialised]
        placeholders = ", ".join(["%s"] * len(materialised))
        return f"{col} IN ({placeholders})", materialised

    if lookup == "range":
        lo, hi = value
        return template.format(col=col), [lo, hi]

    # Vendor-aware regex lookups. SQLite uses ``REGEXP`` (requires
    # the ``re`` extension to be loaded; sqlite3 ships it on Linux
    # builds via ``conn.create_function``) — see the connection
    # wrapper. PostgreSQL has native POSIX regex operators ``~``
    # (case-sensitive) and ``~*`` (case-insensitive); using the
    # SQLite ``REGEXP`` keyword on PG raises ``syntax error at or
    # near "REGEXP"``.
    if vendor == "postgresql" and lookup == "regex":
        return f"{col} ~ %s", [value]
    if vendor == "postgresql" and lookup == "iregex":
        return f"{col} ~* %s", [value]

    # Vendor-aware full-text search config. ``settings.SEARCH_CONFIG``
    # picks the dictionary used by ``to_tsvector`` /
    # ``plainto_tsquery``; defaults to ``'english'``. Validated as a
    # plain identifier so we can splice it into SQL without bound
    # parameters (PG accepts ``::regconfig`` casts but not a bound
    # parameter as the dictionary argument).
    if lookup == "search":
        try:
            from .conf import settings as _settings

            cfg = getattr(_settings, "SEARCH_CONFIG", "english") or "english"
        except Exception:
            cfg = "english"
        if not _SAFE_CONFIG.match(cfg):
            raise ValueError(
                f"Invalid SEARCH_CONFIG {cfg!r}: only letters, digits, "
                f"and underscores are allowed."
            )
        return (
            f"to_tsvector('{cfg}', {col}) @@ plainto_tsquery('{cfg}', %s)",
            [value],
        )

    # Vendor-aware date-part lookups.
    if vendor == "postgresql" and lookup in _PG_DATE_UNITS:
        unit = _PG_DATE_UNITS[lookup]
        # ``EXTRACT(...)`` returns a numeric on PG; compare against an
        # int so a caller passing ``year=2026`` (int) and one passing
        # ``year="2026"`` (str via ``STRFTIME`` historical convention)
        # both work consistently.
        return f"EXTRACT({unit} FROM {col}) = %s", [int(value)]
    if vendor == "postgresql" and lookup == "date":
        # ``DATE(col)`` works on PG too; only the comparison value
        # type matters. Accept ``datetime.date`` directly.
        return f"DATE({col}) = %s", [value]

    transformed = transform(value) if transform else value
    return template.format(col=col), [transformed]
