from __future__ import annotations

import re
from typing import Any

from .exceptions import ImproperlyConfigured
from .expressions import F, Q, Value

# Allowlist for ``Cast(output_field=...)``. ``cast_type`` is spliced into SQL
# (no bind parameter exists for type names), so accepting an arbitrary string
# would be a SQL injection sink — e.g. ``Cast(F("x"), "INTEGER); DROP TABLE--")``.
# The set covers the SQL-92 / SQLite / PostgreSQL types the ORM emits today;
# add new entries here rather than loosening the regex.
_BASE_CAST_TYPES = frozenset({
    "INTEGER", "BIGINT", "SMALLINT",
    "REAL", "DOUBLE PRECISION", "FLOAT",
    "NUMERIC", "DECIMAL",
    "TEXT", "VARCHAR", "CHAR",
    "BLOB", "BYTEA",
    "BOOLEAN", "BOOL",
    "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME",
    "JSON", "JSONB", "UUID",
})

# Allow optional ``(N)`` or ``(N, M)`` length / precision spec, e.g.
# ``VARCHAR(255)`` or ``NUMERIC(10, 2)``. The base name still has to be
# in ``_BASE_CAST_TYPES`` after stripping the parenthesised tail.
_CAST_TYPE_RE = re.compile(
    r"^([A-Z][A-Z ]*[A-Z]|[A-Z]+)(\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?$"
)


def _validate_cast_type(value: str) -> str:
    """Return *value* normalized (uppercased, single-spaced) if it matches the
    allowlist; raise :class:`ImproperlyConfigured` otherwise. The base type
    name (everything before the optional ``(N)`` / ``(N, M)`` modifier) must
    be in :data:`_BASE_CAST_TYPES`."""
    if not isinstance(value, str) or not value.strip():
        raise ImproperlyConfigured(
            f"Cast(output_field=...) must be a non-empty string, got {value!r}."
        )
    normalized = " ".join(value.strip().upper().split())
    match = _CAST_TYPE_RE.match(normalized)
    if not match:
        raise ImproperlyConfigured(
            f"Invalid Cast type {value!r}. Allowed base types: "
            f"{sorted(_BASE_CAST_TYPES)} optionally followed by ``(N)`` or "
            "``(N, M)``."
        )
    base = match.group(1)
    if base not in _BASE_CAST_TYPES:
        raise ImproperlyConfigured(
            f"Invalid Cast base type {base!r}. Allowed: {sorted(_BASE_CAST_TYPES)}."
        )
    return normalized


def _compile_expr(expr: Any, table_alias: str | None = None) -> tuple[str, list]:
    """Compile an expression (F, Value, When, Func, literal) to (sql, params)."""
    if isinstance(expr, F):
        ta = f'"{table_alias}".' if table_alias else ""
        return f'{ta}"{expr.name}"', []
    if isinstance(expr, Value):
        return "%s", [expr.value]
    if expr is None:
        return "NULL", []
    if hasattr(expr, "as_sql"):
        return expr.as_sql(table_alias)
    return "%s", [expr]


def _compile_condition(condition: Any, table_alias: str | None = None) -> tuple[str, list]:
    """Compile a Q object to WHERE-style (sql, params)."""
    from .lookups import build_lookup_sql, parse_lookup_key

    if not isinstance(condition, Q):
        return "", []

    parts: list[str] = []
    params: list[Any] = []
    for child in condition.children:
        if isinstance(child, Q):
            sql, p = _compile_condition(child, table_alias)
        elif isinstance(child, tuple) and len(child) == 2:
            key, value = child
            field_parts, lookup = parse_lookup_key(key)
            fname = field_parts[-1]
            ta = f'"{table_alias}".' if table_alias else ""
            col = f'{ta}"{fname}"'
            sql, p = build_lookup_sql(col, lookup, value)
        else:
            continue
        if sql:
            parts.append(f"({sql})")
            params.extend(p)

    if not parts:
        return "", []
    joined = f" {condition.connector} ".join(parts)
    if len(parts) > 1:
        joined = f"({joined})"
    if condition.negated:
        joined = f"NOT {joined}"
    return joined, params


class Func:
    """Base class for SQL function expressions."""

    function: str = ""

    def __init__(self, *expressions: Any, output_field: Any = None) -> None:
        self.expressions = expressions
        self.output_field = output_field

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(sql)
            params.extend(p)
        return f"{self.function}({', '.join(parts)})", params

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(repr(e) for e in self.expressions)})"


class Coalesce(Func):
    """COALESCE(expr1, expr2, ...) — returns first non-null value."""

    function = "COALESCE"


class Length(Func):
    """LENGTH(expr) — string/blob length."""

    function = "LENGTH"


class Upper(Func):
    """UPPER(expr)"""

    function = "UPPER"


class Lower(Func):
    """LOWER(expr)"""

    function = "LOWER"


class Abs(Func):
    """ABS(expr)"""

    function = "ABS"


class Now(Func):
    """CURRENT_TIMESTAMP — current date/time."""

    function = ""

    def __init__(self, output_field: Any = None) -> None:
        super().__init__(output_field=output_field)

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        return "CURRENT_TIMESTAMP", []


class Concat(Func):
    """Concatenate strings using the || operator (works on SQLite and PostgreSQL)."""

    function = "CONCAT"

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(sql)
            params.extend(p)
        return " || ".join(parts), params


class Cast(Func):
    """CAST(expr AS type)"""

    function = "CAST"

    def __init__(self, expression: Any, output_field: str, **kwargs: Any) -> None:
        # Validate eagerly so a bad type triggers at queryset build time, not
        # halfway through SQL emission. ``_validate_cast_type`` returns the
        # normalised (upper-cased, single-spaced) form.
        self.cast_type = _validate_cast_type(output_field)
        super().__init__(expression, **kwargs)

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        sql, params = _compile_expr(self.expressions[0], table_alias)
        return f"CAST({sql} AS {self.cast_type})", params


class When:
    """A WHEN condition THEN value clause for use inside Case."""

    def __init__(self, condition: Any = None, then: Any = None, **kwargs: Any) -> None:
        if condition is None:
            condition = Q(**kwargs)
        self.condition = condition
        self.then = then

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        cond_sql, cond_params = _compile_condition(self.condition, table_alias)
        then_sql, then_params = _compile_expr(self.then, table_alias)
        return f"WHEN {cond_sql} THEN {then_sql}", cond_params + then_params

    def __repr__(self) -> str:
        return f"When({self.condition!r}, then={self.then!r})"


class Case:
    """
    CASE WHEN ... THEN ... [WHEN ... THEN ...] [ELSE ...] END

    Example::

        Article.objects.annotate(
            label=Case(
                When(score__gte=90, then=Value("A")),
                When(score__gte=70, then=Value("B")),
                default=Value("C"),
            )
        )
    """

    def __init__(self, *whens: When, default: Any = None, output_field: Any = None) -> None:
        self.whens = whens
        self.default = default
        self.output_field = output_field

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        parts: list[str] = []
        params: list[Any] = []
        for when in self.whens:
            sql, p = when.as_sql(table_alias)
            parts.append(sql)
            params.extend(p)
        default_sql = ""
        if self.default is not None:
            d_sql, d_params = _compile_expr(self.default, table_alias)
            default_sql = f" ELSE {d_sql}"
            params.extend(d_params)
        return f"CASE {' '.join(parts)}{default_sql} END", params

    def __repr__(self) -> str:
        return f"Case({', '.join(repr(w) for w in self.whens)}, default={self.default!r})"
