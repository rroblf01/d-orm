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


def _compile_condition(
    condition: Any,
    table_alias: str | None = None,
    *,
    connection: Any = None,
    vendor: str | None = None,
) -> tuple[str, list]:
    """Compile a Q object to WHERE-style (sql, params).

    ``connection`` (when known) lets the lookup layer pick the right
    vendor branch — ``EXTRACT`` vs ``STRFTIME`` for ``__year`` /
    ``__date``, ``= ANY(%s)`` vs ``IN (?, …)`` for ``__in``, and so
    on. Without it the function falls back to ``vendor="sqlite"``
    which used to silently produce broken SQL on PostgreSQL inside
    ``Case/When``, ``CheckConstraint`` and partial-index predicates.

    For dotted lookup keys (``Q(author__name="x")``) we walk every
    segment except the last as a relation hop and qualify the last
    column with the relation's table alias instead of dropping the
    prefix. Without this the emitted SQL referenced a column on the
    *current* table that doesn't exist there.
    """
    from .lookups import build_lookup_sql, parse_lookup_key

    if not isinstance(condition, Q):
        return "", []

    if vendor is None:
        vendor = getattr(connection, "vendor", "sqlite")

    parts: list[str] = []
    params: list[Any] = []
    for child in condition.children:
        if isinstance(child, Q):
            sql, p = _compile_condition(
                child, table_alias, connection=connection, vendor=vendor
            )
        elif isinstance(child, tuple) and len(child) == 2:
            key, value = child
            field_parts, lookup = parse_lookup_key(key)
            # Multi-segment paths (``author__name``) — qualify with
            # the deepest path's alias so the column reference is
            # correct, not just the leaf field name on the local
            # table.
            if len(field_parts) > 1:
                join_alias = (table_alias or "_t") + "_" + "_".join(
                    field_parts[:-1]
                )
                fname = field_parts[-1]
                col = f'"{join_alias}"."{fname}"'
            else:
                fname = field_parts[-1]
                ta = f'"{table_alias}".' if table_alias else ""
                col = f'{ta}"{fname}"'
            sql, p = build_lookup_sql(col, lookup, value, vendor=vendor)
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

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
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
    """COALESCE(expr1, expr2, ...) — returns first non-null value.

    Constructing ``Coalesce()`` with no expressions is meaningless —
    every backend rejects ``COALESCE()`` at parse time. Validating
    here turns the eventual ``OperationalError`` into a clear
    ``ValueError`` pointing at the user's code instead of at the
    cursor.
    """

    function = "COALESCE"

    def __init__(self, *expressions: Any, output_field: Any = None) -> None:
        if not expressions:
            raise ValueError(
                "Coalesce() requires at least one expression."
            )
        super().__init__(*expressions, output_field=output_field)


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


class Power(Func):
    """POWER(base, exponent)."""

    function = "POWER"


class Sqrt(Func):
    """SQRT(expr) — square root. SQLite needs ≥3.35 (math built-ins)."""

    function = "SQRT"


class Mod(Func):
    """MOD(dividend, divisor) — integer / float modulo."""

    function = "MOD"


class Sign(Func):
    """SIGN(expr) — -1 / 0 / 1."""

    function = "SIGN"


class Ceil(Func):
    """CEIL(expr) — round up to next integer."""

    function = "CEIL"


class Floor(Func):
    """FLOOR(expr) — round down to next integer."""

    function = "FLOOR"


class Log(Func):
    """LOG(base, expr) — logarithm in arbitrary base. ``LOG(expr)`` for
    natural log on backends that accept the single-arg form; use
    :class:`Ln` for portability."""

    function = "LOG"


class Ln(Func):
    """LN(expr) — natural logarithm."""

    function = "LN"


class Exp(Func):
    """EXP(expr) — Euler's number raised to *expr*."""

    function = "EXP"


class Random(Func):
    """RANDOM() — backend-native random in [0, 1) (PG) or signed
    integer (SQLite). Use ``ABS(Random()) % N`` for an integer in a
    bounded range; emit ``ORDER BY Random()`` for shuffled selection.
    """

    function = "RANDOM"

    def __init__(self, output_field: Any = None) -> None:
        super().__init__(output_field=output_field)


class NullIf(Func):
    """NULLIF(a, b) — returns NULL when ``a == b``, else ``a``. Useful
    for converting sentinel values back to NULL before division /
    aggregation."""

    function = "NULLIF"

    def __init__(self, a: Any, b: Any, output_field: Any = None) -> None:
        super().__init__(a, b, output_field=output_field)


class Trim(Func):
    """TRIM(expr) — strip whitespace from both ends."""

    function = "TRIM"


class LTrim(Func):
    """LTRIM(expr) — strip leading whitespace."""

    function = "LTRIM"


class RTrim(Func):
    """RTRIM(expr) — strip trailing whitespace."""

    function = "RTRIM"


class Now(Func):
    """CURRENT_TIMESTAMP — current date/time."""

    function = ""

    def __init__(self, output_field: Any = None) -> None:
        super().__init__(output_field=output_field)

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        return "CURRENT_TIMESTAMP", []


class Concat(Func):
    """Concatenate string expressions, treating NULL operands as empty.

    Django's ``Concat`` skips NULL operands so the result is always a
    string. The naive ``a || b`` we used previously returns NULL on
    SQLite and PostgreSQL whenever *any* operand is NULL — which
    silently dropped rows from ``filter(full__contains=…)`` queries
    when one of the source columns was nullable.

    Each operand is now wrapped in ``COALESCE(expr, '')`` so a NULL
    contributes the empty string instead of poisoning the whole
    expression. This matches Django's documented behaviour on both
    backends.
    """

    function = "CONCAT"

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(f"COALESCE({sql}, '')")
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

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        sql, params = _compile_expr(self.expressions[0], table_alias)
        return f"CAST({sql} AS {self.cast_type})", params


class When:
    """A WHEN condition THEN value clause for use inside Case."""

    def __init__(self, condition: Any = None, then: Any = None, **kwargs: Any) -> None:
        if condition is None:
            condition = Q(**kwargs)
        self.condition = condition
        self.then = then

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        cond_sql, cond_params = _compile_condition(
            self.condition, table_alias, connection=kwargs.get("connection")
        )
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

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        parts: list[str] = []
        params: list[Any] = []
        for when in self.whens:
            sql, p = when.as_sql(table_alias, connection=kwargs.get("connection"))
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


# ── Extra scalar functions ────────────────────────────────────────────────────


class Greatest(Func):
    """``GREATEST(a, b, ...)`` — largest non-null argument.

    Emits PostgreSQL's ``GREATEST(a, b, ...)`` when the connection is
    PostgreSQL; on SQLite — which has no ``GREATEST`` but accepts a
    multi-arg scalar ``MAX(a, b, ...)`` — falls back to ``MAX``.

    NULL handling differs: PostgreSQL ignores NULLs, SQLite returns
    NULL if any argument is NULL.
    """

    function = "GREATEST"

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        connection = kwargs.get("connection")
        vendor = getattr(connection, "vendor", "postgresql") if connection else "postgresql"
        fn = "GREATEST" if vendor == "postgresql" else "MAX"
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(sql)
            params.extend(p)
        return f"{fn}({', '.join(parts)})", params


class Least(Func):
    """``LEAST(a, b, ...)`` — smallest non-null argument.

    Mirrors :class:`Greatest`; emits ``LEAST`` on PostgreSQL and the
    multi-arg ``MIN`` on SQLite.
    """

    function = "LEAST"

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        connection = kwargs.get("connection")
        vendor = getattr(connection, "vendor", "postgresql") if connection else "postgresql"
        fn = "LEAST" if vendor == "postgresql" else "MIN"
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(sql)
            params.extend(p)
        return f"{fn}({', '.join(parts)})", params


class Round(Func):
    """``ROUND(expr [, places])`` — round half-to-even on PostgreSQL,
    half-away-from-zero on SQLite. Pass ``places`` as a second argument."""

    function = "ROUND"


class Trunc(Func):
    """Date / time truncation. *unit* is one of ``"year"``, ``"month"``,
    ``"day"``, ``"hour"``, ``"minute"``, ``"second"``, ``"week"``::

        # Group orders by month
        Order.objects.annotate(month=Trunc("created_at", "month"))

    Compiles to ``DATE_TRUNC('unit', expr)`` on PostgreSQL. SQLite has
    no native equivalent; this raises ``ImproperlyConfigured`` if the
    unit is unknown but emits the same SQL on both backends — SQLite
    will reject it at execute time. For SQLite-portable truncation use
    a vendor-specific raw expression.
    """

    function = "DATE_TRUNC"

    _UNITS = frozenset(
        {"year", "quarter", "month", "week", "day", "hour", "minute", "second"}
    )

    def __init__(self, expression: Any, unit: str, output_field: Any = None) -> None:
        if unit not in self._UNITS:
            raise ImproperlyConfigured(
                f"Trunc(unit=...) must be one of {sorted(self._UNITS)}, got {unit!r}."
            )
        self.unit = unit
        super().__init__(expression, output_field=output_field)

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        sql, params = _compile_expr(self.expressions[0], table_alias)
        return f"DATE_TRUNC('{self.unit}', {sql})", params


class Extract(Func):
    """``EXTRACT(unit FROM expr)`` — pull a component out of a date/time.

    *unit* is one of ``"year"``, ``"month"``, ``"day"``, ``"hour"``,
    ``"minute"``, ``"second"``, ``"dow"`` (day of week), ``"doy"``
    (day of year), ``"week"``, ``"epoch"``. Compiles to
    ``EXTRACT(UNIT FROM expr)`` — supported by PostgreSQL natively.
    SQLite does not support ``EXTRACT``; use ``strftime`` directly via
    a custom :class:`Func` for those cases.
    """

    function = "EXTRACT"

    _UNITS = frozenset(
        {"year", "month", "day", "hour", "minute", "second",
         "dow", "doy", "week", "epoch", "quarter"}
    )

    def __init__(self, expression: Any, unit: str, output_field: Any = None) -> None:
        if unit not in self._UNITS:
            raise ImproperlyConfigured(
                f"Extract(unit=...) must be one of {sorted(self._UNITS)}, got {unit!r}."
            )
        self.unit = unit
        super().__init__(expression, output_field=output_field)

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        sql, params = _compile_expr(self.expressions[0], table_alias)
        return f"EXTRACT({self.unit.upper()} FROM {sql})", params


# ── Date-part helpers ─────────────────────────────────────────────────────────
#
# Thin subclasses that pin a unit so callers can write
# ``TruncMonth("created_at")`` instead of ``Trunc("created_at", "month")``.
# Match Django's ``django.db.models.functions`` surface so the mental
# model carries over directly.


class TruncDate(Trunc):
    """``DATE_TRUNC('day', expr)`` — strips time-of-day, keeping the
    date. Equivalent to ``Trunc(expr, "day")``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "day", output_field=output_field)


class TruncDay(TruncDate):
    """Alias for :class:`TruncDate` matching Django's name."""


class TruncWeek(Trunc):
    """``DATE_TRUNC('week', expr)``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "week", output_field=output_field)


class TruncMonth(Trunc):
    """``DATE_TRUNC('month', expr)``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "month", output_field=output_field)


class TruncQuarter(Trunc):
    """``DATE_TRUNC('quarter', expr)``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "quarter", output_field=output_field)


class TruncYear(Trunc):
    """``DATE_TRUNC('year', expr)``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "year", output_field=output_field)


class TruncHour(Trunc):
    """``DATE_TRUNC('hour', expr)``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "hour", output_field=output_field)


class TruncMinute(Trunc):
    """``DATE_TRUNC('minute', expr)``."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "minute", output_field=output_field)


class ExtractYear(Extract):
    """``EXTRACT(YEAR FROM expr)`` — useful inside ``annotate`` for
    year-based grouping or ordering."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "year", output_field=output_field)


class ExtractMonth(Extract):
    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "month", output_field=output_field)


class ExtractDay(Extract):
    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "day", output_field=output_field)


class ExtractHour(Extract):
    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "hour", output_field=output_field)


class ExtractMinute(Extract):
    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "minute", output_field=output_field)


class ExtractSecond(Extract):
    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "second", output_field=output_field)


class ExtractWeekDay(Extract):
    """``EXTRACT(DOW FROM expr)`` — day of week, ``0=Sunday`` through
    ``6=Saturday`` on PostgreSQL. (Django's convention is ``1=Sunday``;
    this matches PG and SQLite's ``STRFTIME('%w', …)``.)"""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "dow", output_field=output_field)


class ExtractWeek(Extract):
    """``EXTRACT(WEEK FROM expr)`` — ISO week number."""

    def __init__(self, expression: Any, output_field: Any = None) -> None:
        super().__init__(expression, "week", output_field=output_field)


class Substr(Func):
    """``SUBSTR(expr, pos, length)`` — 1-indexed (matches both backends)."""

    function = "SUBSTR"


class Replace(Func):
    """``REPLACE(expr, old, new)`` — substring replacement."""

    function = "REPLACE"


class StrIndex(Func):
    """1-based position of *needle* within *haystack*; returns ``0`` when
    not found::

        Article.objects.annotate(
            tag_pos=StrIndex(F("title"), Value("[done]"))
        ).filter(tag_pos__gt=0)

    Vendor-aware: emits ``STRPOS(haystack, needle)`` on PostgreSQL and
    ``INSTR(haystack, needle)`` on SQLite. Both return 1-based offsets
    with 0 for "not found".
    """

    function = "STRPOS"

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        connection = kwargs.get("connection")
        vendor = getattr(connection, "vendor", "postgresql") if connection else "postgresql"
        fn = "STRPOS" if vendor == "postgresql" else "INSTR"
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(sql)
            params.extend(p)
        return f"{fn}({', '.join(parts)})", params


# ── Window functions ──────────────────────────────────────────────────────────


class WindowExpression:
    """Base class for window-function expressions used as the first
    argument to :class:`Window`. ``frame_required`` flags whether the
    function needs an ``ORDER BY`` clause to be well-defined (e.g.
    ranking functions); :class:`Window` validates this at queryset
    build time.
    """

    function: str = ""
    frame_required: bool = False

    def __init__(self, *expressions: Any) -> None:
        self.expressions = expressions

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        parts: list[str] = []
        params: list[Any] = []
        for expr in self.expressions:
            sql, p = _compile_expr(expr, table_alias)
            parts.append(sql)
            params.extend(p)
        return f"{self.function}({', '.join(parts)})", params

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{', '.join(repr(e) for e in self.expressions)})"
        )


class RowNumber(WindowExpression):
    """``ROW_NUMBER() OVER (...)`` — unique sequential integer per partition."""

    function = "ROW_NUMBER"
    frame_required = True


class Rank(WindowExpression):
    """``RANK() OVER (...)`` — ties share a rank; gaps after ties."""

    function = "RANK"
    frame_required = True


class DenseRank(WindowExpression):
    """``DENSE_RANK() OVER (...)`` — ties share a rank; no gaps."""

    function = "DENSE_RANK"
    frame_required = True


class NTile(WindowExpression):
    """``NTILE(buckets) OVER (...)`` — equal-size buckets across the
    partition (quartiles → ``NTile(4)``, deciles → ``NTile(10)``)."""

    function = "NTILE"
    frame_required = True

    def __init__(self, buckets: int) -> None:
        if not isinstance(buckets, int) or buckets <= 0:
            raise ValueError("NTile(buckets) requires a positive integer.")
        super().__init__(Value(buckets))


class Lag(WindowExpression):
    """``LAG(expr [, offset [, default]]) OVER (...)`` — value from the
    *previous* row in the partition. Useful for delta calculations."""

    function = "LAG"
    frame_required = True

    def __init__(self, expression: Any, offset: int = 1, default: Any = None) -> None:
        args: list[Any] = [expression, Value(offset)]
        if default is not None:
            args.append(Value(default))
        super().__init__(*args)


class Lead(WindowExpression):
    """``LEAD(expr [, offset [, default]]) OVER (...)`` — value from a
    *later* row in the partition."""

    function = "LEAD"
    frame_required = True

    def __init__(self, expression: Any, offset: int = 1, default: Any = None) -> None:
        args: list[Any] = [expression, Value(offset)]
        if default is not None:
            args.append(Value(default))
        super().__init__(*args)


class FirstValue(WindowExpression):
    """``FIRST_VALUE(expr) OVER (...)`` — value at the first row of the
    window frame."""

    function = "FIRST_VALUE"
    frame_required = True


class LastValue(WindowExpression):
    """``LAST_VALUE(expr) OVER (...)`` — value at the last row of the
    window frame.

    Note: the default frame on PostgreSQL is ``RANGE BETWEEN UNBOUNDED
    PRECEDING AND CURRENT ROW``, so ``LAST_VALUE`` returns the *current*
    row by default. Pass an explicit ``frame=`` to :class:`Window`
    (e.g. ``"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"``)
    when you mean "the actual last value in the partition".
    """

    function = "LAST_VALUE"
    frame_required = True


class Window:
    """Wrap a :class:`WindowExpression` (or aggregate / scalar function)
    as a SQL window: ``<expr>() OVER (PARTITION BY ... ORDER BY ...)``.

    Example — running balance per account::

        Transaction.objects.annotate(
            running_balance=Window(
                Sum("amount"),
                partition_by=["account_id"],
                order_by="date",
            )
        )

    Args:
        expression: a :class:`WindowExpression` (``RowNumber``, ``Lag``,
            ...), an aggregate (``Sum``, ``Avg``, ...), or any
            :class:`Func` that produces a value per row.
        partition_by: field names that define the partition. Empty
            means "the whole result set is one partition".
        order_by: a single field name, an ``F``/``Value`` expression,
            or a list. Prefix with ``-`` to sort descending. Required
            for ranking functions; the constructor raises
            :class:`ImproperlyConfigured` if you pass ``RowNumber``,
            ``Rank``, ``DenseRank``, ``Lag``, ``Lead`` etc. without
            ``order_by=`` because the ranking would be undefined.
        frame: optional explicit frame clause appended after
            ``ORDER BY`` (e.g. ``"ROWS BETWEEN UNBOUNDED PRECEDING AND
            CURRENT ROW"``). Don't pass a frame to ranking functions —
            it has no effect and confuses readers.
    """

    def __init__(
        self,
        expression: Any,
        *,
        partition_by: list[str] | tuple[str, ...] | None = None,
        order_by: str | list[Any] | tuple[Any, ...] | None = None,
        frame: str | None = None,
        output_field: Any = None,
    ) -> None:
        self.expression = expression
        self.partition_by = list(partition_by) if partition_by else []
        if order_by is None:
            self.order_by: list[Any] = []
        elif isinstance(order_by, str):
            self.order_by = [order_by]
        else:
            self.order_by = list(order_by)
        self.frame = frame
        self.output_field = output_field

        # Ranking functions are ill-defined without ORDER BY — most
        # databases accept the SQL but return implementation-defined
        # results, which is the worst kind of bug for a reporting query
        # because it ships and silently corrupts dashboards. Raise at
        # queryset build time so the mistake never reaches production.
        if (
            isinstance(expression, WindowExpression)
            and expression.frame_required
            and not self.order_by
        ):
            raise ImproperlyConfigured(
                f"{type(expression).__name__} requires an explicit "
                "order_by= on its Window — ranking is undefined "
                "without one."
            )

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        from .query import _validate_identifier  # noqa: PLC0415

        # Inner expression (Sum / RowNumber / scalar Func)
        if hasattr(self.expression, "as_sql"):
            try:
                # Aggregate.as_sql signature accepts ``model=`` kwarg.
                expr_sql, expr_params = self.expression.as_sql(
                    table_alias, model=kwargs.get("model")
                )
            except TypeError:
                expr_sql, expr_params = self.expression.as_sql(table_alias)
        else:
            expr_sql, expr_params = _compile_expr(self.expression, table_alias)

        clauses: list[str] = []

        if self.partition_by:
            cols: list[str] = []
            for f in self.partition_by:
                _validate_identifier(f)
                if table_alias:
                    cols.append(f'"{table_alias}"."{f}"')
                else:
                    cols.append(f'"{f}"')
            clauses.append("PARTITION BY " + ", ".join(cols))

        if self.order_by:
            order_parts: list[str] = []
            for entry in self.order_by:
                if isinstance(entry, str):
                    desc = entry.startswith("-")
                    fname = entry[1:] if desc else entry
                    _validate_identifier(fname)
                    col = (
                        f'"{table_alias}"."{fname}"' if table_alias else f'"{fname}"'
                    )
                    order_parts.append(f"{col} {'DESC' if desc else 'ASC'}")
                else:
                    sql, _ = _compile_expr(entry, table_alias)
                    order_parts.append(sql)
            clauses.append("ORDER BY " + ", ".join(order_parts))

        if self.frame:
            clauses.append(self.frame)

        over = " ".join(clauses)
        return f"{expr_sql} OVER ({over})", expr_params

    def __repr__(self) -> str:
        return (
            f"Window({self.expression!r}, partition_by={self.partition_by!r}, "
            f"order_by={self.order_by!r}, frame={self.frame!r})"
        )
