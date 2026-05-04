from __future__ import annotations

from typing import Any


class Aggregate:
    function: str = ""
    template: str = "%(function)s(%(distinct)s%(expressions)s)"

    def __init__(self, expression: str, distinct: bool = False, filter=None, output_field=None):
        self.expression = expression
        self.distinct = distinct
        self.filter = filter
        self.output_field = output_field

    def as_sql(
        self,
        table_alias: str | None = None,
        *,
        model: Any = None,
        connection: Any = None,
        **kwargs: Any,
    ) -> tuple[str, list]:
        """Compile this aggregate to SQL.

        ``model`` is optional but recommended: when supplied,
        ``Count("pk")`` resolves to the model's actual primary-key
        column (e.g. ``"id"``). Without ``model``, ``"pk"`` would fall
        through verbatim and the database would reject the query with
        ``no such column: <table>.pk`` — the bug this parameter
        prevents.

        ``filter=Q(...)`` (when set) emits a conditional aggregate.
        On PostgreSQL we use the ANSI ``FILTER (WHERE …)`` clause;
        on SQLite (no ``FILTER`` support before 3.30 / inconsistent
        ergonomics) we wrap the expression in a ``CASE WHEN …
        THEN expr END`` so the aggregate skips non-matching rows.
        """
        distinct = "DISTINCT " if self.distinct else ""
        expr = self.expression
        if expr == "pk" and model is not None and model._meta.pk:
            expr = model._meta.pk.column
        # Allow the caller (the query compiler) to thread its own
        # ``_resolve_column`` so reverse-FK / M2M descriptors and FK
        # chains traverse the join machinery the same way ``filter``
        # does. Without this, ``Count("book_set")`` would emit
        # ``COUNT("authors"."book_set")`` against a non-existent
        # column.
        query = kwargs.get("query")
        if (
            query is not None
            and isinstance(expr, str)
            and expr not in ("*",)
            and "." not in expr
        ):
            try:
                col = query._resolve_column(expr.split("__"), connection)
            except Exception:
                # Fallback to the literal-column path; whatever
                # ``_resolve_column`` raised will resurface in the
                # outer compile if the column truly doesn't exist.
                col = (
                    f'"{table_alias}"."{expr}"'
                    if table_alias
                    else f'"{expr}"'
                )
            else:
                # Qualify the bare column reference with the outer
                # table alias when ``_resolve_column`` returned
                # unqualified output (i.e. no joins were registered
                # by this lookup). Pinned on purpose: existing tests
                # — and the SQL planner on PG — read better with
                # ``COUNT("authors"."id")`` than ``COUNT("id")``.
                if "." not in col and table_alias:
                    col = f'"{table_alias}".{col}'
        elif expr == "*":
            col = "*"
        elif table_alias:
            col = f'"{table_alias}"."{expr}"'
        else:
            col = f'"{expr}"'

        params: list = []
        filter_sql = ""
        filter_params: list = []
        if self.filter is not None:
            from .functions import _compile_condition

            filter_sql, filter_params = _compile_condition(
                self.filter, table_alias=table_alias, connection=connection
            )

        vendor = getattr(connection, "vendor", None)
        if filter_sql and vendor != "postgresql":
            # SQLite (and the no-connection fallback) — wrap the
            # column in CASE WHEN so non-matching rows contribute
            # NULL, which every aggregate ignores. ``COUNT(*)``
            # becomes ``COUNT(CASE WHEN … THEN 1 END)`` so the
            # filter still applies.
            wrapped = (
                "1" if col == "*" else col
            )
            col = f"CASE WHEN {filter_sql} THEN {wrapped} END"
            params.extend(filter_params)
            sql = self.template % {
                "function": self.function,
                "distinct": distinct,
                "expressions": col,
            }
            return sql, params

        sql = self.template % {
            "function": self.function,
            "distinct": distinct,
            "expressions": col,
        }
        if filter_sql and vendor == "postgresql":
            sql = f"{sql} FILTER (WHERE {filter_sql})"
            params.extend(filter_params)
        return sql, params

    def __repr__(self):
        return f"{self.__class__.__name__}({self.expression!r})"


class Count(Aggregate):
    function = "COUNT"

    def __init__(self, expression: str = "*", **kwargs):
        super().__init__(expression, **kwargs)


class Sum(Aggregate):
    function = "SUM"


class Avg(Aggregate):
    function = "AVG"


class Max(Aggregate):
    function = "MAX"


class Min(Aggregate):
    function = "MIN"


class StdDev(Aggregate):
    function = "STDDEV"


class Variance(Aggregate):
    function = "VARIANCE"


# ── PostgreSQL-only aggregates ────────────────────────────────────────────────
#
# These compile to PG-specific aggregate functions; SQLite has no
# ``STRING_AGG``/``ARRAY_AGG`` (it has ``GROUP_CONCAT`` for strings but
# nothing for arrays). The aggregate emits the same SQL on both
# backends; SQLite will reject ``ARRAY_AGG`` at execute time. For
# SQLite-portable string concatenation use a vendor-specific raw
# expression in ``annotate``.


class StringAgg(Aggregate):
    """``STRING_AGG(expr, separator [ORDER BY ...])`` — concatenate
    values within a GROUP BY using *separator*. PostgreSQL only.

    Example: list every author's books on one row, alphabetically::

        Author.objects.annotate(
            titles=StringAgg("books__title", separator=", ", order_by="books__title")
        )

    *order_by* (3.3+) accepts a string column name, optionally
    prefixed with ``-`` for DESC, and renders inside the
    ``STRING_AGG`` call as ``ORDER BY <col>``. Without it the
    aggregate output order is unspecified — for reproducible joined
    strings, always pass ``order_by``.
    """

    function = "STRING_AGG"

    def __init__(
        self,
        expression: str,
        separator: str = ", ",
        *,
        distinct: bool = False,
        filter: Any = None,
        output_field: Any = None,
        order_by: str | None = None,
    ) -> None:
        self.separator = separator
        self.order_by = order_by
        super().__init__(
            expression,
            distinct=distinct,
            filter=filter,
            output_field=output_field,
        )

    def as_sql(
        self,
        table_alias: str | None = None,
        *,
        model: Any = None,
        **kwargs: Any,
    ) -> tuple[str, list]:
        # Resolve the column reference the same way the base class
        # does, then wedge the separator in as a bound parameter so
        # special characters in the separator can't break the SQL.
        distinct = "DISTINCT " if self.distinct else ""
        expr = self.expression
        if expr == "pk" and model is not None and model._meta.pk:
            expr = model._meta.pk.column
        col = f'"{table_alias}"."{expr}"' if table_alias else f'"{expr}"'
        order_clause = ""
        if self.order_by is not None:
            ob = self.order_by
            direction = "ASC"
            if ob.startswith("-"):
                ob = ob[1:]
                direction = "DESC"
            # Validate as identifier — splice into SQL is safe with
            # the same shape Django uses for ``ordering = ['col']``.
            from .conf import _validate_identifier

            _validate_identifier(ob, kind="order_by column")
            order_col = (
                f'"{table_alias}"."{ob}"' if table_alias else f'"{ob}"'
            )
            order_clause = f" ORDER BY {order_col} {direction}"
        return (
            f"STRING_AGG({distinct}{col}, %s{order_clause})",
            [self.separator],
        )


class ArrayAgg(Aggregate):
    """``ARRAY_AGG(expr)`` — collect values into a PostgreSQL array.

    Example: every tag's set of article ids::

        Tag.objects.annotate(article_ids=ArrayAgg("articles__id"))

    PostgreSQL only. SQLite has no array type — use ``StringAgg`` (or
    a JSON aggregate) for cross-vendor work.
    """

    function = "ARRAY_AGG"


class JSONBAgg(Aggregate):
    """``JSONB_AGG(expr)`` — aggregate every group's values into a
    JSON array. PostgreSQL-only counterpart of :class:`ArrayAgg` for
    callers that want a homogeneous-but-typed-JSON output (Pydantic
    serialisation, edge functions).
    """

    function = "JSONB_AGG"


class BoolOr(Aggregate):
    """``BOOL_OR(expr)`` — TRUE if at least one row in the group has
    a truthy value. PostgreSQL-only at the SQL function level;
    SQLite users can express the same intent with
    ``Max(Case(...))``.
    """

    function = "BOOL_OR"


class BoolAnd(Aggregate):
    """``BOOL_AND(expr)`` — TRUE only when every row in the group
    has a truthy value. PostgreSQL-only.
    """

    function = "BOOL_AND"


class BitOr(Aggregate):
    """``BIT_OR(expr)`` — bitwise OR across the group. Supported on
    both PostgreSQL and MySQL; SQLite needs an extension."""

    function = "BIT_OR"


class BitAnd(Aggregate):
    """``BIT_AND(expr)`` — bitwise AND across the group. Same vendor
    notes as :class:`BitOr`."""

    function = "BIT_AND"


# ───────────────────────────────────────────────────────────────────────────
# Ordered-set aggregates (PostgreSQL only — MODE / PERCENTILE_*)
# ───────────────────────────────────────────────────────────────────────────


class _OrderedSetAggregate(Aggregate):
    """Base for PostgreSQL ordered-set aggregates emitted as
    ``FUNC(args) WITHIN GROUP (ORDER BY expr)``.

    The aggregated *expression* lives inside the ``WITHIN GROUP``
    clause, not as the function's positional argument — that's the
    SQL distinction from a regular aggregate. Parameters that go in
    the function call (e.g. the percentile fraction) are bound via
    ``%s`` placeholders so callers can't smuggle expressions there.
    """

    function: str = ""

    def __init__(
        self,
        expression: str,
        *,
        filter: Any = None,
        output_field: Any = None,
    ) -> None:
        super().__init__(
            expression, filter=filter, output_field=output_field
        )

    # Subclasses fill in the function-arg list (params bound at SQL time).
    def _func_args(self) -> tuple[str, list]:
        return "", []

    def as_sql(
        self,
        table_alias: str | None = None,
        *,
        model: Any = None,
        **kwargs: Any,
    ) -> tuple[str, list]:
        expr = self.expression
        if expr == "pk" and model is not None and model._meta.pk:
            expr = model._meta.pk.column
        col = f'"{table_alias}"."{expr}"' if table_alias else f'"{expr}"'
        args_sql, params = self._func_args()
        return (
            f"{self.function}({args_sql}) WITHIN GROUP (ORDER BY {col})",
            params,
        )


class Mode(_OrderedSetAggregate):
    """``MODE() WITHIN GROUP (ORDER BY expr)`` — most-frequent value
    in the group. Ties resolve to the first one in the order.
    PostgreSQL only.

    Example: most-popular tag colour::

        Tag.objects.aggregate(top_color=Mode("color"))
    """

    function = "MODE"


class PercentileCont(_OrderedSetAggregate):
    """``PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY expr)`` —
    *continuous* percentile (interpolates between adjacent values).
    *fraction* in ``[0.0, 1.0]``. PostgreSQL only.

    ``PercentileCont("response_ms", fraction=0.95)`` reads the p95
    latency over a group.
    """

    function = "PERCENTILE_CONT"

    def __init__(
        self,
        expression: str,
        *,
        fraction: float,
        filter: Any = None,
        output_field: Any = None,
    ) -> None:
        if not (0.0 <= fraction <= 1.0):
            raise ValueError(
                f"PercentileCont(fraction={fraction!r}): fraction must be "
                "in [0.0, 1.0]."
            )
        self.fraction = fraction
        super().__init__(
            expression, filter=filter, output_field=output_field
        )

    def _func_args(self) -> tuple[str, list]:
        return "%s", [self.fraction]


class PercentileDisc(PercentileCont):
    """``PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)`` —
    *discrete* percentile (returns one of the actual values, no
    interpolation). Same args as :class:`PercentileCont`."""

    function = "PERCENTILE_DISC"
