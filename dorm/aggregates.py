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
        if expr == "*":
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
    """``STRING_AGG(expr, separator)`` — concatenate values within a
    GROUP BY using *separator*. PostgreSQL only.

    Example: list every author's books on one row::

        Author.objects.annotate(
            titles=StringAgg("books__title", separator=", ")
        )
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
    ) -> None:
        self.separator = separator
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
        return f"STRING_AGG({distinct}{col}, %s)", [self.separator]


class ArrayAgg(Aggregate):
    """``ARRAY_AGG(expr)`` — collect values into a PostgreSQL array.

    Example: every tag's set of article ids::

        Tag.objects.annotate(article_ids=ArrayAgg("articles__id"))

    PostgreSQL only. SQLite has no array type — use ``StringAgg`` (or
    a JSON aggregate) for cross-vendor work.
    """

    function = "ARRAY_AGG"
