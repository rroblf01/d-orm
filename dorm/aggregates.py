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
    ) -> tuple[str, list]:
        """Compile this aggregate to SQL.

        ``model`` is optional but recommended: when supplied,
        ``Count("pk")`` resolves to the model's actual primary-key
        column (e.g. ``"id"``). Without ``model``, ``"pk"`` would fall
        through verbatim and the database would reject the query with
        ``no such column: <table>.pk`` — the bug this parameter
        prevents.
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
        sql = self.template % {
            "function": self.function,
            "distinct": distinct,
            "expressions": col,
        }
        return sql, []

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
