from __future__ import annotations


class Aggregate:
    function: str = ""
    template: str = "%(function)s(%(distinct)s%(expressions)s)"

    def __init__(self, expression: str, distinct: bool = False, filter=None, output_field=None):
        self.expression = expression
        self.distinct = distinct
        self.filter = filter
        self.output_field = output_field

    def as_sql(self, table_alias: str | None = None) -> tuple[str, list]:
        distinct = "DISTINCT " if self.distinct else ""
        if self.expression == "*":
            col = "*"
        elif table_alias:
            col = f'"{table_alias}"."{self.expression}"'
        else:
            col = f'"{self.expression}"'
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
