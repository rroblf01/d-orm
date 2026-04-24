from __future__ import annotations


class Q:
    AND = "AND"
    OR = "OR"

    def __init__(self, *args, **kwargs):
        self.connector = Q.AND
        self.negated = False
        self.children: list = []
        for q in args:
            self.children.append(q)
        for key, value in kwargs.items():
            self.children.append((key, value))

    def _combine(self, other, connector: str) -> "Q":
        if not isinstance(other, Q):
            raise TypeError(f"Cannot combine Q with {type(other)}")
        q = Q()
        q.connector = connector
        q.children = [self, other]
        return q

    def __and__(self, other: "Q") -> "Q":
        return self._combine(other, Q.AND)

    def __or__(self, other: "Q") -> "Q":
        return self._combine(other, Q.OR)

    def __invert__(self) -> "Q":
        q = Q(*[c for c in self.children if isinstance(c, Q)])
        q.children = list(self.children)
        q.connector = self.connector
        q.negated = not self.negated
        return q

    def __repr__(self):
        prefix = "~" if self.negated else ""
        return f"{prefix}Q({self.connector}: {self.children!r})"


class CombinedExpression:
    def __init__(self, lhs, operator: str, rhs):
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs

    def as_sql(self, compiler, connection):
        lhs_sql, lhs_params = compiler.compile(self.lhs, connection)
        rhs_sql, rhs_params = compiler.compile(self.rhs, connection)
        return f"({lhs_sql} {self.operator} {rhs_sql})", lhs_params + rhs_params


class F:
    """Reference to a model field for use in expressions."""

    def __init__(self, name: str):
        self.name = name

    def __add__(self, other):
        return CombinedExpression(self, "+", other)

    def __sub__(self, other):
        return CombinedExpression(self, "-", other)

    def __mul__(self, other):
        return CombinedExpression(self, "*", other)

    def __truediv__(self, other):
        return CombinedExpression(self, "/", other)

    def __neg__(self):
        return CombinedExpression(Value(0), "-", self)

    def __repr__(self):
        return f"F({self.name!r})"


class Value:
    """Wraps a constant value for use in expressions."""

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f"Value({self.value!r})"


class RawSQL:
    """Embeds raw SQL with parameters."""

    def __init__(self, sql: str, params: tuple = ()):
        self.sql = sql
        self.params = params

    def as_sql(self):
        return self.sql, list(self.params)
