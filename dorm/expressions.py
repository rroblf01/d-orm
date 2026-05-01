from __future__ import annotations

from typing import Any


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
        # Recursively copy nested Q nodes so a mutation on the
        # inverted Q (or on the original) doesn't bleed across.
        # The previous shallow copy shared every nested Q
        # instance, so ``q = Q(a=1) & Q(b=2); ~q`` returned a Q
        # whose ``children[0]`` was the *same object* as
        # ``q.children[0]`` — appending to one mutated both.
        # Tuple children (``(key, value)``) are immutable so we
        # can keep the references; only the Q wrappers need a
        # fresh copy.
        new_children: list = []
        for c in self.children:
            if isinstance(c, Q):
                copy = Q()
                copy.connector = c.connector
                copy.negated = c.negated
                copy.children = list(c.children)
                new_children.append(copy)
            else:
                new_children.append(c)
        q = Q()
        q.children = new_children
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

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        return self.sql, list(self.params)


class OuterRef:
    """Reference to a column on the *outer* queryset, used inside
    :class:`Subquery` / :class:`Exists` to build correlated subqueries.

    ``OuterRef("pk")`` resolves to the outer model's primary-key column
    when the subquery is compiled. Any other name is taken as a field
    name on the outer model::

        # "Authors with at least one published book"
        Author.objects.filter(
            Exists(Book.objects.filter(author=OuterRef("pk"), published=True))
        )
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"OuterRef({self.name!r})"


class Subquery:
    """Embed a QuerySet as a scalar subquery — typically as the
    expression of an :meth:`annotate` or as the right-hand side of a
    filter.

    The wrapped queryset's ``SELECT`` list is used as-is; project a single
    column with ``.values("col")`` (or ``.values_list("col", flat=True)``)
    when you want a true scalar::

        # Each Author annotated with the title of their latest book.
        latest = (
            Book.objects
                .filter(author=OuterRef("pk"))
                .order_by("-published_on")
                .values("title")[:1]
        )
        Author.objects.annotate(latest_title=Subquery(latest))
    """

    def __init__(self, queryset: Any, output_field: Any = None) -> None:
        self.queryset = queryset
        self.output_field = output_field

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        outer_model = kwargs.get("model")
        sub_sql, sub_params = self.queryset._query.as_subquery_sql(
            outer_alias=table_alias, outer_model=outer_model
        )
        return f"({sub_sql})", sub_params

    def __repr__(self) -> str:
        return f"Subquery({self.queryset!r})"


class Exists:
    """``EXISTS (subquery)`` — boolean test. Negate with ``~Exists(...)``::

        # "Active customers with at least one paid order"
        Customer.objects.filter(
            is_active=True,
            ...
        ).filter(
            Exists(Order.objects.filter(customer=OuterRef("pk"), paid=True))
        )

    The subquery's ``SELECT`` list is irrelevant — the database only
    looks at row presence — so don't bother projecting columns.
    """

    def __init__(self, queryset: Any, *, negated: bool = False) -> None:
        self.queryset = queryset
        self.negated = negated

    def __invert__(self) -> "Exists":
        return Exists(self.queryset, negated=not self.negated)

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        outer_model = kwargs.get("model")
        sub_sql, sub_params = self.queryset._query.as_subquery_sql(
            outer_alias=table_alias, outer_model=outer_model
        )
        prefix = "NOT EXISTS" if self.negated else "EXISTS"
        return f"{prefix} ({sub_sql})", sub_params

    def __repr__(self) -> str:
        prefix = "~" if self.negated else ""
        return f"{prefix}Exists({self.queryset!r})"
