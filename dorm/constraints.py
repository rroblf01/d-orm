"""Database-level constraints declared on ``Meta.constraints``.

Two flavours are supported in 2.1:

- :class:`CheckConstraint` — an arbitrary boolean predicate enforced by
  the database (``CHECK (col > 0)``).
- :class:`UniqueConstraint` — a uniqueness rule, optionally restricted
  by a ``condition`` to make a *partial* unique index (the canonical
  "only one active row per user" pattern).

Both render to backend-specific DDL during :class:`CreateModel` and the
``AddConstraint`` / ``RemoveConstraint`` migration operations. The
underlying SQL is identical on PostgreSQL and SQLite (≥ 3.0 for CHECK,
≥ 3.8.0 for partial indexes — every supported Python ships newer).
"""
from __future__ import annotations

from typing import Any

from .conf import _validate_identifier
from .exceptions import ImproperlyConfigured
from .expressions import Q


class BaseConstraint:
    """Common protocol — every constraint exposes a ``name`` and is
    serialisable for migrations via :meth:`describe`."""

    def __init__(self, *, name: str) -> None:
        _validate_identifier(name, kind="constraint name")
        self.name = name

    def constraint_sql(self, table: str, connection: Any) -> str:
        raise NotImplementedError

    def remove_sql(self, table: str, connection: Any) -> str:
        # PostgreSQL: ``ALTER TABLE ... DROP CONSTRAINT``. SQLite has no
        # ``DROP CONSTRAINT`` but accepts the same ``DROP INDEX`` for
        # unique constraints implemented as partial indexes. Subclasses
        # override when needed.
        return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{self.name}"'

    def describe(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseConstraint):
            return NotImplemented
        return type(self) is type(other) and self.name == other.name

    def __hash__(self) -> int:
        return hash((type(self), self.name))

    def __repr__(self) -> str:
        return self.describe()


class CheckConstraint(BaseConstraint):
    """Database-level ``CHECK (predicate)`` constraint.

    *check* is a :class:`~dorm.expressions.Q` object (or a Q-equivalent
    keyword form via :func:`Q`). Lookups follow the same rules as
    :meth:`QuerySet.filter`::

        from dorm import CheckConstraint, Q

        class Order(dorm.Model):
            quantity = dorm.IntegerField()
            price    = dorm.DecimalField(max_digits=10, decimal_places=2)

            class Meta:
                constraints = [
                    CheckConstraint(
                        check=Q(quantity__gt=0) & Q(price__gte=0),
                        name="order_qty_and_price_positive",
                    ),
                ]

    The predicate is compiled once at migration emit time. Field-name
    lookups translate to column references on the target table; the
    predicate must reference columns of *that* table only — joins are
    not supported in CHECK clauses by either backend.
    """

    def __init__(self, *, check: Q, name: str) -> None:
        if not isinstance(check, Q):
            raise ImproperlyConfigured(
                "CheckConstraint(check=...) must be a Q object, got "
                f"{type(check).__name__}."
            )
        super().__init__(name=name)
        self.check = check

    def _compile_check(self, table: str) -> tuple[str, list]:
        from .functions import _compile_condition

        return _compile_condition(self.check, table_alias=None)

    def constraint_sql(self, table: str, connection: Any) -> str:
        sql, params = self._compile_check(table)
        if params:
            # Splicing user-controlled values into a CHECK predicate is
            # safe (the values come from the developer's source code,
            # not user input) but Django's ABI keeps the params anyway.
            # Inline literals so the resulting DDL is portable across
            # ``execute_script`` paths that don't bind params on DDL.
            from .fields import _inline_literal  # noqa: PLC0415

            sql = _inline_literal(sql, params)
        return f'ALTER TABLE "{table}" ADD CONSTRAINT "{self.name}" CHECK ({sql})'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CheckConstraint):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(("CheckConstraint", self.name))

    def describe(self) -> str:
        return f"CheckConstraint(check=..., name={self.name!r})"


class UniqueConstraint(BaseConstraint):
    """Uniqueness across a set of columns.

    Three forms:

    - ``UniqueConstraint(fields=["email"], name="uniq_user_email")`` —
      same as ``unique=True`` on a single field, but reified so the
      autodetector can manage it.
    - ``UniqueConstraint(fields=["org_id", "slug"], name="...")`` —
      replaces ``unique_together``; clearer, equivalent semantics.
    - ``UniqueConstraint(fields=["user_id"], condition=Q(active=True),
      name="uniq_active_user")`` — *partial* unique index. Only rows
      matching ``condition`` participate. The canonical "one active
      session per user" / "soft-deleted rows don't collide" pattern.

    Partial unique constraints are emitted as ``CREATE UNIQUE INDEX
    name ON table (cols) WHERE predicate`` (PostgreSQL + SQLite ≥ 3.8).
    Plain ones use ``ALTER TABLE ... ADD CONSTRAINT name UNIQUE (cols)``
    on PostgreSQL and ``CREATE UNIQUE INDEX`` on SQLite (which has no
    ``ALTER TABLE ADD CONSTRAINT``).
    """

    def __init__(
        self,
        *,
        fields: list[str] | tuple[str, ...],
        name: str,
        condition: Q | None = None,
    ) -> None:
        if not fields:
            raise ImproperlyConfigured("UniqueConstraint(fields=...) cannot be empty.")
        for f in fields:
            _validate_identifier(f, kind="UniqueConstraint field")
        if condition is not None and not isinstance(condition, Q):
            raise ImproperlyConfigured(
                "UniqueConstraint(condition=...) must be a Q object."
            )
        super().__init__(name=name)
        self.fields = list(fields)
        self.condition = condition

    def constraint_sql(self, table: str, connection: Any) -> str:
        cols = ", ".join(f'"{c}"' for c in self.fields)
        if self.condition is not None:
            from .functions import _compile_condition
            from .fields import _inline_literal

            pred_sql, pred_params = _compile_condition(self.condition, table_alias=None)
            if pred_params:
                pred_sql = _inline_literal(pred_sql, pred_params)
            return (
                f'CREATE UNIQUE INDEX "{self.name}" ON "{table}" '
                f"({cols}) WHERE {pred_sql}"
            )
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "sqlite":
            # SQLite has no ALTER TABLE ADD CONSTRAINT; a unique index
            # achieves the same uniqueness guarantee.
            return f'CREATE UNIQUE INDEX "{self.name}" ON "{table}" ({cols})'
        return (
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{self.name}" UNIQUE ({cols})'
        )

    def remove_sql(self, table: str, connection: Any) -> str:
        if self.condition is not None:
            return f'DROP INDEX IF EXISTS "{self.name}"'
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "sqlite":
            return f'DROP INDEX IF EXISTS "{self.name}"'
        return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{self.name}"'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, UniqueConstraint):
            return NotImplemented
        return (
            self.name == other.name
            and self.fields == other.fields
            and self.condition is other.condition
        )

    def __hash__(self) -> int:
        return hash(("UniqueConstraint", self.name, tuple(self.fields)))

    def describe(self) -> str:
        cond = ", condition=..." if self.condition is not None else ""
        return f"UniqueConstraint(fields={self.fields!r}, name={self.name!r}{cond})"
