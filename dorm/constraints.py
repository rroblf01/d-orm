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

    def _compile_check(
        self, table: str, connection: Any = None
    ) -> tuple[str, list]:
        from .functions import _compile_condition

        return _compile_condition(
            self.check, table_alias=None, connection=connection
        )

    def _check_body(self, connection: Any) -> str:
        sql, params = self._compile_check("", connection=connection)
        if params:
            # Splicing user-controlled values into a CHECK predicate is
            # safe (the values come from the developer's source code,
            # not user input) but Django's ABI keeps the params anyway.
            # Inline literals so the resulting DDL is portable across
            # ``execute_script`` paths that don't bind params on DDL.
            from .fields import _inline_literal  # noqa: PLC0415

            sql = _inline_literal(sql, params)
        return sql

    def create_sql_inline(self, connection: Any) -> str:
        """In-table CHECK clause: ``CONSTRAINT "name" CHECK (...)``.

        Required on SQLite — its ``ALTER TABLE`` does not support
        ``ADD CONSTRAINT`` (only ADD COLUMN / DROP COLUMN / RENAME),
        so check constraints must live inside ``CREATE TABLE``.
        """
        return f'CONSTRAINT "{self.name}" CHECK ({self._check_body(connection)})'

    def constraint_sql(self, table: str, connection: Any) -> str:
        return (
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{self.name}" '
            f"CHECK ({self._check_body(connection)})"
        )

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
        deferrable: str | None = None,
        include: list[str] | None = None,
    ) -> None:
        if not fields:
            raise ImproperlyConfigured("UniqueConstraint(fields=...) cannot be empty.")
        for f in fields:
            _validate_identifier(f, kind="UniqueConstraint field")
        if condition is not None and not isinstance(condition, Q):
            raise ImproperlyConfigured(
                "UniqueConstraint(condition=...) must be a Q object."
            )
        if deferrable is not None and deferrable not in (
            "deferred",
            "immediate",
        ):
            raise ImproperlyConfigured(
                "UniqueConstraint(deferrable=...) must be one of "
                "'deferred', 'immediate', or None."
            )
        if include is not None:
            for col in include:
                _validate_identifier(col, kind="UniqueConstraint include column")
        super().__init__(name=name)
        self.fields = list(fields)
        self.condition = condition
        self.deferrable = deferrable
        self.include = list(include) if include else []

    def constraint_sql(self, table: str, connection: Any) -> str:
        cols = ", ".join(f'"{c}"' for c in self.fields)
        vendor = getattr(connection, "vendor", "sqlite")
        include_clause = ""
        if self.include and vendor == "postgresql":
            inc = ", ".join(f'"{c}"' for c in self.include)
            include_clause = f" INCLUDE ({inc})"
        if self.condition is not None:
            from .functions import _compile_condition
            from .fields import _inline_literal

            pred_sql, pred_params = _compile_condition(
                self.condition, table_alias=None, connection=connection
            )
            if pred_params:
                pred_sql = _inline_literal(pred_sql, pred_params)
            return (
                f'CREATE UNIQUE INDEX "{self.name}" ON "{table}" '
                f"({cols}){include_clause} WHERE {pred_sql}"
            )
        if vendor == "sqlite":
            # SQLite has no ALTER TABLE ADD CONSTRAINT; a unique index
            # achieves the same uniqueness guarantee.
            return f'CREATE UNIQUE INDEX "{self.name}" ON "{table}" ({cols})'
        # ``DEFERRABLE INITIALLY DEFERRED`` lets the unique check
        # run at COMMIT instead of statement-end — useful for
        # row-swaps inside a transaction (Django parity, PG only).
        # MySQL doesn't support deferrable constraints; ignore the
        # flag silently there.
        deferr_clause = ""
        if vendor == "postgresql" and self.deferrable is not None:
            mode = "DEFERRED" if self.deferrable == "deferred" else "IMMEDIATE"
            deferr_clause = f" DEFERRABLE INITIALLY {mode}"
        return (
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{self.name}" '
            f"UNIQUE ({cols}){include_clause}{deferr_clause}"
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


class ExclusionConstraint(BaseConstraint):
    """PostgreSQL ``EXCLUDE`` constraint — guarantees no two rows
    in the table satisfy the same operator over the named
    expressions. Most common use case: range-overlap exclusion
    for "no two reservations for the same room can overlap"
    (``EXCLUDE USING gist (room_id WITH =, slot WITH &&)``).

    Mirrors Django's ``ExclusionConstraint``. PostgreSQL only —
    SQLite + MySQL silently downgrade to a no-op
    (``constraint_sql`` returns the empty string and
    :meth:`AddConstraint` skips application).

    *expressions* is a list of ``(column_or_expression, operator)``
    pairs. Each operator is validated against the safe-identifier
    regex so user input can't splice arbitrary SQL into the
    ``EXCLUDE`` clause.
    """

    def __init__(
        self,
        *,
        name: str,
        expressions: list[tuple[str, str]],
        index_type: str = "gist",
        condition: Q | None = None,
        deferrable: str | None = None,
    ) -> None:
        if not expressions:
            raise ImproperlyConfigured(
                "ExclusionConstraint requires at least one (column, operator) pair."
            )
        for col, op in expressions:
            _validate_identifier(col, kind="ExclusionConstraint column")
            # Operators are PG-supported strings like ``=``, ``&&``,
            # ``<@``. Whitelist by character class to avoid splicing
            # arbitrary SQL.
            if not all(ch in "<>=!&|@~?+*-/" for ch in op):
                raise ImproperlyConfigured(
                    f"ExclusionConstraint operator {op!r} contains "
                    "unexpected characters."
                )
        if index_type.lower() not in ("gist", "spgist", "btree"):
            raise ImproperlyConfigured(
                f"ExclusionConstraint(index_type={index_type!r}) — must be "
                "'gist', 'spgist', or 'btree'."
            )
        if condition is not None and not isinstance(condition, Q):
            raise ImproperlyConfigured(
                "ExclusionConstraint(condition=...) must be a Q object."
            )
        if deferrable is not None and deferrable not in (
            "deferred",
            "immediate",
        ):
            raise ImproperlyConfigured(
                "ExclusionConstraint(deferrable=...) must be one of "
                "'deferred', 'immediate', or None."
            )
        super().__init__(name=name)
        self.expressions = list(expressions)
        self.index_type = index_type.lower()
        self.condition = condition
        self.deferrable = deferrable

    def constraint_sql(self, table: str, connection: Any) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            return ""
        parts = ", ".join(
            f'"{col}" WITH {op}' for col, op in self.expressions
        )
        where_clause = ""
        if self.condition is not None:
            from .functions import _compile_condition
            from .fields import _inline_literal

            pred_sql, pred_params = _compile_condition(
                self.condition, table_alias=None, connection=connection
            )
            if pred_params:
                pred_sql = _inline_literal(pred_sql, pred_params)
            where_clause = f" WHERE ({pred_sql})"
        deferr = ""
        if self.deferrable is not None:
            mode = "DEFERRED" if self.deferrable == "deferred" else "IMMEDIATE"
            deferr = f" DEFERRABLE INITIALLY {mode}"
        return (
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{self.name}" '
            f"EXCLUDE USING {self.index_type} ({parts}){where_clause}{deferr}"
        )

    def remove_sql(self, table: str, connection: Any) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            return ""
        return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{self.name}"'

    def describe(self) -> str:
        return (
            f"ExclusionConstraint(name={self.name!r}, "
            f"expressions={self.expressions!r})"
        )
