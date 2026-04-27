"""Index definitions for dorm models.

The 2.1 release extends :class:`Index` to cover the production-grade
patterns most ORMs eventually have to grow:

- **Method**: ``GIN`` / ``GIST`` / ``BRIN`` / ``HASH`` (PostgreSQL),
  in addition to the default B-tree. SQLite ignores the method (it
  only ships B-tree) but accepts the keyword silently.
- **Partial indexes**: pass ``condition=Q(...)`` to emit ``CREATE
  INDEX ... WHERE predicate``. Massive space win for the common
  "soft-deleted rows" / "active rows" patterns.
- **Expression indexes**: pass strings like ``"LOWER(email)"`` in
  ``fields=[]`` (or ``expressions=[...]`` for clarity). Quoted as raw
  SQL — the caller is responsible for the literal.

The plain ``fields=[col1, col2]`` form is unchanged.
"""
from __future__ import annotations

import re
from typing import Any

from .conf import _validate_identifier


_VALID_METHODS = frozenset(
    {"btree", "hash", "gin", "gist", "brin", "spgist", "bloom"}
)

# Indexable expressions accept a small grammar of SQL forms (function
# calls on column names + simple coalesce / casts). Anything more
# adventurous should be issued as a ``RunSQL`` migration, not built
# into an ``Index`` literal.
_EXPRESSION_RE = re.compile(
    r"^[A-Z_][A-Z0-9_]*\(\s*"
    r"(?:[a-zA-Z_][a-zA-Z0-9_]*"
    r"(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*"
    r"|\*)"
    r"\s*\)$",
    re.IGNORECASE,
)


def _validate_index_expression(expr: str) -> str:
    """Accept a small allowlist of expression-index forms. Reject
    anything that looks like raw SQL injection vectors (semicolons,
    parentheses outside the function form, comment markers).
    """
    if not isinstance(expr, str) or not expr.strip():
        raise ValueError("Index expression must be a non-empty string.")
    if not _EXPRESSION_RE.match(expr.strip()):
        raise ValueError(
            f"Index expression {expr!r} not in the supported allowlist. "
            "Use plain column names in fields=, or call "
            "``Index(fields=['LOWER(email)'])`` only when the expression "
            "matches the documented grammar (FN(col1, col2)). For more "
            "complex indexes use a RunSQL migration."
        )
    return expr.strip()


class Index:
    """Database index for use in ``Meta.indexes``.

    Args:
        fields: column names (or simple expressions like ``"LOWER(col)"``).
        name: optional explicit index name. Auto-derived if omitted.
        unique: whether the index enforces uniqueness.
        method: index method — ``"btree"`` (default), ``"hash"``,
            ``"gin"``, ``"gist"``, ``"brin"``. PostgreSQL-only beyond
            B-tree; SQLite silently uses B-tree regardless.
        condition: a :class:`~dorm.expressions.Q` predicate. When set,
            emits ``CREATE INDEX ... WHERE predicate`` (a partial
            index). Both PostgreSQL and SQLite ≥ 3.8 support this.
        opclasses: PostgreSQL-only — operator classes per column for
            specialised indexing (e.g. ``opclasses=["text_pattern_ops"]``
            for ``LIKE 'prefix%'`` lookups). Length must match
            ``fields``.

    Examples::

        from dorm import Index, Q

        # Partial index — only active rows participate.
        Index(fields=["email"], name="uniq_active_email", unique=True,
              condition=Q(deleted_at__isnull=True))

        # GIN index for JSONB containment lookups.
        Index(fields=["payload"], name="ix_event_payload_gin", method="gin")

        # Expression index for case-insensitive lookup.
        Index(fields=["LOWER(email)"], name="ix_user_email_lower")
    """

    def __init__(
        self,
        fields: list[str],
        name: str | None = None,
        unique: bool = False,
        *,
        method: str = "btree",
        condition: Any = None,
        opclasses: list[str] | None = None,
    ) -> None:
        if not fields:
            raise ValueError("Index requires at least one field/expression.")
        self.fields: list[str] = []
        # ``fields`` accepts either bare identifiers or simple expression
        # forms; we validate each at construction so a typo crashes
        # ``makemigrations``, not a downstream ``CREATE INDEX``.
        # A leading ``-`` is permitted as Django's convention for a
        # descending index column; stripped for validation but kept in
        # the stored string so the SQL emit step honours the order.
        for f in fields:
            if "(" in f:
                self.fields.append(_validate_index_expression(f))
            else:
                bare = f[1:] if f.startswith("-") else f
                _validate_identifier(bare, kind="Index field")
                self.fields.append(f)

        method_norm = method.lower() if method else "btree"
        if method_norm not in _VALID_METHODS:
            raise ValueError(
                f"Index method {method!r} is not supported. Choose from: "
                f"{sorted(_VALID_METHODS)}."
            )
        self.method = method_norm

        from .expressions import Q  # local import to avoid cycles

        if condition is not None and not isinstance(condition, Q):
            raise ValueError("Index(condition=...) must be a Q object or None.")
        self.condition = condition

        if opclasses is not None:
            if len(opclasses) != len(self.fields):
                raise ValueError(
                    "Index(opclasses=...) length must match fields."
                )
            for op in opclasses:
                _validate_identifier(op, kind="Index opclass")
        self.opclasses = list(opclasses) if opclasses else []

        self.unique = unique
        self._name = name

    def get_name(self, model_name: str) -> str:
        if self._name:
            return self._name
        # Derive a name from the field list. Strip non-identifier chars
        # from expression-style fields (``LOWER(email)`` → ``loweremail``)
        # so the auto-name is still a valid SQL identifier.
        clean = [re.sub(r"[^A-Za-z0-9_]", "", f) for f in self.fields]
        suffix = "_".join(c for c in clean if c)
        prefix = "uniq" if self.unique else "idx"
        return f"{prefix}_{model_name.lower()}_{suffix}"

    @property
    def name(self) -> str:
        return self._name or ""

    def _column_sql(self) -> str:
        """Render the per-field column list, including operator classes
        when present. Expression-style fields are emitted verbatim
        because they were already validated against the grammar.
        A leading ``-`` triggers ``DESC`` ordering on that column."""
        parts: list[str] = []
        for i, f in enumerate(self.fields):
            if "(" in f:
                base = f
            elif f.startswith("-"):
                base = f'"{f[1:]}" DESC'
            else:
                base = f'"{f}"'
            if self.opclasses:
                base = f"{base} {self.opclasses[i]}"
            parts.append(base)
        return ", ".join(parts)

    def create_sql(self, table: str, *, vendor: str = "sqlite") -> tuple[str, str]:
        """Return ``(forward_sql, reverse_sql)`` for this index.

        Splits naming concerns from migration emit so the same logic
        can be used by both :class:`AddIndex` and the in-line
        ``CreateModel`` indexes loop.
        """
        idx_name = self.get_name(table.split("_", 1)[-1] if "_" in table else table)
        from .fields import _inline_literal

        from .functions import _compile_condition

        cols = self._column_sql()
        unique = "UNIQUE " if self.unique else ""

        if vendor == "postgresql" and self.method != "btree":
            method_clause = f" USING {self.method}"
        else:
            method_clause = ""

        where_clause = ""
        if self.condition is not None:
            pred_sql, pred_params = _compile_condition(self.condition, table_alias=None)
            if pred_params:
                pred_sql = _inline_literal(pred_sql, pred_params)
            where_clause = f" WHERE {pred_sql}"

        forward = (
            f'CREATE {unique}INDEX IF NOT EXISTS "{idx_name}" ON "{table}"'
            f'{method_clause} ({cols}){where_clause}'
        )
        reverse = f'DROP INDEX IF EXISTS "{idx_name}"'
        return forward, reverse

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Index):
            return NotImplemented
        return (
            self.fields == other.fields
            and self.unique == other.unique
            and self._name == other._name
            and self.method == other.method
            and self.opclasses == other.opclasses
            # Q objects don't compare structurally; fall back to repr.
            and repr(self.condition) == repr(other.condition)
        )

    def __hash__(self) -> int:
        return hash(
            (tuple(self.fields), self.unique, self._name, self.method)
        )

    def __repr__(self) -> str:
        extras = []
        if self.method != "btree":
            extras.append(f"method={self.method!r}")
        if self.condition is not None:
            extras.append("condition=...")
        if self.opclasses:
            extras.append(f"opclasses={self.opclasses!r}")
        suffix = (", " + ", ".join(extras)) if extras else ""
        return (
            f"Index(fields={self.fields!r}, unique={self.unique!r}, "
            f"name={self._name!r}{suffix})"
        )
