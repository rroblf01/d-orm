from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .expressions import CombinedExpression, Exists, F, OuterRef, Q, Subquery, Value
from .lookups import build_lookup_sql, parse_lookup_key

if TYPE_CHECKING:
    pass

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, kind: str = "field") -> None:
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid {kind} name '{name}': only letters, digits, and underscores are allowed."
        )


def _compile_expr(val) -> tuple[str, list]:
    """Convert a Python value or expression (F, CombinedExpression, Value) to (sql, params)."""
    if isinstance(val, F):
        return f'"{val.name}"', []
    if isinstance(val, Value):
        return "%s", [val.value]
    if isinstance(val, CombinedExpression):
        lhs_sql, lhs_p = _compile_expr(val.lhs)
        rhs_sql, rhs_p = _compile_expr(val.rhs)
        return f"({lhs_sql} {val.operator} {rhs_sql})", lhs_p + rhs_p
    return "%s", [val]


class SQLQuery:
    """Translates QuerySet state into SQL strings."""

    def __init__(self, model):
        self.model = model
        self.where_nodes: list = []  # list of Q objects or (field, lookup, value)
        self.order_by_fields: list[str] = []
        self.limit_val: int | None = None
        self.offset_val: int | None = None
        self.selected_fields: list[str] | None = None  # for .values()
        self.deferred_loading: bool = False  # True when using only()/defer()
        self.annotations: dict[str, Any] = {}
        # Names in ``annotations`` that should be available for WHERE /
        # ORDER BY but NOT included in the SELECT column list. Populated
        # by :meth:`QuerySet.alias`. Treated like annotations in every
        # other respect (FK joins, expression compilation) — only the
        # SELECT-projection step honours this set.
        self.alias_only_names: set[str] = set()
        self.distinct_flag: bool = False
        self.for_update_flag: bool = False
        # Detail flags for ``SELECT … FOR UPDATE`` clauses. Only meaningful
        # on PostgreSQL; SQLite raises if any is set (see queryset's
        # ``select_for_update``). ``for_update_of`` is a tuple of relation
        # names — empty means lock the whole row, not specific tables.
        self.for_update_skip_locked: bool = False
        self.for_update_no_wait: bool = False
        self.for_update_of: tuple[str, ...] = ()
        self.joins: list[tuple] = []  # (join_type, table, alias, on_condition)
        self.group_by_fields: list[str] = []
        self.having_nodes: list = []
        self.select_related_fields: list[str] = []
        self.prefetch_related_fields: list[str] = []
        # CTEs declared via ``QuerySet.with_cte(name=qs)``. Each entry is
        # (name, queryset). Compiled into a ``WITH name AS (sub)`` prefix
        # in :meth:`as_select`.
        self.ctes: list[tuple[str, Any]] = []
        # When this SQLQuery is compiled as a *correlated* subquery (used
        # inside :class:`Subquery` / :class:`Exists`), the outer query's
        # table alias and model class are stamped here so any
        # :class:`OuterRef` value resolves to ``"<outer_alias>"."<col>"``
        # at compile time. ``None`` outside subqueries.
        self._outer_alias: str | None = None
        self._outer_model: Any = None

    def clone(self) -> "SQLQuery":
        q = SQLQuery(self.model)
        q.where_nodes = list(self.where_nodes)
        q.order_by_fields = list(self.order_by_fields)
        q.limit_val = self.limit_val
        q.offset_val = self.offset_val
        q.selected_fields = list(self.selected_fields) if self.selected_fields is not None else None
        q.deferred_loading = self.deferred_loading
        q.annotations = dict(self.annotations)
        q.alias_only_names = set(self.alias_only_names)
        q.distinct_flag = self.distinct_flag
        q.for_update_flag = self.for_update_flag
        q.for_update_skip_locked = self.for_update_skip_locked
        q.for_update_no_wait = self.for_update_no_wait
        q.for_update_of = self.for_update_of
        q.joins = list(self.joins)
        q.group_by_fields = list(self.group_by_fields)
        q.having_nodes = list(self.having_nodes)
        q.select_related_fields = list(self.select_related_fields)
        q.prefetch_related_fields = list(self.prefetch_related_fields)
        q.ctes = list(self.ctes)
        q._outer_alias = self._outer_alias
        q._outer_model = self._outer_model
        return q

    # ── SQL builders ──────────────────────────────────────────────────────────

    def _compile_for_update(self, connection) -> str:
        """Return the trailing ``FOR UPDATE`` clause SQL for this query.

        On SQLite there is no row-level locking; we emit nothing and rely
        on the file-level lock + ``BEGIN IMMEDIATE`` semantics. The
        queryset method validates that no PG-only flags were set so the
        user gets a clear error rather than silent SQL corruption.

        On PostgreSQL we map directly:

            select_for_update()                       → FOR UPDATE
            select_for_update(skip_locked=True)       → FOR UPDATE SKIP LOCKED
            select_for_update(no_wait=True)           → FOR UPDATE NOWAIT
            select_for_update(of=("authors", ...))    → FOR UPDATE OF "authors"

        ``of`` names are validated through ``_validate_identifier`` so
        they can never inject SQL.
        """
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            # SQLite: row-level locking is not a thing — the OS-level
            # file lock + ``BEGIN IMMEDIATE`` already serialise writers.
            # Emit nothing; the queryset validated the args already.
            return ""

        clause = " FOR UPDATE"
        if self.for_update_of:
            of_parts: list[str] = []
            for name in self.for_update_of:
                _validate_identifier(name)
                of_parts.append(f'"{name}"')
            clause += " OF " + ", ".join(of_parts)
        if self.for_update_skip_locked:
            clause += " SKIP LOCKED"
        elif self.for_update_no_wait:
            clause += " NOWAIT"
        return clause

    def get_table(self) -> str:
        return self.model._meta.db_table

    def get_columns(self, table_alias: str | None = None) -> str:
        ta = f'"{table_alias}".' if table_alias else ""
        if self.selected_fields is not None:
            for f in self.selected_fields:
                _validate_identifier(f)
            return ", ".join(f'{ta}"{f}"' for f in self.selected_fields)
        concrete = [f for f in self.model._meta.fields if f.column]
        return ", ".join(f'{ta}"{f.column}"' for f in concrete)

    def _compile_ctes(self, connection) -> tuple[str, list]:
        """Build the leading ``WITH name AS (sub), name2 AS (sub2)`` clause.

        Returns ``("", [])`` when no CTEs were declared. The CTEs are
        compiled via :meth:`as_subquery_sql` so they're expressed in the
        same ``%s``-placeholder dialect; the outer
        :meth:`_adapt_placeholders` pass renumbers everything to ``$N``
        on PostgreSQL.
        """
        if not self.ctes:
            return "", []
        parts: list[str] = []
        params: list[Any] = []
        for name, qs in self.ctes:
            _validate_identifier(name, "CTE name")
            sub_sql, sub_params = qs._query.as_subquery_sql(
                outer_alias=self.model._meta.db_table,
                outer_model=self.model,
            )
            parts.append(f'"{name}" AS ({sub_sql})')
            params.extend(sub_params)
        return "WITH " + ", ".join(parts) + " ", params

    def as_select(self, connection) -> tuple[str, list]:
        table = self.get_table()
        alias = table
        distinct = "DISTINCT " if self.distinct_flag else ""

        # CTE prefix (``WITH name AS (...) ...``) appears before the SELECT
        # clause — its params are the very first in the bound list.
        cte_prefix, cte_params = self._compile_ctes(connection)

        # Annotations — collect SQL and params separately (appear before WHERE in SQL).
        # Names listed in ``alias_only_names`` came from :meth:`QuerySet.alias`
        # and must not be emitted in the SELECT list (they're only usable
        # in WHERE / ORDER BY). Their expressions are still compiled here
        # so any FK joins they trigger get registered on ``self.joins``.
        annotation_params: list = []
        extra_select = ""
        if self.annotations:
            parts = []
            for alias_name, agg in self.annotations.items():
                _validate_identifier(alias_name, "annotation alias")
                # Pass the model so aggregates can translate ``pk`` to
                # the actual PK column (e.g. ``Count("pk")`` →
                # ``COUNT("table"."id")``). Without this, the SQL
                # references a non-existent ``"pk"`` column and the
                # query fails with a clear-but-confusing error.
                # Pass the connection so vendor-specific functions
                # (Greatest/Least, StrIndex) can pick the right SQL
                # idiom — PG's ``GREATEST`` vs SQLite's variadic ``MAX``,
                # ``STRPOS`` vs ``INSTR``.
                agg_sql, agg_p = agg.as_sql(
                    alias, model=self.model, connection=connection
                )
                if alias_name in self.alias_only_names:
                    # alias()-only: skip the SELECT projection. We still
                    # compiled the SQL above so any side-effects (joins,
                    # validation) happened.
                    continue
                parts.append(f'{agg_sql} AS "{alias_name}"')
                annotation_params.extend(agg_p)
            if parts:
                extra_select = ", " + ", ".join(parts)

        params: list = []
        params.extend(cte_params)  # WITH params come first in SQL

        # Compile WHERE first so _resolve_column can populate self.joins via FK traversal
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        params.extend(annotation_params)  # SELECT annotations come after WITH
        params.extend(where_params)

        # select_related: build LEFT OUTER JOINs and prefixed columns (supports nested paths)
        sr_join_clauses: list[str] = []
        sr_extra_cols = ""
        if self.select_related_fields and self.selected_fields is None:
            sr_parts: list[str] = []
            added_aliases: set[str] = set()
            for path_str in self.select_related_fields:
                path = path_str.split("__")
                current_model = self.model
                current_table_alias = alias
                for depth, step in enumerate(path):
                    try:
                        field = current_model._meta.get_field(step)
                        if not hasattr(field, "_resolve_related_model"):
                            break
                        rel_model = field._resolve_related_model()
                        step_path = "__".join(path[: depth + 1])
                        sr_alias = f"_sr_{step_path}"
                        if sr_alias not in added_aliases:
                            pk_col = rel_model._meta.pk.column
                            join_table = rel_model._meta.db_table
                            on_cond = (
                                f'"{sr_alias}"."{pk_col}" = '
                                f'"{current_table_alias}"."{field.column}"'
                            )
                            sr_join_clauses.append(
                                f'LEFT OUTER JOIN "{join_table}" AS "{sr_alias}" ON {on_cond}'
                            )
                            for rf in rel_model._meta.fields:
                                if rf.column:
                                    sr_parts.append(
                                        f'"{sr_alias}"."{rf.column}" '
                                        f'AS "_sr_{step_path}_{rf.column}"'
                                    )
                            added_aliases.add(sr_alias)
                        current_model = rel_model
                        current_table_alias = sr_alias
                    except Exception:
                        break
            if sr_parts:
                sr_extra_cols = ", " + ", ".join(sr_parts)

        cols = self.get_columns(alias)
        select = (
            f'{cte_prefix}'
            f'SELECT {distinct}{cols}{extra_select}{sr_extra_cols} FROM "{table}"'
        )

        # ORDER BY — resolve FK traversal paths (may add JOINs) before emitting JOIN clauses
        order_by_sql = ""
        if self.order_by_fields:
            order_parts = []
            for f in self.order_by_fields:
                desc = f.startswith("-")
                fname = f[1:] if desc else f
                if "__" in fname:
                    col = self._resolve_column(fname.split("__"))
                else:
                    _validate_identifier(fname)
                    col = (
                        f'"{self.model._meta.db_table}"."{fname}"'
                        if self.joins
                        else f'"{fname}"'
                    )
                order_parts.append(f"{col} {'DESC' if desc else 'ASC'}")
            order_by_sql = " ORDER BY " + ", ".join(order_parts)

        # WHERE-derived JOINs (populated by _resolve_column during WHERE/ORDER BY compilation)
        for join_type, join_table, join_alias, on_cond in self.joins:
            select += f' {join_type} JOIN "{join_table}" AS "{join_alias}" ON {on_cond}'

        # select_related JOINs
        for sr_join in sr_join_clauses:
            select += f" {sr_join}"

        # WHERE
        if where_sql:
            select += f" WHERE {where_sql}"

        # GROUP BY
        if self.group_by_fields:
            for f in self.group_by_fields:
                _validate_identifier(f)
            gb = ", ".join(f'"{f}"' for f in self.group_by_fields)
            select += f" GROUP BY {gb}"

        # HAVING
        if self.having_nodes:
            having_sql, having_params = self._compile_nodes(self.having_nodes, connection)
            if having_sql:
                select += f" HAVING {having_sql}"
                params.extend(having_params)

        # ORDER BY
        if order_by_sql:
            select += order_by_sql

        # LIMIT / OFFSET
        # SQLite's parser rejects ``OFFSET`` without a preceding
        # ``LIMIT``, so ``qs[N:]`` (offset only) used to crash with
        # ``near "OFFSET": syntax error``. PostgreSQL accepts the
        # bare offset but not ``LIMIT -1`` (which SQLite uses as its
        # "no limit" sentinel). The portable workaround is the
        # maximum signed 64-bit int — both backends store it cleanly
        # and treat it as "all remaining rows" in practice.
        _LIMIT_NONE_SENTINEL = 9223372036854775807  # 2**63 - 1
        if self.limit_val is not None:
            select += f" LIMIT {int(self.limit_val)}"
        elif self.offset_val is not None:
            select += f" LIMIT {_LIMIT_NONE_SENTINEL}"
        if self.offset_val is not None:
            select += f" OFFSET {int(self.offset_val)}"

        if self.for_update_flag:
            select += self._compile_for_update(connection)

        return self._adapt_placeholders(select, connection), params

    def as_count(self, connection) -> tuple[str, list]:
        table = self.get_table()
        sql = f'SELECT COUNT(*) AS "count" FROM "{table}"'
        params: list = []

        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return self._adapt_placeholders(sql, connection), params

    def as_exists(self, connection) -> tuple[str, list]:
        table = self.get_table()
        sql = f'SELECT 1 FROM "{table}"'
        params: list = []
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)
        sql += " LIMIT 1"
        return self._adapt_placeholders(sql, connection), params

    def as_insert(self, fields: list, values: list, connection) -> tuple[str, list]:
        table = self.get_table()
        cols = ", ".join(f'"{f.column}"' for f in fields)
        placeholders = ", ".join(["%s"] * len(fields))
        sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'
        return self._adapt_placeholders(sql, connection), values

    def as_bulk_insert(
        self,
        fields: list,
        rows_values: list[list],
        connection,
        *,
        ignore_conflicts: bool = False,
        update_conflicts: bool = False,
        update_fields: list[str] | None = None,
        unique_fields: list[str] | None = None,
    ) -> tuple[str, list]:
        """Build a multi-row ``INSERT`` statement.

        With ``ignore_conflicts`` or ``update_conflicts`` set, append the
        backend's upsert clause:

        - PostgreSQL: ``ON CONFLICT (cols) DO NOTHING`` /
          ``ON CONFLICT (cols) DO UPDATE SET col = EXCLUDED.col``.
        - SQLite (≥ 3.24): same syntax — sqlite3 ships ON CONFLICT support
          since 3.24, which is older than every supported Python release.

        ``unique_fields`` is required for ``update_conflicts`` (PG needs
        the conflict target; SQLite accepts it bare but we mirror PG's
        contract for portability). ``update_fields`` defaults to every
        non-PK field — usually what you want.
        """
        table = self.get_table()
        cols = ", ".join(f'"{f.column}"' for f in fields)
        row_ph = f"({', '.join(['%s'] * len(fields))})"
        all_ph = ", ".join([row_ph] * len(rows_values))
        sql = f'INSERT INTO "{table}" ({cols}) VALUES {all_ph}'
        params = [v for row in rows_values for v in row]

        if ignore_conflicts and update_conflicts:
            raise ValueError(
                "as_bulk_insert: ignore_conflicts and update_conflicts "
                "are mutually exclusive."
            )

        if ignore_conflicts:
            # ON CONFLICT without a target = "any conflict" — works on
            # both PG and SQLite. Cheaper than enumerating every unique
            # constraint and equally correct for the "skip duplicates"
            # use case.
            sql += " ON CONFLICT DO NOTHING"
        elif update_conflicts:
            if not unique_fields:
                raise ValueError(
                    "bulk_create(update_conflicts=True) requires "
                    "unique_fields= to identify the conflict target."
                )
            for name in unique_fields:
                _validate_identifier(name)
            target_cols = ", ".join(f'"{c}"' for c in unique_fields)

            # Default update_fields = all non-PK, non-unique columns.
            if update_fields is None:
                update_cols = [
                    f.column
                    for f in fields
                    if not f.primary_key and f.column not in unique_fields
                ]
            else:
                # Resolve user-supplied attnames to columns. Validate
                # each so an attacker-controlled list can't smuggle SQL.
                update_cols = []
                meta = self.model._meta
                for name in update_fields:
                    _validate_identifier(name)
                    field = None
                    try:
                        field = meta.get_field(name)
                    except Exception:
                        pass
                    update_cols.append(field.column if field else name)

            if not update_cols:
                # Nothing meaningful to update — degrade to DO NOTHING so
                # the statement still parses. Mirrors Django's behaviour.
                sql += f" ON CONFLICT ({target_cols}) DO NOTHING"
            else:
                set_clauses = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                )
                sql += (
                    f" ON CONFLICT ({target_cols}) "
                    f"DO UPDATE SET {set_clauses}"
                )

        return self._adapt_placeholders(sql, connection), params

    def as_update(self, update_kwargs: dict, connection) -> tuple[str, list]:
        table = self.get_table()
        set_parts = []
        params: list = []
        for col, val in update_kwargs.items():
            expr_sql, expr_params = _compile_expr(val)
            set_parts.append(f'"{col}" = {expr_sql}')
            params.extend(expr_params)

        sql = f'UPDATE "{table}" SET {", ".join(set_parts)}'

        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return self._adapt_placeholders(sql, connection), params

    def as_delete(self, connection) -> tuple[str, list]:
        table = self.get_table()
        sql = f'DELETE FROM "{table}"'
        params: list = []

        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return self._adapt_placeholders(sql, connection), params

    # ── WHERE compilation ─────────────────────────────────────────────────────

    def _compile_nodes(self, nodes: list, connection) -> tuple[str, list]:
        if not nodes:
            return "", []
        parts = []
        params: list = []
        for node in nodes:
            sql, p = self._compile_node(node, connection)
            if sql:
                parts.append(sql)
                params.extend(p)
        return " AND ".join(parts), params

    def _compile_node(self, node, connection) -> tuple[str, list]:
        if isinstance(node, Q):
            return self._compile_q(node, connection)
        if isinstance(node, (Exists, Subquery)):
            # Bare ``filter(Exists(...))`` / ``filter(Subquery(...))`` —
            # treat as a standalone WHERE predicate. Subquery is unusual
            # here (boolean coercion of a scalar) but matches Django's
            # behaviour for completeness.
            sub_sql, sub_params = node.as_sql(
                table_alias=self.model._meta.db_table, model=self.model
            )
            return sub_sql, sub_params
        if isinstance(node, tuple):
            field_path, lookup, value = node
            return self._compile_leaf(field_path, lookup, value, connection)
        return "", []

    def _compile_q(self, q: Q, connection) -> tuple[str, list]:
        parts = []
        params: list = []
        for child in q.children:
            if isinstance(child, Q):
                sql, p = self._compile_q(child, connection)
            elif isinstance(child, (Exists, Subquery)):
                sql, p = child.as_sql(
                    table_alias=self.model._meta.db_table, model=self.model
                )
            elif isinstance(child, tuple) and len(child) == 2:
                key, value = child
                field_parts, lookup = parse_lookup_key(key)
                sql, p = self._compile_leaf(field_parts, lookup, value, connection)
            else:
                continue
            if sql:
                parts.append(f"({sql})")
                params.extend(p)

        if not parts:
            return "", []

        joined = f" {q.connector} ".join(parts)
        if len(parts) > 1:
            joined = f"({joined})"
        if q.negated:
            joined = f"NOT {joined}"
        # Return raw %s — outer as_* method adapts placeholders once for the full SQL
        return joined, params

    def _compile_leaf(self, field_parts: list[str], lookup: str, value, connection) -> tuple[str, list]:
        col = self._resolve_column(field_parts)

        # QuerySet as value → compile as an IN subquery
        if lookup == "in" and hasattr(value, "_query") and hasattr(value, "model"):
            sub_sql, sub_params = self._compile_subquery(value, connection)
            return f"{col} IN ({sub_sql})", sub_params

        # Subquery() / Exists() as a scalar comparison RHS — compile in
        # place against this query's outer-context state so any
        # OuterRef resolves correctly.
        if isinstance(value, (Subquery, Exists)):
            sub_sql, sub_params = value.as_sql(
                table_alias=self.model._meta.db_table,
                model=self.model,
            )
            if isinstance(value, Exists):
                # ``filter(Exists(...))`` → emit just the EXISTS clause;
                # the column we built above is ignored.
                return sub_sql, sub_params
            return f"{col} = {sub_sql}", sub_params

        # OuterRef: compile to a column reference on the outer query
        # rather than a bound parameter.
        if isinstance(value, OuterRef):
            outer_col = self._resolve_outer_ref(value)
            return f"{col} = {outer_col}", []

        # Extract PK from model instances (e.g. filter(author=instance))
        if hasattr(value, "_meta") and hasattr(value, "pk"):
            value = value.pk
        elif isinstance(value, (list, tuple)):
            value = [
                v.pk if hasattr(v, "_meta") and hasattr(v, "pk") else v
                for v in value
            ]

        # Route through the field's own binding adapter so custom types
        # (``EnumField`` → enum value, ``DurationField`` on PG, etc.)
        # reach the cursor in their wire form. Skipped for lookups that
        # imply scalar / structural values the field shouldn't transform
        # (range tuples, NULL checks, regex patterns).
        leaf_field = self._resolve_field(field_parts)
        if leaf_field is not None and lookup not in (
            "isnull", "in", "range", "regex", "iregex"
        ):
            try:
                value = leaf_field.get_db_prep_value(value)
            except Exception:
                # Custom adapters that don't accept lookup-shaped values
                # (e.g. partial datetimes for ``__date``) shouldn't break
                # the query — leave the raw value for the lookup builder.
                pass
        elif leaf_field is not None and lookup == "in" and isinstance(value, (list, tuple)):
            try:
                value = [leaf_field.get_db_prep_value(v) for v in value]
            except Exception:
                pass

        vendor = getattr(connection, "vendor", "sqlite")
        sql, params = build_lookup_sql(col, lookup, value, vendor=vendor)
        # Return raw %s — outer as_* method adapts placeholders once for the full SQL
        return sql, params

    def _resolve_outer_ref(self, ref: OuterRef) -> str:
        """Translate :class:`OuterRef` to ``"<outer_alias>"."<col>"``.

        Raises ``ValueError`` if used outside a Subquery/Exists context
        (i.e. ``_outer_alias`` was never stamped). ``"pk"`` is resolved
        to the outer model's primary-key column when ``_outer_model`` is
        known; otherwise ``"pk"`` is left as the literal column name and
        the database will report it.
        """
        if self._outer_alias is None:
            raise ValueError(
                "OuterRef can only be used inside Subquery() or Exists(); "
                "the outer queryset's alias was not propagated."
            )
        name = ref.name
        if name == "pk" and self._outer_model is not None:
            pk = self._outer_model._meta.pk
            if pk is not None:
                name = pk.column
        else:
            # If the outer model is known and the name matches a declared
            # field (e.g. attname ``author_id`` or relation name
            # ``author``), prefer the underlying column.
            if self._outer_model is not None:
                from .exceptions import FieldDoesNotExist

                try:
                    f = self._outer_model._meta.get_field(name)
                    if f.column:
                        name = f.column
                except FieldDoesNotExist:
                    pass
        _validate_identifier(name)
        return f'"{self._outer_alias}"."{name}"'

    def _compile_subquery(self, qs, connection) -> tuple[str, list]:
        """Compile a QuerySet as a SELECT subquery returning the PK column."""
        inner = qs._query.clone()
        pk_col = qs.model._meta.pk.column
        table = qs.model._meta.db_table

        where_sql, where_params = inner._compile_nodes(inner.where_nodes, connection)
        sql = f'SELECT "{pk_col}" FROM "{table}"'
        if where_sql:
            sql += f" WHERE {where_sql}"
        if inner.limit_val is not None:
            sql += f" LIMIT {int(inner.limit_val)}"
        if inner.offset_val is not None:
            sql += f" OFFSET {int(inner.offset_val)}"
        return sql, where_params

    def as_subquery_sql(
        self,
        outer_alias: str | None = None,
        outer_model: Any = None,
    ) -> tuple[str, list]:
        """Compile this query as a correlated subquery body.

        Used by :class:`~dorm.expressions.Subquery` and
        :class:`~dorm.expressions.Exists`. The compiled SQL keeps ``%s``
        placeholders (the outer query's :meth:`_adapt_placeholders` runs
        once on the full statement, which avoids double-rewriting on
        PostgreSQL where ``$N`` would otherwise be re-numbered twice).

        ``outer_alias`` and ``outer_model`` propagate the enclosing
        query's table alias and model so :class:`OuterRef` references
        resolve to ``"<outer_alias>"."<col>"`` instead of bound
        parameters.
        """
        inner = self.clone()
        inner._outer_alias = outer_alias
        inner._outer_model = outer_model

        # Use a sentinel "connection" so vendor-specific behaviour
        # (currently just ``__in`` → ANY) defaults to the sqlite shape.
        # The outer ``_adapt_placeholders`` rewrites the full SQL once;
        # double-rewriting here would break ``$N`` numbering on PG.
        class _DummyConn:
            vendor = "sqlite"

        conn = _DummyConn()

        table = inner.get_table()
        alias = table

        annotation_params: list = []
        extra_select = ""
        if inner.annotations:
            parts = []
            for alias_name, agg in inner.annotations.items():
                _validate_identifier(alias_name, "annotation alias")
                agg_sql, agg_p = agg.as_sql(alias, model=inner.model)
                if alias_name in inner.alias_only_names:
                    continue
                parts.append(f'{agg_sql} AS "{alias_name}"')
                annotation_params.extend(agg_p)
            if parts:
                extra_select = ", " + ", ".join(parts)

        if inner.selected_fields is not None:
            cols = inner.get_columns(alias)
            select_list = cols + extra_select
        else:
            cols = inner.get_columns(alias)
            select_list = cols + extra_select

        params: list = []
        where_sql, where_params = inner._compile_nodes(inner.where_nodes, conn)
        params.extend(annotation_params)
        params.extend(where_params)

        sql = f'SELECT {select_list} FROM "{table}"'
        for join_type, jt, jalias, on_cond in inner.joins:
            sql += f' {join_type} JOIN "{jt}" AS "{jalias}" ON {on_cond}'
        if where_sql:
            sql += f" WHERE {where_sql}"

        if inner.group_by_fields:
            for f in inner.group_by_fields:
                _validate_identifier(f)
            sql += " GROUP BY " + ", ".join(f'"{f}"' for f in inner.group_by_fields)

        if inner.having_nodes:
            having_sql, having_params = inner._compile_nodes(inner.having_nodes, conn)
            if having_sql:
                sql += f" HAVING {having_sql}"
                params.extend(having_params)

        if inner.order_by_fields:
            order_parts = []
            for f in inner.order_by_fields:
                desc = f.startswith("-")
                fname = f[1:] if desc else f
                _validate_identifier(fname)
                order_parts.append(f'"{fname}" {"DESC" if desc else "ASC"}')
            sql += " ORDER BY " + ", ".join(order_parts)

        if inner.limit_val is not None:
            sql += f" LIMIT {int(inner.limit_val)}"
        if inner.offset_val is not None:
            sql += f" OFFSET {int(inner.offset_val)}"
        return sql, params

    def _resolve_field(self, field_parts: list[str]) -> Any | None:
        """Walk *field_parts* through FK relations and return the leaf
        :class:`Field`, or ``None`` if no field of that name exists on
        the resolved model. Used by :meth:`_compile_leaf` to route the
        bound value through ``field.get_db_prep_value`` so custom field
        types (``EnumField``, ``DurationField``, ``RangeField`` …) are
        bound in their wire form rather than as opaque Python objects.

        This walks the relation chain *without* mutating ``self.joins``;
        :meth:`_resolve_column` is still the source of truth for the SQL
        column reference.
        """
        from .exceptions import FieldDoesNotExist

        model = self.model
        parts = list(field_parts)
        if parts and parts[0] == "pk" and model._meta.pk:
            parts[0] = model._meta.pk.column
        while len(parts) > 1:
            fname = parts.pop(0)
            try:
                field = model._meta.get_field(fname)
            except FieldDoesNotExist:
                return None
            if hasattr(field, "remote_field_to"):
                model = field._resolve_related_model()
            else:
                return None
        try:
            return model._meta.get_field(parts[0])
        except FieldDoesNotExist:
            return None

    def _resolve_column(self, field_parts: list[str]) -> str:
        from .exceptions import FieldDoesNotExist

        model = self.model
        current_alias = model._meta.db_table
        parts = list(field_parts)
        # Resolve "pk" alias to the actual primary key column
        if parts[0] == "pk" and model._meta.pk:
            parts[0] = model._meta.pk.column
        while len(parts) > 1:
            fname = parts.pop(0)
            # Catch only "field not declared on this model" — anything else
            # (broken descriptors, runtime AttributeError in custom fields,
            # etc.) should propagate so users see the real bug instead of
            # silently getting a stale column reference.
            try:
                field = model._meta.get_field(fname)
            except FieldDoesNotExist:
                break
            if hasattr(field, "remote_field_to"):
                rel_model = field._resolve_related_model()
                table = rel_model._meta.db_table
                join_alias = f"{current_alias}_{fname}"
                on_cond = (
                    f'"{join_alias}"."{rel_model._meta.pk.column}" = '
                    f'"{current_alias}"."{field.column}"'
                )
                if not any(j[2] == join_alias for j in self.joins):
                    self.joins.append(("INNER", table, join_alias, on_cond))
                model = rel_model
                current_alias = join_alias
            else:
                break

        fname = parts[0]
        try:
            field = model._meta.get_field(fname)
            col_name = field.column
        except FieldDoesNotExist:
            # Falling back to a literal column name — re-validate so a
            # user-supplied raw identifier can never reach the SQL.
            _validate_identifier(fname)
            col_name = fname

        if self.joins:
            return f'"{current_alias}"."{col_name}"'
        return f'"{col_name}"'

    # ── Placeholder adaptation ────────────────────────────────────────────────

    def _adapt_placeholders(self, sql: str, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            # Replace %s with $1, $2, ...
            idx = [0]

            def repl(m):
                idx[0] += 1
                return f"${idx[0]}"

            import re
            return re.sub(r"%s", repl, sql)
        return sql
