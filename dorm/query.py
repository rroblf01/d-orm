from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .expressions import CombinedExpression, F, Q, Value
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
        self.distinct_flag: bool = False
        self.for_update_flag: bool = False
        self.joins: list[tuple] = []  # (join_type, table, alias, on_condition)
        self.group_by_fields: list[str] = []
        self.having_nodes: list = []
        self.select_related_fields: list[str] = []
        self.prefetch_related_fields: list[str] = []

    def clone(self) -> "SQLQuery":
        q = SQLQuery(self.model)
        q.where_nodes = list(self.where_nodes)
        q.order_by_fields = list(self.order_by_fields)
        q.limit_val = self.limit_val
        q.offset_val = self.offset_val
        q.selected_fields = list(self.selected_fields) if self.selected_fields is not None else None
        q.deferred_loading = self.deferred_loading
        q.annotations = dict(self.annotations)
        q.distinct_flag = self.distinct_flag
        q.for_update_flag = self.for_update_flag
        q.joins = list(self.joins)
        q.group_by_fields = list(self.group_by_fields)
        q.having_nodes = list(self.having_nodes)
        q.select_related_fields = list(self.select_related_fields)
        q.prefetch_related_fields = list(self.prefetch_related_fields)
        return q

    # ── SQL builders ──────────────────────────────────────────────────────────

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

    def as_select(self, connection) -> tuple[str, list]:
        table = self.get_table()
        alias = table
        distinct = "DISTINCT " if self.distinct_flag else ""

        # Annotations — collect SQL and params separately (appear before WHERE in SQL)
        annotation_params: list = []
        extra_select = ""
        if self.annotations:
            parts = []
            for alias_name, agg in self.annotations.items():
                _validate_identifier(alias_name, "annotation alias")
                agg_sql, agg_p = agg.as_sql(alias)
                parts.append(f'{agg_sql} AS "{alias_name}"')
                annotation_params.extend(agg_p)
            extra_select = ", " + ", ".join(parts)

        params: list = []

        # Compile WHERE first so _resolve_column can populate self.joins via FK traversal
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        params.extend(annotation_params)  # SELECT annotations come first in SQL
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
        select = f'SELECT {distinct}{cols}{extra_select}{sr_extra_cols} FROM "{table}"'

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
        if self.limit_val is not None:
            select += f" LIMIT {int(self.limit_val)}"
        if self.offset_val is not None:
            select += f" OFFSET {int(self.offset_val)}"

        if self.for_update_flag:
            select += " FOR UPDATE"

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

    def as_bulk_insert(self, fields: list, rows_values: list[list], connection) -> tuple[str, list]:
        table = self.get_table()
        cols = ", ".join(f'"{f.column}"' for f in fields)
        row_ph = f"({', '.join(['%s'] * len(fields))})"
        all_ph = ", ".join([row_ph] * len(rows_values))
        sql = f'INSERT INTO "{table}" ({cols}) VALUES {all_ph}'
        params = [v for row in rows_values for v in row]
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
        return self._adapt_placeholders(joined, connection), params

    def _compile_leaf(self, field_parts: list[str], lookup: str, value, connection) -> tuple[str, list]:
        col = self._resolve_column(field_parts)
        # Extract PK from model instances (e.g. filter(author=instance))
        if hasattr(value, "_meta") and hasattr(value, "pk"):
            value = value.pk
        elif isinstance(value, (list, tuple)):
            value = [
                v.pk if hasattr(v, "_meta") and hasattr(v, "pk") else v
                for v in value
            ]
        sql, params = build_lookup_sql(col, lookup, value)
        return self._adapt_placeholders(sql, connection), params

    def _resolve_column(self, field_parts: list[str]) -> str:
        model = self.model
        current_alias = model._meta.db_table
        parts = list(field_parts)
        # Resolve "pk" alias to the actual primary key column
        if parts[0] == "pk" and model._meta.pk:
            parts[0] = model._meta.pk.column
        while len(parts) > 1:
            fname = parts.pop(0)
            try:
                field = model._meta.get_field(fname)
            except Exception:
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
        except Exception:
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
