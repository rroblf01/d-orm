from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .expressions import CombinedExpression, F, Q, Value
from .lookups import build_lookup_sql, parse_lookup_key

if TYPE_CHECKING:
    pass


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
        self.annotations: dict[str, Any] = {}
        self.distinct_flag: bool = False
        self.for_update_flag: bool = False
        self.joins: list[tuple] = []  # (join_type, table, alias, on_condition)
        self.group_by_fields: list[str] = []
        self.having_nodes: list = []

    def clone(self) -> "SQLQuery":
        q = SQLQuery(self.model)
        q.where_nodes = list(self.where_nodes)
        q.order_by_fields = list(self.order_by_fields)
        q.limit_val = self.limit_val
        q.offset_val = self.offset_val
        q.selected_fields = list(self.selected_fields) if self.selected_fields is not None else None
        q.annotations = dict(self.annotations)
        q.distinct_flag = self.distinct_flag
        q.for_update_flag = self.for_update_flag
        q.joins = list(self.joins)
        q.group_by_fields = list(self.group_by_fields)
        q.having_nodes = list(self.having_nodes)
        return q

    # ── SQL builders ──────────────────────────────────────────────────────────

    def get_table(self) -> str:
        return self.model._meta.db_table

    def get_columns(self, table_alias: str | None = None) -> str:
        if self.selected_fields is not None:
            parts = []
            for f in self.selected_fields:
                parts.append(f'"{f}"')
            return ", ".join(parts)
        ta = f'"{table_alias}".' if table_alias else ""
        concrete = [f for f in self.model._meta.fields if f.column]
        return ", ".join(f'{ta}"{f.column}"' for f in concrete)

    def as_select(self, connection) -> tuple[str, list]:
        table = self.get_table()
        alias = table
        distinct = "DISTINCT " if self.distinct_flag else ""

        # Annotations
        extra_select = ""
        if self.annotations:
            parts = []
            for alias_name, agg in self.annotations.items():
                agg_sql, _ = agg.as_sql(alias)
                parts.append(f'{agg_sql} AS "{alias_name}"')
            extra_select = ", " + ", ".join(parts)

        cols = self.get_columns(alias)
        select = f'SELECT {distinct}{cols}{extra_select} FROM "{table}"'

        params: list = []

        # JOINs
        for join_type, join_table, join_alias, on_cond in self.joins:
            select += f' {join_type} JOIN "{join_table}" AS "{join_alias}" ON {on_cond}'

        # WHERE
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        if where_sql:
            select += f" WHERE {where_sql}"
            params.extend(where_params)

        # GROUP BY
        if self.group_by_fields:
            gb = ", ".join(f'"{f}"' for f in self.group_by_fields)
            select += f" GROUP BY {gb}"

        # HAVING
        if self.having_nodes:
            having_sql, having_params = self._compile_nodes(self.having_nodes, connection)
            if having_sql:
                select += f" HAVING {having_sql}"
                params.extend(having_params)

        # ORDER BY
        if self.order_by_fields:
            order_parts = []
            for f in self.order_by_fields:
                if f.startswith("-"):
                    order_parts.append(f'"{f[1:]}" DESC')
                else:
                    order_parts.append(f'"{f}" ASC')
            select += " ORDER BY " + ", ".join(order_parts)

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
        sql = f'SELECT COUNT(*) FROM "{table}"'
        params: list = []

        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return self._adapt_placeholders(sql, connection), params

    def as_insert(self, fields: list, values: list, connection) -> tuple[str, list]:
        table = self.get_table()
        cols = ", ".join(f'"{f.column}"' for f in fields)
        placeholders = ", ".join(["%s"] * len(fields))
        sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'
        return self._adapt_placeholders(sql, connection), values

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
        # Resolve column reference, handling FK traversal
        col = self._resolve_column(field_parts)
        sql, params = build_lookup_sql(col, lookup, value)
        return self._adapt_placeholders(sql, connection), params

    def _resolve_column(self, field_parts: list[str]) -> str:
        model = self.model
        parts = list(field_parts)
        while len(parts) > 1:
            fname = parts.pop(0)
            try:
                field = model._meta.get_field(fname)
            except Exception:
                break
            if hasattr(field, "remote_field_to"):
                rel_model = field._resolve_related_model()
                table = rel_model._meta.db_table
                local_table = model._meta.db_table
                join_alias = f"{local_table}_{fname}"
                on_cond = (
                    f'"{join_alias}"."{rel_model._meta.pk.column}" = '
                    f'"{local_table}"."{field.column}"'
                )
                if not any(j[2] == join_alias for j in self.joins):
                    self.joins.append(("INNER", table, join_alias, on_cond))
                model = rel_model
            else:
                break

        fname = parts[0]
        try:
            field = model._meta.get_field(fname)
            col_name = field.column
        except Exception:
            col_name = fname

        # Prefix with table if joins present
        if self.joins:
            return f'"{model._meta.db_table}"."{col_name}"'
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
