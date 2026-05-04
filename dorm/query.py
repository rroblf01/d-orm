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


def _json_extract_sql(
    column_expr: str, path: list[str], vendor: str | None
) -> str:
    """Render a JSON path-extraction expression for the given vendor.

    *column_expr* is the already-quoted column reference (``"t"."col"``).
    *path* is a list of validated identifier segments; their order
    is preserved.

    PostgreSQL emits ``col #>> '{a,b,c}'`` (text result) — comparable
    against string literals, which is the common case. Use the JSON
    cast helpers in user code for typed comparisons. SQLite emits
    ``json_extract(col, '$.a.b.c')``.

    Both forms quote the path segments inside string literals; the
    caller has already validated each segment as a SQL-safe
    identifier so embedded ``"``/``'`` are impossible.
    """
    if not path:
        return column_expr
    if vendor == "postgresql":
        # ``#>>`` returns text; nicer for equality comparisons. Use
        # ``#>`` if a future caller needs the typed JSON path result.
        components = ",".join(path)
        return f"{column_expr} #>> '{{{components}}}'"
    # Default: SQLite ``json_extract`` (also accepted by libsql and
    # any other backend that ships a JSON1 module).
    dotted = ".".join(path)
    return f"json_extract({column_expr}, '$.{dotted}')"


def _compile_expr(
    val, table_alias: str | None = None, model: Any = None
) -> tuple[str, list]:
    """Convert a Python value or expression (F, CombinedExpression,
    Value, Subquery, Exists, anything with ``as_sql``) to ``(sql, params)``.

    The ``hasattr(val, "as_sql")`` branch is the catch-all that
    routes :class:`Subquery` / :class:`Exists` / function expressions
    through their own SQL emitter. Without it the value would be
    bound as a parameter and the driver would crash with ``cannot
    adapt type 'Subquery'``.

    ``table_alias`` / ``model`` propagate the enclosing query's
    context so a wrapped :class:`Subquery` carrying an
    :class:`OuterRef` resolves the outer reference correctly.
    """
    if isinstance(val, F):
        return f'"{val.name}"', []
    if isinstance(val, Value):
        return "%s", [val.value]
    if isinstance(val, CombinedExpression):
        lhs_sql, lhs_p = _compile_expr(val.lhs, table_alias, model)
        rhs_sql, rhs_p = _compile_expr(val.rhs, table_alias, model)
        return f"({lhs_sql} {val.operator} {rhs_sql})", lhs_p + rhs_p
    if isinstance(val, (Subquery, Exists)):
        # Pass the outer alias down so ``OuterRef`` inside the
        # subquery resolves; ``Subquery.as_sql`` already wraps the
        # body in parentheses, ``Exists.as_sql`` already prepends
        # the ``EXISTS`` / ``NOT EXISTS`` prefix.
        sql, params = val.as_sql(table_alias=table_alias, model=model)
        return sql, list(params)
    if hasattr(val, "as_sql") and callable(val.as_sql):
        try:
            sql, params = val.as_sql(table_alias=table_alias, model=model)
            return sql, list(params or [])
        except TypeError:
            try:
                sql, params = val.as_sql()
                return sql, list(params or [])
            except TypeError:
                pass
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
        # Populated by ``QuerySet.distinct(*fields)`` on PostgreSQL —
        # emits ``SELECT DISTINCT ON (col1, col2) …``. SQLite has no
        # equivalent and raises in :meth:`as_select` if non-empty.
        self.distinct_on_fields: list[str] = []
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
        # When ``only()`` / ``defer()`` is called with a dotted path
        # (``only("name", "publisher__name")``) the trailing parts apply
        # to a select_related-loaded relation rather than the parent
        # model. Layout: ``{relation_path: {col_name, …}}``. The PK of
        # the related model is always implicitly included so the
        # hydrated instance has a valid identity even when the user
        # only listed non-PK columns. Empty / missing entry =
        # "load every column on this relation" (the legacy default).
        self.selected_related_fields: dict[str, set[str]] = {}
        # Strings are bare relation names (``"books"``); ``Prefetch``
        # instances carry an alternate queryset and/or ``to_attr``.
        # Both shapes are accepted by ``QuerySet.prefetch_related``.
        self.prefetch_related_fields: list[Any] = []
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
        # Stamped by :meth:`as_subquery_sql` when this query is
        # compiled as a self-correlated subquery (outer and inner
        # share the same ``db_table``). ``_resolve_column`` reads
        # this so inner column references qualify with the
        # uniquified alias instead of colliding with the outer
        # query's use of the same table name. ``None`` means the
        # plain ``db_table`` is used as the FROM alias.
        self._self_alias: str | None = None

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
        q.distinct_on_fields = list(self.distinct_on_fields)
        q.for_update_flag = self.for_update_flag
        q.for_update_skip_locked = self.for_update_skip_locked
        q.for_update_no_wait = self.for_update_no_wait
        q.for_update_of = self.for_update_of
        q.joins = list(self.joins)
        q.group_by_fields = list(self.group_by_fields)
        q.having_nodes = list(self.having_nodes)
        q.select_related_fields = list(self.select_related_fields)
        q.selected_related_fields = {
            k: set(v) for k, v in self.selected_related_fields.items()
        }
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
            parts: list[str] = []
            for f in self.selected_fields:
                if "__" in f:
                    # FK-traversal in ``.values("publisher__name")``
                    # — register the relation hop join via
                    # ``_resolve_column`` and emit the qualified
                    # column reference. Alias the projection back
                    # to the user-visible dotted name so
                    # ``row["publisher__name"]`` Just Works in
                    # ``values()`` / ``values_list()`` consumers.
                    resolved = self._resolve_column(f.split("__"))
                    parts.append(f'{resolved} AS "{f}"')
                else:
                    _validate_identifier(f)
                    parts.append(f'{ta}"{f}"')
            return ", ".join(parts)
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
        any_recursive = False
        # Lazy import to avoid the queryset → query circular at import time.
        from .queryset import CTE as _CTE

        for name, body in self.ctes:
            _validate_identifier(name, "CTE name")
            if isinstance(body, _CTE):
                # Raw-SQL CTE — params survive the outer placeholder
                # rewrite because they're appended in the same order
                # as the rendered ``%s`` markers.
                parts.append(f'"{name}" AS ({body.sql.strip()})')
                params.extend(body.params)
                if body.recursive:
                    any_recursive = True
            else:
                sub_sql, sub_params = body._query.as_subquery_sql(
                    outer_alias=self.model._meta.db_table,
                    outer_model=self.model,
                )
                parts.append(f'"{name}" AS ({sub_sql})')
                params.extend(sub_params)
        prefix = "WITH RECURSIVE " if any_recursive else "WITH "
        return prefix + ", ".join(parts) + " ", params

    def as_select(self, connection) -> tuple[str, list]:
        table = self.get_table()
        alias = table
        # ``distinct_on_fields`` is set by ``QuerySet.distinct(*fields)``
        # for the PG ``SELECT DISTINCT ON (cols)`` form. Plain
        # ``distinct()`` (no args) keeps ``distinct_flag=True`` and an
        # empty ``distinct_on_fields`` list — emits the classic
        # ``SELECT DISTINCT``.
        if self.distinct_on_fields:
            vendor = getattr(connection, "vendor", "sqlite")
            if vendor != "postgresql":
                raise NotImplementedError(
                    "distinct(*fields) → SELECT DISTINCT ON is "
                    "PostgreSQL-only. SQLite has no equivalent — use "
                    "a window function or a GROUP BY subquery instead."
                )
            # Translate Python field names to DB columns the same way
            # ``order_by`` does — ``distinct("author")`` on a FK named
            # ``author`` must emit ``"author_id"``, not the missing
            # ``"author"`` column.
            resolved = []
            for field_name in self.distinct_on_fields:
                leaf = self._resolve_field([field_name])
                column = (
                    leaf.column if leaf is not None and leaf.column else field_name
                )
                resolved.append(f'"{table}"."{column}"')
            distinct = f"DISTINCT ON ({', '.join(resolved)}) "
        else:
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
                    alias,
                    model=self.model,
                    connection=connection,
                    query=self,
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
        # Previously this short-circuited when ``selected_fields`` was
        # set (i.e. ``only()`` / ``defer()`` restricted the parent
        # projection). That meant the user couldn't compose
        # ``select_related("publisher").only("name", "publisher__name")``
        # — SR was silently dropped. The aliased SR columns
        # (``"_sr_<path>_<col>"``) don't collide with the parent
        # projection by construction, so it's safe to emit both.
        if self.select_related_fields:
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
                            # ``only()`` / ``defer()`` may restrict which
                            # columns of this relation we hydrate. The
                            # restriction set always includes the PK
                            # (added by ``QuerySet.only``) so the
                            # hydrated instance keeps its identity.
                            allowed = self.selected_related_fields.get(step_path)
                            for rf in rel_model._meta.fields:
                                if not rf.column:
                                    continue
                                if allowed is not None and rf.column not in allowed:
                                    continue
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
                    # Translate Python field names to their DB column.
                    # ``order_by("publisher")`` on a FK named ``publisher``
                    # has to emit ``"publisher_id"`` — the column the FK
                    # actually stores. Without this lookup the SQL would
                    # reference a non-existent ``"publisher"`` column.
                    leaf = self._resolve_field([fname])
                    column = leaf.column if leaf is not None and leaf.column else fname
                    # Qualify the column whenever JOINs are in flight —
                    # both WHERE-derived (``self.joins``) and
                    # select_related JOINs make a bare ``"id"``
                    # ambiguous because the related table also has one.
                    needs_qualification = bool(self.joins) or bool(self.select_related_fields)
                    col = (
                        f'"{self.model._meta.db_table}"."{column}"'
                        if needs_qualification
                        else f'"{column}"'
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

        # GROUP BY — explicit field list wins; otherwise auto-emit
        # the outer model's columns when an annotation aggregate
        # forced a JOIN. PG rejects mixed aggregate + scalar SELECT
        # without GROUP BY ("column X must appear in the GROUP BY
        # clause or be used in an aggregate function"); SQLite is
        # lenient but emits implementation-defined values for the
        # bare scalar columns. The explicit GROUP BY makes the
        # ``Author.objects.annotate(n=Count("book_set"))`` shape
        # work uniformly across backends.
        if self.group_by_fields:
            for f in self.group_by_fields:
                _validate_identifier(f)
            gb = ", ".join(f'"{f}"' for f in self.group_by_fields)
            select += f" GROUP BY {gb}"
        elif self.annotations and self.joins and self._annotations_have_aggregate():
            # Group by every selected outer column so the join
            # multiplication collapses to one row per outer entity.
            outer_table = self.model._meta.db_table
            cols = [
                f'"{outer_table}"."{f.column}"'
                for f in self.model._meta.fields
                if f.column and not getattr(f, "many_to_many", False)
            ]
            if cols:
                select += " GROUP BY " + ", ".join(cols)

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
        params: list = []

        # Compile WHERE first — it populates ``self.joins`` via
        # ``_resolve_column`` for FK-traversal lookups
        # (``filter(author__name="x")``). If we emit the SELECT
        # before compiling we miss those JOINs and the WHERE
        # references a non-existent alias.
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)

        sql = f'SELECT COUNT(*) AS "count" FROM "{table}"'
        for join_type, join_table, join_alias, on_cond in self.joins:
            sql += f' {join_type} JOIN "{join_table}" AS "{join_alias}" ON {on_cond}'
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return self._adapt_placeholders(sql, connection), params

    def as_exists(self, connection) -> tuple[str, list]:
        table = self.get_table()
        params: list = []
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)
        sql = f'SELECT 1 FROM "{table}"'
        for join_type, join_table, join_alias, on_cond in self.joins:
            sql += f' {join_type} JOIN "{join_table}" AS "{join_alias}" ON {on_cond}'
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
        returning_cols: list[str] | None = None,
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
            # Resolve the user-supplied attnames to actual columns
            # (so ``unique_fields=["external_id"]`` lands on its
            # ``db_column="ext_uid"`` declaration). Previously we
            # interpolated the attname verbatim, producing
            # ``ON CONFLICT ("external_id")`` against a column
            # that doesn't exist — PG raises *no unique constraint
            # matching the columns*.
            unique_cols: list[str] = []
            unique_meta = self.model._meta
            for name in unique_fields:
                _validate_identifier(name)
                try:
                    f = unique_meta.get_field(name)
                    unique_cols.append(f.column or name)
                except Exception:
                    unique_cols.append(name)
            target_cols = ", ".join(f'"{c}"' for c in unique_cols)
            # ``update_cols`` (below) excludes columns that are part
            # of the conflict target — keep that comparison aligned
            # with the resolved column names rather than attnames.
            unique_fields = unique_cols

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

        if returning_cols:
            # PG and SQLite (≥ 3.35) accept a multi-column RETURNING tail on
            # INSERT. Order matters: must come *after* any ON CONFLICT clause.
            for c in returning_cols:
                _validate_identifier(c)
            ret_cols = ", ".join(f'"{c}"' for c in returning_cols)
            sql += f" RETURNING {ret_cols}"

        return self._adapt_placeholders(sql, connection), params

    def as_update(self, update_kwargs: dict, connection) -> tuple[str, list]:
        table = self.get_table()
        set_parts = []
        set_params: list = []
        for col, val in update_kwargs.items():
            expr_sql, expr_params = _compile_expr(val, table, self.model)
            set_parts.append(f'"{col}" = {expr_sql}')
            set_params.extend(expr_params)

        # Compile WHERE first so ``_resolve_column`` populates
        # ``self.joins``. If joins were registered (FK-traversal
        # lookups like ``filter(author__name=…)``) we cannot emit
        # them in the UPDATE itself portably across SQLite/PG;
        # fall back to ``WHERE pk IN (SELECT pk FROM table JOIN
        # … WHERE …)`` which both backends accept.
        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)

        if self.joins:
            pk_col = self.model._meta.pk.column
            sub = f'SELECT "{table}"."{pk_col}" FROM "{table}"'
            for jt, jtbl, jalias, jon in self.joins:
                sub += f' {jt} JOIN "{jtbl}" AS "{jalias}" ON {jon}'
            if where_sql:
                sub += f" WHERE {where_sql}"
            sql = (
                f'UPDATE "{table}" SET {", ".join(set_parts)} '
                f'WHERE "{pk_col}" IN ({sub})'
            )
            params = set_params + where_params
        else:
            sql = f'UPDATE "{table}" SET {", ".join(set_parts)}'
            if where_sql:
                sql += f" WHERE {where_sql}"
            params = set_params + where_params

        return self._adapt_placeholders(sql, connection), params

    def as_delete(self, connection) -> tuple[str, list]:
        table = self.get_table()
        params: list = []

        where_sql, where_params = self._compile_nodes(self.where_nodes, connection)

        if self.joins:
            # Same rationale as ``as_update``: portable JOIN-aware
            # DELETE is ``DELETE FROM t WHERE pk IN (SELECT pk
            # FROM t JOIN … WHERE …)``.
            pk_col = self.model._meta.pk.column
            sub = f'SELECT "{table}"."{pk_col}" FROM "{table}"'
            for jt, jtbl, jalias, jon in self.joins:
                sub += f' {jt} JOIN "{jtbl}" AS "{jalias}" ON {jon}'
            if where_sql:
                sub += f" WHERE {where_sql}"
            sql = f'DELETE FROM "{table}" WHERE "{pk_col}" IN ({sub})'
            params.extend(where_params)
        else:
            sql = f'DELETE FROM "{table}"'
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
        # An empty ``Q()`` represents the unconditional tautology
        # ``TRUE`` — that's how Django interprets it, and it's what
        # makes ``Q() | Q(x)`` evaluate to "match everything" rather
        # than "match Q(x)" only. The previous compiler returned ``""``
        # for an empty Q, which the OR-join then dropped silently —
        # callers got the wrong row set with no error to point at it.
        # ``(1=0)`` is the negated form (``~Q()``); both literals work
        # on every supported backend.
        if not q.children:
            sql = "(1=0)" if q.negated else "(1=1)"
            return sql, []

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
        col = self._resolve_column(field_parts, connection)

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

        # ``F()`` on the RHS compiles to a column reference, not a
        # bound parameter — the canonical "compare two columns of the
        # same row" pattern (``filter(price__gte=F("cost"))``).
        # Without this branch the ``F`` object falls through to the
        # bound-parameter path and the cursor errors out with "type
        # 'F' is not supported".
        if isinstance(value, F):
            rhs_col = self._resolve_column([value.name])
            op_map = {
                "exact": "=",
                "iexact": "=",   # collation-insensitive comparison handled by column
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
            }
            op = op_map.get(lookup)
            if op is None:
                raise NotImplementedError(
                    f"F() reference not supported with lookup {lookup!r}; "
                    "use one of: exact, gt, gte, lt, lte. For other "
                    "comparisons, evaluate the right-hand side in Python "
                    "first or wrap in an annotate() expression."
                )
            return f"{col} {op} {rhs_col}", []

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

        # ``filter(field=None)`` is sugar for ``filter(field__isnull=True)``
        # in Django. Without this rewrite the lookup builder would emit
        # ``col = NULL`` which is always FALSE in standard SQL — so the
        # query silently returned 0 rows when the user expected the
        # NULL-valued rows. Equally for ``exclude(field=None)`` (the
        # caller's negation flips the predicate to ``IS NOT NULL``).
        if value is None and lookup == "exact":
            return f"{col} IS NULL", []

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
        """Compile a QuerySet as a SELECT subquery for ``__in`` lookups.

        Three things have to be honoured for the subquery to mean
        what the caller wrote:

        - ``selected_fields`` (set by ``.values("col")``) — the
          subquery must project that column, not the model's PK.
        - ``self.joins`` — FK-traversal lookups
          (``Book.objects.filter(genre__name="x")``) register joins
          that the bare ``SELECT pk FROM table WHERE …`` form
          dropped, leading to *missing FROM-clause entry* on PG and
          *no such column* on SQLite.
        - ``order_by`` is intentionally NOT carried over: ordering
          inside an ``IN`` subquery is meaningless and PG raises if
          ``ORDER BY`` references a column not in the SELECT list.

        Annotations stay on the inner query so any FK joins they
        triggered get registered when WHERE compiles.
        """
        inner = qs._query.clone()
        table = qs.model._meta.db_table

        # Pick the projected column. ``.values("col")`` populated
        # ``selected_fields``; otherwise default to the PK.
        if inner.selected_fields:
            projected = inner.selected_fields[0]
            try:
                leaf = inner._resolve_field([projected])
                col_name = (
                    leaf.column if leaf is not None and leaf.column else projected
                )
            except Exception:
                col_name = projected
            project_sql = f'"{table}"."{col_name}"'
        else:
            pk_col = qs.model._meta.pk.column
            project_sql = f'"{table}"."{pk_col}"'

        # Compile WHERE first so ``_resolve_column`` populates
        # ``inner.joins`` for FK-traversal predicates.
        where_sql, where_params = inner._compile_nodes(
            inner.where_nodes, connection
        )

        sql = f'SELECT {project_sql} FROM "{table}"'
        for join_type, join_table, join_alias, on_cond in inner.joins:
            sql += f' {join_type} JOIN "{join_table}" AS "{join_alias}" ON {on_cond}'
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
        # Self-correlated subqueries (outer and inner reference the
        # same table) need a distinct inner alias so the inner WHERE
        # / SELECT references unambiguously identify the inner row,
        # not the outer one. Without this rename, an
        # ``OuterRef("pk")`` against a self-join produced ambiguous
        # column references on PG and silently wrong results on
        # SQLite (the inner side won the lookup).
        if outer_alias is not None and outer_alias == table:
            alias = f"{table}_sub"
            from_clause = f'"{table}" AS "{alias}"'
        else:
            alias = table
            from_clause = f'"{table}"'
        inner._self_alias = alias

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

        sql = f"SELECT {select_list} FROM {from_clause}"
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

    def _annotations_have_aggregate(self) -> bool:
        """``True`` when any annotation is an aggregate (Count / Sum /
        Avg / Max / Min / StringAgg / ArrayAgg) — used to decide
        whether to auto-emit ``GROUP BY`` on a query that joined for
        the aggregate's expression."""
        from .aggregates import Aggregate

        return any(isinstance(a, Aggregate) for a in self.annotations.values())

    def _resolve_column(self, field_parts: list[str], connection: Any = None) -> str:
        from .exceptions import FieldDoesNotExist

        model = self.model
        # Self-correlated subqueries set ``_self_alias`` to a
        # unique-per-subquery name so the inner column references
        # don't collide with the outer query's use of the same
        # table name. Fall back to the bare ``db_table`` when no
        # such alias was stamped.
        current_alias = getattr(self, "_self_alias", None) or model._meta.db_table
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
                is_forward_relation = hasattr(field, "remote_field_to")
            except FieldDoesNotExist:
                field = None
                is_forward_relation = False

            if is_forward_relation:
                assert field is not None
                rel_model = field._resolve_related_model()
                table = rel_model._meta.db_table
                join_alias = f"{current_alias}_{fname}"
                on_cond = (
                    f'"{join_alias}"."{rel_model._meta.pk.column}" = '
                    f'"{current_alias}"."{field.column}"'
                )
                # Nullable FKs MUST use LEFT OUTER JOIN, otherwise
                # ``filter(fk__isnull=True)`` silently excludes the
                # rows that have a NULL FK (the very rows the user
                # asked for), and any LEFT-side semantics are lost.
                # Non-nullable FKs use INNER which is equivalent and
                # cheaper for the query planner.
                join_type = "LEFT OUTER" if getattr(field, "null", False) else "INNER"
                existing = next(
                    (j for j in self.joins if j[2] == join_alias), None
                )
                if existing is None:
                    self.joins.append((join_type, table, join_alias, on_cond))
                elif existing[0] == "INNER" and join_type == "LEFT OUTER":
                    # Promote the existing INNER (rare — same alias
                    # registered twice through different code paths)
                    # to LEFT OUTER so isnull semantics survive.
                    idx = self.joins.index(existing)
                    self.joins[idx] = (join_type, table, join_alias, on_cond)
                model = rel_model
                current_alias = join_alias
                continue

            # Reverse-FK / reverse-O2O traversal: the descriptor lives
            # on the model class (installed by ``ForeignKey.contribute_to_class``
            # under ``related_name`` or ``<lower>_set``). Look it up
            # by attribute name and emit the equivalent JOIN from the
            # outer table's PK to the source's FK column.
            descriptor = getattr(model, fname, None)
            from .related_managers import (
                ManyToManyDescriptor,
                ReverseFKDescriptor,
                ReverseOneToOneDescriptor,
            )

            if isinstance(descriptor, (ReverseFKDescriptor, ReverseOneToOneDescriptor)):
                source_model = descriptor.source_model
                fk_field = descriptor.fk_field
                table = source_model._meta.db_table
                join_alias = f"{current_alias}_{fname}"
                on_cond = (
                    f'"{join_alias}"."{fk_field.column}" = '
                    f'"{current_alias}"."{model._meta.pk.column}"'
                )
                # Reverse relations are always LEFT OUTER — a parent
                # row that has zero children must still appear in
                # the result (filtered out later by the WHERE if it
                # mentioned the reverse alias) without dropping
                # legitimate parent-only rows from outer queries.
                existing = next(
                    (j for j in self.joins if j[2] == join_alias), None
                )
                if existing is None:
                    self.joins.append(("LEFT OUTER", table, join_alias, on_cond))
                model = source_model
                current_alias = join_alias
                continue

            if isinstance(descriptor, ManyToManyDescriptor):
                m2m_field = descriptor.field
                source = model
                target = m2m_field._resolve_related_model()
                junction = m2m_field._get_through_table()
                src_col, tgt_col = m2m_field._get_through_columns()
                # Two joins: outer → junction, junction → target.
                j_alias = f"{current_alias}_{fname}_j"
                t_alias = f"{current_alias}_{fname}"
                self.joins.append((
                    "LEFT OUTER", junction, j_alias,
                    f'"{j_alias}"."{src_col}" = "{current_alias}"."{source._meta.pk.column}"',
                ))
                self.joins.append((
                    "LEFT OUTER", target._meta.db_table, t_alias,
                    f'"{t_alias}"."{target._meta.pk.column}" = "{j_alias}"."{tgt_col}"',
                ))
                model = target
                current_alias = t_alias
                continue

            # JSON path traversal: ``filter(jsonfield__nested__key=…)``
            # emits the vendor's JSON extract operator. Path keys are
            # validated as identifiers — splice into SQL is safe
            # (raw user input never touches the literal).
            from .fields import JSONField

            if field is not None and isinstance(field, JSONField):
                json_path = list(parts)  # remaining keys after fname
                for key in json_path:
                    _validate_identifier(
                        key, kind=f"JSON path component on {fname!r}"
                    )
                # Returns the extract expression directly — caller
                # bypasses the trailing column-resolve below.
                vendor = getattr(connection, "vendor", None) or getattr(
                    getattr(self, "_connection", None), "vendor", None
                )
                col_qual = (
                    f'"{current_alias}"."{field.column}"'
                    if (self.joins or self.select_related_fields)
                    else f'"{field.column}"'
                )
                return _json_extract_sql(col_qual, json_path, vendor)

            # Stash for the post-loop diagnostic — ``parts`` already
            # popped *fname*, so reinsert for the error message.
            parts.insert(0, fname)
            if field is None:
                # Truly unknown identifier — keep traversal output for
                # legacy "treat as raw column" fallback below.
                break
            # Trying to traverse INTO a scalar field that isn't JSON.
            raise FieldDoesNotExist(
                f"Cannot resolve lookup path "
                f"{'__'.join(field_parts)!r} on model "
                f"{model.__name__!r}: field {fname!r} is not a "
                f"relation or a JSONField. Remove the sub-lookup, "
                f"or use a built-in lookup suffix "
                f"(``__exact``, ``__icontains``, ...)."
            )

        fname = parts[0]
        try:
            field = model._meta.get_field(fname)
            col_name = field.column
        except FieldDoesNotExist:
            # Single-part lookups for reverse-FK / reverse-O2O / M2M
            # accessors — the descriptor lives on the model class.
            # Resolve it as "join to the target and reference the
            # target's pk" so callers like ``Count("book_set")`` see
            # a real column.
            from .related_managers import (
                ManyToManyDescriptor,
                ReverseFKDescriptor,
                ReverseOneToOneDescriptor,
            )

            descriptor = getattr(model, fname, None)
            if isinstance(descriptor, (ReverseFKDescriptor, ReverseOneToOneDescriptor)):
                source_model = descriptor.source_model
                fk_field = descriptor.fk_field
                table = source_model._meta.db_table
                join_alias = f"{current_alias}_{fname}"
                on_cond = (
                    f'"{join_alias}"."{fk_field.column}" = '
                    f'"{current_alias}"."{model._meta.pk.column}"'
                )
                if not any(j[2] == join_alias for j in self.joins):
                    self.joins.append(("LEFT OUTER", table, join_alias, on_cond))
                return f'"{join_alias}"."{source_model._meta.pk.column}"'
            if isinstance(descriptor, ManyToManyDescriptor):
                m2m_field = descriptor.field
                target = m2m_field._resolve_related_model()
                junction = m2m_field._get_through_table()
                src_col, tgt_col = m2m_field._get_through_columns()
                j_alias = f"{current_alias}_{fname}_j"
                t_alias = f"{current_alias}_{fname}"
                if not any(j[2] == j_alias for j in self.joins):
                    self.joins.append((
                        "LEFT OUTER", junction, j_alias,
                        f'"{j_alias}"."{src_col}" = '
                        f'"{current_alias}"."{model._meta.pk.column}"',
                    ))
                if not any(j[2] == t_alias for j in self.joins):
                    self.joins.append((
                        "LEFT OUTER", target._meta.db_table, t_alias,
                        f'"{t_alias}"."{target._meta.pk.column}" = '
                        f'"{j_alias}"."{tgt_col}"',
                    ))
                return f'"{t_alias}"."{target._meta.pk.column}"'
            # Falling back to a literal column name — re-validate so a
            # user-supplied raw identifier can never reach the SQL.
            _validate_identifier(fname)
            col_name = fname

        # Qualify with the table alias whenever JOINs are in flight —
        # both WHERE-derived FK joins (``self.joins``) and
        # ``select_related`` joins create the chance of an identical
        # column name on multiple tables. A bare ``"name"`` would
        # then trigger ``ambiguous column name`` on PG (and silently
        # pick the wrong table on SQLite).
        if self.joins or self.select_related_fields:
            return f'"{current_alias}"."{col_name}"'
        return f'"{col_name}"'

    # ── Placeholder adaptation ────────────────────────────────────────────────

    def _adapt_placeholders(self, sql: str, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            return sql
        # Replace ``%s`` with ``$1``, ``$2``, … but skip occurrences
        # inside SQL string literals (single-quoted with ``''``
        # escaping). The previous naive regex rewrote every ``%s``
        # including ones inside ``'foo%s_bar'`` — corrupting raw
        # SQL and any LIKE pattern containing the placeholder
        # sequence as part of the literal text.
        import re

        # Tokenise: alternation matches a quoted-string run OR a
        # bare ``%s``. Bare matches are renumbered, quoted runs
        # pass through verbatim. Doubled quotes (``''``) inside a
        # literal are part of the literal — captured by the
        # ``(?:[^']|'')*`` body.
        token_re = re.compile(r"'(?:[^']|'')*'|%s")
        idx = [0]

        def repl(m: re.Match[str]) -> str:
            tok = m.group(0)
            if tok == "%s":
                idx[0] += 1
                return f"${idx[0]}"
            return tok

        return token_re.sub(repl, sql)
