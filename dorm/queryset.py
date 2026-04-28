from __future__ import annotations

import asyncio
import copy
from typing import (
    Any,
    AsyncIterator,
    Generic,
    Iterator,
    TypeVar,
    overload,
)

from .expressions import Q
from .lookups import parse_lookup_key
from .models import Model
from .query import SQLQuery, _validate_identifier

_T = TypeVar("_T", bound=Model)


class CursorPage(Generic[_T]):
    """One page of keyset-paginated results returned by
    :meth:`QuerySet.cursor_paginate` / :meth:`QuerySet.acursor_paginate`.

    Attributes:
        items: list of model instances (or values()-shaped dicts) for
            this page, ordered as requested.
        next_cursor: dict to pass as ``after=`` on the next call. ``None``
            when there are no more rows (last page).
    """

    __slots__ = ("items", "next_cursor")

    def __init__(self, items: list, next_cursor: dict | None) -> None:
        self.items = items
        self.next_cursor = next_cursor

    def __iter__(self):
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __bool__(self) -> bool:
        return bool(self.items)

    @property
    def has_next(self) -> bool:
        return self.next_cursor is not None

    def __repr__(self) -> str:
        return f"CursorPage(items={len(self.items)}, has_next={self.has_next})"


def _explain_row_to_str(row: Any) -> str:
    """Render a single ``EXPLAIN`` output row as a string. PG returns one
    column per row containing the plan line; SQLite returns multiple
    columns (``id``, ``parent``, ``notused``, ``detail``)."""
    if hasattr(row, "keys"):
        # dict-like row (psycopg dict_row, sqlite3.Row).
        d = dict(row)
        # PG: one column ``QUERY PLAN``. SQLite: ``detail`` is the only
        # column users actually care about.
        for key in ("QUERY PLAN", "detail"):
            if key in d:
                return str(d[key])
        return " ".join(str(v) for v in d.values())
    if isinstance(row, (list, tuple)):
        return " ".join(str(v) for v in row)
    return str(row)


class QuerySet(Generic[_T]):
    """
    Lazy, chainable query API compatible with Django's QuerySet.
    Supports both synchronous and asynchronous execution.
    """

    def __init__(self, model: type[_T], using: str = "default") -> None:
        self.model = model
        self._db = using
        self._query = SQLQuery(model)
        self._result_cache: list[_T] | None = None

    # ── Cloning ───────────────────────────────────────────────────────────────

    def _clone(self) -> QuerySet[_T]:
        qs: QuerySet[_T] = QuerySet(self.model, self._db)
        qs._query = self._query.clone()
        return qs

    def _get_connection(self):
        from .db.connection import get_connection

        return get_connection(self._db)

    def _get_async_connection(self):
        from .db.connection import get_async_connection

        return get_async_connection(self._db)

    # ── Filtering ─────────────────────────────────────────────────────────────

    def filter(self, *args: Q, **kwargs: Any) -> QuerySet[_T]:
        qs = self._clone()
        qs._add_conditions(args, kwargs)
        return qs

    def exclude(self, *args: Q, **kwargs: Any) -> QuerySet[_T]:
        qs = self._clone()
        q = Q(**kwargs)
        q.negated = True
        for a in args:
            a_copy = copy.deepcopy(a)
            a_copy.negated = not a_copy.negated
            qs._query.where_nodes.append(a_copy)
        if kwargs:
            qs._query.where_nodes.append(q)
        return qs

    def _add_conditions(self, q_args, kwargs):
        for q in q_args:
            self._query.where_nodes.append(q)
        for key, value in kwargs.items():
            key = self._resolve_pk_alias(key)
            field_parts, lookup = parse_lookup_key(key)
            self._query.where_nodes.append((field_parts, lookup, value))

    def _resolve_pk_alias(self, key: str) -> str:
        """Replace 'pk' with the actual primary key field name."""
        from .lookups import LOOKUP_SEP

        parts = key.split(LOOKUP_SEP)
        if parts[0] == "pk" and self.model._meta.pk:
            parts[0] = self.model._meta.pk.column
            return LOOKUP_SEP.join(parts)
        return key

    # ── Chaining ──────────────────────────────────────────────────────────────

    def all(self) -> QuerySet[_T]:
        return self._clone()

    def none(self) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.where_nodes.append(Q(pk__in=[]))
        qs._result_cache = []
        return qs

    def order_by(self, *fields: str) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.order_by_fields = list(fields)
        return qs

    def reverse(self) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.order_by_fields = [
            f[1:] if f.startswith("-") else f"-{f}" for f in self._query.order_by_fields
        ]
        return qs

    def distinct(self) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.distinct_flag = True
        return qs

    def union(self, *other_qs: QuerySet[_T], all: bool = False) -> "CombinedQuerySet[_T]":
        return CombinedQuerySet._combine(self, list(other_qs), "UNION", all)

    def intersection(self, *other_qs: QuerySet[_T]) -> "CombinedQuerySet[_T]":
        return CombinedQuerySet._combine(self, list(other_qs), "INTERSECT", False)

    def difference(self, *other_qs: QuerySet[_T]) -> "CombinedQuerySet[_T]":
        return CombinedQuerySet._combine(self, list(other_qs), "EXCEPT", False)

    def select_related(self, *fields: str) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.select_related_fields = list(fields)
        return qs

    def prefetch_related(self, *fields: str) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.prefetch_related_fields = list(fields)
        return qs

    def only(self, *fields: str) -> QuerySet[_T]:
        qs = self._clone()
        pk_col = self.model._meta.pk.column if self.model._meta.pk else "id"
        field_names = list(fields)
        if pk_col not in field_names:
            field_names = [pk_col] + field_names
        qs._query.selected_fields = field_names
        qs._query.deferred_loading = True
        return qs

    def defer(self, *fields: str) -> QuerySet[_T]:
        qs = self._clone()
        for f in fields:
            _validate_identifier(f)
        defer_set = set(fields)
        pk_col = self.model._meta.pk.column if self.model._meta.pk else "id"
        all_cols = [f.column for f in self.model._meta.fields if f.column]
        selected = [c for c in all_cols if c not in defer_set or c == pk_col]
        qs._query.selected_fields = selected
        qs._query.deferred_loading = True
        return qs

    def values(self, *fields: str) -> QuerySet[Any]:
        qs: QuerySet[Any] = QuerySet(self.model, self._db)  # type: ignore[arg-type]
        qs._query = self._query.clone()
        qs._query.selected_fields = (
            list(fields)
            if fields
            else [f.column for f in self.model._meta.fields if f.column]
        )
        return qs

    def values_list(self, *fields: str, flat: bool = False) -> ValuesListQuerySet:
        if flat and len(fields) != 1:
            raise ValueError(
                "'flat' is not valid when values_list is called with more than one field."
            )
        qs = ValuesListQuerySet(self.model, self._db)
        qs._query = self._query.clone()
        qs._query.selected_fields = list(fields) if fields else None
        qs._flat = flat
        qs._fields = list(fields)
        return qs

    def alias(self, **kwargs: Any) -> QuerySet[_T]:
        """Add named expressions usable in :meth:`filter`, :meth:`exclude`
        and :meth:`order_by` without including them in the ``SELECT`` list.

        Same shape as :meth:`annotate` — pass ``name=expression`` pairs —
        but the expression is **not** projected into the result rows. Use
        this when you only need the value to build a predicate or sort
        and don't care about reading it back, so you skip the bandwidth
        and per-row hydration cost::

            qs = (
                Author.objects
                .alias(book_count=Count("books"))
                .filter(book_count__gte=5)
            )

        ``alias()`` and :meth:`annotate` can be mixed freely; aliased
        names that you later promote to a SELECT can be re-declared via
        ``annotate(name=F("name"))`` (Django pattern)."""
        qs = self._clone()
        for name in kwargs:
            qs._query.alias_only_names.add(name)
        qs._query.annotations.update(kwargs)
        return qs

    def annotate(self, **kwargs: Any) -> QuerySet[_T]:
        qs = self._clone()
        # If a name was previously declared as alias-only and is now
        # being annotated, promote it to a real SELECT projection
        # (matches Django's behaviour: ``alias().annotate()`` chains
        # turn the alias into a returned column).
        for name in kwargs:
            qs._query.alias_only_names.discard(name)
        qs._query.annotations.update(kwargs)
        return qs

    def _build_aggregate_sql(
        self, kwargs: dict[str, Any], connection: Any
    ) -> tuple[str, list[Any], list[str]]:
        table = self.model._meta.db_table
        parts = []
        for alias, agg in kwargs.items():
            _validate_identifier(alias, "aggregate alias")
            # Pass model so ``Count("pk")`` resolves to the actual
            # PK column. See note in ``Aggregate.as_sql``.
            parts.append(f'{agg.as_sql(table, model=self.model)[0]} AS "{alias}"')
        sql = f'SELECT {", ".join(parts)} FROM "{table}"'
        where_sql, where_params = self._query._compile_nodes(
            self._query.where_nodes, connection
        )
        if where_sql:
            sql += f" WHERE {where_sql}"
        sql = self._query._adapt_placeholders(sql, connection)
        return sql, where_params, list(kwargs.keys())

    def aggregate(self, **kwargs: Any) -> dict[str, Any]:
        connection = self._get_connection()
        sql, params, cols = self._build_aggregate_sql(kwargs, connection)
        rows = connection.execute(sql, params)
        if rows:
            row = rows[0]
            return dict(row) if hasattr(row, "keys") else dict(zip(cols, row))
        return {}

    async def aaggregate(self, **kwargs: Any) -> dict[str, Any]:
        conn = self._get_async_connection()
        sql, params, cols = self._build_aggregate_sql(kwargs, conn)
        rows = await conn.execute(sql, params)
        if rows:
            row = rows[0]
            return dict(row) if hasattr(row, "keys") else dict(zip(cols, row))
        return {}

    def select_for_update(
        self,
        *,
        skip_locked: bool = False,
        no_wait: bool = False,
        of: tuple[str, ...] | list[str] | None = None,
    ) -> QuerySet[_T]:
        """Lock the rows returned by this query until the surrounding
        transaction ends. Must be called inside an :func:`atomic` /
        :func:`aatomic` block — otherwise PostgreSQL silently treats it as
        a no-op (the lock would be released immediately at autocommit).

        Args:
            skip_locked: Skip rows already locked by other transactions
                instead of waiting. The canonical "task queue" pattern —
                each worker pops the next unlocked job.
            no_wait: Raise immediately if any matched row is locked,
                instead of waiting. Useful for bailing fast on contention.
            of: Tuple of relation names (typically table aliases from
                joins) to lock; defaults to all referenced tables. Use
                this with ``select_related`` to lock only the parent row
                without also locking joined parent tables.

        ``skip_locked`` and ``no_wait`` are mutually exclusive — passing
        both raises ``ValueError``. Both are PostgreSQL-only; on SQLite
        they raise ``NotImplementedError`` so the caller learns instead
        of silently getting a different lock model.
        """
        if skip_locked and no_wait:
            raise ValueError(
                "select_for_update(): skip_locked and no_wait are "
                "mutually exclusive — choose one."
            )
        if (skip_locked or no_wait or of):
            # Only PG supports these. We can't check the connection
            # here without resolving it, so the validation happens at
            # SQL emission time on backends that don't support it.
            from .db.connection import get_connection

            conn = get_connection(self._db)
            vendor = getattr(conn, "vendor", "sqlite")
            if vendor != "postgresql":
                raise NotImplementedError(
                    f"select_for_update(skip_locked=, no_wait=, of=) "
                    f"is PostgreSQL-only; backend {vendor!r} does not "
                    f"support row-level lock variants."
                )
        qs = self._clone()
        qs._query.for_update_flag = True
        qs._query.for_update_skip_locked = bool(skip_locked)
        qs._query.for_update_no_wait = bool(no_wait)
        qs._query.for_update_of = tuple(of) if of else ()
        return qs

    def using(self, alias: str) -> QuerySet[_T]:
        qs = self._clone()
        qs._db = alias
        return qs

    def with_cte(self, **named_querysets: "QuerySet[Any]") -> QuerySet[_T]:
        """Attach one or more non-recursive Common Table Expressions to
        this queryset.

        Each ``name=qs`` pair is emitted as ``WITH name AS (sub)`` ahead
        of the main ``SELECT``::

            recent = Order.objects.filter(created_at__gte=cutoff)
            qs = (
                Customer.objects
                    .with_cte(recent_orders=recent)
                    .filter(...)
            )

        CTE names are validated as SQL identifiers. The CTE bodies see
        the same backend / parameter dialect as the outer query — the
        outer query's placeholder rewrite covers them in one pass, so
        you don't pay double-quoting on PostgreSQL.

        Recursive CTEs (``WITH RECURSIVE ...``) are not supported in
        this release; reach for :meth:`raw` for tree-walking queries.
        """
        qs = self._clone()
        for name, sub in named_querysets.items():
            _validate_identifier(name, "CTE name")
            if not (hasattr(sub, "_query") and hasattr(sub, "model")):
                raise TypeError(
                    f"with_cte({name}=...) expects a QuerySet, got {type(sub).__name__}."
                )
            qs._query.ctes.append((name, sub))
        return qs

    def cursor_paginate(
        self,
        *,
        after: dict[str, Any] | None = None,
        order_by: str = "pk",
        page_size: int = 50,
    ) -> "CursorPage[_T]":
        """Keyset (cursor) pagination — stable across writes and orders
        of magnitude faster than ``OFFSET`` for deep pages.

        Args:
            after: an optional cursor dict from the previous page's
                :attr:`CursorPage.next_cursor`. ``None`` returns the
                first page.
            order_by: a single field name. Prefix with ``-`` for
                descending. Defaults to the model's primary key. The
                ordering must include something unique (typically the
                PK) — otherwise ``after`` can't reliably resume across
                ties.
            page_size: number of rows to return.

        Example::

            page = Article.objects.cursor_paginate(
                order_by="-created_at", page_size=20
            )
            # Send page.items + page.next_cursor to the client.
            # On the next request:
            page = Article.objects.cursor_paginate(
                order_by="-created_at", page_size=20, after=cursor
            )

        Implementation: emits ``WHERE col > :v`` (asc) /
        ``col < :v`` (desc) and ``LIMIT page_size``. ``next_cursor`` is
        the last row's ``order_by`` field value, or ``None`` when the
        page wasn't full (no more rows).
        """
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer.")
        desc = order_by.startswith("-")
        fname = order_by[1:] if desc else order_by
        if fname == "pk" and self.model._meta.pk:
            fname = self.model._meta.pk.column
        _validate_identifier(fname)

        qs = self._clone()
        if after is not None:
            cursor_value = after.get(fname)
            if cursor_value is not None:
                lookup = f"{fname}__{'lt' if desc else 'gt'}"
                qs = qs.filter(**{lookup: cursor_value})
        qs = qs.order_by(f"-{fname}" if desc else fname)
        qs._query.limit_val = page_size

        items = list(qs)
        next_cursor: dict[str, Any] | None = None
        if len(items) == page_size:
            last: Any = items[-1]
            if isinstance(last, dict):
                # ty narrows ``last`` to an empty dict shape after the
                # ``isinstance`` check; cast to a permissive ``Mapping``
                # so the indexing reads as ``Any``.
                next_cursor = {fname: dict(last).get(fname)}
            elif hasattr(last, "__dict__"):
                try:
                    f = self.model._meta.get_field(fname)
                    next_cursor = {fname: last.__dict__.get(f.attname)}
                except Exception:
                    next_cursor = {fname: getattr(last, fname, None)}
        return CursorPage(items=items, next_cursor=next_cursor)

    async def acursor_paginate(
        self,
        *,
        after: dict[str, Any] | None = None,
        order_by: str = "pk",
        page_size: int = 50,
    ) -> "CursorPage[_T]":
        """Async counterpart of :meth:`cursor_paginate`."""
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer.")
        desc = order_by.startswith("-")
        fname = order_by[1:] if desc else order_by
        if fname == "pk" and self.model._meta.pk:
            fname = self.model._meta.pk.column
        _validate_identifier(fname)

        qs = self._clone()
        if after is not None:
            cursor_value = after.get(fname)
            if cursor_value is not None:
                lookup = f"{fname}__{'lt' if desc else 'gt'}"
                qs = qs.filter(**{lookup: cursor_value})
        qs = qs.order_by(f"-{fname}" if desc else fname)
        qs._query.limit_val = page_size

        items = await qs
        next_cursor: dict[str, Any] | None = None
        if len(items) == page_size:
            last = items[-1]
            if isinstance(last, dict):
                next_cursor = {fname: last[fname]}
            elif hasattr(last, "__dict__"):
                try:
                    f = self.model._meta.get_field(fname)
                    next_cursor = {fname: last.__dict__.get(f.attname)}
                except Exception:
                    next_cursor = {fname: getattr(last, fname, None)}
        return CursorPage(items=items, next_cursor=next_cursor)

    # ── Sync execution ────────────────────────────────────────────────────────

    def __iter__(self) -> Iterator[_T]:
        self._fetch_all()
        assert self._result_cache is not None
        return iter(self._result_cache)

    def __await__(self):
        """Materialize the queryset asynchronously: ``rows = await qs``.
        Lets users compose chainable methods (``values()``, ``filter()``,
        ``order_by()``...) and consume the result with a single await,
        instead of having to call a terminal ``avalues()``/``alist()``."""
        async def _materialize():
            return [item async for item in self._aiterator()]
        return _materialize().__await__()

    def __len__(self) -> int:
        self._fetch_all()
        assert self._result_cache is not None
        return len(self._result_cache)

    def __bool__(self) -> bool:
        self._fetch_all()
        return bool(self._result_cache)

    @overload
    def __getitem__(self, k: int) -> _T: ...
    @overload
    def __getitem__(self, k: slice) -> QuerySet[_T]: ...
    def __getitem__(self, k: int | slice) -> _T | QuerySet[_T]:
        if isinstance(k, slice):
            # ``step`` and negative bounds aren't expressible in SQL
            # ``LIMIT`` / ``OFFSET``; silently ignoring them (the prior
            # behaviour) made ``qs[::2]`` return the *same* rows as
            # ``qs[:]`` — a confusing data-loss-shaped bug. Reject so
            # the caller learns and can use ``list(qs)[::2]`` if
            # in-memory step actually is what they wanted.
            if k.step is not None and k.step != 1:
                raise ValueError(
                    "QuerySet slicing does not support a step (got "
                    f"step={k.step!r}). Materialise with ``list(qs)`` first "
                    "if you need step-based iteration."
                )
            if (k.start is not None and k.start < 0) or (
                k.stop is not None and k.stop < 0
            ):
                raise ValueError(
                    "QuerySet slicing does not support negative indices."
                )
            qs = self._clone()
            start = k.start or 0
            stop = k.stop
            qs._query.offset_val = start
            if stop is not None:
                # Python list semantics: ``lst[5:3]`` is empty. Without
                # the ``max(0, ...)`` clamp we'd compute ``LIMIT -2``,
                # which SQLite (silently) reads as "no limit" and
                # PostgreSQL rejects with a syntax error — both wrong.
                qs._query.limit_val = max(0, stop - start)
            return qs
        if isinstance(k, int):
            qs = self._clone()
            if k < 0:
                raise ValueError("Negative indexing is not supported.")
            qs._query.offset_val = k
            qs._query.limit_val = 1
            results = list(qs._iterator())
            if not results:
                raise IndexError("QuerySet index out of range")
            return results[0]
        raise TypeError(f"Invalid index type: {type(k)}")

    def _fetch_all(self) -> None:
        if self._result_cache is None:
            self._result_cache = list(self._iterator())  # type: ignore[assignment]

    @staticmethod
    def _hydrate_select_related(
        model: type[Model], instance: _T, sr_fields: list[str], row_dict: dict
    ) -> None:
        # Collect all unique path prefixes, sorted by depth (shortest first)
        all_paths: list[str] = []
        for path_str in sr_fields:
            parts = path_str.split("__")
            for depth in range(len(parts)):
                step_path = "__".join(parts[: depth + 1])
                if step_path not in all_paths:
                    all_paths.append(step_path)

        # Map path → created related instance
        created: dict[str, Any] = {}

        for step_path in all_paths:
            parts = step_path.split("__")
            # Resolve model for this path
            current_model: Any = model
            for step in parts:
                try:
                    field = current_model._meta.get_field(step)
                    if not hasattr(field, "_resolve_related_model"):
                        current_model = None
                        break
                    current_model = field._resolve_related_model()
                except Exception:
                    current_model = None
                    break
            if current_model is None:
                created[step_path] = None
                continue

            prefix = f"_sr_{step_path}_"
            rel_data = {k[len(prefix):]: v for k, v in row_dict.items() if k.startswith(prefix)}
            if rel_data and any(v is not None for v in rel_data.values()):
                rel_inst = current_model.__new__(current_model)
                rel_inst.__dict__ = {}
                for rf in current_model._meta.fields:
                    if rf.column in rel_data:
                        rel_inst.__dict__[rf.attname] = rf.from_db_value(rel_data[rf.column])
                created[step_path] = rel_inst
            else:
                created[step_path] = None

            # Attach to parent
            cache_key = f"_cache_{parts[-1]}"
            if len(parts) == 1:
                instance.__dict__[cache_key] = created[step_path]
            else:
                parent_path = "__".join(parts[:-1])
                parent_inst = created.get(parent_path)
                if parent_inst is not None:
                    parent_inst.__dict__[cache_key] = created[step_path]

    def _do_prefetch_related(self, instances: list[_T]) -> None:
        from .exceptions import FieldDoesNotExist

        for fname in self._query.prefetch_related_fields:
            field = None
            try:
                field = self.model._meta.get_field(fname)
            except FieldDoesNotExist:
                # Not a declared field on this model — could still be a
                # reverse-FK relation discovered via the descriptor scan
                # in ``_prefetch_reverse_fk``. Fall through with
                # ``field=None``; the reverse-FK path validates the name
                # and raises if it doesn't resolve there either.
                pass

            if field is not None and getattr(field, "many_to_many", False):
                self._prefetch_m2m(instances, fname, field)
            elif field is not None and hasattr(field, "_resolve_related_model"):
                # Forward FK
                rel_model = field._resolve_related_model()
                pk_vals = list(
                    {
                        obj.__dict__.get(field.attname)
                        for obj in instances
                        if obj.__dict__.get(field.attname) is not None
                    }
                )
                cache_key = f"_cache_{fname}"
                if not pk_vals:
                    for inst in instances:
                        inst.__dict__.setdefault(cache_key, None)
                    continue
                related_objs: dict = {
                    obj.pk: obj
                    for obj in QuerySet(rel_model, self._db).filter(pk__in=pk_vals)  # type: ignore[arg-type]
                }
                for inst in instances:
                    fk_val = inst.__dict__.get(field.attname)
                    inst.__dict__[cache_key] = related_objs.get(fk_val)
            else:
                # Reverse FK
                self._prefetch_reverse_fk(instances, fname)

    # Special alias used by the M2M prefetch JOIN to carry the source-side
    # PK back alongside the target row. Picked unlikely to clash with any
    # user column name.
    _M2M_SRC_PK_ALIAS = "__dorm_m2m_src_pk__"

    def _build_m2m_prefetch_sql(
        self, src_pks: list[Any], rel_model: Any, field: Any, conn: Any
    ) -> tuple[str, list[Any]]:
        """Build a single SELECT that joins the through table to the target
        table, returning each target row plus the source pk it links to."""
        from .query import SQLQuery

        rel_meta = rel_model._meta
        rel_table = rel_meta.db_table
        rel_pk = rel_meta.pk.column
        rel_cols = [f.column for f in rel_meta.fields if f.column]
        through = field._get_through_table()
        src_col, tgt_col = field._get_through_columns()

        cols_sql = ", ".join(f't."{c}"' for c in rel_cols)
        ph = ", ".join(["%s"] * len(src_pks))
        sql = (
            f'SELECT j."{src_col}" AS "{self._M2M_SRC_PK_ALIAS}", {cols_sql} '
            f'FROM "{through}" j '
            f'JOIN "{rel_table}" t ON t."{rel_pk}" = j."{tgt_col}" '
            f'WHERE j."{src_col}" IN ({ph})'
        )
        sql = SQLQuery(self.model)._adapt_placeholders(sql, conn)
        return sql, list(src_pks)

    def _hydrate_m2m_join_rows(
        self, rows: Any, src_pks: list[Any], rel_model: Any, conn: Any
    ) -> dict[Any, list[Any]]:
        """Group rows from :meth:`_build_m2m_prefetch_sql` by source pk,
        hydrating each target row into a model instance."""
        rel_meta = rel_model._meta
        rel_cols = [f.column for f in rel_meta.fields if f.column]

        src_to_objs: dict[Any, list[Any]] = {pk: [] for pk in src_pks}
        for row in rows:
            if hasattr(row, "keys"):
                row_dict = dict(row)
                src = row_dict.pop(self._M2M_SRC_PK_ALIAS, None)
                # _from_db_row will look up by field.column; the popped alias
                # is no longer present, so it can't shadow anything.
                obj = rel_model._from_db_row(row_dict, conn)
            else:
                # Sequence row: alias is the first column, then rel_cols.
                src = row[0]
                obj = rel_model._from_db_row(list(row[1 : 1 + len(rel_cols)]), conn)
            if src in src_to_objs:
                src_to_objs[src].append(obj)
        return src_to_objs

    def _prefetch_m2m(self, instances: list[_T], fname: str, field: Any) -> None:
        cache_key = f"_prefetch_{fname}"
        src_pks = [inst.pk for inst in instances if inst.pk is not None]
        if not src_pks:
            for inst in instances:
                inst.__dict__[cache_key] = []
            return

        conn = self._get_connection()
        rel_model = field._resolve_related_model()
        sql, params = self._build_m2m_prefetch_sql(src_pks, rel_model, field, conn)
        rows = conn.execute(sql, params)
        src_to_objs = self._hydrate_m2m_join_rows(rows, src_pks, rel_model, conn)

        for inst in instances:
            inst.__dict__[cache_key] = src_to_objs.get(inst.pk, [])

    async def _aprefetch_m2m(
        self, instances: list[_T], fname: str, field: Any
    ) -> None:
        cache_key = f"_prefetch_{fname}"
        src_pks = [inst.pk for inst in instances if inst.pk is not None]
        if not src_pks:
            for inst in instances:
                inst.__dict__[cache_key] = []
            return

        conn = self._get_async_connection()
        rel_model = field._resolve_related_model()
        sql, params = self._build_m2m_prefetch_sql(src_pks, rel_model, field, conn)
        rows = await conn.execute(sql, params)
        src_to_objs = self._hydrate_m2m_join_rows(rows, src_pks, rel_model, conn)

        for inst in instances:
            inst.__dict__[cache_key] = src_to_objs.get(inst.pk, [])

    def _prefetch_reverse_fk(self, instances: list[_T], fname: str) -> None:
        from .related_managers import ReverseFKDescriptor

        cache_key = f"_prefetch_{fname}"

        # Primary path: ReverseFKDescriptor installed directly on model class
        descriptor = self.model.__dict__.get(fname)
        if isinstance(descriptor, ReverseFKDescriptor):
            target_model = descriptor.source_model
            target_field = descriptor.fk_field
        else:
            # Fallback: scan model registry (less reliable across name clashes)
            from .models import _model_registry
            from .fields import ForeignKey, OneToOneField

            target_field = None
            target_model = None
            seen: set[Any] = set()
            for model_cls in _model_registry.values():
                if model_cls in seen:
                    continue
                seen.add(model_cls)
                for f in model_cls._meta.fields:
                    if not isinstance(f, (ForeignKey, OneToOneField)):
                        continue
                    try:
                        rel = f._resolve_related_model()
                    except Exception:
                        continue
                    if rel is not self.model:
                        continue
                    rel_name = f.related_name or f"{model_cls.__name__.lower()}_set"
                    if rel_name == fname:
                        target_field = f
                        target_model = model_cls
                        break
                if target_field:
                    break

            if target_field is None or target_model is None:
                # Neither the descriptor nor the registry scan resolved
                # ``fname`` to a relation. Previously this silently
                # returned and the caller assumed the prefetch had run —
                # so a typo in ``prefetch_related("authrs")`` would just
                # degrade back to N+1 with no warning. Raise so the
                # mistake surfaces.
                from .exceptions import FieldDoesNotExist

                raise FieldDoesNotExist(
                    f"Cannot resolve {fname!r} on "
                    f"{self.model.__name__} for prefetch_related(): "
                    f"no field, reverse-FK descriptor, or registry match."
                )

        src_pks = [inst.pk for inst in instances if inst.pk is not None]
        if not src_pks:
            for inst in instances:
                inst.__dict__[cache_key] = []
            return

        related_objs = list(
            QuerySet(target_model, self._db).filter(**{f"{target_field.name}__in": src_pks})  # type: ignore[arg-type]
        )

        fk_attname = target_field.attname
        grouped: dict[Any, list[Any]] = {pk: [] for pk in src_pks}
        for obj in related_objs:
            fk_val = obj.__dict__.get(fk_attname)
            if fk_val in grouped:
                grouped[fk_val].append(obj)

        for inst in instances:
            inst.__dict__[cache_key] = grouped.get(inst.pk, [])

    @staticmethod
    def _row_to_values_dict(row: Any, fields: list[str]) -> dict[str, Any]:
        """Shape a DB row as a {field: value} dict for values()-mode results."""
        if hasattr(row, "keys"):
            return dict(row)
        return dict(zip(fields, row))

    def _iter_setup(self):
        """Resolve the query (applying default ordering) and compute iter
        flags. Shared between :meth:`_iterator` and :meth:`_aiterator`."""
        query = self._query
        if not query.order_by_fields and self.model._meta.ordering:
            query = query.clone()
            query.order_by_fields = list(self.model._meta.ordering)
        sf = query.selected_fields
        values_mode = sf is not None and not query.deferred_loading
        sr_fields = query.select_related_fields
        collect_for_prefetch = (
            bool(query.prefetch_related_fields) and not values_mode
        )
        return query, sf, values_mode, sr_fields, collect_for_prefetch

    def _iterator(self) -> Iterator[_T]:
        connection = self._get_connection()
        query, sf, values_mode, sr_fields, collect_for_prefetch = self._iter_setup()
        sql, params = query.as_select(connection)
        rows = connection.execute(sql, params)

        instances: list[_T] = []

        for row in rows:
            if values_mode:
                assert sf is not None
                yield self._row_to_values_dict(row, sf)  # type: ignore
                continue

            instance = self.model._from_db_row(row, connection)  # type: ignore[misc]

            # Hydrate annotation values onto the instance
            if self._query.annotations:
                row_dict = dict(row) if hasattr(row, "keys") else {}
                for alias in self._query.annotations:
                    if alias in row_dict:
                        instance.__dict__[alias] = row_dict[alias]

            if sr_fields:
                self._hydrate_select_related(
                    self.model, instance, sr_fields, dict(row) if hasattr(row, "keys") else {}
                )

            if collect_for_prefetch:
                instances.append(instance)
            else:
                yield instance

        if collect_for_prefetch and instances:
            self._do_prefetch_related(instances)
            yield from instances

    def explain(self, *, analyze: bool = False) -> str:
        """Return the database's query plan for this queryset.

        On PostgreSQL, ``analyze=True`` runs the query and includes
        actual timing / row counts (``EXPLAIN (ANALYZE TRUE, BUFFERS
        TRUE)``). On SQLite, ``EXPLAIN QUERY PLAN`` is used; the
        ``analyze`` flag is ignored (SQLite has no equivalent).

        Useful for diagnosing slow production queries::

            slow_qs = Author.objects.filter(age__gte=18).select_related("publisher")
            print(slow_qs.explain(analyze=True))
        """
        connection = self._get_connection()
        query, _, _, _, _ = self._iter_setup()
        sql, params = query.as_select(connection)
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            prefix = "EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)" if analyze else "EXPLAIN"
        else:
            prefix = "EXPLAIN QUERY PLAN"
        rows = connection.execute(f"{prefix} {sql}", params)
        return "\n".join(_explain_row_to_str(r) for r in rows)

    async def aexplain(self, *, analyze: bool = False) -> str:
        """Async counterpart of :meth:`explain`."""
        connection = self._get_async_connection()
        query, _, _, _, _ = self._iter_setup()
        sql, params = query.as_select(connection)
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            prefix = "EXPLAIN (ANALYZE TRUE, BUFFERS TRUE)" if analyze else "EXPLAIN"
        else:
            prefix = "EXPLAIN QUERY PLAN"
        rows = await connection.execute(f"{prefix} {sql}", params)
        return "\n".join(_explain_row_to_str(r) for r in rows)

    def iterator(self, chunk_size: int | None = None) -> Iterator[_T]:
        """Stream results one by one without populating the result cache.

        When ``chunk_size`` is given, uses a server-side cursor on PG
        (so the entire result set never lands in client memory) and
        ``cursor.arraysize`` on SQLite. Without ``chunk_size``, the
        previous all-rows-then-iterate path is preserved for back-compat.
        """
        if chunk_size is None:
            return self._iterator()
        return self._iterator_streaming(chunk_size)

    async def aiterator(self, chunk_size: int | None = None) -> AsyncIterator[_T]:
        """Async stream results one by one without populating the result cache.

        Same chunk_size semantics as :meth:`iterator`.
        """
        if chunk_size is None:
            async for item in self._aiterator():
                yield item
        else:
            async for item in self._aiterator_streaming(chunk_size):
                yield item

    # ── Streaming iterator (chunk_size opt-in) ────────────────────────────────

    def _iterator_streaming(self, chunk_size: int) -> Iterator[_T]:
        """Server-side-cursor variant of :meth:`_iterator`. select_related
        and prefetch_related are NOT supported here — they require the
        full row set up front to issue follow-up queries. Plain rows /
        values() are fine."""
        connection = self._get_connection()
        query, sf, values_mode, _sr, _prefetch = self._iter_setup()
        sql, params = query.as_select(connection)
        for row in connection.execute_streaming(sql, params, chunk_size):
            if values_mode:
                assert sf is not None
                yield self._row_to_values_dict(row, sf)  # type: ignore
            else:
                yield self.model._from_db_row(row, connection)  # type: ignore[misc]

    async def _aiterator_streaming(self, chunk_size: int) -> AsyncIterator[_T]:
        conn = self._get_async_connection()
        query, sf, values_mode, _sr, _prefetch = self._iter_setup()
        sql, params = query.as_select(conn)
        async for row in conn.execute_streaming(sql, params, chunk_size):
            if values_mode:
                assert sf is not None
                yield self._row_to_values_dict(row, sf)  # type: ignore
            else:
                yield self.model._from_db_row(row, conn)  # type: ignore[misc]

    def get(self, *args: Q, **kwargs: Any) -> _T:
        qs = self.filter(*args, **kwargs)
        qs._query.limit_val = 2
        results = list(qs._iterator())
        if len(results) == 0:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching {kwargs} does not exist."
            )
        if len(results) > 1:
            raise self.model.MultipleObjectsReturned(
                f"get() returned more than one {self.model.__name__} — filter: {kwargs}"
            )
        return results[0]

    def first(self) -> _T | None:
        qs = self._clone()
        if not qs._query.order_by_fields:
            pk_col = self.model._meta.pk.column if self.model._meta.pk else "id"
            qs._query.order_by_fields = [pk_col]
        qs._query.limit_val = 1
        results = list(qs._iterator())
        return results[0] if results else None

    def last(self) -> _T | None:
        qs = self._clone()
        if not qs._query.order_by_fields:
            pk_col = self.model._meta.pk.column if self.model._meta.pk else "id"
            qs._query.order_by_fields = [f"-{pk_col}"]
        else:
            qs._query.order_by_fields = [
                f[1:] if f.startswith("-") else f"-{f}"
                for f in qs._query.order_by_fields
            ]
        qs._query.limit_val = 1
        results = list(qs._iterator())
        return results[0] if results else None

    def count(self) -> int:
        connection = self._get_connection()
        sql, params = self._query.as_count(connection)
        rows = connection.execute(sql, params)
        row = rows[0]
        return row["count"]

    def get_or_none(self, *args: Q, **kwargs: Any) -> _T | None:
        try:
            return self.get(*args, **kwargs)
        except self.model.DoesNotExist:
            return None

    def exists(self) -> bool:
        connection = self._get_connection()
        sql, params = self._query.as_exists(connection)
        return bool(connection.execute(sql, params))

    def create(self, **kwargs: Any) -> _T:
        obj = self.model(**kwargs)
        obj.save(using=self._db, force_insert=True)
        return obj

    def get_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        from .transaction import atomic
        from .exceptions import IntegrityError

        with atomic(using=self._db):
            try:
                return self.get(**kwargs), False
            except self.model.DoesNotExist:
                params = dict(kwargs)
                if defaults:
                    params.update(defaults)
                try:
                    return self.create(**params), True
                except IntegrityError:
                    return self.get(**kwargs), False

    def update_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        from .transaction import atomic
        from .exceptions import IntegrityError

        defaults = defaults or {}
        with atomic(using=self._db):
            try:
                obj = self.get(**kwargs)
                for k, v in defaults.items():
                    setattr(obj, k, v)
                obj.save(using=self._db)
                return obj, False
            except self.model.DoesNotExist:
                params = dict(kwargs)
                params.update(defaults)
                try:
                    return self.create(**params), True
                except IntegrityError:
                    obj = self.get(**kwargs)
                    for k, v in defaults.items():
                        setattr(obj, k, v)
                    obj.save(using=self._db)
                    return obj, False

    def update(self, **kwargs: Any) -> int:
        from .expressions import CombinedExpression, F, Value

        connection = self._get_connection()
        col_kwargs = {}
        for k, v in kwargs.items():
            try:
                field = self.model._meta.get_field(k)
                if isinstance(v, (F, Value, CombinedExpression)):
                    col_kwargs[field.column] = v
                else:
                    col_kwargs[field.column] = field.get_db_prep_value(v)
            except Exception:
                col_kwargs[k] = v
        sql, params = self._query.as_update(col_kwargs, connection)
        return connection.execute_write(sql, params)

    def delete(self) -> tuple[int, dict[str, int]]:
        from .exceptions import ProtectedError
        from .fields import CASCADE, DO_NOTHING, PROTECT, SET_DEFAULT, SET_NULL
        from .related_managers import ReverseFKDescriptor

        pk_attname = self.model._meta.pk.attname
        pks = list(self.values_list(pk_attname, flat=True))
        model_label = f"{self.model._meta.app_label}.{self.model.__name__}"
        if not pks:
            return 0, {model_label: 0}

        total_counts: dict[str, int] = {}

        for attr_val in self.model.__dict__.values():
            if not isinstance(attr_val, ReverseFKDescriptor):
                continue
            fk_field = attr_val.fk_field
            on_delete = getattr(fk_field, "on_delete", DO_NOTHING)
            if on_delete == DO_NOTHING:
                continue

            related_qs = QuerySet(attr_val.source_model, self._db).filter(
                **{f"{fk_field.name}__in": pks}
            )

            if on_delete == PROTECT:
                if related_qs.exists():
                    raise ProtectedError(
                        f"Cannot delete {self.model.__name__} objects because related "
                        f"{attr_val.source_model.__name__} objects exist.",
                        list(related_qs[:5]),
                    )
            elif on_delete == CASCADE:
                sub_count, sub_detail = related_qs.delete()
                for label, cnt in sub_detail.items():
                    total_counts[label] = total_counts.get(label, 0) + cnt
            elif on_delete == SET_NULL:
                related_qs.update(**{fk_field.name: None})
            elif on_delete == SET_DEFAULT:
                related_qs.update(**{fk_field.name: fk_field.get_default()})

        connection = self._get_connection()
        sql, params = self._query.as_delete(connection)
        count = connection.execute_write(sql, params)
        total_counts[model_label] = total_counts.get(model_label, 0) + count
        return sum(total_counts.values()), total_counts

    def bulk_create(
        self,
        objs: list[_T],
        batch_size: int = 1000,
        *,
        ignore_conflicts: bool = False,
        update_conflicts: bool = False,
        update_fields: list[str] | None = None,
        unique_fields: list[str] | None = None,
    ) -> list[_T]:
        """Insert *objs* in batches of ``batch_size``.

        With ``ignore_conflicts=True``, duplicate-key conflicts are
        silently skipped (``ON CONFLICT DO NOTHING``). With
        ``update_conflicts=True``, the conflicting row is *updated* —
        the canonical "upsert" pattern. ``unique_fields`` is required
        when updating so the conflict target is unambiguous; if
        ``update_fields`` is omitted, every non-PK / non-unique column
        is updated, which is almost always what you want for an idempotent
        sync from an external source.

        Note: when conflicts are skipped or updated, returned PKs may be
        ``None`` for affected rows (the database doesn't report which
        rows actually wrote new data). Re-fetch by ``unique_fields`` if
        you need the full set of PKs back.
        """
        if not objs:
            return objs
        from .transaction import atomic
        from .fields import AutoField

        if ignore_conflicts and update_conflicts:
            raise ValueError(
                "bulk_create(): ignore_conflicts and update_conflicts "
                "are mutually exclusive — choose one."
            )
        if update_conflicts and not unique_fields:
            raise ValueError(
                "bulk_create(update_conflicts=True) requires "
                "unique_fields= to identify the conflict target."
            )

        connection = self._get_connection()
        meta = self.model._meta
        # Include the auto-PK in ``concrete_fields`` only when the
        # caller explicitly set it on the first object. Excluding all
        # ``AutoField`` columns unconditionally — the previous
        # behaviour — silently dropped the user's pre-assigned PK,
        # so ``bulk_create([Publisher(pk=4242, …)])`` ended up with
        # an auto-generated id instead. Match Django: trust the
        # caller's pk if present.
        concrete_fields = [
            f
            for f in meta.fields
            if f.column
            and (
                not isinstance(f, AutoField)
                or objs[0].__dict__.get(f.attname) is not None
            )
        ]
        pk_col = meta.pk.column if meta.pk else "id"

        # Field list is determined once from the first object — every batch
        # shares the same shape. Previously this was recomputed per batch,
        # which on bulk_create(100k objs, batch_size=1000) was 100 wasted
        # passes over concrete_fields. Match Django's behaviour: assume all
        # objects in a single bulk_create call have the same PK presence.
        fields = [
            f
            for f in concrete_fields
            if not f.primary_key or objs[0].__dict__.get(f.attname) is not None
        ]

        with atomic(using=self._db):
            for i in range(0, len(objs), batch_size):
                batch = objs[i : i + batch_size]
                rows_values = [
                    [
                        f.get_db_prep_value(
                            obj.__dict__.get(f.attname, f.get_default())
                        )
                        for f in fields
                    ]
                    for obj in batch
                ]
                sql, params = self._query.as_bulk_insert(
                    fields,
                    rows_values,
                    connection,
                    ignore_conflicts=ignore_conflicts,
                    update_conflicts=update_conflicts,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
                pks = connection.execute_bulk_insert(
                    sql, params, pk_col=pk_col, count=len(batch)
                )
                if meta.pk and pks and not (ignore_conflicts or update_conflicts):
                    # PK assignment is unsafe when conflicts may have
                    # skipped rows: the returned ``pks`` list count
                    # can be shorter than ``batch`` and the alignment
                    # between objects and inserted rows is no longer
                    # 1:1. Skip assignment in upsert mode; callers can
                    # re-fetch by ``unique_fields`` if needed.
                    for obj, pk in zip(batch, pks):
                        if obj.__dict__.get(meta.pk.attname) is None:
                            obj.__dict__[meta.pk.attname] = pk
        return objs

    def _build_bulk_update_sql(
        self, batch: list[_T], fields: list[str], conn: Any
    ) -> tuple[str, list[Any]] | None:
        """Build a single ``UPDATE ... SET col = CASE pk WHEN ... END WHERE pk IN (...)``
        statement for a batch of objects. Returns ``None`` if every row in the
        batch lacks a primary key."""
        from .query import SQLQuery

        meta = self.model._meta
        pk_col = meta.pk.column
        table = meta.db_table

        # Filter out objects without a pk (would be unaddressable).
        rows = [obj for obj in batch if obj.pk is not None]
        if not rows:
            return None

        field_objs: list[Any] = []
        for fname in fields:
            try:
                f = meta.get_field(fname)
            except Exception as exc:
                raise ValueError(f"Unknown field for bulk_update: {fname!r}") from exc
            if not f.column:
                raise ValueError(
                    f"Field {fname!r} has no DB column and can't be bulk-updated."
                )
            field_objs.append(f)

        params: list[Any] = []
        set_clauses: list[str] = []
        for f in field_objs:
            parts = [f'"{f.column}" = CASE "{pk_col}"']
            for obj in rows:
                parts.append(" WHEN %s THEN %s")
                params.append(obj.pk)
                params.append(f.get_db_prep_value(obj.__dict__.get(f.attname)))
            parts.append(f' ELSE "{f.column}" END')
            set_clauses.append("".join(parts))

        pk_placeholders = ", ".join(["%s"] * len(rows))
        params.extend(obj.pk for obj in rows)

        sql = (
            f'UPDATE "{table}" SET '
            + ", ".join(set_clauses)
            + f' WHERE "{pk_col}" IN ({pk_placeholders})'
        )
        sql = SQLQuery(self.model)._adapt_placeholders(sql, conn)
        return sql, params

    def bulk_update(
        self, objs: list[_T], fields: list[str], batch_size: int = 1000
    ) -> int:
        """Update *fields* on *objs* with a single ``UPDATE ... SET col = CASE pk
        WHEN ...`` statement per batch (one round-trip per ``batch_size``
        objects, instead of one per object).

        Raises :class:`ValueError` if *fields* is empty — without
        columns to set, the generated SQL would be malformed (``UPDATE
        … WHERE …`` with no ``SET`` clause), so we fail fast at the
        Python boundary instead of at the database parser.
        """
        if not objs:
            return 0
        if not fields:
            raise ValueError(
                "bulk_update() requires at least one column name in *fields*; "
                "got an empty list."
            )
        from .transaction import atomic

        count = 0
        with atomic(using=self._db):
            connection = self._get_connection()
            for i in range(0, len(objs), batch_size):
                batch = objs[i : i + batch_size]
                built = self._build_bulk_update_sql(batch, fields, connection)
                if built is None:
                    continue
                sql, params = built
                count += connection.execute_write(sql, params)
        return count

    def in_bulk(self, id_list: list[Any], field_name: str = "pk") -> dict[Any, _T]:
        if not id_list:
            return {}
        qs = self.filter(**{f"{field_name}__in": id_list})
        result: dict[Any, _T] = {}
        for obj in qs:
            key = getattr(
                obj, field_name if field_name != "pk" else self.model._meta.pk.attname
            )
            result[key] = obj
        return result

    # ── Async execution ───────────────────────────────────────────────────────

    def __aiter__(self) -> AsyncIterator[_T]:
        return self._aiterator()

    async def _aprefetch_reverse_fk(self, instances: list[_T], fname: str) -> None:
        """Async counterpart of :meth:`_prefetch_reverse_fk`."""
        from .related_managers import ReverseFKDescriptor
        from .fields import ForeignKey, OneToOneField

        cache_key = f"_prefetch_{fname}"
        descriptor = self.model.__dict__.get(fname)
        if isinstance(descriptor, ReverseFKDescriptor):
            target_model = descriptor.source_model
            target_field = descriptor.fk_field
        else:
            from .models import _model_registry

            target_field = None
            target_model = None
            seen: set[Any] = set()
            for model_cls in _model_registry.values():
                if model_cls in seen:
                    continue
                seen.add(model_cls)
                for f in model_cls._meta.fields:
                    if not isinstance(f, (ForeignKey, OneToOneField)):
                        continue
                    try:
                        rel = f._resolve_related_model()
                    except Exception:
                        continue
                    if rel is not self.model:
                        continue
                    rel_name = f.related_name or f"{model_cls.__name__.lower()}_set"
                    if rel_name == fname:
                        target_field = f
                        target_model = model_cls
                        break
                if target_field:
                    break

            if target_field is None or target_model is None:
                # Same rationale as :meth:`_prefetch_reverse_fk`: surface
                # typos instead of degrading to N+1 silently.
                from .exceptions import FieldDoesNotExist

                raise FieldDoesNotExist(
                    f"Cannot resolve {fname!r} on "
                    f"{self.model.__name__} for prefetch_related(): "
                    f"no field, reverse-FK descriptor, or registry match."
                )

        src_pks = [inst.pk for inst in instances if inst.pk is not None]
        if not src_pks:
            for inst in instances:
                inst.__dict__[cache_key] = []
            return

        related_objs: list[Any] = []
        async for obj in QuerySet(target_model, self._db).filter(  # type: ignore[arg-type]
            **{f"{target_field.name}__in": src_pks}
        ):
            related_objs.append(obj)

        fk_attname = target_field.attname
        grouped: dict[Any, list[Any]] = {pk: [] for pk in src_pks}
        for obj in related_objs:
            fk_val = obj.__dict__.get(fk_attname)
            if fk_val in grouped:
                grouped[fk_val].append(obj)
        for inst in instances:
            inst.__dict__[cache_key] = grouped.get(inst.pk, [])

    async def _ado_prefetch_related(self, instances: list[_T]) -> None:
        """Run every prefetch concurrently with ``asyncio.gather``.

        Each prefetch is an independent SQL round-trip (different table /
        join), so we can fire them all at once and let the event loop
        await them in parallel. Previously the ``for fname in ...`` loop
        awaited each one sequentially, multiplying latency by the number
        of prefetched relations.
        """
        if not self._query.prefetch_related_fields:
            return

        async def _one_forward_fk(fname: str, field: Any) -> None:
            rel_model = field._resolve_related_model()
            pk_vals = list(
                {
                    obj.__dict__.get(field.attname)
                    for obj in instances
                    if obj.__dict__.get(field.attname) is not None
                }
            )
            cache_key = f"_cache_{fname}"
            if not pk_vals:
                for inst in instances:
                    inst.__dict__.setdefault(cache_key, None)
                return
            related_objs: dict = {}
            async for obj in QuerySet(rel_model, self._db).filter(pk__in=pk_vals):  # type: ignore[arg-type]
                related_objs[obj.pk] = obj
            for inst in instances:
                fk_val = inst.__dict__.get(field.attname)
                inst.__dict__[cache_key] = related_objs.get(fk_val)

        from .exceptions import FieldDoesNotExist

        coros: list = []
        names: list[str] = []
        for fname in self._query.prefetch_related_fields:
            field = None
            try:
                field = self.model._meta.get_field(fname)
            except FieldDoesNotExist:
                # Same rationale as the sync path: could still resolve as
                # a reverse-FK descriptor; let that branch validate and
                # raise if needed.
                pass

            if field is not None and getattr(field, "many_to_many", False):
                coros.append(self._aprefetch_m2m(instances, fname, field))
            elif field is not None and hasattr(field, "_resolve_related_model"):
                coros.append(_one_forward_fk(fname, field))
            else:
                coros.append(self._aprefetch_reverse_fk(instances, fname))
            names.append(fname)

        if coros:
            # ``return_exceptions=True`` keeps a single failing prefetch
            # from cancelling the others mid-flight (psycopg cancellation
            # can leave the connection in a bad state). We re-raise the
            # first failure with the relation name attached so the
            # traceback says *which* prefetch blew up — previously it was
            # just an opaque exception from somewhere in gather().
            results = await asyncio.gather(*coros, return_exceptions=True)
            for fname, result in zip(names, results):
                if isinstance(result, BaseException):
                    raise RuntimeError(
                        f"prefetch_related({fname!r}) failed: {result}"
                    ) from result

    async def _aiterator(self) -> AsyncIterator[_T]:
        conn = self._get_async_connection()
        query, sf, values_mode, sr_fields, collect_for_prefetch = self._iter_setup()
        sql, params = query.as_select(conn)
        rows = await conn.execute(sql, params)

        instances: list[_T] = []

        for row in rows:
            if values_mode:
                assert sf is not None
                yield self._row_to_values_dict(row, sf)  # type: ignore
                continue

            instance = self.model._from_db_row(row, conn)  # type: ignore[misc]

            if sr_fields:
                self._hydrate_select_related(
                    self.model, instance, sr_fields, dict(row) if hasattr(row, "keys") else {}
                )

            if collect_for_prefetch:
                instances.append(instance)
            else:
                yield instance

        if collect_for_prefetch and instances:
            await self._ado_prefetch_related(instances)
            for inst in instances:
                yield inst

    async def aget(self, *args: Q, **kwargs: Any) -> _T:
        qs = self.filter(*args, **kwargs)
        qs._query.limit_val = 2
        results = [obj async for obj in qs]
        if len(results) == 0:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching {kwargs} does not exist."
            )
        if len(results) > 1:
            raise self.model.MultipleObjectsReturned(
                f"aget() returned more than one {self.model.__name__} — "
                f"filter: {kwargs}"
            )
        return results[0]

    async def acreate(self, **kwargs: Any) -> _T:
        obj = self.model(**kwargs)
        await obj.asave(using=self._db, force_insert=True)
        return obj

    async def aget_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        from .transaction import aatomic
        from .exceptions import IntegrityError

        async with aatomic(using=self._db):
            try:
                return await self.aget(**kwargs), False
            except self.model.DoesNotExist:
                params = dict(kwargs)
                if defaults:
                    params.update(defaults)
                try:
                    return await self.acreate(**params), True
                except IntegrityError:
                    return await self.aget(**kwargs), False

    async def aupdate_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        from .transaction import aatomic
        from .exceptions import IntegrityError

        defaults = defaults or {}
        async with aatomic(using=self._db):
            try:
                obj = await self.aget(**kwargs)
                for k, v in defaults.items():
                    setattr(obj, k, v)
                await obj.asave(using=self._db)
                return obj, False
            except self.model.DoesNotExist:
                params = dict(kwargs)
                params.update(defaults)
                try:
                    return await self.acreate(**params), True
                except IntegrityError:
                    obj = await self.aget(**kwargs)
                    for k, v in defaults.items():
                        setattr(obj, k, v)
                    await obj.asave(using=self._db)
                    return obj, False

    async def aupdate(self, **kwargs: Any) -> int:
        from .expressions import CombinedExpression, F, Value

        conn = self._get_async_connection()
        col_kwargs = {}
        for k, v in kwargs.items():
            try:
                field = self.model._meta.get_field(k)
                if isinstance(v, (F, Value, CombinedExpression)):
                    col_kwargs[field.column] = v
                else:
                    col_kwargs[field.column] = field.get_db_prep_value(v)
            except Exception:
                col_kwargs[k] = v
        sql, params = self._query.as_update(col_kwargs, conn)
        return await conn.execute_write(sql, params)

    async def adelete(self) -> tuple[int, dict[str, int]]:
        from .exceptions import ProtectedError
        from .fields import CASCADE, DO_NOTHING, PROTECT, SET_DEFAULT, SET_NULL
        from .related_managers import ReverseFKDescriptor

        pk_attname = self.model._meta.pk.attname
        pks = await self.avalues_list(pk_attname, flat=True)
        model_label = f"{self.model._meta.app_label}.{self.model.__name__}"
        if not pks:
            return 0, {model_label: 0}

        total_counts: dict[str, int] = {}

        # Two-pass strategy:
        #   1. PROTECT first — these are guards that abort the whole
        #      delete. We run them sequentially because the first
        #      ProtectedError must propagate cleanly (gather + raise
        #      cancels the others, leaking partial state).
        #   2. CASCADE / SET_NULL / SET_DEFAULT in parallel via gather().
        #      Each hits a different table, so there's no row-level race.
        #      Inside an aatomic() block they share one PG connection,
        #      so psycopg serialises them anyway — but outside aatomic()
        #      this fans out to separate pool connections and gives a
        #      real speedup on wide cascade trees.
        protect_descs: list[Any] = []
        cascade_qs: list[QuerySet[Any]] = []
        set_null_specs: list[tuple[QuerySet[Any], str]] = []
        set_default_specs: list[tuple[QuerySet[Any], str, Any]] = []

        for attr_val in self.model.__dict__.values():
            if not isinstance(attr_val, ReverseFKDescriptor):
                continue
            fk_field = attr_val.fk_field
            on_delete = getattr(fk_field, "on_delete", DO_NOTHING)
            if on_delete == DO_NOTHING:
                continue

            related_qs = QuerySet(attr_val.source_model, self._db).filter(
                **{f"{fk_field.name}__in": pks}
            )

            if on_delete == PROTECT:
                protect_descs.append((attr_val, related_qs))
            elif on_delete == CASCADE:
                cascade_qs.append(related_qs)
            elif on_delete == SET_NULL:
                set_null_specs.append((related_qs, fk_field.name))
            elif on_delete == SET_DEFAULT:
                set_default_specs.append(
                    (related_qs, fk_field.name, fk_field.get_default())
                )

        for attr_val, related_qs in protect_descs:
            if await related_qs.aexists():
                raise ProtectedError(
                    f"Cannot delete {self.model.__name__} objects because related "
                    f"{attr_val.source_model.__name__} objects exist.",
                    [obj async for obj in related_qs[:5]],
                )

        cascade_coros = [qs.adelete() for qs in cascade_qs]
        update_coros = [
            qs.aupdate(**{fname: None}) for qs, fname in set_null_specs
        ] + [
            qs.aupdate(**{fname: default})
            for qs, fname, default in set_default_specs
        ]
        if cascade_coros or update_coros:
            # asyncio.gather returns a heterogeneous list (cascade coros
            # return ``(int, dict)``, update coros return ``int``). The
            # type hint loses that structure; cast to Any when indexing
            # so the type checker doesn't block on the union.
            results: list[Any] = list(
                await asyncio.gather(*cascade_coros, *update_coros)
            )
            for cascade_result in results[: len(cascade_coros)]:
                sub_detail = cascade_result[1]
                for label, cnt in sub_detail.items():
                    total_counts[label] = total_counts.get(label, 0) + cnt

        conn = self._get_async_connection()
        sql, params = self._query.as_delete(conn)
        count = await conn.execute_write(sql, params)
        total_counts[model_label] = total_counts.get(model_label, 0) + count
        return sum(total_counts.values()), total_counts

    async def avalues(self, *fields: str) -> list[dict[str, Any]]:
        qs = self.values(*fields)
        conn = self._get_async_connection()
        sql, params = qs._query.as_select(conn)
        rows = await conn.execute(sql, params)
        sf = qs._query.selected_fields
        assert sf is not None
        if rows and hasattr(rows[0], "keys"):
            return [{f: row[f] for f in sf} for row in rows]
        return [dict(zip(sf, row)) for row in rows]

    async def avalues_list(self, *fields: str, flat: bool = False) -> list[Any]:
        if flat and len(fields) != 1:
            raise ValueError(
                "'flat' is not valid when values_list is called with more than one field."
            )
        qs = self.values_list(*fields, flat=flat)
        conn = self._get_async_connection()
        sql, params = qs._query.as_select(conn)
        rows = await conn.execute(sql, params)
        result_fields = qs._resolve_fields()
        return [qs._extract_row(row, result_fields) for row in rows]

    async def acount(self) -> int:
        conn = self._get_async_connection()
        sql, params = self._query.as_count(conn)
        rows = await conn.execute(sql, params)
        row = rows[0]
        return row["count"]

    async def aget_or_none(self, *args: Q, **kwargs: Any) -> _T | None:
        try:
            return await self.aget(*args, **kwargs)
        except self.model.DoesNotExist:
            return None

    async def aexists(self) -> bool:
        conn = self._get_async_connection()
        sql, params = self._query.as_exists(conn)
        return bool(await conn.execute(sql, params))

    async def afirst(self) -> _T | None:
        qs = self._clone()
        if not qs._query.order_by_fields:
            pk_col = self.model._meta.pk.column if self.model._meta.pk else "id"
            qs._query.order_by_fields = [pk_col]
        qs._query.limit_val = 1
        results = [obj async for obj in qs]
        return results[0] if results else None

    async def alast(self) -> _T | None:
        qs = self._clone()
        if not qs._query.order_by_fields:
            pk_col = self.model._meta.pk.column if self.model._meta.pk else "id"
            qs._query.order_by_fields = [f"-{pk_col}"]
        else:
            qs._query.order_by_fields = [
                f[1:] if f.startswith("-") else f"-{f}"
                for f in qs._query.order_by_fields
            ]
        qs._query.limit_val = 1
        results = [obj async for obj in qs]
        return results[0] if results else None

    async def abulk_create(
        self,
        objs: list[_T],
        batch_size: int = 1000,
        *,
        ignore_conflicts: bool = False,
        update_conflicts: bool = False,
        update_fields: list[str] | None = None,
        unique_fields: list[str] | None = None,
    ) -> list[_T]:
        """Async counterpart of :meth:`bulk_create`. See the sync version
        for ``ignore_conflicts`` / ``update_conflicts`` semantics."""
        if not objs:
            return objs
        from .transaction import aatomic
        from .fields import AutoField

        if ignore_conflicts and update_conflicts:
            raise ValueError(
                "abulk_create(): ignore_conflicts and update_conflicts "
                "are mutually exclusive — choose one."
            )
        if update_conflicts and not unique_fields:
            raise ValueError(
                "abulk_create(update_conflicts=True) requires "
                "unique_fields= to identify the conflict target."
            )

        conn = self._get_async_connection()
        meta = self.model._meta
        # See ``bulk_create`` for the rationale on AutoField inclusion
        # when the caller pre-assigned the PK.
        concrete_fields = [
            f
            for f in meta.fields
            if f.column
            and (
                not isinstance(f, AutoField)
                or objs[0].__dict__.get(f.attname) is not None
            )
        ]
        pk_col = meta.pk.column if meta.pk else "id"

        # Hoisted: compute the field list once from objs[0]. See the
        # comment in `bulk_create` for the reasoning.
        fields = [
            f
            for f in concrete_fields
            if not f.primary_key or objs[0].__dict__.get(f.attname) is not None
        ]

        async with aatomic(using=self._db):
            for i in range(0, len(objs), batch_size):
                batch = objs[i : i + batch_size]
                rows_values = [
                    [
                        f.get_db_prep_value(
                            obj.__dict__.get(f.attname, f.get_default())
                        )
                        for f in fields
                    ]
                    for obj in batch
                ]
                sql, params = self._query.as_bulk_insert(
                    fields,
                    rows_values,
                    conn,
                    ignore_conflicts=ignore_conflicts,
                    update_conflicts=update_conflicts,
                    update_fields=update_fields,
                    unique_fields=unique_fields,
                )
                pks = await conn.execute_bulk_insert(
                    sql, params, pk_col=pk_col, count=len(batch)
                )
                if meta.pk and pks and not (ignore_conflicts or update_conflicts):
                    # See sync ``bulk_create``: skip PK assignment in
                    # upsert mode because the returned-PK list may not
                    # align 1:1 with the input batch.
                    for obj, pk in zip(batch, pks):
                        if obj.__dict__.get(meta.pk.attname) is None:
                            obj.__dict__[meta.pk.attname] = pk
        return objs

    async def abulk_update(
        self, objs: list[_T], fields: list[str], batch_size: int = 1000
    ) -> int:
        """Async version of :meth:`bulk_update`. Same single-query batching
        strategy: one UPDATE statement per batch of ``batch_size`` objects."""
        if not objs:
            return 0
        if not fields:
            # Same fast-fail as ``bulk_update``: empty ``fields`` would
            # build malformed SQL.
            raise ValueError(
                "abulk_update() requires at least one column name in *fields*; "
                "got an empty list."
            )
        from .transaction import aatomic

        count = 0
        async with aatomic(using=self._db):
            conn = self._get_async_connection()
            for i in range(0, len(objs), batch_size):
                batch = objs[i : i + batch_size]
                built = self._build_bulk_update_sql(batch, fields, conn)
                if built is None:
                    continue
                sql, params = built
                count += await conn.execute_write(sql, params)
        return count

    async def ain_bulk(
        self, id_list: list[Any], field_name: str = "pk"
    ) -> dict[Any, _T]:
        if not id_list:
            return {}
        qs = self.filter(**{f"{field_name}__in": id_list})
        result: dict[Any, _T] = {}
        async for obj in qs:
            key = getattr(
                obj, field_name if field_name != "pk" else self.model._meta.pk.attname
            )
            result[key] = obj
        return result

    # ── Representation ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        self._fetch_all()
        assert self._result_cache is not None
        data = self._result_cache[:21]
        truncated = len(self._result_cache) > 20
        rep = repr(data[:20])
        if truncated:
            rep = rep[:-1] + ", ...]"
        return f"<QuerySet {rep}>"


class ValuesListQuerySet(QuerySet[Any]):
    _flat: bool = False
    _fields: list[str] = []

    def _clone(self) -> ValuesListQuerySet:
        qs = ValuesListQuerySet(self.model, self._db)
        qs._query = self._query.clone()
        qs._flat = self._flat
        qs._fields = list(self._fields)
        return qs

    def _resolve_fields(self) -> list[str]:
        return self._fields or [f.column for f in self.model._meta.fields if f.column]

    def _extract_row(self, row: Any, fields: list[str]) -> Any:
        values = (
            tuple(row[f] for f in fields)
            if hasattr(row, "keys")
            else tuple(row[: len(fields)])
        )
        return values[0] if self._flat else values

    def _iterator(self) -> Iterator[Any]:
        connection = self._get_connection()
        sql, params = self._query.as_select(connection)
        rows = connection.execute(sql, params)
        fields = self._resolve_fields()
        for row in rows:
            yield self._extract_row(row, fields)

    async def _aiterator(self) -> AsyncIterator[Any]:
        conn = self._get_async_connection()
        sql, params = self._query.as_select(conn)
        rows = await conn.execute(sql, params)
        fields = self._resolve_fields()
        for row in rows:
            yield self._extract_row(row, fields)


class CombinedQuerySet(QuerySet[_T]):
    """Produced by .union() / .intersection() / .difference()."""

    def __init__(self, model: type[_T], using: str = "default") -> None:
        super().__init__(model, using)
        self._combined_queries: list[tuple[str, list]] = []
        self._combinator: str = "UNION"
        self._union_all: bool = False

    @classmethod
    def _combine(
        cls,
        base_qs: QuerySet[_T],
        other_qs: list[QuerySet[_T]],
        combinator: str,
        union_all: bool,
    ) -> "CombinedQuerySet[_T]":
        result: CombinedQuerySet[_T] = cls(base_qs.model, base_qs._db)
        result._combinator = combinator
        result._union_all = union_all
        connection = base_qs._get_connection()
        sql, params = base_qs._query.as_select(connection)
        result._combined_queries.append((sql, params))
        for oqs in other_qs:
            osql, oparams = oqs._query.as_select(connection)
            result._combined_queries.append((osql, oparams))
        return result

    def _clone(self) -> "CombinedQuerySet[_T]":
        qs: CombinedQuerySet[_T] = CombinedQuerySet(self.model, self._db)
        qs._query = self._query.clone()
        qs._combined_queries = list(self._combined_queries)
        qs._combinator = self._combinator
        qs._union_all = self._union_all
        return qs

    def _build_sql(self, connection) -> tuple[str, list]:
        import re as _re

        vendor = getattr(connection, "vendor", "sqlite")
        parts: list[str] = []
        all_params: list = []

        for sub_sql, sub_params in self._combined_queries:
            if vendor == "postgresql":
                sub_sql = _re.sub(r"\$\d+", "%s", sub_sql)
            parts.append(sub_sql)
            all_params.extend(sub_params)

        op = "UNION ALL" if self._combinator == "UNION" and self._union_all else self._combinator
        combined = f" {op} ".join(parts)

        if self._query.order_by_fields:
            order_parts = []
            for f in self._query.order_by_fields:
                fname = f[1:] if f.startswith("-") else f
                order_parts.append(f'"{fname}" {"DESC" if f.startswith("-") else "ASC"}')
            combined += " ORDER BY " + ", ".join(order_parts)

        if self._query.limit_val is not None:
            combined += f" LIMIT {int(self._query.limit_val)}"
        if self._query.offset_val is not None:
            combined += f" OFFSET {int(self._query.offset_val)}"

        if vendor == "postgresql":
            idx = [0]

            def _repl(m: Any) -> str:
                idx[0] += 1
                return f"${idx[0]}"

            combined = _re.sub(r"%s", _repl, combined)

        return combined, all_params

    def _iterator(self) -> Iterator[_T]:
        connection = self._get_connection()
        sql, params = self._build_sql(connection)
        rows = connection.execute(sql, params)
        for row in rows:
            yield self.model._from_db_row(row, connection)  # type: ignore[misc]

    async def _aiterator(self) -> AsyncIterator[_T]:  # type: ignore[override]
        conn = self._get_async_connection()
        sql, params = self._build_sql(conn)
        rows = await conn.execute(sql, params)
        for row in rows:
            yield self.model._from_db_row(row, conn)  # type: ignore[misc]

    def count(self) -> int:
        connection = self._get_connection()
        sql, params = self._build_sql(connection)
        count_sql = f'SELECT COUNT(*) AS "count" FROM ({sql}) AS "_combined"'
        rows = connection.execute(count_sql, params)
        return rows[0]["count"]

    async def acount(self) -> int:
        conn = self._get_async_connection()
        sql, params = self._build_sql(conn)
        count_sql = f'SELECT COUNT(*) AS "count" FROM ({sql}) AS "_combined"'
        rows = await conn.execute(count_sql, params)
        return rows[0]["count"]


# ── RawQuerySet ────────────────────────────────────────────────────────────────


def _count_placeholders(sql: str) -> int | None:
    """Count ``%s`` and ``$N`` placeholders outside quoted literals.

    Returns ``None`` if a ``%(name)s`` named placeholder is detected — those
    are valid for psycopg/sqlite3 but they take a dict of params so a
    positional length check would give a false alarm.

    Both ``%s`` and ``$N`` are counted as positional placeholders. Each
    ``$N`` becomes a separate ``%s`` after :func:`_to_pyformat`, so reusing
    the same ``$N`` index needs one bound value per occurrence — that
    matches what psycopg expects on the wire.
    """
    count = 0
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            i += 1
            while i < n:
                if sql[i] == "'" and i + 1 < n and sql[i + 1] == "'":
                    i += 2
                    continue
                if sql[i] == "'":
                    i += 1
                    break
                i += 1
            continue
        if c == '"':
            i += 1
            while i < n:
                if sql[i] == '"' and i + 1 < n and sql[i + 1] == '"':
                    i += 2
                    continue
                if sql[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == "%" and i + 1 < n:
            nxt = sql[i + 1]
            if nxt == "s":
                count += 1
                i += 2
                continue
            if nxt == "(":
                # Named placeholder — bail out of the count.
                return None
            if nxt == "%":
                # Escaped percent — not a placeholder.
                i += 2
                continue
        if c == "$" and i + 1 < n and sql[i + 1].isdigit():
            j = i + 1
            while j < n and sql[j].isdigit():
                j += 1
            count += 1
            i = j
            continue
        i += 1
    return count


class RawQuerySet(Generic[_T]):
    """
    Executes a raw SQL query and hydrates the results as model instances.
    Columns returned by the query are mapped to field attnames; unknown columns
    are stored as plain attributes on the instance.

    .. warning::
       ``raw_sql`` is sent to the database verbatim. **Never** build it by
       string-interpolating user input — use placeholders (``%s`` for
       PostgreSQL / SQLite, or ``$1`` / ``$2`` for the dorm builder, which
       this class adapts) and pass values via ``params``::

           # SAFE
           Author.objects.raw("SELECT * FROM authors WHERE id = %s", [user_id])

           # UNSAFE — direct string concatenation defeats parameterisation
           Author.objects.raw(f"SELECT * FROM authors WHERE id = {user_id}")

       For dynamic identifiers (table or column names that aren't fixed at
       coding time), validate them against an allowlist before splicing.
    """

    def __init__(
        self,
        model: type[_T],
        raw_sql: str,
        params: list[Any] | None = None,
        using: str = "default",
    ) -> None:
        if not isinstance(raw_sql, str) or not raw_sql.strip():
            raise ValueError("raw_sql must be a non-empty string.")
        params_list = list(params) if params is not None else []
        # Cheap parameter-count sanity check: detects the most common raw()
        # mistake — building the SQL with f-strings and passing no ``params``,
        # or copy-pasting a query with ``%s`` placeholders without binding
        # values for them. We count ``%s`` and ``$N`` placeholders outside
        # of quoted literals; if the totals disagree with len(params), warn
        # eagerly so the bug surfaces at construction time rather than as a
        # confusing DB-side error.
        expected = _count_placeholders(raw_sql)
        if expected is not None and expected != len(params_list):
            raise ValueError(
                f"RawQuerySet: SQL has {expected} placeholder(s) but "
                f"{len(params_list)} param(s) were provided. Did you forget "
                "to pass values via the ``params`` kwarg, or interpolate "
                "user input into the SQL string by mistake?"
            )
        self.model = model
        self.raw_sql = raw_sql
        self.params = params_list
        self._db = using
        self._result_cache: list[_T] | None = None

    def _get_connection(self):
        from .db.connection import get_connection
        return get_connection(self._db)

    def _get_async_connection(self):
        from .db.connection import get_async_connection
        return get_async_connection(self._db)

    def _adapt(self, connection) -> str:
        from .query import SQLQuery
        return SQLQuery(self.model)._adapt_placeholders(self.raw_sql, connection)

    def _hydrate(self, row, column_names: list[str]) -> _T:
        instance = self.model.__new__(self.model)
        instance.__dict__["_state"] = None
        col_to_field: dict[str, Any] = {
            f.column: f for f in self.model._meta.fields if f.column
        }
        col_to_attname: dict[str, str] = {
            f.column: f.attname for f in self.model._meta.fields if f.column
        }
        for i, col in enumerate(column_names):
            val = row[col] if hasattr(row, "keys") else row[i]
            attname = col_to_attname.get(col, col)
            field = col_to_field.get(col)
            instance.__dict__[attname] = field.from_db_value(val) if field else val
        return instance

    def _fetch_all(self) -> list[_T]:
        if self._result_cache is None:
            conn = self._get_connection()
            sql = self._adapt(conn)
            rows = conn.execute(sql, self.params)
            if not rows:
                self._result_cache = []
                return self._result_cache
            cols = (
                list(rows[0].keys())
                if hasattr(rows[0], "keys")
                else [f.column for f in self.model._meta.fields if f.column]
            )
            self._result_cache = [self._hydrate(row, cols) for row in rows]
        return self._result_cache

    def __iter__(self) -> Iterator[_T]:
        return iter(self._fetch_all())

    def __len__(self) -> int:
        return len(self._fetch_all())

    def __repr__(self) -> str:
        return f"<RawQuerySet: {self.raw_sql!r}>"

    async def _afetch_all(self) -> list[_T]:
        conn = self._get_async_connection()
        sql = self._adapt(conn)
        rows = await conn.execute(sql, self.params)
        if not rows:
            return []
        cols = (
            list(rows[0].keys())
            if hasattr(rows[0], "keys")
            else [f.column for f in self.model._meta.fields if f.column]
        )
        return [self._hydrate(row, cols) for row in rows]

    def __aiter__(self) -> AsyncIterator[_T]:
        return self._aiterator()

    async def _aiterator(self) -> AsyncIterator[_T]:
        for obj in await self._afetch_all():
            yield obj
