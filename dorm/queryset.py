from __future__ import annotations

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
from .query import SQLQuery

_T = TypeVar("_T", bound=Model)


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
        qs._query.where_nodes.append(("__none__", "exact", "__none__"))
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

    def annotate(self, **kwargs: Any) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.annotations.update(kwargs)
        return qs

    def aggregate(self, **kwargs: Any) -> dict[str, Any]:
        connection = self._get_connection()
        table = self.model._meta.db_table
        parts = []
        for alias_name, agg in kwargs.items():
            agg_sql, _ = agg.as_sql(table)
            parts.append(f'{agg_sql} AS "{alias_name}"')

        sql = f'SELECT {", ".join(parts)} FROM "{table}"'
        where_sql, where_params = self._query._compile_nodes(
            self._query.where_nodes, connection
        )
        if where_sql:
            sql += f" WHERE {where_sql}"
        sql = self._query._adapt_placeholders(sql, connection)

        rows = connection.execute(sql, where_params)
        if rows:
            row = rows[0]
            if hasattr(row, "keys"):
                return dict(row)
            cols = list(kwargs.keys())
            return dict(zip(cols, row))
        return {}

    async def aaggregate(self, **kwargs: Any) -> dict[str, Any]:
        conn = self._get_async_connection()
        table = self.model._meta.db_table
        parts = []
        for alias_name, agg in kwargs.items():
            agg_sql, _ = agg.as_sql(table)
            parts.append(f'{agg_sql} AS "{alias_name}"')

        sql = f'SELECT {", ".join(parts)} FROM "{table}"'
        where_sql, where_params = self._query._compile_nodes(
            self._query.where_nodes, conn
        )
        if where_sql:
            sql += f" WHERE {where_sql}"
        sql = self._query._adapt_placeholders(sql, conn)

        rows = await conn.execute(sql, where_params)
        if rows:
            row = rows[0]
            if hasattr(row, "keys"):
                return dict(row)
            cols = list(kwargs.keys())
            return dict(zip(cols, row))
        return {}

    def select_for_update(self) -> QuerySet[_T]:
        qs = self._clone()
        qs._query.for_update_flag = True
        return qs

    def using(self, alias: str) -> QuerySet[_T]:
        qs = self._clone()
        qs._db = alias
        return qs

    # ── Sync execution ────────────────────────────────────────────────────────

    def __iter__(self) -> Iterator[_T]:
        self._fetch_all()
        assert self._result_cache is not None
        return iter(self._result_cache)

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
            qs = self._clone()
            start = k.start or 0
            stop = k.stop
            qs._query.offset_val = start
            if stop is not None:
                qs._query.limit_val = stop - start
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

    def _hydrate_select_related(
        self, instance: _T, sr_fields: list[str], row_dict: dict
    ) -> None:
        for fname in sr_fields:
            try:
                field = self.model._meta.get_field(fname)
                if not hasattr(field, "_resolve_related_model"):
                    continue
                rel_model = field._resolve_related_model()
                prefix = f"_sr_{fname}_"
                rel_data = {
                    k[len(prefix) :]: v
                    for k, v in row_dict.items()
                    if k.startswith(prefix)
                }
                if rel_data and any(v is not None for v in rel_data.values()):
                    rel_inst = rel_model.__new__(rel_model)
                    rel_inst.__dict__ = {}
                    for rf in rel_model._meta.fields:
                        if rf.column in rel_data:
                            rel_inst.__dict__[rf.attname] = rf.from_db_value(
                                rel_data[rf.column]
                            )
                    instance.__dict__[f"_cache_{fname}"] = rel_inst
                else:
                    instance.__dict__[f"_cache_{fname}"] = None
            except Exception:
                pass

    def _do_prefetch_related(self, instances: list[_T]) -> None:
        for fname in self._query.prefetch_related_fields:
            try:
                field = self.model._meta.get_field(fname)
            except Exception:
                continue
            if not hasattr(field, "_resolve_related_model"):
                continue
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

    def _iterator(self) -> Iterator[_T]:
        connection = self._get_connection()
        sql, params = self._query.as_select(connection)
        rows = connection.execute(sql, params)

        sf = self._query.selected_fields
        values_mode = sf is not None and not self._query.deferred_loading
        sr_fields = self._query.select_related_fields
        collect_for_prefetch = (
            bool(self._query.prefetch_related_fields) and not values_mode
        )

        instances: list[_T] = []

        for row in rows:
            if values_mode:
                assert sf is not None
                if hasattr(row, "keys"):
                    yield dict(row)  # type: ignore
                else:
                    yield dict(zip(sf, row))  # type: ignore
                continue

            instance = self.model._from_db_row(row, connection)  # type: ignore[misc]

            if sr_fields:
                self._hydrate_select_related(
                    instance, sr_fields, dict(row) if hasattr(row, "keys") else {}
                )

            if collect_for_prefetch:
                instances.append(instance)
            else:
                yield instance

        if collect_for_prefetch and instances:
            self._do_prefetch_related(instances)
            yield from instances

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
        connection = self._get_connection()
        sql, params = self._query.as_delete(connection)
        count = connection.execute_write(sql, params)
        model_label = f"{self.model._meta.app_label}.{self.model.__name__}"
        return count, {model_label: count}

    def bulk_create(self, objs: list[_T], batch_size: int = 1000) -> list[_T]:
        if not objs:
            return objs
        from .transaction import atomic
        from .fields import AutoField

        connection = self._get_connection()
        meta = self.model._meta
        concrete_fields = [
            f for f in meta.fields if f.column and not isinstance(f, AutoField)
        ]
        pk_col = meta.pk.column if meta.pk else "id"

        with atomic(using=self._db):
            for i in range(0, len(objs), batch_size):
                batch = objs[i : i + batch_size]
                fields = [
                    f
                    for f in concrete_fields
                    if not f.primary_key or batch[0].__dict__.get(f.attname) is not None
                ]
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
                    fields, rows_values, connection
                )
                pks = connection.execute_bulk_insert(
                    sql, params, pk_col=pk_col, count=len(batch)
                )
                if meta.pk and pks:
                    for obj, pk in zip(batch, pks):
                        if obj.__dict__.get(meta.pk.attname) is None:
                            obj.__dict__[meta.pk.attname] = pk
        return objs

    def bulk_update(
        self, objs: list[_T], fields: list[str], batch_size: int = 1000
    ) -> int:
        if not objs:
            return 0
        from .transaction import atomic

        count = 0
        with atomic(using=self._db):
            for obj in objs:
                update_kwargs = {}
                for fname in fields:
                    try:
                        field = self.model._meta.get_field(fname)
                        update_kwargs[fname] = obj.__dict__.get(field.attname)
                    except Exception:
                        update_kwargs[fname] = obj.__dict__.get(fname)
                count += self.filter(pk=obj.pk).update(**update_kwargs)
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

    async def _ado_prefetch_related(self, instances: list[_T]) -> None:
        for fname in self._query.prefetch_related_fields:
            try:
                field = self.model._meta.get_field(fname)
            except Exception:
                continue
            if not hasattr(field, "_resolve_related_model"):
                continue
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
            related_objs: dict = {}
            async for obj in QuerySet(rel_model, self._db).filter(pk__in=pk_vals):  # type: ignore[arg-type]
                related_objs[obj.pk] = obj
            for inst in instances:
                fk_val = inst.__dict__.get(field.attname)
                inst.__dict__[cache_key] = related_objs.get(fk_val)

    async def _aiterator(self) -> AsyncIterator[_T]:
        conn = self._get_async_connection()
        sql, params = self._query.as_select(conn)
        rows = await conn.execute(sql, params)

        sf = self._query.selected_fields
        values_mode = sf is not None and not self._query.deferred_loading
        sr_fields = self._query.select_related_fields
        collect_for_prefetch = (
            bool(self._query.prefetch_related_fields) and not values_mode
        )

        instances: list[_T] = []

        for row in rows:
            if values_mode:
                assert sf is not None
                if hasattr(row, "keys"):
                    yield dict(row)  # type: ignore
                else:
                    yield dict(zip(sf, row))  # type: ignore
                continue

            instance = self.model._from_db_row(row, conn)  # type: ignore[misc]

            if sr_fields:
                self._hydrate_select_related(
                    instance, sr_fields, dict(row) if hasattr(row, "keys") else {}
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
        conn = self._get_async_connection()
        sql, params = self._query.as_delete(conn)
        count = await conn.execute_write(sql, params)
        model_label = f"{self.model._meta.app_label}.{self.model.__name__}"
        return count, {model_label: count}

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
        result_fields = qs._fields or [
            f.column for f in self.model._meta.fields if f.column
        ]
        result: list[Any] = []
        for row in rows:
            if hasattr(row, "keys"):
                values = tuple(row[f] for f in result_fields)
            else:
                values = tuple(row[: len(result_fields)])
            result.append(values[0] if flat else values)
        return result

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

    async def abulk_create(self, objs: list[_T], batch_size: int = 1000) -> list[_T]:
        if not objs:
            return objs
        from .transaction import aatomic
        from .fields import AutoField

        conn = self._get_async_connection()
        meta = self.model._meta
        concrete_fields = [
            f for f in meta.fields if f.column and not isinstance(f, AutoField)
        ]
        pk_col = meta.pk.column if meta.pk else "id"

        async with aatomic(using=self._db):
            for i in range(0, len(objs), batch_size):
                batch = objs[i : i + batch_size]
                fields = [
                    f
                    for f in concrete_fields
                    if not f.primary_key or batch[0].__dict__.get(f.attname) is not None
                ]
                rows_values = [
                    [
                        f.get_db_prep_value(
                            obj.__dict__.get(f.attname, f.get_default())
                        )
                        for f in fields
                    ]
                    for obj in batch
                ]
                sql, params = self._query.as_bulk_insert(fields, rows_values, conn)
                pks = await conn.execute_bulk_insert(
                    sql, params, pk_col=pk_col, count=len(batch)
                )
                if meta.pk and pks:
                    for obj, pk in zip(batch, pks):
                        if obj.__dict__.get(meta.pk.attname) is None:
                            obj.__dict__[meta.pk.attname] = pk
        return objs

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

    def _iterator(self) -> Iterator[Any]:
        connection = self._get_connection()
        sql, params = self._query.as_select(connection)
        rows = connection.execute(sql, params)
        fields = self._fields or [f.column for f in self.model._meta.fields if f.column]
        for row in rows:
            if hasattr(row, "keys"):
                values = tuple(row[f] for f in fields)
            else:
                values = tuple(row[: len(fields)])
            if self._flat:
                yield values[0]
            else:
                yield values

    async def _aiterator(self) -> AsyncIterator[Any]:
        conn = self._get_async_connection()
        sql, params = self._query.as_select(conn)
        rows = await conn.execute(sql, params)
        fields = self._fields or [f.column for f in self.model._meta.fields if f.column]
        for row in rows:
            if hasattr(row, "keys"):
                values = tuple(row[f] for f in fields)
            else:
                values = tuple(row[: len(fields)])
            if self._flat:
                yield values[0]
            else:
                yield values
