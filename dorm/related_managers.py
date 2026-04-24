from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from .models import Model


class ManyRelatedManager:
    """
    Manager returned when accessing a ManyToManyField on a model instance.
    Provides add/remove/set/clear/all/filter/create operations on the junction table.
    """

    def __init__(self, instance: "Model", field: Any, using: str = "default") -> None:
        self.instance = instance
        self.field = field
        self._db = using

    # ── Internal helpers ──────────────────────────────────────────────────────

    @property
    def _rel_model(self) -> Any:
        return self.field._resolve_related_model()

    @property
    def _through_table(self) -> str:
        return self.field._get_through_table()

    @property
    def _through_columns(self) -> tuple[str, str]:
        return self.field._get_through_columns()

    def _get_connection(self):
        from .db.connection import get_connection
        return get_connection(self._db)

    def _get_async_connection(self):
        from .db.connection import get_async_connection
        return get_async_connection(self._db)

    def _adapt(self, sql: str, conn) -> str:
        from .query import SQLQuery
        return SQLQuery(self._rel_model)._adapt_placeholders(sql, conn)

    # ── Queryset access ───────────────────────────────────────────────────────

    def get_queryset(self):
        from .queryset import QuerySet

        # Return prefetch cache if populated
        cache_key = f"_prefetch_{self.field.name}"
        if cache_key in self.instance.__dict__:
            qs: QuerySet = QuerySet(self._rel_model, self._db)
            qs._result_cache = list(self.instance.__dict__[cache_key])
            return qs

        conn = self._get_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        sql = self._adapt(
            f'SELECT "{tgt_col}" FROM "{through}" WHERE "{src_col}" = %s', conn
        )
        rows = conn.execute(sql, [self.instance.pk])
        pks = [r[tgt_col] if hasattr(r, "keys") else r[0] for r in rows]
        if not pks:
            return QuerySet(self._rel_model, self._db).none()
        return QuerySet(self._rel_model, self._db).filter(pk__in=pks)

    def all(self):
        return self.get_queryset()

    def filter(self, **kwargs: Any):
        return self.get_queryset().filter(**kwargs)

    def count(self) -> int:
        return self.get_queryset().count()

    def __iter__(self) -> Iterator:
        return iter(self.get_queryset())

    # ── Mutations ─────────────────────────────────────────────────────────────

    def add(self, *objs: Any, through_defaults: dict | None = None) -> None:
        conn = self._get_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        extra_cols = ""
        extra_phs = ""
        extra_vals: list[Any] = []
        if through_defaults:
            cols = list(through_defaults.keys())
            extra_cols = ", " + ", ".join(f'"{c}"' for c in cols)
            extra_phs = ", " + ", ".join(["%s"] * len(cols))
            extra_vals = list(through_defaults.values())

        check_sql = self._adapt(
            f'SELECT 1 FROM "{through}" WHERE "{src_col}" = %s AND "{tgt_col}" = %s LIMIT 1',
            conn,
        )
        ins_sql = self._adapt(
            f'INSERT INTO "{through}" ("{src_col}", "{tgt_col}"{extra_cols}) '
            f"VALUES (%s, %s{extra_phs})",
            conn,
        )
        for obj in objs:
            pk = obj.pk if hasattr(obj, "pk") else obj
            if not conn.execute(check_sql, [self.instance.pk, pk]):
                conn.execute_write(ins_sql, [self.instance.pk, pk] + extra_vals)

    def remove(self, *objs: Any) -> None:
        conn = self._get_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        sql = self._adapt(
            f'DELETE FROM "{through}" WHERE "{src_col}" = %s AND "{tgt_col}" = %s',
            conn,
        )
        for obj in objs:
            pk = obj.pk if hasattr(obj, "pk") else obj
            conn.execute_write(sql, [self.instance.pk, pk])

    def set(
        self,
        objs: Any,
        *,
        clear: bool = False,
        through_defaults: dict | None = None,
    ) -> None:
        objs_list = list(objs)
        if clear:
            self.clear()
            self.add(*objs_list, through_defaults=through_defaults)
            return
        new_pks = {o.pk if hasattr(o, "pk") else o for o in objs_list}
        current_pks = {obj.pk for obj in self.get_queryset()}
        to_remove = list(current_pks - new_pks)
        to_add = [pk for pk in new_pks if pk not in current_pks]
        if to_remove:
            self.remove(*to_remove)
        if to_add:
            self.add(*to_add, through_defaults=through_defaults)

    def clear(self) -> None:
        conn = self._get_connection()
        through = self._through_table
        src_col, _ = self._through_columns
        sql = self._adapt(
            f'DELETE FROM "{through}" WHERE "{src_col}" = %s', conn
        )
        conn.execute_write(sql, [self.instance.pk])

    def create(self, **kwargs: Any) -> "Model":
        obj = self._rel_model.objects.create(**kwargs)
        self.add(obj)
        return obj

    # ── Async variants ────────────────────────────────────────────────────────

    async def aget_queryset(self):
        from .queryset import QuerySet

        conn = self._get_async_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        sql = self._adapt(
            f'SELECT "{tgt_col}" FROM "{through}" WHERE "{src_col}" = %s', conn
        )
        rows = await conn.execute(sql, [self.instance.pk])
        pks = [r[tgt_col] if hasattr(r, "keys") else r[0] for r in rows]
        if not pks:
            return QuerySet(self._rel_model, self._db).none()
        return QuerySet(self._rel_model, self._db).filter(pk__in=pks)

    async def aadd(self, *objs: Any, through_defaults: dict | None = None) -> None:
        conn = self._get_async_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        extra_cols = ""
        extra_phs = ""
        extra_vals: list[Any] = []
        if through_defaults:
            cols = list(through_defaults.keys())
            extra_cols = ", " + ", ".join(f'"{c}"' for c in cols)
            extra_phs = ", " + ", ".join(["%s"] * len(cols))
            extra_vals = list(through_defaults.values())

        check_sql = self._adapt(
            f'SELECT 1 FROM "{through}" WHERE "{src_col}" = %s AND "{tgt_col}" = %s LIMIT 1',
            conn,
        )
        ins_sql = self._adapt(
            f'INSERT INTO "{through}" ("{src_col}", "{tgt_col}"{extra_cols}) '
            f"VALUES (%s, %s{extra_phs})",
            conn,
        )
        for obj in objs:
            pk = obj.pk if hasattr(obj, "pk") else obj
            if not await conn.execute(check_sql, [self.instance.pk, pk]):
                await conn.execute_write(ins_sql, [self.instance.pk, pk] + extra_vals)

    async def aremove(self, *objs: Any) -> None:
        conn = self._get_async_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        sql = self._adapt(
            f'DELETE FROM "{through}" WHERE "{src_col}" = %s AND "{tgt_col}" = %s',
            conn,
        )
        for obj in objs:
            pk = obj.pk if hasattr(obj, "pk") else obj
            await conn.execute_write(sql, [self.instance.pk, pk])

    async def aset(
        self,
        objs: Any,
        *,
        clear: bool = False,
        through_defaults: dict | None = None,
    ) -> None:
        objs_list = list(objs)
        if clear:
            await self.aclear()
            await self.aadd(*objs_list, through_defaults=through_defaults)
            return
        qs = await self.aget_queryset()
        current_pks = {obj.pk async for obj in qs}
        new_pks = {o.pk if hasattr(o, "pk") else o for o in objs_list}
        to_remove = list(current_pks - new_pks)
        to_add = [pk for pk in new_pks if pk not in current_pks]
        if to_remove:
            await self.aremove(*to_remove)
        if to_add:
            await self.aadd(*to_add, through_defaults=through_defaults)

    async def aclear(self) -> None:
        conn = self._get_async_connection()
        through = self._through_table
        src_col, _ = self._through_columns
        sql = self._adapt(
            f'DELETE FROM "{through}" WHERE "{src_col}" = %s', conn
        )
        await conn.execute_write(sql, [self.instance.pk])

    async def acreate(self, **kwargs: Any) -> "Model":
        obj = await self._rel_model.objects.acreate(**kwargs)
        await self.aadd(obj)
        return obj

    def __repr__(self) -> str:
        return (
            f"<ManyRelatedManager: {self.field.model.__name__}."
            f"{self.field.name} → {self._rel_model.__name__}>"
        )


class ManyToManyDescriptor:
    """Descriptor installed on the model class for ManyToManyField access."""

    def __init__(self, field: Any) -> None:
        self.field = field

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        return ManyRelatedManager(instance, self.field)

    def __set__(self, instance: Any, value: Any) -> None:
        raise AttributeError(
            f"Direct assignment to '{self.field.name}' is not allowed. "
            "Use .set() instead."
        )


class ReverseFKManager:
    """Manager returned when accessing a reverse FK relation (e.g. author.book_set)."""

    def __init__(self, instance: Any, source_model: Any, fk_field: Any, using: str = "default") -> None:
        self.instance = instance
        self.source_model = source_model
        self.fk_field = fk_field
        self._db = using

    @property
    def _rel_name(self) -> str:
        return self.fk_field.related_name or f"{self.source_model.__name__.lower()}_set"

    def get_queryset(self):
        from .queryset import QuerySet

        cache_key = f"_prefetch_{self._rel_name}"
        if cache_key in self.instance.__dict__:
            qs: QuerySet = QuerySet(self.source_model, self._db)
            qs._result_cache = list(self.instance.__dict__[cache_key])
            return qs
        return QuerySet(self.source_model, self._db).filter(
            **{self.fk_field.name: self.instance.pk}
        )

    def all(self):
        return self.get_queryset()

    def filter(self, **kwargs: Any):
        return self.get_queryset().filter(**kwargs)

    def count(self) -> int:
        return self.get_queryset().count()

    def create(self, **kwargs: Any) -> Any:
        kwargs[self.fk_field.name] = self.instance
        return self.source_model.objects.create(**kwargs)

    def __iter__(self) -> Iterator:
        return iter(self.get_queryset())

    def __repr__(self) -> str:
        return (
            f"<ReverseFKManager: {self.source_model.__name__}."
            f"{self.fk_field.name} → {self.instance.__class__.__name__}>"
        )


class ReverseFKDescriptor:
    """Descriptor installed on the target model for reverse FK access."""

    def __init__(self, source_model: Any, fk_field: Any) -> None:
        self.source_model = source_model
        self.fk_field = fk_field

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        return ReverseFKManager(instance, self.source_model, self.fk_field)

    def __set__(self, instance: Any, value: Any) -> None:
        raise AttributeError("Direct assignment to reverse relation is not allowed.")
