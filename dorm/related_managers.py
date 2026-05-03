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

    def _invalidate_prefetch_cache(self) -> None:
        """Drop the ``_prefetch_<name>`` slot on the owning instance.

        Mutations (``add`` / ``remove`` / ``set`` / ``clear`` /
        ``create``) change the underlying through-table state.
        Without invalidation a subsequent ``manager.all()`` would
        return the stale cache populated by an earlier
        ``prefetch_related``, hiding the mutation from the
        caller's perspective.
        """
        cache_key = f"_prefetch_{self.field.name}"
        self.instance.__dict__.pop(cache_key, None)

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
        """Add *objs* to the M2M relation, skipping duplicates.

        **Batched in two queries** regardless of how many objects are passed:
        one SELECT to find which targets already exist for this source, then
        one multi-row INSERT for the missing ones. The previous "SELECT then
        INSERT per object" loop was 2 round-trips per object — adding 1000
        tags meant 2000 queries; now it's 2.

        The SELECT-then-INSERT pair runs inside an :func:`atomic`
        block: without it, two concurrent ``add()`` calls could
        both observe ``existing=∅`` and race-INSERT duplicate
        through rows (or trip the through table's UNIQUE
        constraint with an :class:`IntegrityError`).
        """
        if not objs:
            return
        target_pks = [obj.pk if hasattr(obj, "pk") else obj for obj in objs]
        # Drop dupes within the call itself while preserving order.
        seen: set = set()
        target_pks = [pk for pk in target_pks if pk not in seen and not seen.add(pk)]

        from .transaction import atomic

        conn = self._get_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns

        with atomic(using=self._db):
            existing = self._fetch_existing_targets(conn, target_pks)
            to_add = [pk for pk in target_pks if pk not in existing]
            if not to_add:
                self._invalidate_prefetch_cache()
                return

            extra_cols, extra_phs, extra_vals = self._through_defaults_sql(through_defaults)
            # Multi-row VALUES (...), (...), ... — psycopg adapts the parameter list
            # in one statement. SQLite >=3.7.11 (Python ships 3.x with it) too.
            row_phs = ", ".join([f"(%s, %s{extra_phs})"] * len(to_add))
            ins_sql = self._adapt(
                f'INSERT INTO "{through}" ("{src_col}", "{tgt_col}"{extra_cols}) '
                f"VALUES {row_phs}",
                conn,
            )
            params: list[Any] = []
            for pk in to_add:
                params.append(self.instance.pk)
                params.append(pk)
                params.extend(extra_vals)
            conn.execute_write(ins_sql, params)
        self._invalidate_prefetch_cache()

    # ── helpers shared between sync and async ────────────────────────────────

    @staticmethod
    def _through_defaults_sql(
        through_defaults: dict | None,
    ) -> tuple[str, str, list[Any]]:
        if not through_defaults:
            return "", "", []
        cols = list(through_defaults.keys())
        extra_cols = ", " + ", ".join(f'"{c}"' for c in cols)
        extra_phs = ", " + ", ".join(["%s"] * len(cols))
        extra_vals = list(through_defaults.values())
        return extra_cols, extra_phs, extra_vals

    def _fetch_existing_targets(self, conn, target_pks: list):
        """Return the set of *target_pks* already linked to ``self.instance``.

        Return type annotation is omitted intentionally: this class has a
        ``set()`` instance method, which shadows ``builtins.set`` in the
        annotation namespace and trips ty.
        """
        if not target_pks:
            return set()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        placeholders = ", ".join(["%s"] * len(target_pks))
        sql = self._adapt(
            f'SELECT "{tgt_col}" FROM "{through}" '
            f'WHERE "{src_col}" = %s AND "{tgt_col}" IN ({placeholders})',
            conn,
        )
        rows = conn.execute(sql, [self.instance.pk] + list(target_pks))
        return {r[tgt_col] if hasattr(r, "keys") else r[0] for r in rows}

    async def _afetch_existing_targets(self, conn, target_pks: list):
        # Return type annotation omitted: see _fetch_existing_targets.
        if not target_pks:
            return set()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        placeholders = ", ".join(["%s"] * len(target_pks))
        sql = self._adapt(
            f'SELECT "{tgt_col}" FROM "{through}" '
            f'WHERE "{src_col}" = %s AND "{tgt_col}" IN ({placeholders})',
            conn,
        )
        rows = await conn.execute(sql, [self.instance.pk] + list(target_pks))
        return {r[tgt_col] if hasattr(r, "keys") else r[0] for r in rows}

    def remove(self, *objs: Any) -> None:
        """Remove *objs* from the M2M relation. **Batched in one query**
        with ``DELETE ... WHERE tgt IN (...)`` instead of N per-object
        DELETEs."""
        if not objs:
            return
        target_pks = [obj.pk if hasattr(obj, "pk") else obj for obj in objs]
        conn = self._get_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        placeholders = ", ".join(["%s"] * len(target_pks))
        sql = self._adapt(
            f'DELETE FROM "{through}" WHERE "{src_col}" = %s '
            f'AND "{tgt_col}" IN ({placeholders})',
            conn,
        )
        conn.execute_write(sql, [self.instance.pk] + list(target_pks))
        self._invalidate_prefetch_cache()

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
        # ``set()`` diffs against the *live* through-table state.
        # If a previous ``add()``/``remove()`` ran without
        # invalidating the prefetch cache, ``get_queryset()``
        # would return that stale list and we'd compute the wrong
        # diff (silent INSERT/DELETE errors). Force a fresh
        # query by dropping the cache slot before the read.
        self._invalidate_prefetch_cache()
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
        self._invalidate_prefetch_cache()

    def create(self, **kwargs: Any) -> "Model":
        obj = self._rel_model.objects.create(**kwargs)
        self.add(obj)
        return obj

    # ── Async variants ────────────────────────────────────────────────────────

    async def aget_queryset(self):
        from .queryset import QuerySet

        # Mirror :meth:`get_queryset`: if the instance was the
        # target of a ``prefetch_related`` pass, the cache slot
        # holds the resolved list — return that instead of a
        # fresh DB round-trip. Without this, ``await
        # mgr.aget_queryset()`` produced an N+1 silently in async
        # code despite the user asking for prefetch.
        cache_key = f"_prefetch_{self.field.name}"
        if cache_key in self.instance.__dict__:
            qs: QuerySet = QuerySet(self._rel_model, self._db)
            qs._result_cache = list(self.instance.__dict__[cache_key])
            return qs

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
        """Async counterpart of :meth:`add`. Same 2-query batching.

        Wrapped in :func:`aatomic` to close the same SELECT-then-
        INSERT race window the sync ``add`` covers.
        """
        if not objs:
            return
        target_pks = [obj.pk if hasattr(obj, "pk") else obj for obj in objs]
        seen: set = set()
        target_pks = [pk for pk in target_pks if pk not in seen and not seen.add(pk)]

        from .transaction import aatomic

        conn = self._get_async_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns

        async with aatomic(using=self._db):
            existing = await self._afetch_existing_targets(conn, target_pks)
            to_add = [pk for pk in target_pks if pk not in existing]
            if not to_add:
                self._invalidate_prefetch_cache()
                return

            extra_cols, extra_phs, extra_vals = self._through_defaults_sql(through_defaults)
            row_phs = ", ".join([f"(%s, %s{extra_phs})"] * len(to_add))
            ins_sql = self._adapt(
                f'INSERT INTO "{through}" ("{src_col}", "{tgt_col}"{extra_cols}) '
                f"VALUES {row_phs}",
                conn,
            )
            params: list[Any] = []
            for pk in to_add:
                params.append(self.instance.pk)
                params.append(pk)
                params.extend(extra_vals)
            await conn.execute_write(ins_sql, params)
        self._invalidate_prefetch_cache()

    async def aremove(self, *objs: Any) -> None:
        """Async counterpart of :meth:`remove`. Single batched DELETE."""
        if not objs:
            return
        target_pks = [obj.pk if hasattr(obj, "pk") else obj for obj in objs]
        conn = self._get_async_connection()
        through = self._through_table
        src_col, tgt_col = self._through_columns
        placeholders = ", ".join(["%s"] * len(target_pks))
        sql = self._adapt(
            f'DELETE FROM "{through}" WHERE "{src_col}" = %s '
            f'AND "{tgt_col}" IN ({placeholders})',
            conn,
        )
        await conn.execute_write(sql, [self.instance.pk] + list(target_pks))
        self._invalidate_prefetch_cache()

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
        # Same rationale as :meth:`set`: invalidate so the diff
        # is computed from live state rather than stale prefetch
        # cache.
        self._invalidate_prefetch_cache()
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
        self._invalidate_prefetch_cache()

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

    def get_or_create(
        self, defaults: dict | None = None, **kwargs: Any
    ) -> tuple[Any, bool]:
        """``Author.book_set.get_or_create(title="X", defaults={...})`` —
        the FK back to the parent is auto-injected so the new row
        always points at ``self.instance``."""
        kwargs[self.fk_field.name] = self.instance
        return self.source_model.objects.get_or_create(
            defaults=defaults, **kwargs
        )

    def update_or_create(
        self, defaults: dict | None = None, **kwargs: Any
    ) -> tuple[Any, bool]:
        kwargs[self.fk_field.name] = self.instance
        return self.source_model.objects.update_or_create(
            defaults=defaults, **kwargs
        )

    def add(self, *objs: Any, bulk: bool = True) -> None:
        """Re-parent each *obj* under the current instance. With
        ``bulk=False`` each object is saved individually (signals
        fire); the default ``bulk=True`` issues a single UPDATE
        for all rows."""
        if not objs:
            return
        ids = [o.pk for o in objs]
        if bulk:
            self.source_model.objects.filter(pk__in=ids).update(
                **{self.fk_field.name: self.instance}
            )
            for o in objs:
                setattr(o, self.fk_field.name, self.instance)
        else:
            for o in objs:
                setattr(o, self.fk_field.name, self.instance)
                o.save()

    def remove(self, *objs: Any) -> None:
        """Disassociate each *obj* by NULL-ing the FK column. Only
        valid when the FK is nullable; otherwise a NOT NULL
        violation surfaces."""
        if not objs:
            return
        if not getattr(self.fk_field, "null", False):
            raise ValueError(
                f"Cannot remove() — {self.source_model.__name__}.{self.fk_field.name} "
                "is NOT NULL. Use ``.delete()`` to remove the rows entirely."
            )
        ids = [o.pk for o in objs]
        self.source_model.objects.filter(pk__in=ids).update(
            **{self.fk_field.name: None}
        )

    def clear(self) -> None:
        """Disassociate every related row. Same NOT NULL guard as
        :meth:`remove`."""
        if not getattr(self.fk_field, "null", False):
            raise ValueError(
                f"Cannot clear() — {self.source_model.__name__}.{self.fk_field.name} "
                "is NOT NULL. Use ``.delete()`` instead."
            )
        self.get_queryset().update(**{self.fk_field.name: None})

    def set(self, objs: list, *, bulk: bool = True, clear: bool = False) -> None:
        """Replace the related set with *objs*. ``clear=True`` runs
        :meth:`clear` first; otherwise rows already pointing at the
        parent stay put and only the diff is applied."""
        existing = {o.pk for o in self.get_queryset()}
        target = {o.pk for o in objs}
        to_add_ids = target - existing
        to_remove_ids = existing - target
        if to_add_ids:
            adds = [o for o in objs if o.pk in to_add_ids]
            self.add(*adds, bulk=bulk)
        if to_remove_ids:
            if not getattr(self.fk_field, "null", False):
                raise ValueError(
                    "set() with diff would orphan NOT NULL rows. "
                    "Use ``clear=True`` only when the FK is nullable, "
                    "or pass the full target set so nothing is removed."
                )
            self.source_model.objects.filter(
                pk__in=list(to_remove_ids)
            ).update(**{self.fk_field.name: None})

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


class ReverseOneToOneDescriptor:
    """Descriptor installed on the target model for reverse one-to-one
    access — ``OneToOneField`` is the source side, this is the
    accessor on the *target* side that returns a single related
    instance (or raises ``RelatedObjectDoesNotExist``).

    Mirrors Django's contract: ``user.profile`` returns the single
    ``Profile`` row whose ``user_id`` equals ``user.pk``, with the
    result cached on the instance to skip a re-query.
    """

    def __init__(self, source_model: Any, fk_field: Any) -> None:
        self.source_model = source_model
        self.fk_field = fk_field

    def _cache_name(self) -> str:
        return f"_o2o_cache_{self.fk_field.name}"

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        cache = self._cache_name()
        if cache in instance.__dict__:
            return instance.__dict__[cache]
        from .queryset import QuerySet

        qs = QuerySet(self.source_model, "default").filter(
            **{self.fk_field.name: instance.pk}
        )
        try:
            obj = qs.get()
        except self.source_model.DoesNotExist:
            raise self.source_model.DoesNotExist(
                f"{type(instance).__name__} has no {self.fk_field.name}."
            )
        instance.__dict__[cache] = obj
        return obj

    def __set__(self, instance: Any, value: Any) -> None:
        # Reverse-side assignment: stamp the source instance's FK
        # to point at *this* instance and persist it. Matches
        # Django's ``user.profile = some_profile`` behaviour.
        if value is None:
            instance.__dict__.pop(self._cache_name(), None)
            return
        setattr(value, self.fk_field.name, instance)
        instance.__dict__[self._cache_name()] = value
