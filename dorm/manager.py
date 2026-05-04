from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, Generic, Iterator, TypeVar, overload

from .models import Model

if TYPE_CHECKING:
    from .queryset import Prefetch, QuerySet, RawQuerySet, ValuesListQuerySet

_T = TypeVar("_T", bound=Model)


class BaseManager(Generic[_T]):
    auto_created = False
    creation_counter = 0
    use_in_migrations = False

    def __init__(self) -> None:
        self.model: type[Any] | None = None
        self.name: str | None = None
        self._db = "default"
        self.creation_counter = BaseManager.creation_counter
        BaseManager.creation_counter += 1

    def contribute_to_class(self, cls: type, name: str) -> None:
        self.model = cls
        self.name = name
        setattr(cls, name, ManagerDescriptor(self))
        cls._meta.managers.append(self)  # type: ignore

    def db_manager(self, using: str) -> BaseManager[_T]:
        mgr: BaseManager[_T] = self.__class__()
        mgr.model = self.model  # type: ignore[assignment]
        mgr.name = self.name
        mgr._db = using
        return mgr

    def using(self, alias: str) -> QuerySet[_T]:
        """Manager-level shortcut for ``Manager.all().using(alias)``.

        Mirrors Django's pattern where ``Model.objects.using("replica")``
        returns a queryset bound to *alias* in one call. Equivalent to
        ``Manager.get_queryset().using(alias)``."""
        return self.get_queryset().using(alias)

    def get_queryset(self) -> QuerySet[_T]:
        from .db.connection import router_db_for_read
        from .queryset import QuerySet
        assert self.model is not None
        # When the user pinned the manager via .using("alias") or
        # db_manager(), respect that. Otherwise consult DATABASE_ROUTERS
        # for read routing — supports replica fan-out with zero changes
        # to query call sites.
        alias = self._db
        if alias == "default":
            alias = router_db_for_read(self.model, default=alias)
        return QuerySet(self.model, alias)  # type: ignore[arg-type]

    # ── Proxy all QuerySet methods ────────────────────────────────────────────

    def all(self) -> QuerySet[_T]:
        return self.get_queryset().all()

    def none(self) -> QuerySet[_T]:
        return self.get_queryset().none()

    def filter(self, *args: Any, **kwargs: Any) -> QuerySet[_T]:
        return self.get_queryset().filter(*args, **kwargs)

    def exclude(self, *args: Any, **kwargs: Any) -> QuerySet[_T]:
        return self.get_queryset().exclude(*args, **kwargs)

    def order_by(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().order_by(*fields)

    def distinct(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().distinct(*fields)

    def select_related(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().select_related(*fields)

    def prefetch_related(self, *fields: "str | Prefetch") -> QuerySet[_T]:
        return self.get_queryset().prefetch_related(*fields)

    def select_for_update(
        self,
        *,
        skip_locked: bool = False,
        no_wait: bool = False,
        of: tuple[str, ...] | list[str] | None = None,
    ) -> QuerySet[_T]:
        return self.get_queryset().select_for_update(
            skip_locked=skip_locked,
            no_wait=no_wait,
            of=of,
        )

    def annotate(self, **kwargs: Any) -> QuerySet[_T]:
        return self.get_queryset().annotate(**kwargs)

    def alias(self, **kwargs: Any) -> QuerySet[_T]:
        return self.get_queryset().alias(**kwargs)

    def with_cte(self, **named_ctes: Any) -> QuerySet[_T]:
        # ``named_ctes`` accepts either a ``QuerySet`` or a ``CTE``;
        # the QuerySet method validates the actual type.
        return self.get_queryset().with_cte(**named_ctes)

    def cursor_paginate(
        self,
        *,
        after: dict[str, Any] | None = None,
        order_by: str = "pk",
        page_size: int = 50,
    ) -> Any:
        return self.get_queryset().cursor_paginate(
            after=after, order_by=order_by, page_size=page_size
        )

    async def acursor_paginate(
        self,
        *,
        after: dict[str, Any] | None = None,
        order_by: str = "pk",
        page_size: int = 50,
    ) -> Any:
        return await self.get_queryset().acursor_paginate(
            after=after, order_by=order_by, page_size=page_size
        )

    def values(self, *fields: str) -> QuerySet[Any]:
        return self.get_queryset().values(*fields)

    def values_list(self, *fields: str, flat: bool = False) -> ValuesListQuerySet:
        return self.get_queryset().values_list(*fields, flat=flat)

    def dates(self, field: str, kind: str, order: str = "ASC") -> list:
        return self.get_queryset().dates(field, kind, order)

    def datetimes(self, field: str, kind: str, order: str = "ASC") -> list:
        return self.get_queryset().datetimes(field, kind, order)

    def get(self, *args: Any, **kwargs: Any) -> _T:
        return self.get_queryset().get(*args, **kwargs)

    def get_or_none(self, *args: Any, **kwargs: Any) -> _T | None:
        return self.get_queryset().get_or_none(*args, **kwargs)

    def cache_get(
        self,
        *,
        pk: Any,
        timeout: int | None = None,
        using: str = "default",
    ) -> _T:
        """Read a single row by primary key, going through the cache
        layer first.

        Calling ``cache_get`` lazy-attaches the queryset-cache
        invalidation handlers (``post_save`` / ``post_delete`` ➜
        ``bump_model_cache_version``) — without this step, a
        subsequent ``Model.save()`` would NOT bump the per-model
        version and the row cache would return the stale snapshot.

        Lookup flow:

        1. Build a cache key namespaced by the model's app/name and
           the per-model invalidation version. A racing
           ``Model.save()`` bumps the version, so the entry written
           by an in-flight reader points at a key no later read will
           ask for — same anti-stale-read invariant the queryset
           cache uses.
        2. ``get`` → unpickle → return when present (HMAC verified
           inside :func:`dorm.cache.verify_payload`; tampered or
           unsigned blobs are dropped silently and treated as a
           miss).
        3. On miss, fall through to ``Manager.get(pk=…)``, write the
           pickle of the result back into the cache, return.

        Cache miss is silent — the database read is the source of
        truth. Cache outages also fall through; the queryset layer's
        ``try / except`` policy applies here too.
        """
        from .cache import (
            get_cache,
            model_cache_namespace,
            model_cache_version,
            sign_payload,
            verify_payload,
        )
        from .cache.invalidation import ensure_signals_connected
        from .exceptions import ImproperlyConfigured
        import pickle

        assert self.model is not None
        try:
            cache = get_cache(using)
        except ImproperlyConfigured:
            return self.get(pk=pk)
        # Wire up the post_save / post_delete invalidation hooks
        # the first time anyone uses the cache. Idempotent.
        ensure_signals_connected()

        version = model_cache_version(self.model)
        key = f"dormrow:{model_cache_namespace(self.model)}:v{version}:{pk}"

        try:
            blob = cache.get(key)
        except Exception:
            blob = None
        if blob is not None:
            payload = verify_payload(blob)
            if payload is not None:
                try:
                    return pickle.loads(payload)
                except Exception:
                    # Corrupt entry: drop and fall through to the DB.
                    try:
                        cache.delete(key)
                    except Exception:
                        pass

        instance = self.get(pk=pk)
        # Re-read the version after the DB hit so a write that landed
        # mid-fetch lands its bytes under a key nobody will read.
        version_after = model_cache_version(self.model)
        store_key = f"dormrow:{model_cache_namespace(self.model)}:v{version_after}:{pk}"
        try:
            cache.set(store_key, sign_payload(pickle.dumps(instance)), timeout)
        except Exception:
            pass
        return instance

    def cache_get_many(
        self,
        *,
        pks: list[Any],
        timeout: int | None = None,
        using: str = "default",
    ) -> dict[Any, _T]:
        """Fetch many rows by primary key, going through the cache for
        each. Misses are batched into a single ``IN (...)`` query
        instead of N round-trips, then written back to the cache.

        Returns a ``{pk: instance}`` dict — pks not found in the DB
        are simply absent.
        """
        from .cache import (
            get_cache,
            model_cache_namespace,
            model_cache_version,
            sign_payload,
            verify_payload,
        )
        from .cache.invalidation import ensure_signals_connected
        from .exceptions import ImproperlyConfigured
        import pickle

        assert self.model is not None
        if not pks:
            return {}
        try:
            cache = get_cache(using)
        except ImproperlyConfigured:
            return {obj.pk: obj for obj in self.filter(pk__in=pks)}
        ensure_signals_connected()

        version = model_cache_version(self.model)
        ns = model_cache_namespace(self.model)
        out: dict[Any, _T] = {}
        misses: list[Any] = []
        for pk in pks:
            key = f"dormrow:{ns}:v{version}:{pk}"
            try:
                blob = cache.get(key)
            except Exception:
                blob = None
            if blob is None:
                misses.append(pk)
                continue
            payload = verify_payload(blob)
            if payload is None:
                misses.append(pk)
                continue
            try:
                out[pk] = pickle.loads(payload)
            except Exception:
                try:
                    cache.delete(key)
                except Exception:
                    pass
                misses.append(pk)

        if misses:
            fetched = list(self.filter(pk__in=misses))
            version_after = model_cache_version(self.model)
            for obj in fetched:
                out[obj.pk] = obj
                store_key = f"dormrow:{ns}:v{version_after}:{obj.pk}"
                try:
                    cache.set(store_key, sign_payload(pickle.dumps(obj)), timeout)
                except Exception:
                    pass
        return out

    async def acache_get(
        self,
        *,
        pk: Any,
        timeout: int | None = None,
        using: str = "default",
    ) -> _T:
        """Async counterpart of :meth:`cache_get`."""
        from .cache import (
            get_cache,
            model_cache_namespace,
            model_cache_version,
            sign_payload,
            verify_payload,
        )
        from .cache.invalidation import ensure_signals_connected
        from .exceptions import ImproperlyConfigured
        import pickle

        assert self.model is not None
        try:
            cache = get_cache(using)
        except ImproperlyConfigured:
            return await self.aget(pk=pk)
        ensure_signals_connected()

        version = model_cache_version(self.model)
        key = f"dormrow:{model_cache_namespace(self.model)}:v{version}:{pk}"

        try:
            blob = await cache.aget(key)
        except Exception:
            blob = None
        if blob is not None:
            payload = verify_payload(blob)
            if payload is not None:
                try:
                    return pickle.loads(payload)
                except Exception:
                    try:
                        await cache.adelete(key)
                    except Exception:
                        pass

        instance = await self.aget(pk=pk)
        version_after = model_cache_version(self.model)
        store_key = f"dormrow:{model_cache_namespace(self.model)}:v{version_after}:{pk}"
        try:
            await cache.aset(store_key, sign_payload(pickle.dumps(instance)), timeout)
        except Exception:
            pass
        return instance

    async def acache_get_many(
        self,
        *,
        pks: list[Any],
        timeout: int | None = None,
        using: str = "default",
    ) -> dict[Any, _T]:
        """Async counterpart of :meth:`cache_get_many`."""
        from .cache import (
            get_cache,
            model_cache_namespace,
            model_cache_version,
            sign_payload,
            verify_payload,
        )
        from .cache.invalidation import ensure_signals_connected
        from .exceptions import ImproperlyConfigured
        import pickle

        assert self.model is not None
        if not pks:
            return {}
        try:
            cache = get_cache(using)
        except ImproperlyConfigured:
            return {
                obj.pk: obj
                async for obj in self.filter(pk__in=pks).aiterator()
            }
        ensure_signals_connected()

        version = model_cache_version(self.model)
        ns = model_cache_namespace(self.model)
        out: dict[Any, _T] = {}
        misses: list[Any] = []
        for pk in pks:
            key = f"dormrow:{ns}:v{version}:{pk}"
            try:
                blob = await cache.aget(key)
            except Exception:
                blob = None
            if blob is None:
                misses.append(pk)
                continue
            payload = verify_payload(blob)
            if payload is None:
                misses.append(pk)
                continue
            try:
                out[pk] = pickle.loads(payload)
            except Exception:
                try:
                    await cache.adelete(key)
                except Exception:
                    pass
                misses.append(pk)

        if misses:
            fetched = [
                obj async for obj in self.filter(pk__in=misses).aiterator()
            ]
            version_after = model_cache_version(self.model)
            for obj in fetched:
                out[obj.pk] = obj
                store_key = f"dormrow:{ns}:v{version_after}:{obj.pk}"
                try:
                    await cache.aset(store_key, sign_payload(pickle.dumps(obj)), timeout)
                except Exception:
                    pass
        return out

    def only(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().only(*fields)

    def defer(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().defer(*fields)

    def create(self, **kwargs: Any) -> _T:
        return self.get_queryset().create(**kwargs)

    def get_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        return self.get_queryset().get_or_create(defaults=defaults, **kwargs)

    def update_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        return self.get_queryset().update_or_create(defaults=defaults, **kwargs)

    def update(self, **kwargs: Any) -> int:
        return self.get_queryset().update(**kwargs)

    def delete(self) -> tuple[int, dict[str, int]]:
        return self.get_queryset().delete()

    def bulk_create(
        self,
        objs: list[_T],
        batch_size: int = 1000,
        *,
        ignore_conflicts: bool = False,
        update_conflicts: bool = False,
        update_fields: list[str] | None = None,
        unique_fields: list[str] | None = None,
        returning: list[str] | None = None,
    ) -> list[_T]:
        return self.get_queryset().bulk_create(
            objs,
            batch_size,
            ignore_conflicts=ignore_conflicts,
            update_conflicts=update_conflicts,
            update_fields=update_fields,
            unique_fields=unique_fields,
            returning=returning,
        )

    def bulk_update(self, objs: list[_T], fields: list[str], batch_size: int = 1000) -> int:
        return self.get_queryset().bulk_update(objs, fields, batch_size)

    def in_bulk(self, id_list: list[Any], field_name: str = "pk") -> dict[Any, _T]:
        return self.get_queryset().in_bulk(id_list, field_name)

    def count(self) -> int:
        return self.get_queryset().count()

    def exists(self) -> bool:
        return self.get_queryset().exists()

    def first(self) -> _T | None:
        return self.get_queryset().first()

    def last(self) -> _T | None:
        return self.get_queryset().last()

    def aggregate(self, **kwargs: Any) -> dict[str, Any]:
        return self.get_queryset().aggregate(**kwargs)

    def iterator(self, chunk_size: int | None = None) -> Iterator[_T]:
        return self.get_queryset().iterator(chunk_size)

    def aiterator(self, chunk_size: int | None = None) -> AsyncIterator[_T]:
        return self.get_queryset().aiterator(chunk_size)

    # ── Async proxy methods ───────────────────────────────────────────────────

    async def avalues(self, *fields: str) -> list[dict[str, Any]]:
        return await self.get_queryset().avalues(*fields)

    async def avalues_list(self, *fields: str, flat: bool = False) -> list[Any]:
        return await self.get_queryset().avalues_list(*fields, flat=flat)

    async def aget(self, *args: Any, **kwargs: Any) -> _T:
        return await self.get_queryset().aget(*args, **kwargs)

    async def aget_or_none(self, *args: Any, **kwargs: Any) -> _T | None:
        return await self.get_queryset().aget_or_none(*args, **kwargs)

    async def acreate(self, **kwargs: Any) -> _T:
        return await self.get_queryset().acreate(**kwargs)

    async def aget_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        return await self.get_queryset().aget_or_create(defaults=defaults, **kwargs)

    async def aupdate_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs: Any
    ) -> tuple[_T, bool]:
        return await self.get_queryset().aupdate_or_create(defaults=defaults, **kwargs)

    async def aupdate(self, **kwargs: Any) -> int:
        return await self.get_queryset().aupdate(**kwargs)

    async def adelete(self) -> tuple[int, dict[str, int]]:
        return await self.get_queryset().adelete()

    async def acount(self) -> int:
        return await self.get_queryset().acount()

    async def aexists(self) -> bool:
        return await self.get_queryset().aexists()

    async def afirst(self) -> _T | None:
        return await self.get_queryset().afirst()

    async def alast(self) -> _T | None:
        return await self.get_queryset().alast()

    async def abulk_create(
        self,
        objs: list[_T],
        batch_size: int = 1000,
        *,
        ignore_conflicts: bool = False,
        update_conflicts: bool = False,
        update_fields: list[str] | None = None,
        unique_fields: list[str] | None = None,
        returning: list[str] | None = None,
    ) -> list[_T]:
        return await self.get_queryset().abulk_create(
            objs,
            batch_size,
            ignore_conflicts=ignore_conflicts,
            update_conflicts=update_conflicts,
            update_fields=update_fields,
            unique_fields=unique_fields,
            returning=returning,
        )

    async def abulk_update(
        self, objs: list[_T], fields: list[str], batch_size: int = 1000
    ) -> int:
        return await self.get_queryset().abulk_update(objs, fields, batch_size)

    async def ain_bulk(self, id_list: list[Any], field_name: str = "pk") -> dict[Any, _T]:
        return await self.get_queryset().ain_bulk(id_list, field_name)

    async def aaggregate(self, **kwargs: Any) -> dict[str, Any]:
        return await self.get_queryset().aaggregate(**kwargs)

    def raw(self, sql: str, params: list[Any] | None = None) -> "RawQuerySet[_T]":
        from .queryset import RawQuerySet
        assert self.model is not None
        return RawQuerySet(self.model, sql, params, using=self._db)  # type: ignore[arg-type]

    async def araw(self, sql: str, params: list[Any] | None = None) -> list[_T]:
        return list(await self.raw(sql, params)._afetch_all())

    @classmethod
    def from_queryset(
        cls,
        queryset_class: type,
        class_name: str | None = None,
    ) -> type["BaseManager[_T]"]:
        """Build a Manager subclass that proxies methods of *queryset_class*.

        The canonical Django pattern for adding query-language methods
        to a manager. Usage::

            class PublishedQuerySet(dorm.QuerySet):
                def published(self):
                    return self.filter(is_active=True)

                def recent(self, days=30):
                    cutoff = ...
                    return self.filter(created_at__gte=cutoff)

            class Author(dorm.Model):
                ...
                objects = dorm.Manager.from_queryset(PublishedQuerySet)()

            # Now both work end-to-end:
            Author.objects.published().recent()
            Author.objects.filter(...).published()  # via get_queryset

        Mechanics: the generated subclass overrides ``get_queryset`` to
        instantiate ``queryset_class``, and reflects every public method
        on ``queryset_class`` (anything not starting with ``_``) as a
        manager-level passthrough that calls ``self.get_queryset().method(...)``.
        That mirrors how the default Manager already proxies the built-in
        QuerySet API.

        *class_name* customises the generated class's ``__name__`` for
        nicer reprs in tracebacks; defaults to ``f"{cls.__name__}From{queryset_class.__name__}"``.
        """
        from .queryset import QuerySet as _DefaultQuerySet

        if not isinstance(queryset_class, type):
            raise TypeError(
                "Manager.from_queryset(queryset_class=…) expects a class, "
                f"got {queryset_class!r}."
            )
        if not issubclass(queryset_class, _DefaultQuerySet):
            raise TypeError(
                f"{queryset_class.__name__} must subclass dorm.QuerySet."
            )

        new_name = class_name or f"{cls.__name__}From{queryset_class.__name__}"

        def get_queryset(self: "BaseManager[_T]") -> "QuerySet[_T]":
            from .db.connection import router_db_for_read
            from typing import cast as _cast

            assert self.model is not None
            alias = self._db
            if alias == "default":
                alias = router_db_for_read(self.model, default=alias)
            # ``queryset_class`` is statically a ``type[QuerySet]``
            # subclass; ty's narrowing loses the model parameter, so
            # the call site needs an explicit cast for ``self.model``.
            return queryset_class(_cast(Any, self.model), alias)

        attrs: dict[str, Any] = {"get_queryset": get_queryset}

        # Reflect each public queryset method onto the manager. We
        # only skip names that come from ``object`` itself, dunders,
        # and private names — when the user's QuerySet subclass
        # overrides a method that BaseManager also exposes (e.g.
        # ``count``, ``filter``, ``update``), the override MUST
        # reach the manager. Previously ``existing = set(dir(cls))``
        # included every BaseManager proxy and silently shadowed
        # the user's QS overrides.
        object_names = set(dir(object))
        # Methods declared directly on the user's queryset_class
        # (not inherited from QuerySet base) are always reflected
        # — even when their name collides with a BaseManager proxy.
        own = {
            n for n in vars(queryset_class)
            if not n.startswith("_")
        }
        for name in dir(queryset_class):
            if name.startswith("_") or name in object_names:
                continue
            attr = getattr(queryset_class, name, None)
            if not callable(attr):
                continue
            # Skip an inherited-from-QuerySet method only when
            # BaseManager *and* a parent class already proxy it.
            # Custom overrides (declared on ``queryset_class`` itself)
            # always win.
            if name not in own and hasattr(cls, name):
                continue
            # Closure capture by default kwarg so each generated proxy
            # binds *its own* queryset method name.
            def _proxy(self: "BaseManager[_T]", *args: Any, _name: str = name, **kwargs: Any) -> Any:
                return getattr(self.get_queryset(), _name)(*args, **kwargs)
            _proxy.__name__ = name
            _proxy.__qualname__ = f"{new_name}.{name}"
            attrs[name] = _proxy

        return type(new_name, (cls,), attrs)


class Manager(BaseManager[_T]):
    pass


class ManagerDescriptor(Generic[_T]):
    """Descriptor that exposes a Manager on the model class only.

    Generic in the model type so static type checkers see
    ``Author.objects`` as ``BaseManager[Author]`` (instead of
    ``BaseManager[Any]``), preserving the row type through queryset
    chains: ``Author.objects.filter(...).first()`` is typed as
    ``Author | None``.
    """

    def __init__(self, manager: BaseManager[_T]) -> None:
        self.manager = manager

    @overload
    def __get__(self, instance: None, cls: type[_T]) -> BaseManager[_T]: ...
    @overload
    def __get__(self, instance: _T, cls: type[_T]) -> "ManagerDescriptor[_T]": ...
    def __get__(self, instance: Any, cls: type | None = None) -> Any:
        if instance is not None:
            raise AttributeError("Manager isn't accessible via model instances")
        return self.manager
