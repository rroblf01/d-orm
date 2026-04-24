from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


class BaseManager:
    auto_created = False
    creation_counter = 0
    use_in_migrations = False

    def __init__(self):
        self.model = None
        self.name = None
        self._db = "default"
        self.creation_counter = BaseManager.creation_counter
        BaseManager.creation_counter += 1

    def contribute_to_class(self, cls, name: str):
        self.model = cls
        self.name = name
        setattr(cls, name, ManagerDescriptor(self))
        cls._meta.managers.append(self)

    def db_manager(self, using: str) -> "BaseManager":
        mgr = self.__class__()
        mgr.model = self.model
        mgr.name = self.name
        mgr._db = using
        return mgr

    def get_queryset(self):
        from .queryset import QuerySet
        return QuerySet(self.model, self._db)

    # ── Proxy all QuerySet methods ────────────────────────────────────────────

    def all(self):
        return self.get_queryset().all()

    def none(self):
        return self.get_queryset().none()

    def filter(self, *args, **kwargs):
        return self.get_queryset().filter(*args, **kwargs)

    def exclude(self, *args, **kwargs):
        return self.get_queryset().exclude(*args, **kwargs)

    def get(self, *args, **kwargs) -> Any:
        return self.get_queryset().get(*args, **kwargs)

    def create(self, **kwargs) -> Any:
        return self.get_queryset().create(**kwargs)

    def get_or_create(self, defaults=None, **kwargs) -> tuple[Any, bool]:
        return self.get_queryset().get_or_create(defaults=defaults, **kwargs)

    def update_or_create(self, defaults=None, **kwargs) -> tuple[Any, bool]:
        return self.get_queryset().update_or_create(defaults=defaults, **kwargs)

    def update(self, **kwargs) -> int:
        return self.get_queryset().update(**kwargs)

    def delete(self) -> tuple[int, dict]:
        return self.get_queryset().delete()

    def bulk_create(self, objs: list, batch_size: int = 1000) -> list:
        return self.get_queryset().bulk_create(objs, batch_size)

    def bulk_update(self, objs: list, fields: list[str], batch_size: int = 1000) -> int:
        return self.get_queryset().bulk_update(objs, fields, batch_size)

    def in_bulk(self, id_list: list, field_name: str = "pk") -> dict:
        return self.get_queryset().in_bulk(id_list, field_name)

    def count(self) -> int:
        return self.get_queryset().count()

    def exists(self) -> bool:
        return self.get_queryset().exists()

    def first(self) -> Any | None:
        return self.get_queryset().first()

    def last(self) -> Any | None:
        return self.get_queryset().last()

    def order_by(self, *fields: str):
        return self.get_queryset().order_by(*fields)

    def values(self, *fields: str):
        return self.get_queryset().values(*fields)

    def values_list(self, *fields: str, flat: bool = False):
        return self.get_queryset().values_list(*fields, flat=flat)

    def annotate(self, **kwargs):
        return self.get_queryset().annotate(**kwargs)

    def aggregate(self, **kwargs) -> dict:
        return self.get_queryset().aggregate(**kwargs)

    def distinct(self):
        return self.get_queryset().distinct()

    def select_related(self, *fields: str):
        return self.get_queryset().select_related(*fields)

    def prefetch_related(self, *fields: str):
        return self.get_queryset().prefetch_related(*fields)

    def select_for_update(self):
        return self.get_queryset().select_for_update()

    # ── Async proxy methods ───────────────────────────────────────────────────

    async def aget(self, *args, **kwargs) -> Any:
        return await self.get_queryset().aget(*args, **kwargs)

    async def acreate(self, **kwargs) -> Any:
        return await self.get_queryset().acreate(**kwargs)

    async def aget_or_create(self, defaults=None, **kwargs) -> tuple[Any, bool]:
        return await self.get_queryset().aget_or_create(defaults=defaults, **kwargs)

    async def aupdate_or_create(self, defaults=None, **kwargs) -> tuple[Any, bool]:
        return await self.get_queryset().aupdate_or_create(defaults=defaults, **kwargs)

    async def aupdate(self, **kwargs) -> int:
        return await self.get_queryset().aupdate(**kwargs)

    async def adelete(self) -> tuple[int, dict]:
        return await self.get_queryset().adelete()

    async def acount(self) -> int:
        return await self.get_queryset().acount()

    async def aexists(self) -> bool:
        return await self.get_queryset().aexists()

    async def afirst(self) -> Any | None:
        return await self.get_queryset().afirst()

    async def alast(self) -> Any | None:
        return await self.get_queryset().alast()

    async def abulk_create(self, objs: list, batch_size: int = 1000) -> list:
        return await self.get_queryset().abulk_create(objs, batch_size)

    async def ain_bulk(self, id_list: list, field_name: str = "pk") -> dict:
        return await self.get_queryset().ain_bulk(id_list, field_name)

    async def aaggregate(self, **kwargs) -> dict:
        return await self.get_queryset().aaggregate(**kwargs)


class Manager(BaseManager):
    pass


class ManagerDescriptor:
    def __init__(self, manager: BaseManager):
        self.manager = manager

    def __get__(self, instance, cls=None):
        if instance is not None:
            raise AttributeError("Manager isn't accessible via model instances")
        return self.manager
