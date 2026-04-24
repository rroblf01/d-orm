from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .models import Model

if TYPE_CHECKING:
    from .queryset import QuerySet, ValuesListQuerySet

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

    def get_queryset(self) -> QuerySet[_T]:
        from .queryset import QuerySet
        assert self.model is not None
        return QuerySet(self.model, self._db)  # type: ignore[arg-type]

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

    def distinct(self) -> QuerySet[_T]:
        return self.get_queryset().distinct()

    def select_related(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().select_related(*fields)

    def prefetch_related(self, *fields: str) -> QuerySet[_T]:
        return self.get_queryset().prefetch_related(*fields)

    def select_for_update(self) -> QuerySet[_T]:
        return self.get_queryset().select_for_update()

    def annotate(self, **kwargs: Any) -> QuerySet[_T]:
        return self.get_queryset().annotate(**kwargs)

    def values(self, *fields: str) -> QuerySet[Any]:
        return self.get_queryset().values(*fields)

    def values_list(self, *fields: str, flat: bool = False) -> ValuesListQuerySet:
        return self.get_queryset().values_list(*fields, flat=flat)

    def get(self, *args: Any, **kwargs: Any) -> _T:
        return self.get_queryset().get(*args, **kwargs)

    def get_or_none(self, *args: Any, **kwargs: Any) -> _T | None:
        return self.get_queryset().get_or_none(*args, **kwargs)

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

    def bulk_create(self, objs: list[_T], batch_size: int = 1000) -> list[_T]:
        return self.get_queryset().bulk_create(objs, batch_size)

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

    async def abulk_create(self, objs: list[_T], batch_size: int = 1000) -> list[_T]:
        return await self.get_queryset().abulk_create(objs, batch_size)

    async def ain_bulk(self, id_list: list[Any], field_name: str = "pk") -> dict[Any, _T]:
        return await self.get_queryset().ain_bulk(id_list, field_name)

    async def aaggregate(self, **kwargs: Any) -> dict[str, Any]:
        return await self.get_queryset().aaggregate(**kwargs)


class Manager(BaseManager[_T]):
    pass


class ManagerDescriptor:
    def __init__(self, manager: BaseManager[Any]) -> None:
        self.manager = manager

    def __get__(self, instance: Any, cls: type | None = None) -> BaseManager[Any]:
        if instance is not None:
            raise AttributeError("Manager isn't accessible via model instances")
        return self.manager
