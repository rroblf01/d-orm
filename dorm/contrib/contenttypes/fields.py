"""Generic relation field types: ``GenericForeignKey`` and
``GenericRelation``.

A :class:`GenericForeignKey` is a *virtual* field — it doesn't get its
own column. It composes two real columns the model already declares
(``content_type`` FK + ``object_id`` integer) into a single descriptor
that returns a polymorphic instance::

    class Tag(dorm.Model):
        label = dorm.CharField(max_length=50)
        content_type = dorm.ForeignKey(ContentType, on_delete=dorm.CASCADE)
        object_id = dorm.PositiveIntegerField()
        target = GenericForeignKey('content_type', 'object_id')

A :class:`GenericRelation` is the *reverse* accessor placed on the
model being pointed at::

    class Article(dorm.Model):
        title = dorm.CharField(max_length=200)
        tags = GenericRelation(Tag)
"""

from __future__ import annotations

from typing import Any, ClassVar

from ...fields import Field
from ...models import Model


class GenericForeignKey(Field[Any]):
    """Virtual FK pointing at any model via (``content_type``,
    ``object_id``).

    The two backing columns are regular fields on the host model — this
    descriptor only orchestrates them. On read it loads the
    :class:`ContentType` row, resolves it to a model class, then
    ``get(pk=object_id)``. On write it expects a model instance and
    sets ``content_type`` + ``object_id`` from it (or clears both for
    ``None``).
    """

    concrete = False
    many_to_one = True
    auto_created = False

    def __init__(
        self,
        ct_field: str = "content_type",
        fk_field: str = "object_id",
        for_concrete_model: bool = True,
    ) -> None:
        self.ct_field = ct_field
        self.fk_field = fk_field
        self.for_concrete_model = for_concrete_model
        super().__init__(null=True, blank=True, editable=False, serialize=False)
        self.concrete = False
        self.many_to_one = True

    def contribute_to_class(self, cls: Any, name: str) -> None:
        self.name = name
        self.attname = name
        self.column = None
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)
        setattr(cls, name, self)

    def db_type(self, connection: Any) -> None:
        return None

    def get_cache_name(self) -> str:
        return f"_cache_{self.name}"

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        cache = self.get_cache_name()
        if cache in instance.__dict__:
            return instance.__dict__[cache]
        ct_id = instance.__dict__.get(f"{self.ct_field}_id")
        obj_id = instance.__dict__.get(self.fk_field)
        if ct_id is None or obj_id is None:
            return None
        from .models import ContentType

        ct = ContentType.objects.get_for_id(ct_id)
        model = ct.model_class()
        if model is None:
            return None
        try:
            obj = model.objects.get(pk=obj_id)
        except model.DoesNotExist:
            return None
        instance.__dict__[cache] = obj
        return obj

    def __set__(self, instance: Any, value: Any) -> None:
        cache = self.get_cache_name()
        if value is None:
            instance.__dict__[f"{self.ct_field}_id"] = None
            instance.__dict__.pop(f"_cache_{self.ct_field}", None)
            instance.__dict__[self.fk_field] = None
            instance.__dict__.pop(cache, None)
            return
        from .models import ContentType

        ct = ContentType.objects.get_for_model(type(value))
        instance.__dict__[f"{self.ct_field}_id"] = ct.pk
        instance.__dict__.pop(f"_cache_{self.ct_field}", None)
        instance.__dict__[self.fk_field] = value.pk
        instance.__dict__[cache] = value

    async def aget(self, instance: Any) -> Any:
        """Async resolver — same job as descriptor read but uses async
        managers. Call this directly when you're already in async code:
        ``obj = await Tag.target.aget(tag_instance)``."""
        cache = self.get_cache_name()
        if cache in instance.__dict__:
            return instance.__dict__[cache]
        ct_id = instance.__dict__.get(f"{self.ct_field}_id")
        obj_id = instance.__dict__.get(self.fk_field)
        if ct_id is None or obj_id is None:
            return None
        from .models import ContentType

        ct = await ContentType.objects.aget_for_id(ct_id)
        model = ct.model_class()
        if model is None:
            return None
        try:
            obj = await model.objects.aget(pk=obj_id)
        except model.DoesNotExist:
            return None
        instance.__dict__[cache] = obj
        return obj


class _GenericRelatedManager:
    """Manager-like object returned by a :class:`GenericRelation`
    descriptor when accessed on an instance.

    Mirrors a reverse FK manager: ``article.tags.all()``,
    ``article.tags.filter(...)``, ``article.tags.create(...)`` all work
    and automatically narrow to ``content_type=ct_for_article,
    object_id=article.pk``."""

    def __init__(
        self,
        related_model: type[Model],
        instance: Model,
        ct_field: str,
        fk_field: str,
    ) -> None:
        self.related_model = related_model
        self.instance = instance
        self.ct_field = ct_field
        self.fk_field = fk_field

    def _ct_filter(self) -> dict[str, Any]:
        from .models import ContentType

        ct = ContentType.objects.get_for_model(type(self.instance))
        return {f"{self.ct_field}_id": ct.pk, self.fk_field: self.instance.pk}

    async def _act_filter(self) -> dict[str, Any]:
        from .models import ContentType

        ct = await ContentType.objects.aget_for_model(type(self.instance))
        return {f"{self.ct_field}_id": ct.pk, self.fk_field: self.instance.pk}

    def get_queryset(self) -> Any:
        return self.related_model.objects.filter(**self._ct_filter())

    def all(self) -> Any:
        return self.get_queryset()

    def filter(self, *args: Any, **kwargs: Any) -> Any:
        return self.get_queryset().filter(*args, **kwargs)

    def exclude(self, *args: Any, **kwargs: Any) -> Any:
        return self.get_queryset().exclude(*args, **kwargs)

    def count(self) -> int:
        return self.get_queryset().count()

    def exists(self) -> bool:
        return self.get_queryset().exists()

    def first(self) -> Any:
        return self.get_queryset().first()

    def create(self, **kwargs: Any) -> Model:
        kwargs.update(self._ct_filter())
        kwargs.pop(f"{self.ct_field}_id", None)
        from .models import ContentType

        ct = ContentType.objects.get_for_model(type(self.instance))
        kwargs[self.ct_field] = ct
        kwargs[self.fk_field] = self.instance.pk
        return self.related_model.objects.create(**kwargs)

    async def acreate(self, **kwargs: Any) -> Model:
        from .models import ContentType

        ct = await ContentType.objects.aget_for_model(type(self.instance))
        kwargs[self.ct_field] = ct
        kwargs[self.fk_field] = self.instance.pk
        return await self.related_model.objects.acreate(**kwargs)

    def add(self, *objs: Model) -> None:
        from .models import ContentType

        ct = ContentType.objects.get_for_model(type(self.instance))
        for obj in objs:
            setattr(obj, self.ct_field, ct)
            setattr(obj, self.fk_field, self.instance.pk)
            obj.save()


class GenericRelation(Field[Any]):
    """Reverse-side descriptor for a :class:`GenericForeignKey`.

    Place on the *target* model (the one being pointed at). On instance
    access, returns a manager scoped to ``content_type=ct_for_self,
    object_id=self.pk``. The descriptor adds no column — ``db_type``
    returns ``None`` so the migration writer skips it."""

    concrete = False
    one_to_many = True
    auto_created = False

    def __init__(
        self,
        to: type[Model] | str,
        content_type_field: str = "content_type",
        object_id_field: str = "object_id",
        related_query_name: str | None = None,
    ) -> None:
        self.to = to
        self.content_type_field = content_type_field
        self.object_id_field = object_id_field
        self.related_query_name = related_query_name
        super().__init__(null=True, blank=True, editable=False, serialize=False)
        self.concrete = False
        self.one_to_many = True

    def contribute_to_class(self, cls: Any, name: str) -> None:
        self.name = name
        self.attname = name
        self.column = None
        self.model = cls
        if self.verbose_name is None:
            self.verbose_name = name.replace("_", " ")
        cls._meta.add_field(self)
        setattr(cls, name, self)

    def db_type(self, connection: Any) -> None:
        return None

    def _resolve_related(self) -> type[Model]:
        if isinstance(self.to, str):
            from ...models import _model_registry

            if self.to in _model_registry:
                return _model_registry[self.to]
            raise LookupError(f"GenericRelation target {self.to!r} is not registered")
        return self.to

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        return _GenericRelatedManager(
            self._resolve_related(),
            instance,
            self.content_type_field,
            self.object_id_field,
        )


__all__ = ["GenericForeignKey", "GenericRelation"]
