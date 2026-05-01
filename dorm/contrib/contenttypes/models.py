"""``ContentType`` registry: one row per model class, keyed by
``(app_label, model)``.

The :class:`ContentType` table is small and its rows are immutable in
practice — once a model is registered, its ``ContentType`` row stays the
same across deploys. The manager caches lookups so polymorphic queries
don't pay a round-trip per access.
"""

from __future__ import annotations

from typing import Any, ClassVar

from ... import fields
from ...manager import Manager
from ...models import Model, _model_registry


class ContentTypeManager(Manager):
    """Manager that memoises ``(app_label, model)`` → ``ContentType`` so
    repeated polymorphic lookups (``GenericForeignKey``, generic
    relations) hit the database at most once per process per model.

    The cache is local to the manager instance — call
    :meth:`clear_cache` from tests when models are recreated mid-run."""

    _cache: ClassVar[dict[tuple[str, str], "ContentType"]] = {}

    def get_for_model(self, model: type[Model]) -> "ContentType":
        """Return (creating if missing) the :class:`ContentType` row for
        ``model``. Result is memoised."""
        opts = model._meta
        key = (opts.app_label, opts.model_name)
        if key in self._cache:
            return self._cache[key]
        ct, _ = self.get_or_create(
            app_label=opts.app_label,
            model=opts.model_name,
        )
        self._cache[key] = ct
        return ct

    async def aget_for_model(self, model: type[Model]) -> "ContentType":
        """Async counterpart of :meth:`get_for_model`."""
        opts = model._meta
        key = (opts.app_label, opts.model_name)
        if key in self._cache:
            return self._cache[key]
        ct, _ = await self.aget_or_create(
            app_label=opts.app_label,
            model=opts.model_name,
        )
        self._cache[key] = ct
        return ct

    def get_for_id(self, ct_id: int) -> "ContentType":
        """Convenience for descriptor code paths that already have an
        integer ``content_type_id`` and don't want to repeat the
        ``get(pk=…)`` boilerplate. On ``DoesNotExist`` we drop any
        cached entry that pointed at the missing pk before re-raising
        — the cache is now stale (table likely truncated +
        re-migrated mid-process) and the next ``get_for_model`` call
        must re-fetch instead of returning the dangling row."""
        try:
            return self.get(pk=ct_id)
        except ContentType.DoesNotExist:
            self._evict_pk(ct_id)
            raise

    async def aget_for_id(self, ct_id: int) -> "ContentType":
        try:
            return await self.aget(pk=ct_id)
        except ContentType.DoesNotExist:
            self._evict_pk(ct_id)
            raise

    def _evict_pk(self, ct_id: int) -> None:
        for k, v in list(self._cache.items()):
            if v.pk == ct_id:
                del self._cache[k]

    def clear_cache(self) -> None:
        """Drop the in-memory ``(app_label, model)`` cache. Useful in
        tests that recreate models or truncate the table between runs."""
        self._cache.clear()


class ContentType(Model):
    """One row per registered model. Identified by ``(app_label,
    model)``; the pair is unique. ``model`` is stored lower-cased to
    match :attr:`Options.model_name`.

    Don't instantiate directly — use
    :meth:`ContentTypeManager.get_for_model` so the cache stays hot."""

    app_label = fields.CharField(max_length=100)
    model = fields.CharField(max_length=100, verbose_name="python model class name")

    objects: ClassVar[ContentTypeManager] = ContentTypeManager()

    class Meta:
        db_table = "django_content_type"
        unique_together = [("app_label", "model")]
        ordering = ["app_label", "model"]

    def __str__(self) -> str:
        return f"{self.app_label} | {self.model}"

    def __repr__(self) -> str:
        return f"<ContentType: {self.app_label}.{self.model}>"

    def model_class(self) -> type[Model] | None:
        """Resolve the Python class this row points at. Returns ``None``
        if the model is no longer registered (e.g. removed app)."""
        full_key = f"{self.app_label}.{self._capitalize_or_lookup()}"
        if full_key in _model_registry:
            return _model_registry[full_key]
        for k, cls in _model_registry.items():
            if "." not in k:
                continue
            label, name = k.rsplit(".", 1)
            if label == self.app_label and name.lower() == self.model:
                return cls
        return None

    def _capitalize_or_lookup(self) -> str:
        for k in _model_registry:
            if "." in k:
                label, name = k.rsplit(".", 1)
                if label == self.app_label and name.lower() == self.model:
                    return name
        return self.model

    def get_object_for_this_type(self, **kwargs: Any) -> Model:
        """Fetch one row of the underlying model. Raises
        :class:`~dorm.DoesNotExist` if absent. Equivalent to
        ``self.model_class().objects.get(**kwargs)``."""
        cls = self.model_class()
        if cls is None:
            raise LookupError(
                f"Model {self.app_label}.{self.model} is not registered"
            )
        return cls.objects.get(**kwargs)

    async def aget_object_for_this_type(self, **kwargs: Any) -> Model:
        """Async counterpart of :meth:`get_object_for_this_type`."""
        cls = self.model_class()
        if cls is None:
            raise LookupError(
                f"Model {self.app_label}.{self.model} is not registered"
            )
        return await cls.objects.aget(**kwargs)


__all__ = ["ContentType", "ContentTypeManager"]
