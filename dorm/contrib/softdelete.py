"""Soft-delete mixin: replace ``DELETE FROM`` with ``UPDATE ... SET deleted_at = now()``.

Usage::

    from dorm.contrib.softdelete import SoftDeleteModel

    class Article(SoftDeleteModel):
        title = dorm.CharField(max_length=200)

    # Default queries skip soft-deleted rows automatically.
    Article.objects.filter(...)            # WHERE deleted_at IS NULL
    Article.all_objects.filter(...)        # includes soft-deleted rows
    Article.deleted_objects.filter(...)    # only soft-deleted rows

    # delete() / adelete() set deleted_at instead of running DELETE.
    art = Article.objects.first()
    art.delete()                           # UPDATE ... SET deleted_at = now()
    art.delete(hard=True)                  # real DELETE (e.g. GDPR purge)

The mixin is an *abstract* model — inherit from it instead of
``dorm.Model``. Manager attributes (``objects``, ``all_objects``,
``deleted_objects``) are installed automatically by the metaclass.

**Caveats:**

- Soft delete and ``on_delete=CASCADE`` interact subtly: a soft delete
  on the parent does *not* cascade to children (children remain
  visible to ``objects``). If you need cascading soft deletes, override
  :meth:`SoftDeleteModel.delete` to walk relations explicitly.
- Database-level uniqueness constraints don't know about
  ``deleted_at``: a unique column will reject re-inserting a value that
  matches a soft-deleted row. Use a partial index
  (``UNIQUE … WHERE deleted_at IS NULL``) at the schema level if you
  need "unique among live rows only".
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from .. import fields
from ..manager import Manager
from ..models import Model


class SoftDeleteManager(Manager):
    """Default manager for :class:`SoftDeleteModel`. Filters out rows
    where ``deleted_at`` is non-NULL on every queryset construction.

    Override :meth:`get_queryset` in subclasses if you need a different
    default filter (e.g. multi-tenant scoping)."""

    def get_queryset(self):
        return super().get_queryset().filter(deleted_at__isnull=True)


class _AllObjectsManager(Manager):
    """Manager that returns *every* row, including soft-deleted ones.

    Use sparingly — most call sites should use the default manager so
    soft-deleted rows stay invisible. Common legitimate uses: admin /
    audit dashboards, GDPR export, hard-delete jobs.
    """


class _DeletedObjectsManager(Manager):
    """Manager that returns *only* soft-deleted rows. Useful for restore
    flows and "trash" views.
    """

    def get_queryset(self):
        return super().get_queryset().filter(deleted_at__isnull=False)


class SoftDeleteModel(Model):
    """Abstract model that swaps the default delete behaviour for a
    timestamp-based soft delete. Concrete subclasses inherit:

    - ``deleted_at`` :class:`~dorm.fields.DateTimeField` (nullable, indexed).
    - ``objects`` — :class:`SoftDeleteManager`, hides soft-deleted rows.
    - ``all_objects`` — every row.
    - ``deleted_objects`` — only soft-deleted rows.
    - :meth:`delete` / :meth:`adelete` accept ``hard=True`` to bypass
      the soft path and issue an actual ``DELETE``.
    """

    deleted_at = fields.DateTimeField(null=True, blank=True, db_index=True)

    # Three managers; the metaclass installs the descriptor for each.
    objects = SoftDeleteManager()
    all_objects = _AllObjectsManager()
    deleted_objects = _DeletedObjectsManager()

    class Meta:
        abstract = True

    def delete(self, using: str = "default", *, hard: bool = False) -> Any:
        """Mark this row as soft-deleted by setting ``deleted_at`` to
        ``utcnow()``. Pass ``hard=True`` to bypass the soft path and
        delete the row for real (e.g. GDPR purge).
        """
        if hard:
            return super().delete(using=using)
        if self.deleted_at is None:
            self.deleted_at = _dt.datetime.now(_dt.timezone.utc)
            self.save(using=using)
        return 1, {f"{self._meta.app_label}.{type(self).__name__}": 1}

    async def adelete(self, using: str = "default", *, hard: bool = False) -> Any:
        """Async counterpart of :meth:`delete`."""
        if hard:
            return await super().adelete(using=using)
        if self.deleted_at is None:
            self.deleted_at = _dt.datetime.now(_dt.timezone.utc)
            await self.asave(using=using)
        return 1, {f"{self._meta.app_label}.{type(self).__name__}": 1}

    def restore(self, using: str = "default") -> None:
        """Undo a previous soft delete by clearing ``deleted_at``. No-op
        if the row was never soft-deleted."""
        if self.deleted_at is not None:
            self.deleted_at = None
            self.save(using=using)

    async def arestore(self, using: str = "default") -> None:
        """Async counterpart of :meth:`restore`."""
        if self.deleted_at is not None:
            self.deleted_at = None
            await self.asave(using=using)


__all__ = [
    "SoftDeleteManager",
    "SoftDeleteModel",
]
