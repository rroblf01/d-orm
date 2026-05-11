"""Row-level cache with automatic invalidation.

Wraps :meth:`Manager.get(pk=...)` with a process-local LRU. Writes
through :meth:`Model.save` / :meth:`Model.delete` (and async siblings)
fire ``post_save`` / ``post_delete`` signals — the row cache listens
on both and invalidates the affected key.

Use case: tight read loops that fetch the same row repeatedly
(per-request authentication lookups, feature flags, settings tables)
without paying for Redis. Bounded by *maxsize* per model so a hot
table can't evict everything else.

Usage::

    from dorm.contrib.row_cache import RowCache

    cache = RowCache(User, maxsize=1000)

    user = cache.get(42)  # SELECT once, cached thereafter
    cache.invalidate(42)  # explicit purge
    cache.clear()         # nuke everything

The cache also wires automatically into ``post_save`` / ``post_delete``
so manual invalidation is rarely needed — the explicit hooks exist for
test fixtures and out-of-band writes (raw SQL, bulk operations).
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Generic, TypeVar

from .. import signals
from ..models import Model

_T = TypeVar("_T", bound=Model)


class RowCache(Generic[_T]):
    """Per-model LRU cache keyed by primary key.

    Args:
        model_cls: the model class to cache.
        maxsize: per-cache row ceiling. The LRU evicts the
            least-recently-accessed entry when full.
        invalidate_on_write: when True (default), the cache
            subscribes to ``post_save`` / ``post_delete`` and purges
            the affected key automatically.
    """

    def __init__(
        self,
        model_cls: type[_T],
        *,
        maxsize: int = 1024,
        invalidate_on_write: bool = True,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self._model = model_cls
        self._maxsize = maxsize
        self._store: OrderedDict[Any, _T] = OrderedDict()
        self._lock = threading.Lock()
        self._dispatch_uid = f"row-cache-{id(self)}"
        if invalidate_on_write:
            signals.post_save.connect(
                self._on_save,
                sender=model_cls,
                weak=False,
                dispatch_uid=f"{self._dispatch_uid}-save",
            )
            signals.post_delete.connect(
                self._on_delete,
                sender=model_cls,
                weak=False,
                dispatch_uid=f"{self._dispatch_uid}-delete",
            )

    def get(self, pk: Any) -> _T | None:
        with self._lock:
            inst = self._store.get(pk)
            if inst is not None:
                self._store.move_to_end(pk)
                return inst
        # Cache miss — fetch via the ORM.
        try:
            inst = self._model.objects.get(pk=pk)  # type: ignore[attr-defined]
        except self._model.DoesNotExist:  # type: ignore[attr-defined]
            return None
        self._put(pk, inst)
        return inst

    def _put(self, pk: Any, inst: _T) -> None:
        with self._lock:
            self._store[pk] = inst
            self._store.move_to_end(pk)
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)

    def invalidate(self, pk: Any) -> None:
        with self._lock:
            self._store.pop(pk, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        return len(self._store)

    # ── Signal receivers ───────────────────────────────────────────────────

    def _on_save(self, sender, instance, **_kwargs):
        pk = getattr(instance, self._model._meta.pk.attname, None)  # type: ignore[attr-defined]
        if pk is not None:
            self.invalidate(pk)

    def _on_delete(self, sender, instance, **_kwargs):
        pk = getattr(instance, self._model._meta.pk.attname, None)  # type: ignore[attr-defined]
        if pk is not None:
            self.invalidate(pk)

    # ── Cleanup ────────────────────────────────────────────────────────────

    def detach(self) -> None:
        """Disconnect signal receivers. Useful in tests that re-create
        the cache between runs to avoid stacking listeners."""
        for suffix in ("save", "delete"):
            try:
                getattr(signals, f"post_{suffix}").disconnect(
                    dispatch_uid=f"{self._dispatch_uid}-{suffix}"
                )
            except Exception:  # pragma: no cover
                pass


__all__ = ["RowCache"]
