"""Auto-invalidation hooks for queryset result caches.

Connected lazily on first :meth:`QuerySet.cache` call so projects
that never opt into caching pay zero cost. The handlers fire after
every ``Model.save()`` / ``Model.delete()`` (and the matching
async variants) and call ``cache.delete_pattern(f"{namespace}:*")``
on every configured cache so a stale row can't survive a write.

Trade-off:

- Coarse-grained: a single save invalidates *every* cached
  queryset for the model, including ones that wouldn't have
  matched the new row. Fine for typical use; if you cache a
  hot list page that rebuilds on every write, consider a
  smaller TTL or a manual key scheme rather than disabling
  invalidation.
- Cross-model writes (e.g. updating a row that another model's
  cached queryset filters on) are NOT auto-invalidated — only
  the saved model is. Document this on ``qs.cache()``.
"""

from __future__ import annotations

import threading
from typing import Any

from . import get_cache, model_cache_namespace

_signals_connected: bool = False
_lock = threading.Lock()


def _drop_model(sender: Any, **_kwargs: Any) -> None:
    """Synchronous post-save / post-delete handler."""
    try:
        namespace = model_cache_namespace(sender)
        from ..conf import settings

        caches = getattr(settings, "CACHES", {}) or {}
    except Exception:
        # Settings not configured / sender has no _meta. Either
        # way, no cache to invalidate.
        return
    pattern = f"{namespace}:*"
    for alias in caches:
        try:
            backend = get_cache(alias)
            backend.delete_pattern(pattern)
        except Exception:
            # Cache is best-effort; a Redis outage must NEVER take
            # down a save. Log-and-continue is acceptable here.
            pass


async def _adrop_model(sender: Any, **_kwargs: Any) -> None:
    """Asynchronous post-save / post-delete handler."""
    try:
        namespace = model_cache_namespace(sender)
        from ..conf import settings

        caches = getattr(settings, "CACHES", {}) or {}
    except Exception:
        return
    pattern = f"{namespace}:*"
    for alias in caches:
        try:
            backend = get_cache(alias)
            await backend.adelete_pattern(pattern)
        except Exception:
            pass


def ensure_signals_connected() -> None:
    """Wire up post_save / post_delete invalidation handlers once
    per process. Safe to call from every ``qs.cache()`` site —
    repeat calls are no-ops.
    """
    global _signals_connected
    if _signals_connected:
        return
    with _lock:
        if _signals_connected:
            return
        from ..signals import post_delete, post_save

        post_save.connect(_drop_model, dispatch_uid="dorm.cache.invalidation.save")
        post_delete.connect(
            _drop_model, dispatch_uid="dorm.cache.invalidation.delete"
        )
        post_save.connect(
            _adrop_model, dispatch_uid="dorm.cache.invalidation.asave"
        )
        post_delete.connect(
            _adrop_model, dispatch_uid="dorm.cache.invalidation.adelete"
        )
        _signals_connected = True
