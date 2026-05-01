"""Pluggable result-cache layer for djanorm querysets.

Two opt-in entry points:

- :func:`get_cache` returns the configured cache backend for an
  alias (``"default"`` unless overridden). Reads from
  ``settings.CACHES`` and instantiates the BACKEND class lazily so
  installing dorm without :mod:`redis` is safe.
- :meth:`dorm.QuerySet.cache` chain method opts a single queryset
  into result caching for ``timeout`` seconds.

Settings shape::

    CACHES = {
        "default": {
            "BACKEND": "dorm.cache.redis.RedisCache",
            "LOCATION": "redis://localhost:6379/0",
            "OPTIONS": {"socket_timeout": 1.0},
            # default TTL (seconds) when ``qs.cache()`` is called
            # without an explicit ``timeout``.
            "TTL": 300,
        },
    }

Configurations without ``CACHES`` (the default) leave the cache
inert: ``get_cache()`` raises :class:`ImproperlyConfigured` and
``qs.cache()`` falls back to a no-op so existing code paths stay
zero-cost.
"""

from __future__ import annotations

from typing import Any

from ..exceptions import ImproperlyConfigured

_caches: dict[str, "BaseCache"] = {}


class BaseCache:
    """Minimal cache contract every backend implements.

    All methods accept string keys and serialised bytes values; the
    queryset layer takes care of (de)serialising rows so backends
    don't have to know about model classes.
    """

    def get(self, key: str) -> bytes | None:  # pragma: no cover - interface
        raise NotImplementedError

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def delete(self, key: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def delete_pattern(self, pattern: str) -> int:  # pragma: no cover - interface
        """Bulk-evict keys matching a glob ``pattern`` (e.g.
        ``"qs:books:*"``). Returns the number of keys removed.
        Used by signal-driven invalidation to drop every cached
        queryset for a model in one call.
        """
        raise NotImplementedError

    async def aget(self, key: str) -> bytes | None:  # pragma: no cover - interface
        raise NotImplementedError

    async def aset(
        self, key: str, value: bytes, timeout: int | None = None
    ) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def adelete(self, key: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def adelete_pattern(self, pattern: str) -> int:  # pragma: no cover - interface
        raise NotImplementedError

    @property
    def default_timeout(self) -> int:
        """Fallback TTL used by ``qs.cache()`` callers that don't
        pass an explicit ``timeout``. Backends override by setting
        ``self._default_timeout`` from the ``TTL`` settings key."""
        return getattr(self, "_default_timeout", 300)


def _import_class(dotted: str) -> Any:
    module_path, _, attr = dotted.rpartition(".")
    if not module_path:
        raise ImproperlyConfigured(
            f"CACHES.BACKEND must be a dotted path; got {dotted!r}."
        )
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, attr)


def get_cache(alias: str = "default") -> BaseCache:
    """Return (constructing on first use) the cache backend for *alias*.

    Reads ``settings.CACHES`` and instantiates the BACKEND class
    with the alias's configuration. Result is memoised in this
    module so subsequent ``get_cache(alias)`` calls reuse the same
    client (Redis connection pool, in-memory dict, etc.).
    """
    if alias in _caches:
        return _caches[alias]
    from ..conf import settings

    caches = getattr(settings, "CACHES", {}) or {}
    if alias not in caches:
        raise ImproperlyConfigured(
            f"Cache alias {alias!r} is not configured. Add it to "
            "settings.CACHES — e.g. CACHES = {'default': {'BACKEND': "
            "'dorm.cache.redis.RedisCache', 'LOCATION': "
            "'redis://localhost:6379/0'}}"
        )
    cfg = caches[alias]
    backend_path = cfg.get("BACKEND")
    if not backend_path:
        raise ImproperlyConfigured(
            f"Cache {alias!r} is missing a BACKEND key."
        )
    backend_cls = _import_class(backend_path)
    cache = backend_cls(cfg)
    _caches[alias] = cache
    return cache


def reset_caches() -> None:
    """Drop every memoised backend instance.

    Called by :func:`dorm.configure` when the ``CACHES`` setting
    changes; tests can also call it directly to force a re-read.
    """
    for cache in list(_caches.values()):
        close = getattr(cache, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    _caches.clear()


def model_cache_namespace(model: Any) -> str:
    """Build the cache-key prefix shared by every queryset that
    targets ``model``. Signal-driven invalidation calls
    ``delete_pattern(f"{namespace}:*")`` after a save / delete so a
    stale row can't survive a write."""
    meta = getattr(model, "_meta", None)
    label = getattr(meta, "app_label", "") if meta else ""
    name = getattr(model, "__name__", "model")
    return f"dormqs:{label}.{name}"
