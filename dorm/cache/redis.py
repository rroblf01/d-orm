"""Redis cache backend for dorm querysets.

Wraps redis-py for sync calls and ``redis.asyncio`` for the async
side. Both pools are lazily constructed on first use so importing
this module without ``pip install djanorm[redis]`` is a no-op —
the :class:`RedisCache` class still loads, but instantiating it
raises :class:`ImproperlyConfigured` with the install command.

Why a single backend file:

- The serialisation contract (``bytes`` in / ``bytes`` out, opaque
  to the cache) is identical for sync and async — keeping both in
  one place means the queryset layer can serialise once and pick
  the path at runtime.
- Both clients share the same ``LOCATION`` / ``OPTIONS`` shape so
  pool tuning lives in one settings dict.
"""

from __future__ import annotations

from typing import Any

from ..exceptions import ImproperlyConfigured
from . import BaseCache


def _import_redis_sync():
    import importlib

    try:
        return importlib.import_module("redis")
    except ImportError:
        raise ImproperlyConfigured(
            "RedisCache requires redis-py. Install it via:\n"
            "    pip install 'djanorm[redis]'"
        )


def _import_redis_async():
    import importlib

    try:
        return importlib.import_module("redis.asyncio")
    except ImportError:
        raise ImproperlyConfigured(
            "RedisCache async path requires redis-py >= 4.2 with "
            "asyncio support. Install it via:\n"
            "    pip install 'djanorm[redis]'"
        )


class RedisCache(BaseCache):
    """Redis-backed cache with sync + async clients.

    Settings::

        CACHES = {
            "default": {
                "BACKEND": "dorm.cache.redis.RedisCache",
                "LOCATION": "redis://localhost:6379/0",
                "OPTIONS": {"socket_timeout": 1.0},
                "TTL": 300,
            },
        }

    ``LOCATION`` accepts any URL format ``redis-py`` understands —
    ``redis://``, ``rediss://`` (TLS), ``unix://``. ``OPTIONS`` are
    passed through to ``Redis.from_url``. ``TTL`` is the default
    expiry applied by ``qs.cache()`` when no per-call timeout is
    given.

    Connection pooling: redis-py keeps an internal pool per
    ``Redis`` instance, so a single :class:`RedisCache` is enough
    for the whole process. The async client gets its own pool;
    they can't share because the underlying socket protocol is
    blocking vs awaitable.
    """

    def __init__(self, cfg: dict) -> None:
        self.location = cfg.get("LOCATION", "redis://localhost:6379/0")
        self.options = dict(cfg.get("OPTIONS") or {})
        self._default_timeout = int(cfg.get("TTL", 300))
        self._sync_client: Any = None
        self._async_client: Any = None

    def _get_sync(self) -> Any:
        if self._sync_client is None:
            redis = _import_redis_sync()
            self._sync_client = redis.Redis.from_url(
                self.location, **self.options
            )
        return self._sync_client

    def _get_async(self) -> Any:
        if self._async_client is None:
            redis_async = _import_redis_async()
            self._async_client = redis_async.Redis.from_url(
                self.location, **self.options
            )
        return self._async_client

    # ── Sync API ─────────────────────────────────────────────────────────

    def get(self, key: str) -> bytes | None:
        client = self._get_sync()
        try:
            return client.get(key)
        except Exception:
            # A cache outage must NEVER take down the request — the
            # queryset layer falls back to a fresh DB query when
            # ``get`` returns ``None``. Swallowing here keeps the
            # cache strictly best-effort.
            return None

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
        client = self._get_sync()
        ttl = timeout if timeout is not None else self._default_timeout
        try:
            if ttl > 0:
                client.set(key, value, ex=ttl)
            else:
                client.set(key, value)
        except Exception:
            pass

    def delete(self, key: str) -> None:
        client = self._get_sync()
        try:
            client.delete(key)
        except Exception:
            pass

    def delete_pattern(self, pattern: str) -> int:
        """Walk Redis with ``SCAN`` (non-blocking) and unlink every
        matching key. Used by signal-driven invalidation to drop
        every cached queryset for a model in one call.
        """
        client = self._get_sync()
        deleted = 0
        try:
            for key in client.scan_iter(match=pattern, count=200):
                client.delete(key)
                deleted += 1
        except Exception:
            pass
        return deleted

    def close(self) -> None:
        """Release the sync client's connection pool. Called by
        :func:`dorm.cache.reset_caches` and tests that swap
        configs mid-suite."""
        client = self._sync_client
        self._sync_client = None
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    # ── Async API ────────────────────────────────────────────────────────

    async def aget(self, key: str) -> bytes | None:
        client = self._get_async()
        try:
            return await client.get(key)
        except Exception:
            return None

    async def aset(
        self, key: str, value: bytes, timeout: int | None = None
    ) -> None:
        client = self._get_async()
        ttl = timeout if timeout is not None else self._default_timeout
        try:
            if ttl > 0:
                await client.set(key, value, ex=ttl)
            else:
                await client.set(key, value)
        except Exception:
            pass

    async def adelete(self, key: str) -> None:
        client = self._get_async()
        try:
            await client.delete(key)
        except Exception:
            pass

    async def adelete_pattern(self, pattern: str) -> int:
        client = self._get_async()
        deleted = 0
        try:
            async for key in client.scan_iter(match=pattern, count=200):
                await client.delete(key)
                deleted += 1
        except Exception:
            pass
        return deleted
