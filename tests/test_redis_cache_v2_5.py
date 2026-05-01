"""Tests for the v2.5 Redis result-cache layer.

The redis-py client is gated behind ``pip install djanorm[redis]``.
Tests that need a live server use ``fakeredis`` (added to ``[dev]``)
when available; otherwise they fall through to a tiny in-process
fake that satisfies the ``BaseCache`` contract.

Coverage:

- ``QuerySet.cache(timeout=…)`` returns a clone with the alias and
  TTL stamped on it.
- Cached results round-trip without a DB hit on the second call.
- Auto-invalidation: ``Model.save()`` evicts every cached
  queryset for the model class.
- Cache outages never propagate — a broken backend silently
  falls through to the live query.
"""

from __future__ import annotations

import pickle
from typing import Any
from unittest.mock import patch

import pytest

from dorm.cache import BaseCache, get_cache, model_cache_namespace, reset_caches
from tests.models import Author


class _MemCache(BaseCache):
    """Tiny in-process backend used as a stand-in for Redis. The
    queryset layer doesn't care about the underlying store — only
    the bytes-in / bytes-out contract."""

    def __init__(self, cfg: dict | None = None) -> None:
        self._store: dict[str, bytes] = {}
        self._default_timeout = int((cfg or {}).get("TTL", 300))
        # Surface the dict so tests can poke at it (counts, etc.).

    def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
        del timeout
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def delete_pattern(self, pattern: str) -> int:
        prefix = pattern.rstrip("*")
        keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            del self._store[k]
        return len(keys)

    async def aget(self, key: str) -> bytes | None:
        return self.get(key)

    async def aset(
        self, key: str, value: bytes, timeout: int | None = None
    ) -> None:
        self.set(key, value, timeout)

    async def adelete(self, key: str) -> None:
        self.delete(key)

    async def adelete_pattern(self, pattern: str) -> int:
        return self.delete_pattern(pattern)


@pytest.fixture
def memcache():
    """Install ``_MemCache`` as the ``"default"`` cache and tear
    down at end of test. Mirrors what users get with a real
    ``RedisCache`` — same ``BaseCache`` interface."""
    from dorm.conf import settings

    prev_caches = settings.CACHES
    settings.CACHES = {
        "default": {
            "BACKEND": "tests.test_redis_cache_v2_5._MemCache",
            "TTL": 60,
        },
    }
    reset_caches()
    backend = get_cache("default")
    yield backend
    reset_caches()
    settings.CACHES = prev_caches


# ────────────────────────────────────────────────────────────────────────────
# qs.cache() returns a clone
# ────────────────────────────────────────────────────────────────────────────


def test_cache_returns_clone_with_timeout(memcache: BaseCache) -> None:
    qs = Author.objects.filter(name="x")
    cached = qs.cache(timeout=42)
    assert cached is not qs
    assert cached._cache_alias == "default"
    assert cached._cache_timeout == 42
    # Original queryset stays untouched.
    assert qs._cache_alias is None


# ────────────────────────────────────────────────────────────────────────────
# Cached result skips DB on second call
# ────────────────────────────────────────────────────────────────────────────


def test_cache_hit_avoids_db_call(memcache: BaseCache) -> None:
    Author.objects.create(name="CacheMe", age=33)

    # First call: DB hit + cache store.
    rows1 = list(Author.objects.filter(name="CacheMe").cache(timeout=30))
    assert len(rows1) == 1
    assert rows1[0].name == "CacheMe"

    # Patch the connection to throw if anyone tries to query —
    # the second call must come from cache.
    qs = Author.objects.filter(name="CacheMe").cache(timeout=30)
    real_iter = qs._iterator

    def _explode() -> Any:
        raise AssertionError(
            "queryset hit DB on cache-warm call — _iterator should "
            "have been short-circuited by _cache_lookup_sync"
        )

    with patch.object(qs, "_iterator", _explode):
        rows2 = list(qs)
    assert len(rows2) == 1
    assert rows2[0].name == "CacheMe"
    del real_iter  # silence unused


# ────────────────────────────────────────────────────────────────────────────
# Cache key is stable + namespaced per-model
# ────────────────────────────────────────────────────────────────────────────


def test_cache_key_is_namespaced_by_model(memcache: BaseCache) -> None:
    qs = Author.objects.filter(name="x").cache(timeout=10)
    key = qs._cache_key()
    assert key is not None
    assert key.startswith(model_cache_namespace(Author))


def test_cache_key_changes_with_filter(memcache: BaseCache) -> None:
    a = Author.objects.filter(name="a").cache(timeout=10)._cache_key()
    b = Author.objects.filter(name="b").cache(timeout=10)._cache_key()
    assert a != b


# ────────────────────────────────────────────────────────────────────────────
# Auto-invalidation on save
# ────────────────────────────────────────────────────────────────────────────


def test_save_invalidates_cached_queryset_for_model(
    memcache: BaseCache,
) -> None:
    Author.objects.create(name="Invalidate", age=50)
    qs = Author.objects.filter(name="Invalidate").cache(timeout=60)
    list(qs)  # populates cache

    # The cache entry exists.
    key = qs._cache_key()
    assert key is not None
    assert memcache.get(key) is not None

    # Saving any Author must wipe every cached queryset for the
    # Author model — that's the coarse-grained invalidation
    # contract.
    Author.objects.create(name="Trigger", age=51)

    assert memcache.get(key) is None


# ────────────────────────────────────────────────────────────────────────────
# Cache outage falls through silently
# ────────────────────────────────────────────────────────────────────────────


def test_cache_outage_falls_through_to_db(memcache: BaseCache) -> None:
    Author.objects.create(name="Outage", age=12)

    # Wire the cache backend to raise on every operation. The
    # queryset layer must catch and continue with the live query.
    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("cache is down")

    with patch.object(memcache, "get", _boom), patch.object(
        memcache, "set", _boom
    ):
        rows = list(Author.objects.filter(name="Outage").cache(timeout=10))
    assert len(rows) == 1
    assert rows[0].name == "Outage"


# ────────────────────────────────────────────────────────────────────────────
# Pickle round-trip: hydrated instances behave like normal models
# ────────────────────────────────────────────────────────────────────────────


def test_cached_rows_round_trip_via_pickle(memcache: BaseCache) -> None:
    a = Author.objects.create(name="Pickleable", age=99)
    rows = list(Author.objects.filter(pk=a.pk).cache(timeout=10))
    assert rows[0].pk == a.pk

    # Inspect the raw bytes — must round-trip through pickle.
    qs = Author.objects.filter(pk=a.pk).cache(timeout=10)
    key = qs._cache_key()
    assert key is not None
    payload = memcache.get(key)
    assert payload is not None
    blob = pickle.loads(payload)
    assert isinstance(blob, list)
    assert blob and "_dorm_model_row" in blob[0]


# ────────────────────────────────────────────────────────────────────────────
# Async cache path
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_cache_round_trip(memcache: BaseCache) -> None:
    a = await Author.objects.acreate(name="AsyncCache", age=42)

    rows = await Author.objects.filter(pk=a.pk).cache(timeout=30)
    assert len(rows) == 1
    assert rows[0].name == "AsyncCache"

    # Second await: cached path. Patch _aiterator to detect any
    # accidental DB call.
    qs = Author.objects.filter(pk=a.pk).cache(timeout=30)

    async def _explode() -> Any:
        raise AssertionError("async cache miss on warm queryset")
        yield  # pragma: no cover

    with patch.object(qs, "_aiterator", _explode):
        rows2 = await qs
    assert len(rows2) == 1
    assert rows2[0].name == "AsyncCache"


# ────────────────────────────────────────────────────────────────────────────
# RedisCache class loads even without redis installed
# ────────────────────────────────────────────────────────────────────────────


def test_redis_cache_import_does_not_crash() -> None:
    """Importing :mod:`dorm.cache.redis` must succeed even if
    redis-py is not installed; the helpful error surfaces only
    when the user actually tries to query the backend."""
    from dorm.cache import redis as redis_mod  # noqa: F401


def test_redis_cache_get_raises_when_redis_missing() -> None:
    """Calling ``RedisCache.get`` without redis-py installed
    surfaces the install command as :class:`ImproperlyConfigured`."""
    pytest.importorskip("dorm.exceptions")
    import importlib as _il

    real = _il.import_module

    def _block(name: str, *a: Any, **kw: Any) -> Any:
        if name == "redis":
            raise ImportError("redis missing")
        return real(name, *a, **kw)

    from dorm.cache.redis import RedisCache
    from dorm.exceptions import ImproperlyConfigured

    cache = RedisCache({"LOCATION": "redis://localhost:6379/0"})
    with patch.object(_il, "import_module", _block):
        with pytest.raises(ImproperlyConfigured, match="redis-py"):
            cache._get_sync()
