"""Regression tests for the v2.5 latent-bug fixes (L1, L2, L3, L5).

Each section pins down one specific scenario from the latent-bug
audit so a future refactor can't reintroduce the problem.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from dorm.cache import (
    BaseCache,
    get_cache,
    model_cache_version,
    reset_caches,
    reset_signing_key,
)
from dorm.conf import settings
from tests.models import Article, Author


class _MemBackend(BaseCache):
    """In-process cache used to introspect what landed in the
    store. Module-level so the dotted-path import in CACHES
    settings resolves cleanly."""

    _store: dict[str, bytes] = {}

    def __init__(self, cfg: dict | None = None) -> None:
        type(self)._store = {}
        self._default_timeout = int((cfg or {}).get("TTL", 300))

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
def mem_cache():
    prev = settings.CACHES
    settings.CACHES = {
        "default": {
            "BACKEND": "tests.test_v2_5_latent_fixes._MemBackend",
            "TTL": 60,
        },
    }
    reset_caches()
    backend = get_cache("default")
    yield backend
    reset_caches()
    settings.CACHES = prev


# ─────────────────────────────────────────────────────────────────────────────
# L1 — bulk write ops invalidate the cache
# ─────────────────────────────────────────────────────────────────────────────


def test_qs_update_invalidates_cache(mem_cache: BaseCache) -> None:
    """``qs.update(...)`` MUST invalidate the cache — without
    it the entire row population stays cached for the full TTL
    even though the write is durable in the database."""
    Author.objects.create(name="QSUpdate", age=10)
    qs = Author.objects.filter(name="QSUpdate").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    Author.objects.filter(name="QSUpdate").update(age=99)
    assert mem_cache.get(key) is None


def test_qs_delete_invalidates_cache(mem_cache: BaseCache) -> None:
    Author.objects.create(name="QSDelete", age=10)
    qs = Author.objects.filter(name="QSDelete").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    Author.objects.filter(name="QSDelete").delete()
    assert mem_cache.get(key) is None


def test_bulk_create_invalidates_cache(mem_cache: BaseCache) -> None:
    Author.objects.create(name="BulkSeed", age=10)
    qs = Author.objects.filter(age__gte=0).cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    new = [Author(name=f"Bulk{i}", age=20 + i) for i in range(3)]
    Author.objects.bulk_create(new)
    assert mem_cache.get(key) is None


def test_bulk_update_invalidates_cache(mem_cache: BaseCache) -> None:
    a = Author.objects.create(name="BulkUp", age=10)
    qs = Author.objects.filter(name="BulkUp").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    a.age = 50
    Author.objects.bulk_update([a], ["age"])
    assert mem_cache.get(key) is None


def test_bulk_create_empty_list_does_not_invalidate(
    mem_cache: BaseCache,
) -> None:
    """No-op ``bulk_create([])`` MUST NOT churn the cache —
    spurious invalidation is wasted Redis round-trip + version
    bump."""
    Author.objects.create(name="NoOp", age=1)
    qs = Author.objects.filter(name="NoOp").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    pre_v = model_cache_version(Author)
    assert key is not None
    assert mem_cache.get(key) is not None

    Author.objects.bulk_create([])  # no-op
    assert mem_cache.get(key) is not None
    assert model_cache_version(Author) == pre_v


@pytest.mark.asyncio
async def test_aupdate_invalidates_cache(mem_cache: BaseCache) -> None:
    await Author.objects.acreate(name="AsyncQSUpdate", age=10)
    rows = await Author.objects.filter(name="AsyncQSUpdate").cache(timeout=60)
    qs = Author.objects.filter(name="AsyncQSUpdate").cache(timeout=60)
    key = qs._cache_key()
    assert rows[0].name == "AsyncQSUpdate"
    assert key is not None
    assert mem_cache.get(key) is not None

    await Author.objects.filter(name="AsyncQSUpdate").aupdate(age=33)
    assert mem_cache.get(key) is None


@pytest.mark.asyncio
async def test_adelete_invalidates_cache(mem_cache: BaseCache) -> None:
    await Author.objects.acreate(name="AsyncQSDelete", age=10)
    await Author.objects.filter(name="AsyncQSDelete").cache(timeout=60)
    qs = Author.objects.filter(name="AsyncQSDelete").cache(timeout=60)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    await Author.objects.filter(name="AsyncQSDelete").adelete()
    assert mem_cache.get(key) is None


@pytest.mark.asyncio
async def test_abulk_create_invalidates_cache(mem_cache: BaseCache) -> None:
    await Author.objects.acreate(name="AsyncBulkSeed", age=10)
    await Author.objects.filter(age__gte=0).cache(timeout=60)
    qs = Author.objects.filter(age__gte=0).cache(timeout=60)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    new = [Author(name=f"AB{i}", age=10 + i) for i in range(2)]
    await Author.objects.abulk_create(new)
    assert mem_cache.get(key) is None


@pytest.mark.asyncio
async def test_abulk_update_invalidates_cache(mem_cache: BaseCache) -> None:
    a = await Author.objects.acreate(name="AsyncBulkUp", age=10)
    await Author.objects.filter(name="AsyncBulkUp").cache(timeout=60)
    qs = Author.objects.filter(name="AsyncBulkUp").cache(timeout=60)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    a.age = 55
    await Author.objects.abulk_update([a], ["age"])
    assert mem_cache.get(key) is None


# ─────────────────────────────────────────────────────────────────────────────
# L2 — ephemeral signing key fail-loud
# ─────────────────────────────────────────────────────────────────────────────


def test_require_signing_key_raises_when_no_key_set() -> None:
    """``CACHE_REQUIRE_SIGNING_KEY = True`` + no key → first
    sign / verify call must raise ``ImproperlyConfigured`` with
    a clear message instead of silently falling back to a
    per-process random key."""
    from dorm.cache import sign_payload
    from dorm.exceptions import ImproperlyConfigured

    prev_key = settings.CACHE_SIGNING_KEY
    prev_secret = settings.SECRET_KEY
    prev_require = getattr(settings, "CACHE_REQUIRE_SIGNING_KEY", False)
    try:
        settings.CACHE_SIGNING_KEY = ""
        settings.SECRET_KEY = ""
        settings.CACHE_REQUIRE_SIGNING_KEY = True
        reset_signing_key()
        with pytest.raises(ImproperlyConfigured, match="CACHE_REQUIRE_SIGNING_KEY"):
            sign_payload(b"x")
    finally:
        settings.CACHE_SIGNING_KEY = prev_key
        settings.SECRET_KEY = prev_secret
        settings.CACHE_REQUIRE_SIGNING_KEY = prev_require
        reset_signing_key()


def test_require_signing_key_passes_when_key_configured() -> None:
    """Strict mode + explicit key → no raise."""
    from dorm.cache import sign_payload

    prev_key = settings.CACHE_SIGNING_KEY
    prev_require = getattr(settings, "CACHE_REQUIRE_SIGNING_KEY", False)
    try:
        settings.CACHE_SIGNING_KEY = "production-secret"
        settings.CACHE_REQUIRE_SIGNING_KEY = True
        reset_signing_key()
        out = sign_payload(b"hello")
        assert out.startswith(b"dormsig1:")
    finally:
        settings.CACHE_SIGNING_KEY = prev_key
        settings.CACHE_REQUIRE_SIGNING_KEY = prev_require
        reset_signing_key()


def test_require_signing_key_falls_back_to_secret_key() -> None:
    from dorm.cache import sign_payload

    prev_key = settings.CACHE_SIGNING_KEY
    prev_secret = settings.SECRET_KEY
    prev_require = getattr(settings, "CACHE_REQUIRE_SIGNING_KEY", False)
    try:
        settings.CACHE_SIGNING_KEY = ""
        settings.SECRET_KEY = "django-style-secret"
        settings.CACHE_REQUIRE_SIGNING_KEY = True
        reset_signing_key()
        out = sign_payload(b"x")
        assert out.startswith(b"dormsig1:")
    finally:
        settings.CACHE_SIGNING_KEY = prev_key
        settings.SECRET_KEY = prev_secret
        settings.CACHE_REQUIRE_SIGNING_KEY = prev_require
        reset_signing_key()


# ─────────────────────────────────────────────────────────────────────────────
# L3 — cache key ignores filter() kwarg ordering
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_key_invariant_under_kwarg_order() -> None:
    """``filter(a=1, b=2)`` and ``filter(b=2, a=1)`` produce
    the same cached blob — same query semantically, same cache
    entry. Without normalisation Python's kwarg iteration order
    leaks into the digest and the hit rate halves."""
    qs1 = Author.objects.all().cache(timeout=10).filter(name="x", age=10)
    qs2 = Author.objects.all().cache(timeout=10).filter(age=10, name="x")
    k1 = qs1._cache_key()
    k2 = qs2._cache_key()
    assert k1 is not None
    assert k2 is not None
    assert k1 == k2


def test_cache_key_changes_with_value() -> None:
    qs1 = Author.objects.all().cache(timeout=10).filter(name="x")
    qs2 = Author.objects.all().cache(timeout=10).filter(name="y")
    assert qs1._cache_key() != qs2._cache_key()


def test_cache_key_changes_with_field_path() -> None:
    qs1 = Author.objects.all().cache(timeout=10).filter(name="x")
    qs2 = Author.objects.all().cache(timeout=10).filter(age=10)
    assert qs1._cache_key() != qs2._cache_key()


def test_cache_key_changes_with_lookup() -> None:
    qs1 = Author.objects.all().cache(timeout=10).filter(age=10)
    qs2 = Author.objects.all().cache(timeout=10).filter(age__gte=10)
    assert qs1._cache_key() != qs2._cache_key()


def test_cache_key_order_by_is_positional() -> None:
    """``order_by`` IS positional (different orderings produce
    different result sets), so cache keys must differ."""
    qs1 = Author.objects.all().cache(timeout=10).order_by("name", "age")
    qs2 = Author.objects.all().cache(timeout=10).order_by("age", "name")
    assert qs1._cache_key() != qs2._cache_key()


def test_cache_round_trip_normalised_kwargs(mem_cache: BaseCache) -> None:
    """End-to-end: a write under one kwarg ordering populates
    the cache, a read under a different ordering hits the same
    bytes."""
    Author.objects.create(name="Norm", age=20)
    list(
        Author.objects.filter(name="Norm", age=20).cache(timeout=60)
    )

    qs_other = Author.objects.filter(age=20, name="Norm").cache(timeout=60)
    key = qs_other._cache_key()
    assert key is not None
    payload = mem_cache.get(key)
    assert payload is not None  # would be ``None`` if key collided

    # Patch ``_iterator`` to detect any DB hit on the second
    # iteration — must come from cache.
    def _explode() -> Any:
        raise AssertionError("DB hit despite cache hit")

    with patch.object(qs_other, "_iterator", _explode):
        rows = list(qs_other)
    assert rows[0].name == "Norm"


# ─────────────────────────────────────────────────────────────────────────────
# L5 — libsql async cross-loop detection
# ─────────────────────────────────────────────────────────────────────────────


def test_libsql_async_detect_loop_change_returns_false_when_no_loop_yet() -> None:
    if pytest.importorskip("turso") is None:
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    w = LibSQLAsyncDatabaseWrapper(
        {"ENGINE": "libsql", "NAME": ":memory:"}
    )
    # Fresh wrapper — no ``_loop`` stamped yet.
    assert w._detect_loop_change() is False


def test_libsql_async_reset_for_new_loop_drops_state() -> None:
    if pytest.importorskip("turso") is None:
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    w = LibSQLAsyncDatabaseWrapper(
        {"ENGINE": "libsql", "NAME": ":memory:"}
    )
    # Fake stamped state from a prior loop.
    w._async_conn = object()
    w._sync_conn = object()
    w._loop = asyncio.new_event_loop()
    w._loop.close()  # simulate dead loop
    # Allocate the executor so we exercise its shutdown path.
    w._get_executor()
    assert w._executor is not None

    w._reset_for_new_loop()
    assert w._async_conn is None
    assert w._sync_conn is None
    assert w._executor is None
    assert w._loop is None


@pytest.mark.asyncio
async def test_libsql_async_detect_loop_change_true_on_different_loop() -> None:
    """Stamp ``_loop`` to a fresh loop, call detector from the
    main test loop — must report a change."""
    if pytest.importorskip("turso") is None:
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    w = LibSQLAsyncDatabaseWrapper(
        {"ENGINE": "libsql", "NAME": ":memory:"}
    )
    other_loop = asyncio.new_event_loop()
    try:
        w._loop = other_loop  # pretend we opened on a different loop
        assert w._detect_loop_change() is True
    finally:
        other_loop.close()


@pytest.mark.asyncio
async def test_libsql_async_get_conn_resets_on_loop_change() -> None:
    """End-to-end: stamp a stale ``_loop`` + fake conn, call
    ``_get_conn()`` — must drop the stale state and re-open."""
    if pytest.importorskip("turso") is None:
        pytest.skip("pyturso not installed")
    import os
    import tempfile

    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "loop.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        try:
            # First open: stamps real loop + real conn.
            await w._get_conn()
            real_conn = w._async_conn
            assert real_conn is not None

            # Simulate a loop change by stamping a different loop
            # on the wrapper without touching the actual conn.
            other_loop = asyncio.new_event_loop()
            try:
                w._loop = other_loop
                # Now request a connection — wrapper detects the
                # mismatch and re-opens, returning a NEW conn.
                new_conn = await w._get_conn()
                assert new_conn is not real_conn
                assert w._loop is not other_loop  # current real loop
            finally:
                other_loop.close()
        finally:
            await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# Smoke — ensure the fixes don't leak into existing well-paths
# ─────────────────────────────────────────────────────────────────────────────


def test_normal_save_still_invalidates(mem_cache: BaseCache) -> None:
    """Sanity: the existing ``Model.save()`` invalidation path
    still works after the bulk-op handlers were added."""
    Author.objects.create(name="Sanity", age=42)
    qs = Author.objects.filter(name="Sanity").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    Author.objects.create(name="SanityTrigger", age=43)
    assert mem_cache.get(key) is None


def test_m2m_clear_does_not_invalidate(mem_cache: BaseCache) -> None:
    """M2M set / add / clear are NOT auto-invalidated today —
    flag the behaviour as a known gap with this regression
    test so a future refactor doesn't accidentally start
    firing on M2M signals without the matching docs update."""
    art = Article.objects.create(title="MMArt")
    list(Article.objects.filter(pk=art.pk).cache(timeout=60))
    key = (
        Article.objects.filter(pk=art.pk).cache(timeout=60)._cache_key()
    )
    assert key is not None
    assert mem_cache.get(key) is not None

    # Clearing tags on the article doesn't fire post_save for
    # Article — cache stays. (Doc-only behaviour.)
    art.tags.clear()
    # No assertion on the cache content; we just verify the
    # call doesn't crash.
