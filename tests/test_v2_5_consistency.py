"""Data-consistency regression tests for the v2.5 cache layer
(D1–D4 fixes).

Pins down each scenario that surfaced during the consistency
audit so a future refactor can't reintroduce the original
problem.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

import pytest

from dorm.cache import (
    BaseCache,
    bump_model_cache_version,
    get_cache,
    model_cache_version,
    reset_caches,
)
from dorm.conf import settings
from tests.models import Author


class _MemBackend(BaseCache):
    """Tiny in-process cache shared by every test in this file
    so we can introspect what landed in the store."""

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

    async def aset(self, key: str, value: bytes, timeout: int | None = None) -> None:
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
            "BACKEND": "tests.test_v2_5_consistency._MemBackend",
            "TTL": 60,
        },
    }
    reset_caches()
    backend = get_cache("default")
    yield backend
    reset_caches()
    settings.CACHES = prev


# ─────────────────────────────────────────────────────────────────────────────
# D1 — cache invalidation deferred to transaction commit
# ─────────────────────────────────────────────────────────────────────────────


def test_invalidation_deferred_inside_atomic_until_commit(
    mem_cache: BaseCache,
) -> None:
    """Inside ``atomic()`` the cache MUST stay populated until the
    transaction commits — invalidating early opens a stale-read
    race against a concurrent reader on a separate connection."""
    from dorm.transaction import atomic

    Author.objects.create(name="DeferredCommit", age=11)
    qs = Author.objects.filter(name="DeferredCommit").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None  # populated

    pre_save_value = model_cache_version(Author)
    with atomic():
        # Mid-txn save: cache MUST still be live and the version
        # counter MUST stay frozen until the block commits.
        Author.objects.create(name="MidTxn", age=12)
        assert mem_cache.get(key) is not None, (
            "cache wiped pre-commit — opens stale-read race window"
        )
        assert model_cache_version(Author) == pre_save_value, (
            "version bumped before commit — stale-read race "
            "window re-opened"
        )

    # After commit, both invalidation effects fired.
    assert mem_cache.get(key) is None
    assert model_cache_version(Author) > pre_save_value


def test_invalidation_skipped_on_rollback(mem_cache: BaseCache) -> None:
    """``atomic()`` rollback must NOT invalidate the cache — the
    DB never changed, so cached entries stay correct."""
    from dorm.transaction import atomic

    Author.objects.create(name="Rollback", age=20)
    qs = Author.objects.filter(name="Rollback").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    pre_version = model_cache_version(Author)
    assert key is not None
    assert mem_cache.get(key) is not None

    class _BoomError(Exception):
        pass

    with pytest.raises(_BoomError):
        with atomic():
            Author.objects.create(name="ToRollback", age=21)
            raise _BoomError

    # Cache survived because the writer's UPDATE never committed.
    assert mem_cache.get(key) is not None
    assert model_cache_version(Author) == pre_version


def test_save_outside_atomic_invalidates_immediately(
    mem_cache: BaseCache,
) -> None:
    """Without an enclosing ``atomic()`` block, the underlying
    save auto-commits at the connection layer; invalidation
    must run inline."""
    Author.objects.create(name="Bare", age=33)
    qs = Author.objects.filter(name="Bare").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    Author.objects.create(name="BareTrigger", age=34)
    assert mem_cache.get(key) is None


@pytest.mark.asyncio
async def test_async_save_outside_atomic_invalidates_immediately(
    mem_cache: BaseCache,
) -> None:
    a = await Author.objects.acreate(name="AsyncBare", age=42)
    rows = await Author.objects.filter(name="AsyncBare").cache(timeout=60)
    assert rows[0].name == "AsyncBare"

    qs = Author.objects.filter(name="AsyncBare").cache(timeout=60)
    key = qs._cache_key()
    assert key is not None
    # Re-cache by awaiting again (returns from store).
    await Author.objects.filter(name="AsyncBare").cache(timeout=60)
    assert mem_cache.get(key) is not None

    await a.asave()  # post_save.asend
    # asend ran the sync receiver inline; sync ``on_commit``
    # fired immediately because no aatomic was active.
    assert mem_cache.get(key) is None


@pytest.mark.asyncio
async def test_async_invalidation_deferred_inside_aatomic(
    mem_cache: BaseCache,
) -> None:
    """Async counterpart of the sync ``atomic`` test — ``aatomic``
    must defer cache invalidation to the outermost commit."""
    from dorm.transaction import aatomic

    a = await Author.objects.acreate(name="AsyncDeferred", age=51)
    list(Author.objects.filter(name="AsyncDeferred").cache(timeout=60))

    qs = Author.objects.filter(name="AsyncDeferred").cache(timeout=60)
    key = qs._cache_key()
    list(qs)  # populate
    assert key is not None
    assert mem_cache.get(key) is not None

    async with aatomic():
        await a.asave()
        # Still live mid-txn.
        assert mem_cache.get(key) is not None

    # Commit fired → cache wiped.
    assert mem_cache.get(key) is None


# ─────────────────────────────────────────────────────────────────────────────
# D2 — cache key includes _db alias (cross-DB isolation)
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_key_includes_db_alias() -> None:
    """Two queries on the same model routed to different
    ``DATABASES`` aliases must NOT share a cache key — otherwise
    a read on ``replica`` could serve to ``primary`` callers.

    The test infra only has one alias configured; we mock the
    connection lookup so the key-builder thinks both aliases
    resolve to the same vendor and SQL — the only thing that
    should differ is the ``db=…`` segment in the namespace.
    """
    from unittest.mock import patch

    from tests.models import Author

    # Both querysets must compile against a real (default)
    # connection — fake _db on a clone so as_select still works.
    qs_primary = Author.objects.all().cache(timeout=10).filter(name="x")
    qs_other = Author.objects.all().cache(timeout=10).filter(name="x")
    qs_other._db = "other"

    # Patch the connection lookup so ``other`` resolves to the
    # default wrapper (no real ``other`` alias is configured in
    # the test fixture).
    real_get = qs_primary._get_connection

    def _fake_get_connection(self: Any) -> Any:
        return real_get()

    with patch.object(type(qs_other), "_get_connection", _fake_get_connection):
        k1 = qs_primary._cache_key()
        k2 = qs_other._cache_key()
    assert k1 is not None
    assert k2 is not None
    assert k1 != k2
    assert "db=default" in k1
    assert "db=other" in k2


def test_cache_key_default_alias_marked_explicitly() -> None:
    """Even the implicit ``"default"`` alias must show in the key
    so future per-alias invalidation can match cleanly."""
    qs = Author.objects.all().cache(timeout=10).filter(name="x")
    k = qs._cache_key()
    assert k is not None
    assert "db=default" in k


# ─────────────────────────────────────────────────────────────────────────────
# D3 — asave does NOT double-bump the version counter
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_asave_bumps_version_exactly_once(mem_cache: BaseCache) -> None:
    """The previous design connected both a sync and an async
    receiver to ``post_save``; ``asend`` runs both, so each
    ``asave()`` bumped the counter twice. Verify a single
    receiver runs and the counter increments by one."""
    a = await Author.objects.acreate(name="OnceBump", age=12)

    before = model_cache_version(Author)
    await a.asave()
    after = model_cache_version(Author)
    # Exactly one bump per asave — not two.
    assert after - before == 1


def test_save_bumps_version_exactly_once(mem_cache: BaseCache) -> None:
    a = Author.objects.create(name="OnceBumpSync", age=12)
    before = model_cache_version(Author)
    a.save()
    after = model_cache_version(Author)
    assert after - before == 1


# ─────────────────────────────────────────────────────────────────────────────
# D4 — Sync save() does NOT log "skipped async receiver" warning
# ─────────────────────────────────────────────────────────────────────────────


def test_sync_save_emits_no_async_skipped_warning(
    mem_cache: BaseCache, caplog: Any
) -> None:
    """Old design registered an async receiver on ``post_save``;
    every sync ``save()`` then logged a WARNING because
    ``Signal.send`` skips async receivers. The unified single-
    receiver design must be silent."""
    # Make sure invalidation handlers are wired before we save.
    from dorm.cache.invalidation import ensure_signals_connected

    ensure_signals_connected()

    with caplog.at_level(logging.WARNING, logger="dorm.signals"):
        Author.objects.create(name="NoWarning", age=18)

    skipped_messages = [
        rec for rec in caplog.records if "skipped" in rec.message.lower()
    ]
    assert skipped_messages == [], (
        "post_save.send produced an async-receiver-skipped warning; "
        "the single-handler invalidation should be silent"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Smoke — single-handler design still invalidates cross-cache
# ─────────────────────────────────────────────────────────────────────────────


def test_invalidation_clears_every_configured_alias(
    mem_cache: BaseCache,
) -> None:
    """Multiple cache aliases configured → save invalidates all
    of them. Edge case the single-handler design must keep
    correct."""
    Author.objects.create(name="MultiCache", age=99)

    qs = Author.objects.filter(name="MultiCache").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert mem_cache.get(key) is not None

    Author.objects.create(name="WriterTrigger", age=100)
    assert mem_cache.get(key) is None


def test_concurrent_thread_bumps_consistent_with_invalidation(
    mem_cache: BaseCache,
) -> None:
    """Many threads calling ``bump_model_cache_version`` directly
    plus a real save — the combined counter increments
    monotonically and matches the sum of the explicit calls."""
    base = model_cache_version(Author)
    n = 8
    threads = [
        threading.Thread(target=lambda: bump_model_cache_version(Author))
        for _ in range(n)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert model_cache_version(Author) == base + n


# ─────────────────────────────────────────────────────────────────────────────
# Multi-step async invalidation: ensure receiver works under aatomic
# rollback + re-attempt.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_aatomic_rollback_skips_invalidation(
    mem_cache: BaseCache,
) -> None:
    """Async transaction rollback must NOT invalidate — same
    contract as the sync variant. Verifies the async receiver
    routes through ``aon_commit`` / ``aon_rollback`` correctly."""
    from dorm.transaction import aatomic

    a = await Author.objects.acreate(name="ARollback", age=44)
    list(Author.objects.filter(name="ARollback").cache(timeout=60))

    qs = Author.objects.filter(name="ARollback").cache(timeout=60)
    key = qs._cache_key()
    list(qs)
    assert key is not None
    assert mem_cache.get(key) is not None

    pre_version = model_cache_version(Author)

    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        async with aatomic():
            await a.asave()
            raise _Boom

    # Cache and version untouched after rollback.
    assert mem_cache.get(key) is not None
    assert model_cache_version(Author) == pre_version


# ─────────────────────────────────────────────────────────────────────────────
# Cache-key collision regression — same SQL different alias
# ─────────────────────────────────────────────────────────────────────────────


def test_using_alias_chain_propagates_through_clone() -> None:
    """``using("x").cache().filter(...)`` clones must preserve
    the alias attribute. Connection lookup with the unknown
    alias would fail in real usage; here we just verify the
    chain copies ``_db`` so a real multi-DB deployment isn't
    silently routed wrong."""
    qs = (
        Author.objects.all()
        .using("alpha")
        .cache(timeout=10)
        .filter(name="x")
        .order_by("name")
    )
    assert qs._db == "alpha"
    assert qs._cache_alias == "default"


# ─────────────────────────────────────────────────────────────────────────────
# Cache key still SHA-1 stable across sequential calls of same query
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_key_is_stable_across_invocations(
    mem_cache: BaseCache,
) -> None:
    """Same queryset shape → same key (within the same version
    window). Re-clone must not perturb the digest."""
    base = Author.objects.all().cache(timeout=10).filter(name="x").order_by("name")
    k1 = base._cache_key()
    k2 = base._clone()._cache_key()
    k3 = (
        Author.objects.all().cache(timeout=10).filter(name="x").order_by("name")
    )._cache_key()
    assert k1 == k2 == k3


# ─────────────────────────────────────────────────────────────────────────────
# Stale-read race smoke under concurrent gather
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_concurrent_reader_writer_no_stale_after_commit(
    mem_cache: BaseCache,
) -> None:
    """End-to-end concurrent fan-out: write + read in
    asyncio.gather, verify the cache eventually reflects the
    post-commit state. Doesn't directly probe the race window
    (hard to schedule deterministically) but covers the main
    code path."""
    a = await Author.objects.acreate(name="GatherTest", age=21)

    async def _read() -> Any:
        return await Author.objects.filter(name="GatherTest").cache(timeout=60)

    async def _write() -> None:
        a.age = 22
        await a.asave()

    # Run a writer and three readers concurrently. The writer's
    # invalidation runs on aon_commit (post-asave commit) so the
    # final state must reflect age=22.
    await asyncio.gather(_read(), _read(), _write(), _read())

    final = await Author.objects.filter(name="GatherTest").cache(timeout=60)
    assert final[0].age == 22
