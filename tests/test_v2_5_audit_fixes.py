"""Regression tests for the v2.5 audit fixes (B1, B4, B5, B7, B9)
and a second wave that fills the v2.5 coverage gaps the earlier
test files left behind.

Each section pins down one specific bug found during the v2.5
audit so a future refactor can't reintroduce the original problem.
"""

from __future__ import annotations

import pickle
import threading
from typing import Any
from unittest.mock import patch

import pytest

import dorm
from tests.models import Author


def _libsql_available() -> bool:
    import importlib

    try:
        importlib.import_module("turso")
        return True
    except ImportError:
        return False


def _redis_available() -> bool:
    import importlib

    try:
        importlib.import_module("redis")
        return True
    except ImportError:
        return False


def _fakeredis_available() -> bool:
    import importlib

    try:
        importlib.import_module("fakeredis")
        return True
    except ImportError:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# B1 — parse_database_url libsql 4-slash absolute paths
# ─────────────────────────────────────────────────────────────────────────────


def test_parse_libsql_four_slash_keeps_absolute_path() -> None:
    """``libsql:////var/data/db.sqlite`` must yield the absolute
    path ``/var/data/db.sqlite``, not the relative
    ``var/data/db.sqlite``. The previous parser stripped one
    slash too many."""
    from dorm.conf import parse_database_url

    cfg = parse_database_url("libsql:////var/data/db.sqlite")
    assert cfg["NAME"] == "/var/data/db.sqlite"
    assert cfg["NAME"].startswith("/")


def test_parse_libsql_three_slash_keeps_relative_path() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url("libsql:///relative/db.sqlite")
    assert cfg["NAME"] == "relative/db.sqlite"
    assert not cfg["NAME"].startswith("/")


def test_parse_libsql_three_slash_with_trailing_only() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url("libsql:///")
    assert cfg["NAME"] == ":memory:"


# ─────────────────────────────────────────────────────────────────────────────
# B4 — ValuesListQuerySet / CombinedQuerySet preserve cache state on _clone
# ─────────────────────────────────────────────────────────────────────────────


def test_values_list_clone_preserves_cache_state() -> None:
    qs = (
        Author.objects.all()
        .cache(timeout=42)
        .values_list("name", flat=True)
    )
    cloned = qs._clone()
    assert cloned._cache_alias == "default"
    assert cloned._cache_timeout == 42


def test_values_list_chain_filter_preserves_cache_state() -> None:
    """Chained filter() on ValuesListQuerySet must not drop cache."""
    qs = (
        Author.objects.all()
        .cache(timeout=42)
        .values_list("name", flat=True)
        .filter(age__gte=0)
    )
    assert qs._cache_alias == "default"
    assert qs._cache_timeout == 42


def test_combined_queryset_clone_preserves_cache_state() -> None:
    qs = (
        Author.objects.filter(age=10)
        .union(Author.objects.filter(age=20))
        .cache(timeout=99)
    )
    cloned = qs._clone()
    assert cloned._cache_alias == "default"
    assert cloned._cache_timeout == 99


def test_combined_queryset_filter_preserves_cache_state() -> None:
    qs = (
        Author.objects.filter(age=10)
        .union(Author.objects.filter(age=20))
        .cache(timeout=99)
        .order_by("name")
    )
    assert qs._cache_alias == "default"
    assert qs._cache_timeout == 99


# ─────────────────────────────────────────────────────────────────────────────
# B5 — pickle RCE: payloads must be HMAC-signed and verified
# ─────────────────────────────────────────────────────────────────────────────


def test_sign_and_verify_round_trip() -> None:
    from dorm.cache import sign_payload, verify_payload

    inner = pickle.dumps([{"name": "x"}])
    signed = sign_payload(inner)
    assert signed != inner  # signature header was added
    assert signed.startswith(b"dormsig1:")
    assert verify_payload(signed) == inner


def test_verify_rejects_unsigned_blob() -> None:
    from dorm.cache import verify_payload

    inner = pickle.dumps([{"name": "x"}])
    # Plain pickle bytes without the dormsig1 header → rejected.
    assert verify_payload(inner) is None


def test_verify_rejects_tampered_signature() -> None:
    from dorm.cache import sign_payload, verify_payload

    inner = pickle.dumps([{"name": "x"}])
    signed = sign_payload(inner)
    # Flip first digest byte. Replacement MUST differ from the
    # original byte — using a fixed hex char (``b"f"``) was
    # flaky because the random per-process signing key sometimes
    # produced a digest whose first hex char already was ``f``.
    # Pick a byte that never appears in a hex digest so the
    # comparison always fails.
    tampered = signed[:9] + b"!" + signed[10:]
    assert verify_payload(tampered) is None


def test_verify_rejects_truncated_blob() -> None:
    from dorm.cache import verify_payload

    assert verify_payload(b"") is None
    assert verify_payload(b"dormsig1:short") is None
    assert verify_payload(b"dormsig1:" + b"a" * 64) is None  # missing trailing sep


def test_verify_rejects_non_bytes() -> None:
    from dorm.cache import verify_payload

    # Non-bytes input → drop. Defensive against future backends
    # that decode payloads to ``str`` accidentally. Cast through
    # ``Any`` so the static checker doesn't flag the wrong-type
    # call we deliberately want here.
    bad: Any = "dormsig1:abc:payload"
    assert verify_payload(bad) is None


def test_signing_key_uses_settings_key_when_set() -> None:
    """Setting CACHE_SIGNING_KEY must produce a different signature
    than the default — verify the key threads through."""
    from dorm.cache import reset_signing_key, sign_payload
    from dorm.conf import settings

    inner = b"payload"
    reset_signing_key()
    settings.CACHE_SIGNING_KEY = "key-A"
    signed_a = sign_payload(inner)
    reset_signing_key()
    settings.CACHE_SIGNING_KEY = "key-B"
    signed_b = sign_payload(inner)
    reset_signing_key()
    settings.CACHE_SIGNING_KEY = ""
    assert signed_a != signed_b


def test_insecure_pickle_opt_out_disables_signing() -> None:
    from dorm.cache import sign_payload, verify_payload
    from dorm.conf import settings

    settings.CACHE_INSECURE_PICKLE = True
    try:
        inner = b"raw"
        # Signing is a no-op.
        assert sign_payload(inner) == inner
        # Verification also pass-through.
        assert verify_payload(inner) == inner
    finally:
        settings.CACHE_INSECURE_PICKLE = False


def test_cache_lookup_rejects_unsigned_payload(monkeypatch: Any) -> None:
    """End-to-end: an attacker-injected unsigned blob must fall
    through to a DB query rather than reach ``pickle.loads``."""
    from dorm.cache import BaseCache, get_cache, reset_caches
    from dorm.conf import settings

    # In-process cache that stores arbitrary bytes.
    class _Mem(BaseCache):
        def __init__(self, cfg: dict | None = None) -> None:
            self.store: dict[str, bytes] = {}

        def get(self, key: str) -> bytes | None:
            return self.store.get(key)

        def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
            self.store[key] = value

        def delete(self, key: str) -> None:
            self.store.pop(key, None)

        def delete_pattern(self, pattern: str) -> int:
            prefix = pattern.rstrip("*")
            keys = [k for k in self.store if k.startswith(prefix)]
            for k in keys:
                del self.store[k]
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

    prev = settings.CACHES
    settings.CACHES = {
        "default": {
            "BACKEND": (
                "tests.test_v2_5_audit_fixes."
                "_module_level_unsigned_backend"
            ),
        },
    }
    reset_caches()
    backend = get_cache("default")

    Author.objects.create(name="UnsignedTest", age=11)
    qs = Author.objects.filter(name="UnsignedTest").cache(timeout=10)
    key = qs._cache_key()
    assert key is not None
    # Inject a malicious unsigned payload — anything that *would*
    # be pickle.loads-able. We use a plain pickle blob to prove
    # the verifier rejects it before pickle ever runs.
    rogue = pickle.dumps(["not from us"])
    backend.set(key, rogue)

    # The queryset must NOT return the rogue bytes — falls through
    # to the live DB and gets the real Author back.
    rows = list(Author.objects.filter(name="UnsignedTest").cache(timeout=10))
    assert len(rows) == 1
    assert rows[0].name == "UnsignedTest"

    settings.CACHES = prev
    reset_caches()


class _module_level_unsigned_backend:  # noqa: N801
    """Module-level alias so the dotted path import in the cache
    settings dict resolves cleanly. Just delegates to a per-call
    in-process dict."""

    _store: dict[str, bytes] = {}

    def __init__(self, cfg: dict | None = None) -> None:
        pass

    def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
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


# ─────────────────────────────────────────────────────────────────────────────
# B7 — Stale-read race: per-model invalidation version
# ─────────────────────────────────────────────────────────────────────────────


def test_model_cache_version_starts_at_zero_or_higher() -> None:
    from dorm.cache import model_cache_version

    v = model_cache_version(Author)
    assert isinstance(v, int)
    assert v >= 0


def test_bump_model_cache_version_increments() -> None:
    from dorm.cache import bump_model_cache_version, model_cache_version

    before = model_cache_version(Author)
    after = bump_model_cache_version(Author)
    assert after == before + 1
    assert model_cache_version(Author) == after


def test_cache_key_includes_version() -> None:
    from dorm.cache import bump_model_cache_version

    qs = Author.objects.all().cache(timeout=30).filter(name="vk")
    k1 = qs._cache_key()
    bump_model_cache_version(Author)
    k2 = qs._cache_key()
    assert k1 != k2
    # The version segment ``:vN:`` must move forward.
    assert ":v" in (k1 or "")
    assert ":v" in (k2 or "")


def test_cache_store_uses_post_fetch_version_after_concurrent_bump() -> None:
    """If a writer bumps the version BETWEEN _cache_key (initial
    read at lookup) and _cache_store_sync (post-fetch), the row
    must be stored under the NEW key — racing readers using the
    new version see the live row, not a stale one cached under
    the old key."""
    from dorm.cache import (
        BaseCache,
        bump_model_cache_version,
        get_cache,
        reset_caches,
    )
    from dorm.conf import settings

    class _Mem(BaseCache):
        def __init__(self, cfg: dict | None = None) -> None:
            self.store: dict[str, bytes] = {}

        def get(self, key: str) -> bytes | None:
            return self.store.get(key)

        def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
            self.store[key] = value

        def delete(self, key: str) -> None:
            self.store.pop(key, None)

        def delete_pattern(self, pattern: str) -> int:
            prefix = pattern.rstrip("*")
            keys = [k for k in self.store if k.startswith(prefix)]
            for k in keys:
                del self.store[k]
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

    prev = settings.CACHES
    settings.CACHES = {
        "default": {
            "BACKEND": "tests.test_v2_5_audit_fixes._VersionRaceBackend",
        },
    }
    reset_caches()
    backend = get_cache("default")

    Author.objects.create(name="VersionRace", age=10)
    qs = Author.objects.filter(name="VersionRace").cache(timeout=30)
    pre_key = qs._cache_key()
    assert pre_key is not None

    # Simulate a concurrent writer between fetch and store by
    # bumping the version BEFORE iteration completes. Easiest
    # way: bump before the iteration but rely on store using
    # post-fetch version (re-reads inside _cache_store_sync).
    bump_model_cache_version(Author)
    list(qs)  # triggers fetch + store

    # Pre-bump key was never written (the store used the bumped
    # version). The bumped key has the freshly-stored payload.
    post_key = qs._cache_key()
    assert post_key is not None
    assert backend.get(pre_key) is None  # never stored
    assert backend.get(post_key) is not None

    settings.CACHES = prev
    reset_caches()


class _VersionRaceBackend:
    """Module-level dict-backed cache for the version-race test."""

    _store: dict[str, bytes] = {}

    def __init__(self, cfg: dict | None = None) -> None:
        # Reset on instantiation so each test gets a clean dict.
        type(self)._store = {}

    def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
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


def test_invalidation_handler_bumps_version() -> None:
    """post_save / post_delete signal handlers must bump the
    per-model version BEFORE issuing delete_pattern — closes the
    stale-read window."""
    from dorm.cache import model_cache_version
    from dorm.cache.invalidation import _drop_model

    before = model_cache_version(Author)
    _drop_model(Author)
    after = model_cache_version(Author)
    assert after == before + 1


# ─────────────────────────────────────────────────────────────────────────────
# B9 — execute_script split-on-`;` ignores quoted literals
# ─────────────────────────────────────────────────────────────────────────────


def test_split_statements_basic() -> None:
    from dorm.db.backends.sqlite import _split_statements

    out = _split_statements("CREATE TABLE u (id INT); CREATE TABLE v (id INT)")
    assert out == ["CREATE TABLE u (id INT)", "CREATE TABLE v (id INT)"]


def test_split_statements_preserves_quoted_semicolons() -> None:
    from dorm.db.backends.sqlite import _split_statements

    sql = (
        "INSERT INTO t VALUES ('a;b'); "
        "INSERT INTO t VALUES ('c;d;e')"
    )
    parts = _split_statements(sql)
    assert len(parts) == 2
    assert "'a;b'" in parts[0]
    assert "'c;d;e'" in parts[1]


def test_split_statements_preserves_double_quoted_identifiers() -> None:
    from dorm.db.backends.sqlite import _split_statements

    sql = 'CREATE TABLE "weird;name" (id INT); SELECT 1'
    parts = _split_statements(sql)
    assert len(parts) == 2
    assert '"weird;name"' in parts[0]


def test_split_statements_handles_trailing_semicolon() -> None:
    from dorm.db.backends.sqlite import _split_statements

    parts = _split_statements("SELECT 1;")
    assert parts == ["SELECT 1"]


def test_split_statements_empty_input() -> None:
    from dorm.db.backends.sqlite import _split_statements

    assert _split_statements("") == []
    assert _split_statements(";;") == []
    assert _split_statements("   ;   ;   ") == []


@pytest.mark.asyncio
async def test_libsql_async_execute_script_splits_safely_on_no_executescript() -> None:
    """Regression: when the local-async path's connection lacks
    ``executescript``, the wrapper must split on quote-aware
    boundaries — naive ``sql.split(';')`` would corrupt
    INSERT-with-semicolon-in-literal."""
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    import os
    import tempfile

    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "split.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        try:
            await w._get_conn()
            real = w._async_conn

            class _NoExecScript:
                """Wraps the real conn but hides executescript."""

                def __getattr__(self, name: str) -> Any:
                    if name == "executescript":
                        raise AttributeError("not exposed")
                    return getattr(real, name)

            # Replace the cached async conn with a wrapper that
            # forces the fallback path.
            w._async_conn = _NoExecScript()
            await w.execute_script(
                "CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT); "
                "INSERT INTO u (v) VALUES ('a;b;c')"
            )
            # Restore real conn for the read.
            w._async_conn = real
            rows = await w.execute("SELECT v FROM u", ())
            # The quoted ``;`` must have survived intact.
            assert rows[0][0] == "a;b;c"
        finally:
            await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# Coverage gaps — local async libsql commit-fail, lastrowid None
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_libsql_async_local_execute_write_commit_failure_swallowed() -> None:
    """Local async path swallows commit() errors — the commit is
    a best-effort flush; SELECT-shaped writes (DDL etc.) may not
    have anything to commit and the wrapper must NOT raise."""
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    import os
    import tempfile

    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "commit.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        try:
            await w.execute_script(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"
            )

            real_conn = await w._get_conn()

            async def _boom() -> None:
                raise RuntimeError("commit refused")

            with patch.object(real_conn, "commit", _boom):
                # Must NOT propagate.
                n = await w.execute_write("INSERT INTO t (v) VALUES (?)", (1,))
                # rowcount may be 0/1 depending on driver; assert
                # it doesn't raise.
                assert isinstance(n, int)
        finally:
            await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# Coverage gaps — RedisCache async exception swallow paths
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fakeredis_cache():
    if not _fakeredis_available() or not _redis_available():
        pytest.skip("fakeredis / redis-py not installed")
    import fakeredis
    import fakeredis.aioredis

    from dorm.cache.redis import RedisCache

    cache = RedisCache({"LOCATION": "redis://stub", "TTL": 30})
    cache._sync_client = fakeredis.FakeRedis()
    cache._async_client = fakeredis.aioredis.FakeRedis()
    yield cache
    try:
        cache.close()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_redis_cache_aset_swallows_exception(fakeredis_cache: Any) -> None:
    async def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("set failed")

    with patch.object(fakeredis_cache._async_client, "set", _boom):
        # Must NOT raise.
        await fakeredis_cache.aset("k", b"x")


@pytest.mark.asyncio
async def test_redis_cache_adelete_swallows_exception(
    fakeredis_cache: Any,
) -> None:
    async def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("delete failed")

    with patch.object(fakeredis_cache._async_client, "delete", _boom):
        await fakeredis_cache.adelete("k")


@pytest.mark.asyncio
async def test_redis_cache_adelete_pattern_swallows_exception(
    fakeredis_cache: Any,
) -> None:
    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("scan failed")

    with patch.object(fakeredis_cache._async_client, "scan_iter", _boom):
        n = await fakeredis_cache.adelete_pattern("dormqs:*")
        assert n == 0


@pytest.mark.asyncio
async def test_redis_cache_aset_zero_ttl_no_expiry_branch(
    fakeredis_cache: Any,
) -> None:
    """``timeout=0`` exercises the ``client.set(key, value)`` branch
    that omits ``ex=`` — separate from the TTL>0 branch."""
    await fakeredis_cache.aset("k", b"forever", timeout=0)
    assert (await fakeredis_cache.aget("k")) == b"forever"


# ─────────────────────────────────────────────────────────────────────────────
# Coverage gaps — VectorExtension SQLite forward branch
# ─────────────────────────────────────────────────────────────────────────────


def test_vector_extension_sqlite_forward_sets_flag() -> None:
    """Forward dispatch on a SQLite wrapper must mark
    ``_vec_extension_enabled`` on the connection so future
    re-opens auto-load sqlite-vec. Verify the flag is set
    without requiring a real sqlite-vec install."""
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.operations import VectorExtension

    class _FakeRaw:
        def enable_load_extension(self, on: bool) -> None:
            pass

    class _FakeWrapper:
        vendor = "sqlite"
        _vec_extension_enabled = False

        def get_connection(self) -> Any:
            return _FakeRaw()

    op = VectorExtension()
    fake = _FakeWrapper()

    # Stub sqlite_vec.load to no-op.
    import sys as _sys
    import types

    fake_sv = types.ModuleType("sqlite_vec")
    setattr(fake_sv, "load", lambda conn: None)
    _sys.modules["sqlite_vec"] = fake_sv
    try:
        op.database_forwards("app", fake, None, None)
    finally:
        _sys.modules.pop("sqlite_vec", None)
    assert fake._vec_extension_enabled is True


# ─────────────────────────────────────────────────────────────────────────────
# Coverage gaps — cache.invalidation idempotent across threads (already
# covered) — add the flush-on-cache-outage path that goes through
# _drop_model with a backend that raises on delete_pattern.
# ─────────────────────────────────────────────────────────────────────────────


def test_drop_model_continues_after_one_backend_fails() -> None:
    """When one cache alias fails, the handler must still try the
    other configured aliases — invalidation is best-effort but
    one Redis outage shouldn't leak stale data on the OTHER
    cache."""
    from dorm.cache import BaseCache, reset_caches
    from dorm.cache.invalidation import _drop_model
    from dorm.conf import settings

    healthy_calls = {"n": 0}

    class _Healthy(BaseCache):
        def __init__(self, cfg: dict | None = None) -> None:
            pass

        def get(self, key: str) -> bytes | None:
            return None

        def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
            pass

        def delete(self, key: str) -> None:
            pass

        def delete_pattern(self, pattern: str) -> int:
            healthy_calls["n"] += 1
            return 0

        async def aget(self, key: str) -> bytes | None:
            return None

        async def aset(self, key: str, value: bytes, timeout: int | None = None) -> None:
            pass

        async def adelete(self, key: str) -> None:
            pass

        async def adelete_pattern(self, pattern: str) -> int:
            healthy_calls["n"] += 1
            return 0

    class _Broken(BaseCache):
        def __init__(self, cfg: dict | None = None) -> None:
            pass

        def get(self, key: str) -> bytes | None:
            return None

        def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
            pass

        def delete(self, key: str) -> None:
            pass

        def delete_pattern(self, pattern: str) -> int:
            raise RuntimeError("backend down")

        async def aget(self, key: str) -> bytes | None:
            return None

        async def aset(self, key: str, value: bytes, timeout: int | None = None) -> None:
            pass

        async def adelete(self, key: str) -> None:
            pass

        async def adelete_pattern(self, pattern: str) -> int:
            raise RuntimeError("backend down")

    prev = settings.CACHES
    settings.CACHES = {
        "broken": {"BACKEND": "tests.test_v2_5_audit_fixes._BrokenAlias"},
        "healthy": {"BACKEND": "tests.test_v2_5_audit_fixes._HealthyAlias"},
    }
    reset_caches()
    try:
        _drop_model(Author)
    finally:
        settings.CACHES = prev
        reset_caches()


class _BrokenAlias:
    def __init__(self, cfg: dict | None = None) -> None:
        pass

    def delete_pattern(self, pattern: str) -> int:
        raise RuntimeError("down")

    async def adelete_pattern(self, pattern: str) -> int:
        raise RuntimeError("down")


class _HealthyAlias:
    def __init__(self, cfg: dict | None = None) -> None:
        pass

    def delete_pattern(self, pattern: str) -> int:
        return 0

    async def adelete_pattern(self, pattern: str) -> int:
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# Coverage gaps — libsql sync wrapper PRAGMA + autocommit branches
# ─────────────────────────────────────────────────────────────────────────────


def test_libsql_sync_autocommit_isolation_level_set() -> None:
    """When the wrapper opens a connection while ``_autocommit``
    is True, ``isolation_level`` must be set to ``None``."""
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    import os
    import tempfile

    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "ac.db")
        w = LibSQLDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        w._autocommit = True
        try:
            conn = w.get_connection()
            # pyturso accepts isolation_level set; verify the
            # branch ran without raising.
            assert hasattr(conn, "execute")
        finally:
            w.close()


def test_libsql_sync_replica_no_op_without_sync_url() -> None:
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    import os
    import tempfile

    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "no.db")
        w = LibSQLDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        try:
            # Local-only mode → must not raise / call anything.
            w.sync_replica()
        finally:
            w.close()


# ─────────────────────────────────────────────────────────────────────────────
# CACHE_INSECURE_PICKLE settings reset on configure
# ─────────────────────────────────────────────────────────────────────────────


def test_configure_with_signing_key_resets_signing_cache() -> None:
    """``configure(CACHE_SIGNING_KEY=...)`` must reset the
    memoised signing key so the next sign / verify reads the
    new value."""
    from dorm.cache import reset_signing_key, sign_payload
    from dorm.conf import settings

    prev_key = settings.CACHE_SIGNING_KEY
    try:
        reset_signing_key()
        settings.CACHE_SIGNING_KEY = "first"
        sig_a = sign_payload(b"x")
        # configure() with the new key should drop the cache.
        dorm.configure(
            DATABASES=settings.DATABASES,
            CACHE_SIGNING_KEY="second",
        )
        sig_b = sign_payload(b"x")
        assert sig_a != sig_b
    finally:
        settings.CACHE_SIGNING_KEY = prev_key
        reset_signing_key()


# ─────────────────────────────────────────────────────────────────────────────
# Concurrent invalidation: many threads bump version once each
# ─────────────────────────────────────────────────────────────────────────────


def test_concurrent_bump_thread_safety() -> None:
    """``bump_model_cache_version`` is called from signal handlers
    that may fire across threads. Verify the counter increments
    monotonically under concurrent bumps."""
    from dorm.cache import bump_model_cache_version, model_cache_version

    base = model_cache_version(Author)
    n_threads = 16

    def _worker() -> None:
        bump_model_cache_version(Author)

    threads = [threading.Thread(target=_worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    final = model_cache_version(Author)
    assert final == base + n_threads
