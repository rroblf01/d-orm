"""Coverage-driven tests for the v2.5 features (libsql + Redis).

Targets the code paths the smoke-level tests in
``test_libsql_v2_5.py`` and ``test_redis_cache_v2_5.py`` skip:

- ``LibSQLAsyncDatabaseWrapper`` execute / execute_write /
  execute_insert / execute_script / close round-trip on a local
  file (libsql_experimental sync client wrapped in
  ``asyncio.to_thread``).
- ``LibSQLDatabaseWrapper.sync_replica`` no-op + ``_coerce_params``
  shape coercion for stricter libsql binding.
- ``RedisCache`` sync + async paths under ``fakeredis`` —
  ``get`` / ``set`` / ``delete`` / ``delete_pattern`` / ``close`` /
  exception fall-through / TTL=0 indefinite caching.
- ``BaseCache`` interface raises NotImplementedError on every
  method — callers learn at first use, not at lookup time.
- ``_import_class`` rejects bad dotted paths.
- ``get_cache`` rejects missing alias / missing BACKEND.
- ``ensure_signals_connected`` is idempotent across threads.
- Cache invalidation handlers swallow settings errors.
- pgvector vendor branches: libsql ``F32_BLOB`` /
  ``vector_distance_*`` round-trip, Postgres ``vector(N)`` shape,
  unknown vendor returns ``None``.
- conf.parse_database_url libsql edge cases.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
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
# BaseCache interface
# ─────────────────────────────────────────────────────────────────────────────


def test_base_cache_default_timeout_default() -> None:
    from dorm.cache import BaseCache

    cache = BaseCache()
    assert cache.default_timeout == 300


def test_base_cache_default_timeout_overridden() -> None:
    from dorm.cache import BaseCache

    cache = BaseCache()
    setattr(cache, "_default_timeout", 42)
    assert cache.default_timeout == 42


def test_base_cache_methods_raise_not_implemented() -> None:
    from dorm.cache import BaseCache

    cache = BaseCache()
    with pytest.raises(NotImplementedError):
        cache.get("x")
    with pytest.raises(NotImplementedError):
        cache.set("x", b"v")
    with pytest.raises(NotImplementedError):
        cache.delete("x")
    with pytest.raises(NotImplementedError):
        cache.delete_pattern("x:*")


@pytest.mark.asyncio
async def test_base_cache_async_methods_raise_not_implemented() -> None:
    from dorm.cache import BaseCache

    cache = BaseCache()
    with pytest.raises(NotImplementedError):
        await cache.aget("x")
    with pytest.raises(NotImplementedError):
        await cache.aset("x", b"v")
    with pytest.raises(NotImplementedError):
        await cache.adelete("x")
    with pytest.raises(NotImplementedError):
        await cache.adelete_pattern("x:*")


# ─────────────────────────────────────────────────────────────────────────────
# _import_class + get_cache configuration errors
# ─────────────────────────────────────────────────────────────────────────────


def test_import_class_rejects_bare_name() -> None:
    from dorm.cache import _import_class
    from dorm.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="dotted path"):
        _import_class("notdotted")


def test_import_class_resolves_dotted_path() -> None:
    from dorm.cache import _import_class

    cls = _import_class("dorm.cache.BaseCache")
    from dorm.cache import BaseCache

    assert cls is BaseCache


def test_get_cache_missing_alias_raises() -> None:
    from dorm.cache import get_cache, reset_caches
    from dorm.conf import settings
    from dorm.exceptions import ImproperlyConfigured

    reset_caches()
    prev = settings.CACHES
    settings.CACHES = {}
    try:
        with pytest.raises(ImproperlyConfigured, match="not configured"):
            get_cache("nonexistent")
    finally:
        settings.CACHES = prev
        reset_caches()


def test_get_cache_missing_backend_raises() -> None:
    from dorm.cache import get_cache, reset_caches
    from dorm.conf import settings
    from dorm.exceptions import ImproperlyConfigured

    reset_caches()
    prev = settings.CACHES
    settings.CACHES = {"default": {}}
    try:
        with pytest.raises(ImproperlyConfigured, match="missing a BACKEND"):
            get_cache("default")
    finally:
        settings.CACHES = prev
        reset_caches()


def test_get_cache_memoises_per_alias() -> None:
    from dorm.cache import BaseCache, get_cache, reset_caches
    from dorm.conf import settings

    class _Mem(BaseCache):
        def __init__(self, cfg: dict | None = None) -> None:
            self.cfg = cfg or {}

    reset_caches()
    prev = settings.CACHES
    settings.CACHES = {
        "default": {
            "BACKEND": "tests.test_v2_5_coverage._FakeMemBackend",
        },
    }
    try:
        a = get_cache("default")
        b = get_cache("default")
        assert a is b  # memoised
    finally:
        settings.CACHES = prev
        reset_caches()


class _FakeMemBackend:
    """Module-level backend so the dotted-path import resolves."""

    def __init__(self, cfg: dict | None = None) -> None:
        self.cfg = cfg or {}

    def close(self) -> None:
        self.cfg["closed"] = True


def test_reset_caches_calls_close_when_available() -> None:
    from dorm.cache import get_cache, reset_caches
    from dorm.conf import settings

    reset_caches()
    prev = settings.CACHES
    settings.CACHES = {
        "default": {"BACKEND": "tests.test_v2_5_coverage._FakeMemBackend"},
    }
    try:
        backend = get_cache("default")
        reset_caches()
        assert getattr(backend, "cfg").get("closed") is True
    finally:
        settings.CACHES = prev
        reset_caches()


def test_model_cache_namespace_uses_app_label_and_class_name() -> None:
    from dorm.cache import model_cache_namespace

    ns = model_cache_namespace(Author)
    assert ns.startswith("dormqs:")
    assert "Author" in ns


# ─────────────────────────────────────────────────────────────────────────────
# RedisCache against fakeredis
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fakeredis_cache():
    if not _fakeredis_available():
        pytest.skip("fakeredis not installed")
    if not _redis_available():
        pytest.skip("redis-py not installed (djanorm[redis])")
    import fakeredis
    import fakeredis.aioredis

    from dorm.cache.redis import RedisCache

    cache = RedisCache(
        {"LOCATION": "redis://stub", "TTL": 60}
    )
    # Inject fakeredis instances directly so we don't need a real
    # Redis server. Keeping the LOCATION populated keeps the
    # constructor path covered.
    cache._sync_client = fakeredis.FakeRedis()
    cache._async_client = fakeredis.aioredis.FakeRedis()
    yield cache
    try:
        cache.close()
    except Exception:
        pass


def test_redis_cache_default_timeout(fakeredis_cache: Any) -> None:
    assert fakeredis_cache.default_timeout == 60


def test_redis_cache_set_and_get_round_trip(fakeredis_cache: Any) -> None:
    fakeredis_cache.set("k", b"hello", timeout=5)
    assert fakeredis_cache.get("k") == b"hello"


def test_redis_cache_set_default_timeout_used(fakeredis_cache: Any) -> None:
    fakeredis_cache.set("k", b"x")  # no timeout → uses TTL=60
    assert fakeredis_cache.get("k") == b"x"


def test_redis_cache_set_zero_ttl_no_expiry(fakeredis_cache: Any) -> None:
    """``timeout=0`` means "cache indefinitely"; the wrapped
    redis-py call must NOT pass ``ex=0`` (which itself would
    raise ``ValueError: invalid expire time``).
    """
    fakeredis_cache.set("k", b"forever", timeout=0)
    assert fakeredis_cache.get("k") == b"forever"


def test_redis_cache_delete_round_trip(fakeredis_cache: Any) -> None:
    fakeredis_cache.set("k", b"x")
    fakeredis_cache.delete("k")
    assert fakeredis_cache.get("k") is None


def test_redis_cache_delete_pattern_evicts_matching(
    fakeredis_cache: Any,
) -> None:
    fakeredis_cache.set("dormqs:Author:abc", b"1")
    fakeredis_cache.set("dormqs:Author:def", b"2")
    fakeredis_cache.set("dormqs:Book:abc", b"3")
    n = fakeredis_cache.delete_pattern("dormqs:Author:*")
    assert n == 2
    assert fakeredis_cache.get("dormqs:Author:abc") is None
    assert fakeredis_cache.get("dormqs:Author:def") is None
    assert fakeredis_cache.get("dormqs:Book:abc") == b"3"


def test_redis_cache_get_swallows_exception(fakeredis_cache: Any) -> None:
    """A backend outage must NEVER propagate — get returns None
    so the queryset layer falls through to the DB."""
    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("redis down")

    with patch.object(fakeredis_cache._sync_client, "get", _boom):
        assert fakeredis_cache.get("k") is None


def test_redis_cache_set_swallows_exception(fakeredis_cache: Any) -> None:
    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("redis down")

    with patch.object(fakeredis_cache._sync_client, "set", _boom):
        # Must NOT raise.
        fakeredis_cache.set("k", b"x")


def test_redis_cache_delete_swallows_exception(
    fakeredis_cache: Any,
) -> None:
    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("redis down")

    with patch.object(fakeredis_cache._sync_client, "delete", _boom):
        fakeredis_cache.delete("k")


def test_redis_cache_delete_pattern_swallows_exception(
    fakeredis_cache: Any,
) -> None:
    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("redis down")

    with patch.object(fakeredis_cache._sync_client, "scan_iter", _boom):
        n = fakeredis_cache.delete_pattern("dormqs:*")
        assert n == 0


def test_redis_cache_close_releases_client() -> None:
    if not _fakeredis_available() or not _redis_available():
        pytest.skip("fakeredis / redis-py not installed")
    import fakeredis

    from dorm.cache.redis import RedisCache

    cache = RedisCache({"LOCATION": "redis://stub"})
    cache._sync_client = fakeredis.FakeRedis()
    cache.close()
    assert cache._sync_client is None


# ─────────────────────────────────────────────────────────────────────────────
# RedisCache async path
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_redis_cache_aset_aget_round_trip(
    fakeredis_cache: Any,
) -> None:
    await fakeredis_cache.aset("k", b"async-hello", timeout=5)
    val = await fakeredis_cache.aget("k")
    assert val == b"async-hello"


@pytest.mark.asyncio
async def test_redis_cache_aset_zero_ttl(fakeredis_cache: Any) -> None:
    await fakeredis_cache.aset("k", b"forever", timeout=0)
    assert (await fakeredis_cache.aget("k")) == b"forever"


@pytest.mark.asyncio
async def test_redis_cache_adelete(fakeredis_cache: Any) -> None:
    await fakeredis_cache.aset("k", b"x")
    await fakeredis_cache.adelete("k")
    assert (await fakeredis_cache.aget("k")) is None


@pytest.mark.asyncio
async def test_redis_cache_adelete_pattern(fakeredis_cache: Any) -> None:
    await fakeredis_cache.aset("dormqs:A:1", b"a")
    await fakeredis_cache.aset("dormqs:A:2", b"b")
    await fakeredis_cache.aset("dormqs:B:1", b"c")
    n = await fakeredis_cache.adelete_pattern("dormqs:A:*")
    assert n == 2
    assert (await fakeredis_cache.aget("dormqs:A:1")) is None
    assert (await fakeredis_cache.aget("dormqs:B:1")) == b"c"


@pytest.mark.asyncio
async def test_redis_cache_aget_swallows_exception(
    fakeredis_cache: Any,
) -> None:
    async def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("redis down")

    with patch.object(fakeredis_cache._async_client, "get", _boom):
        assert (await fakeredis_cache.aget("k")) is None


# ─────────────────────────────────────────────────────────────────────────────
# Cache invalidation
# ─────────────────────────────────────────────────────────────────────────────


def test_ensure_signals_connected_is_idempotent() -> None:
    from dorm.cache.invalidation import ensure_signals_connected
    from dorm.signals import post_save

    ensure_signals_connected()
    before = len(post_save._receivers)
    ensure_signals_connected()
    ensure_signals_connected()
    after = len(post_save._receivers)
    assert before == after


def test_ensure_signals_connected_thread_safe() -> None:
    from dorm.cache.invalidation import ensure_signals_connected
    from dorm.signals import post_save

    threads = [
        threading.Thread(target=ensure_signals_connected) for _ in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Exactly one sync handler + one async handler dispatch_uid
    # entry for save (the lock + flag prevent duplicates).
    uids = {r[0] for r in post_save._receivers}
    save_uids = {u for u in uids if isinstance(u, str) and "cache.invalidation" in u}
    assert save_uids == {"dorm.cache.invalidation.save", "dorm.cache.invalidation.asave"}


def test_drop_model_handles_missing_settings() -> None:
    from dorm.cache.invalidation import _drop_model

    class _NoMeta:
        pass

    # Sender without _meta — namespace lookup fails, function
    # must not raise.
    _drop_model(_NoMeta)


def test_drop_model_handles_cache_outage() -> None:
    from dorm.cache.invalidation import _drop_model
    from dorm.conf import settings

    prev = settings.CACHES
    settings.CACHES = {
        "default": {"BACKEND": "nonexistent.module.Backend"},
    }
    try:
        _drop_model(Author)  # must not raise
    finally:
        settings.CACHES = prev


@pytest.mark.asyncio
async def test_adrop_model_handles_cache_outage() -> None:
    from dorm.cache.invalidation import _adrop_model
    from dorm.conf import settings

    prev = settings.CACHES
    settings.CACHES = {
        "default": {"BACKEND": "nonexistent.module.Backend"},
    }
    try:
        await _adrop_model(Author)  # must not raise
    finally:
        settings.CACHES = prev


# ─────────────────────────────────────────────────────────────────────────────
# QuerySet cache-state propagation across chain
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_state_survives_filter_chain() -> None:
    qs = Author.objects.all().cache(timeout=99).filter(name="x")
    assert qs._cache_alias == "default"
    assert qs._cache_timeout == 99


def test_cache_state_survives_values_chain() -> None:
    qs = Author.objects.all().cache(timeout=99).values("name")
    assert qs._cache_alias == "default"
    assert qs._cache_timeout == 99


def test_cache_state_survives_values_list_chain() -> None:
    qs = Author.objects.all().cache(timeout=99).values_list("name", flat=True)
    assert qs._cache_alias == "default"
    assert qs._cache_timeout == 99


def test_cache_key_returns_none_without_alias() -> None:
    qs = Author.objects.filter(name="x")
    assert qs._cache_key() is None


def test_cache_lookup_sync_returns_none_without_alias() -> None:
    qs = Author.objects.filter(name="x")
    assert qs._cache_lookup_sync() is None


def test_cache_store_sync_no_op_without_alias() -> None:
    """Store path must early-exit (no errors, no writes) when the
    queryset isn't opted into caching."""
    qs = Author.objects.filter(name="x")
    qs._cache_store_sync([])  # Must NOT raise.


@pytest.mark.asyncio
async def test_cache_lookup_async_returns_none_without_alias() -> None:
    qs = Author.objects.filter(name="x")
    assert (await qs._cache_lookup_async()) is None


@pytest.mark.asyncio
async def test_cache_store_async_no_op_without_alias() -> None:
    qs = Author.objects.filter(name="x")
    await qs._cache_store_async([])  # Must NOT raise.


# ─────────────────────────────────────────────────────────────────────────────
# pgvector vendor branches
# ─────────────────────────────────────────────────────────────────────────────


def test_vector_field_db_type_postgresql() -> None:
    from dorm.contrib.pgvector.fields import VectorField

    class _Conn:
        vendor = "postgresql"

    f = VectorField(dimensions=1536)
    assert f.db_type(_Conn()) == "vector(1536)"


def test_vector_field_db_type_sqlite() -> None:
    from dorm.contrib.pgvector.fields import VectorField

    class _Conn:
        vendor = "sqlite"

    f = VectorField(dimensions=384)
    assert f.db_type(_Conn()) == "BLOB"


def test_vector_field_db_type_libsql() -> None:
    from dorm.contrib.pgvector.fields import VectorField

    class _Conn:
        vendor = "libsql"

    f = VectorField(dimensions=384)
    assert f.db_type(_Conn()) == "F32_BLOB(384)"


def test_vector_field_db_type_unknown_vendor_returns_none() -> None:
    from dorm.contrib.pgvector.fields import VectorField

    class _Conn:
        vendor = "weirdb"

    f = VectorField(dimensions=10)
    assert f.db_type(_Conn()) is None


def test_vector_field_rejects_bad_dimensions() -> None:
    from dorm.contrib.pgvector.fields import VectorField

    with pytest.raises(ValueError):
        VectorField(dimensions=0)
    with pytest.raises(ValueError):
        VectorField(dimensions=-5)
    with pytest.raises(ValueError):
        VectorField(dimensions=20000)  # > 16000 cap


def test_l2_distance_pg_emits_operator() -> None:
    from dorm.contrib.pgvector.expressions import L2Distance

    class _Conn:
        vendor = "postgresql"

    d = L2Distance("embedding", [0.1, 0.2])
    sql, _ = d.as_sql(connection=_Conn())
    assert "<->" in sql


def test_cosine_distance_pg_emits_operator() -> None:
    from dorm.contrib.pgvector.expressions import CosineDistance

    class _Conn:
        vendor = "postgresql"

    d = CosineDistance("embedding", [0.1, 0.2])
    sql, _ = d.as_sql(connection=_Conn())
    assert "<=>" in sql


def test_max_inner_product_pg_emits_operator() -> None:
    from dorm.contrib.pgvector.expressions import MaxInnerProduct

    class _Conn:
        vendor = "postgresql"

    d = MaxInnerProduct("embedding", [0.1, 0.2])
    sql, _ = d.as_sql(connection=_Conn())
    assert "<#>" in sql


def test_max_inner_product_sqlite_raises() -> None:
    from dorm.contrib.pgvector.expressions import MaxInnerProduct

    class _Conn:
        vendor = "sqlite"

    d = MaxInnerProduct("embedding", [0.1, 0.2])
    with pytest.raises(NotImplementedError):
        d.as_sql(connection=_Conn())


def test_distance_validates_column_identifier() -> None:
    from dorm.contrib.pgvector.expressions import L2Distance

    with pytest.raises(ValueError):
        L2Distance("evil; DROP TABLE x", [0.1])


# ─────────────────────────────────────────────────────────────────────────────
# parse_database_url libsql edge cases
# ─────────────────────────────────────────────────────────────────────────────


def test_parse_libsql_memory_default() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url("libsql://my-db.turso.io")
    assert cfg["NAME"] == ":memory:"
    assert "AUTH_TOKEN" not in cfg


def test_parse_libsql_local_root_slash_only() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url("libsql:///")
    assert cfg["NAME"] in ("", ":memory:")


def test_parse_libsql_extra_query_params_go_to_options() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url(
        "libsql://my-db.turso.io?authToken=tok&debug=1&foo=bar"
    )
    assert cfg["AUTH_TOKEN"] == "tok"
    assert cfg.get("OPTIONS") == {"debug": "1", "foo": "bar"}


def test_parse_libsql_ws_scheme() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url(
        "libsql+ws://my-db.turso.io?authToken=tok"
    )
    assert cfg["SYNC_URL"] == "ws://my-db.turso.io"


def test_parse_libsql_http_scheme() -> None:
    from dorm.conf import parse_database_url

    cfg = parse_database_url(
        "libsql+http://my-db.turso.io?authToken=tok"
    )
    assert cfg["SYNC_URL"] == "http://my-db.turso.io"


def test_parse_unrecognised_scheme_raises_with_libsql_in_message() -> None:
    from dorm.conf import parse_database_url
    from dorm.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="libsql"):
        parse_database_url("oracle://x:y@host/db")


# ─────────────────────────────────────────────────────────────────────────────
# LibSQL sync wrapper paths
# ─────────────────────────────────────────────────────────────────────────────


def test_libsql_coerce_params_none_to_empty_tuple() -> None:
    if not _libsql_available():
        pytest.skip("libsql not installed")
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    w = LibSQLDatabaseWrapper({"ENGINE": "libsql", "NAME": ":memory:"})
    try:
        assert w._coerce_params(None) == ()
        assert w._coerce_params([1, 2]) == (1, 2)
        assert w._coerce_params((3, 4)) == (3, 4)
        # Mapping (dict) passes through.
        m = {"a": 1}
        assert w._coerce_params(m) is m
    finally:
        w.close()


def test_libsql_sync_replica_no_op_when_no_sync_attr() -> None:
    if not _libsql_available():
        pytest.skip("libsql not installed")
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    w = LibSQLDatabaseWrapper({"ENGINE": "libsql", "NAME": ":memory:"})
    try:
        # Local-only mode: no sync() method → must not raise.
        w.sync_replica()
    finally:
        w.close()


def test_libsql_round_trip_with_list_params() -> None:
    """Regression: sync wrapper accepts list params (coerces to
    tuple before calling the libsql cursor, which only accepts
    tuples)."""
    if not _libsql_available():
        pytest.skip("libsql not installed")
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "lib.db")
        w = LibSQLDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        w.execute_script(
            'CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)'
        )
        # Pass a list — the override must coerce it to a tuple.
        w.execute_write("INSERT INTO t (v) VALUES (%s)", [42])
        rows = w.execute("SELECT v FROM t WHERE v = %s", [42])
        assert len(rows) == 1
        w.close()


# ─────────────────────────────────────────────────────────────────────────────
# LibSQL async wrapper paths
# ─────────────────────────────────────────────────────────────────────────────


def _make_async_libsql_wrapper(path: str):
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    return LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})


@pytest.mark.asyncio
async def test_libsql_async_round_trip() -> None:
    if not _libsql_available():
        pytest.skip("libsql not installed")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "alib.db")
        w = _make_async_libsql_wrapper(path)
        try:
            await w.execute_script(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"
            )
            await w.execute_write("INSERT INTO t (v) VALUES (%s)", [10])
            new_id = await w.execute_insert(
                "INSERT INTO t (v) VALUES (%s)", [20]
            )
            assert new_id is not None
            rows = await w.execute("SELECT v FROM t ORDER BY id", [])
            vals = [r[0] for r in rows]
            assert vals == [10, 20]
        finally:
            await w.close()


@pytest.mark.asyncio
async def test_libsql_async_get_conn_caches_under_concurrency() -> None:
    """Two concurrent ``_get_conn`` calls must return the same
    connection — without the lock guard one of them would leak a
    second open connection."""
    if not _libsql_available():
        pytest.skip("libsql not installed")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "conc.db")
        w = _make_async_libsql_wrapper(path)
        try:
            results = await asyncio.gather(
                w._get_conn(), w._get_conn(), w._get_conn()
            )
            assert results[0] is results[1] is results[2]
        finally:
            await w.close()


@pytest.mark.asyncio
async def test_libsql_async_close_idempotent() -> None:
    if not _libsql_available():
        pytest.skip("libsql not installed")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "idem.db")
        w = _make_async_libsql_wrapper(path)
        await w._get_conn()
        await w.close()
        await w.close()  # double-close must not raise
        assert w._sync_conn is None


@pytest.mark.asyncio
async def test_libsql_async_force_close_sync_releases_handle() -> None:
    if not _libsql_available():
        pytest.skip("libsql not installed")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "fc.db")
        w = _make_async_libsql_wrapper(path)
        await w._get_conn()
        # Sync teardown path — used by ``reset_connections`` /
        # atexit hooks where awaiting a coroutine isn't possible.
        w.force_close_sync()
        assert w._sync_conn is None


@pytest.mark.asyncio
async def test_libsql_async_execute_script_split_on_attribute_error() -> None:
    """Some libsql client builds expose only ``execute`` (no
    ``executescript``). The wrapper falls back to splitting on
    ``;`` — verify the fallback path runs end-to-end with a
    fake cursor that lacks ``executescript``."""
    if not _libsql_available():
        pytest.skip("libsql not installed")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "split.db")
        w = _make_async_libsql_wrapper(path)
        try:
            await w._get_conn()

            # Wrap the conn so executescript blows up with
            # AttributeError; per-statement execute should still
            # work.
            real_conn = w._sync_conn

            class _Wrap:
                def __getattr__(self, name: str) -> Any:
                    if name == "executescript":
                        raise AttributeError("not exposed")
                    return getattr(real_conn, name)

            w._sync_conn = _Wrap()
            await w.execute_script(
                "CREATE TABLE u (id INTEGER PRIMARY KEY); CREATE TABLE v (id INTEGER PRIMARY KEY)"
            )
            # Restore for cleanup.
            w._sync_conn = real_conn
            rows = await w.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
                [],
            )
            names = [r[0] for r in rows]
            assert "u" in names
            assert "v" in names
        finally:
            await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# Routing through configure(DATABASES=..., CACHES=...)
# ─────────────────────────────────────────────────────────────────────────────


def test_configure_caches_resets_cache_pool() -> None:
    """``configure(CACHES={...})`` must invalidate any memoised
    cache instance so the second call instantiates against the
    new config."""
    from dorm.cache import _caches, get_cache, reset_caches
    from dorm.conf import settings

    reset_caches()
    prev_caches = settings.CACHES

    settings.CACHES = {
        "default": {"BACKEND": "tests.test_v2_5_coverage._FakeMemBackend"},
    }
    first = get_cache("default")
    assert "default" in _caches

    # Re-configure — the conf.configure helper calls reset_caches
    # internally; here we exercise it directly to keep the test
    # zero-dep on a real DB swap.
    dorm.configure(
        DATABASES=settings.DATABASES,
        CACHES=settings.CACHES,
    )
    # After reset, `get_cache` returns a *different* instance.
    second = get_cache("default")
    assert first is not second

    settings.CACHES = prev_caches
    reset_caches()
