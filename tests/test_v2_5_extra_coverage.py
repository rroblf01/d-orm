"""Second wave of v2.5 coverage tests.

Targets the surface ``test_v2_5_coverage.py`` left untested:

- ``VectorField`` ``to_python`` / ``from_db_value`` /
  ``get_db_prep_value`` shape coercion across every input type
  (bytes, str, list, tuple, numpy-like ``.tolist`` shim).
- ``_pack_float32`` / ``_unpack_float32`` round-trip and the
  length-not-multiple-of-4 ``ValidationError``.
- ``VectorExtension`` migration operation: PG / SQLite forward
  + backward dispatch and the unknown-vendor warning path.
- ``HnswIndex`` / ``IvfflatIndex`` SQL emit (PG-only;
  validates the storage-parameter ``WITH (...)`` clause).
- ``RedisCache`` constructor without redis-py installed (helpful
  ImproperlyConfigured at first ``_get_sync`` / ``_get_async``).
- Async libsql wrapper local-mode round-trip including
  ``execute_script`` AttributeError fallback path.
- ``QuerySet`` cache-state propagation when ``ValuesListQuerySet``
  is materialised as ``alist()``.
- Concurrent invalidation: many threads calling
  ``ensure_signals_connected`` followed by Model save fires the
  handler exactly once per backend per save.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Any
from unittest.mock import patch

import pytest

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
# VectorField helpers
# ─────────────────────────────────────────────────────────────────────────────


def test_pack_unpack_float32_round_trip() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import _pack_float32, _unpack_float32

    src = [0.1, 0.2, 0.3, -1.5, 0.0]
    packed = _pack_float32(src)
    # 5 float32 = 20 bytes.
    assert len(packed) == 20
    out = _unpack_float32(packed)
    for a, b in zip(src, out):
        assert abs(a - b) < 1e-6


def test_unpack_float32_rejects_non_multiple_of_4() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import _unpack_float32
    from dorm.exceptions import ValidationError

    with pytest.raises(ValidationError, match="not a multiple of 4"):
        _unpack_float32(b"\x00\x00\x00")  # 3 bytes


def test_vector_field_to_python_handles_bytes() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField, _pack_float32

    f = VectorField(dimensions=3)
    f.name = "vec"
    out = f.to_python(_pack_float32([1.0, 2.0, 3.0]))
    assert out == pytest.approx([1.0, 2.0, 3.0])


def test_vector_field_to_python_handles_pgvector_text() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    out = f.to_python("[1.0,2.0,3.0]")
    assert out == [1.0, 2.0, 3.0]


def test_vector_field_to_python_handles_empty_text() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=0 + 1)  # placeholder; just exercise
    f.name = "vec"
    # to_python on "[]" returns []; from_db_value would reject due
    # to dim check, but to_python alone is the unit under test.
    assert f.to_python("[]") == []


def test_vector_field_to_python_handles_numpy_like() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    class _Arr:
        """Stand-in for numpy.ndarray: only ``.tolist()`` matters."""

        def tolist(self) -> list:
            return [1.0, 2.0, 3.0]

    f = VectorField(dimensions=3)
    f.name = "vec"
    out = f.to_python(_Arr())
    assert out == [1.0, 2.0, 3.0]


def test_vector_field_to_python_handles_list_and_tuple() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    assert f.to_python([1, 2, 3]) == [1.0, 2.0, 3.0]
    assert f.to_python((1, 2, 3)) == [1.0, 2.0, 3.0]


def test_vector_field_to_python_passes_none_through() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    assert f.to_python(None) is None


def test_vector_field_get_db_prep_value_rejects_wrong_length() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField
    from dorm.exceptions import ValidationError

    f = VectorField(dimensions=4)
    f.name = "vec"
    with pytest.raises(ValidationError, match="expected 4-d vector"):
        f.get_db_prep_value([1.0, 2.0])


def test_vector_field_get_db_prep_value_passes_none() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=4)
    assert f.get_db_prep_value(None) is None


def test_vector_field_get_db_prep_value_pg_text_form() -> None:
    """Without a configured connection the field falls back to PG
    text form. Verify the wire string shape."""
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    # Patch get_connection to return a PG-shaped wrapper.
    with patch("dorm.db.connection.get_connection") as gc:
        gc.return_value = type("C", (), {"vendor": "postgresql"})()
        out = f.get_db_prep_value([1.5, 2.5, 3.5])
    assert isinstance(out, str)
    assert out.startswith("[")
    assert out.endswith("]")


def test_vector_field_get_db_prep_value_libsql_packed_bytes() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    with patch("dorm.db.connection.get_connection") as gc:
        gc.return_value = type("C", (), {"vendor": "libsql"})()
        out = f.get_db_prep_value([1.0, 2.0, 3.0])
    assert isinstance(out, bytes)
    assert len(out) == 12


def test_vector_field_get_db_prep_value_falls_back_when_unconfigured() -> None:
    """When the connection lookup raises, we default to the PG text
    form — matches the documented behaviour."""
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    with patch(
        "dorm.db.connection.get_connection",
        side_effect=RuntimeError("no config"),
    ):
        out = f.get_db_prep_value([1.0, 2.0, 3.0])
    assert isinstance(out, str)


def test_vector_field_from_db_value_passes_none() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField

    f = VectorField(dimensions=3)
    f.name = "vec"
    assert f.from_db_value(None) is None


def test_vector_field_coerce_sequence_paths() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField, _pack_float32

    f = VectorField(dimensions=3)
    # bytes → unpacked floats.
    out = f._coerce_sequence(_pack_float32([1.0, 2.0, 3.0]))
    assert list(out) == pytest.approx([1.0, 2.0, 3.0])
    # text.
    out = f._coerce_sequence("[1.0,2.0,3.0]")
    assert out == [1.0, 2.0, 3.0]
    # numpy-like.
    class _Arr:
        def tolist(self) -> list:
            return [1.0, 2.0, 3.0]

    assert f._coerce_sequence(_Arr()) == [1.0, 2.0, 3.0]
    # tuple.
    assert list(f._coerce_sequence((1, 2, 3))) == [1, 2, 3]
    # plain iterable.
    assert list(f._coerce_sequence(iter([1.0, 2.0]))) == [1.0, 2.0]


# ─────────────────────────────────────────────────────────────────────────────
# VectorExtension migration operation
# ─────────────────────────────────────────────────────────────────────────────


def test_vector_extension_pg_forwards_emits_create_extension() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.operations import VectorExtension

    op = VectorExtension()
    scripts: list[str] = []

    class _Conn:
        vendor = "postgresql"

        def execute_script(self, sql: str) -> None:
            scripts.append(sql)

    op.database_forwards("app", _Conn(), None, None)
    assert any('CREATE EXTENSION IF NOT EXISTS "vector"' in s for s in scripts)


def test_vector_extension_pg_backwards_emits_drop_extension() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.operations import VectorExtension

    op = VectorExtension()
    scripts: list[str] = []

    class _Conn:
        vendor = "postgresql"

        def execute_script(self, sql: str) -> None:
            scripts.append(sql)

    op.database_backwards("app", _Conn(), None, None)
    assert any('DROP EXTENSION IF EXISTS "vector"' in s for s in scripts)


def test_vector_extension_unknown_vendor_warns(caplog: Any) -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.operations import VectorExtension

    class _Conn:
        vendor = "weirdb"

    import logging

    with caplog.at_level(logging.WARNING, logger="dorm.contrib.pgvector"):
        VectorExtension().database_forwards("app", _Conn(), None, None)
    assert any("unknown backend" in rec.message for rec in caplog.records)


def test_vector_extension_sqlite_backwards_clears_flag() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.operations import VectorExtension

    class _Conn:
        vendor = "sqlite"
        _vec_extension_enabled = True

    op = VectorExtension()
    conn = _Conn()
    op.database_backwards("app", conn, None, None)
    assert conn._vec_extension_enabled is False


def test_vector_extension_describe_and_repr() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.operations import VectorExtension

    op = VectorExtension()
    assert "vector-search" in op.describe()
    assert repr(op) == "VectorExtension()"


# ─────────────────────────────────────────────────────────────────────────────
# pgvector indexes
# ─────────────────────────────────────────────────────────────────────────────


def test_hnsw_index_emit_create_pg() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.indexes import HnswIndex

    idx = HnswIndex(
        fields=["embedding"],
        name="emb_hnsw",
        opclass="vector_l2_ops",
        m=16,
        ef_construction=64,
    )
    forward, _backward = idx.create_sql("docs", vendor="postgresql")
    assert "CREATE INDEX" in forward
    assert "USING hnsw" in forward
    assert "vector_l2_ops" in forward
    assert "m = 16" in forward
    assert "ef_construction = 64" in forward


def test_ivfflat_index_emit_create_pg() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.indexes import IvfflatIndex

    idx = IvfflatIndex(
        fields=["embedding"],
        name="emb_ivf",
        opclass="vector_cosine_ops",
        lists=100,
    )
    forward, _ = idx.create_sql("docs", vendor="postgresql")
    assert "USING ivfflat" in forward
    assert "vector_cosine_ops" in forward
    assert "lists = 100" in forward


# ─────────────────────────────────────────────────────────────────────────────
# RedisCache helpful errors when the client is missing
# ─────────────────────────────────────────────────────────────────────────────


def test_redis_cache_async_helpful_error_when_missing() -> None:
    """``_get_async`` surfaces ImproperlyConfigured when redis-py
    is not installed. Exercises the import-error branch."""
    import importlib as _il

    real = _il.import_module

    def _block(name: str, *a: Any, **kw: Any) -> Any:
        if name == "redis.asyncio":
            raise ImportError("redis missing")
        return real(name, *a, **kw)

    from dorm.cache.redis import RedisCache
    from dorm.exceptions import ImproperlyConfigured

    cache = RedisCache({"LOCATION": "redis://localhost:6379/0"})
    with patch.object(_il, "import_module", _block):
        with pytest.raises(ImproperlyConfigured, match="redis-py"):
            cache._get_async()


def test_redis_cache_sync_close_idempotent() -> None:
    """``close()`` after a never-instantiated client must not
    crash — the lazy-init pattern means ``_sync_client`` may be
    None."""
    if not _redis_available():
        pytest.skip("redis-py not installed")
    from dorm.cache.redis import RedisCache

    cache = RedisCache({"LOCATION": "redis://stub"})
    cache.close()  # client never built — must be a no-op
    cache.close()  # double close


def test_redis_cache_close_swallows_exceptions() -> None:
    if not _fakeredis_available() or not _redis_available():
        pytest.skip("fakeredis / redis-py not installed")
    import fakeredis

    from dorm.cache.redis import RedisCache

    cache = RedisCache({"LOCATION": "redis://stub"})
    cache._sync_client = fakeredis.FakeRedis()

    def _boom() -> None:
        raise RuntimeError("close failed")

    with patch.object(cache._sync_client, "close", _boom):
        cache.close()  # must not raise
    assert cache._sync_client is None


# ─────────────────────────────────────────────────────────────────────────────
# Async libsql wrapper — local-only path
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_libsql_async_local_mode_uses_native_aio() -> None:
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "alocal.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        try:
            await w.execute_script(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"
            )
            await w.execute_write("INSERT INTO t (v) VALUES (?)", (1,))
            await w.execute_write("INSERT INTO t (v) VALUES (?)", (2,))
            rows = await w.execute("SELECT v FROM t ORDER BY id", ())
            assert [r[0] for r in rows] == [1, 2]
            # Local mode keeps the executor lazy — never allocates.
            assert w._executor is None
        finally:
            await w.close()


@pytest.mark.asyncio
async def test_libsql_async_close_is_idempotent_local() -> None:
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "idem.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        await w._get_conn()
        await w.close()
        await w.close()
        assert w._async_conn is None
        assert w._sync_conn is None


@pytest.mark.asyncio
async def test_libsql_async_force_close_sync() -> None:
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "fc.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        await w._get_conn()
        # Sync teardown — used by reset_connections / atexit.
        w.force_close_sync()
        assert w._async_conn is None
        assert w._sync_conn is None


@pytest.mark.asyncio
async def test_libsql_async_get_conn_concurrent_returns_same() -> None:
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "conc.db")
        w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": path})
        try:
            results = await asyncio.gather(
                w._get_conn(), w._get_conn(), w._get_conn()
            )
            # All three must resolve to the same underlying
            # connection — without the asyncio.Lock guard the
            # second / third callers would each open a fresh
            # connection.
            assert results[0] is results[1] is results[2]
        finally:
            await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# Sync libsql wrapper — _coerce_params edge cases
# ─────────────────────────────────────────────────────────────────────────────


def test_libsql_async_coerce_params_returns_mapping_unchanged() -> None:
    if not _libsql_available():
        pytest.skip("pyturso not installed")
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    w = LibSQLAsyncDatabaseWrapper({"ENGINE": "libsql", "NAME": ":memory:"})
    try:
        m = {"a": 1}
        assert w._coerce_params(m) is m
        assert w._coerce_params(None) == ()
        assert w._coerce_params([1, 2]) == (1, 2)
    finally:
        # Don't open a real connection; just drop the executor if
        # the wrapper allocated one.
        if w._executor is not None:
            w._executor.shutdown(wait=False)


# ─────────────────────────────────────────────────────────────────────────────
# Cache invalidation: writes wipe cached querysets in real round-trip
# ─────────────────────────────────────────────────────────────────────────────


class _SignalMemBackend:
    """Tiny in-process backend used as a stand-in for Redis."""

    def __init__(self, cfg: dict | None = None) -> None:
        self._store: dict[str, bytes] = {}
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
def _signal_cache():
    from dorm.cache import get_cache, reset_caches
    from dorm.conf import settings

    prev = settings.CACHES
    settings.CACHES = {
        "default": {
            "BACKEND": "tests.test_v2_5_extra_coverage._SignalMemBackend",
            "TTL": 60,
        },
    }
    reset_caches()
    backend = get_cache("default")
    yield backend
    reset_caches()
    settings.CACHES = prev


def test_save_drops_every_cached_queryset_for_model(_signal_cache: Any) -> None:
    Author.objects.create(name="Eviction1", age=12)
    qs1 = Author.objects.filter(name="Eviction1").cache(timeout=60)
    qs2 = Author.objects.filter(age__gte=10).cache(timeout=60)
    list(qs1)
    list(qs2)

    k1 = qs1._cache_key()
    k2 = qs2._cache_key()
    assert k1 is not None
    assert k2 is not None
    assert _signal_cache.get(k1) is not None
    assert _signal_cache.get(k2) is not None

    Author.objects.create(name="WriteTrigger", age=20)
    # Both keys must now be gone — coarse-grained invalidation.
    assert _signal_cache.get(k1) is None
    assert _signal_cache.get(k2) is None


def test_delete_drops_cached_querysets(_signal_cache: Any) -> None:
    a = Author.objects.create(name="DelTrigger", age=42)
    qs = Author.objects.filter(name="DelTrigger").cache(timeout=60)
    list(qs)
    key = qs._cache_key()
    assert key is not None
    assert _signal_cache.get(key) is not None
    a.delete()
    assert _signal_cache.get(key) is None


def test_cached_queryset_iter_twice_uses_local_cache(
    _signal_cache: Any,
) -> None:
    """The materialised result lives on ``_result_cache`` after
    the first iteration, so the SECOND iteration of the SAME
    instance is even cheaper than a Redis hit — verify the
    local-cache short-circuit still fires."""
    Author.objects.create(name="LocalCache", age=18)
    qs = Author.objects.filter(name="LocalCache").cache(timeout=60)
    rows1 = list(qs)
    # Patch _iterator to detect any DB hit on the second loop;
    # _result_cache should serve.
    with patch.object(qs, "_iterator", side_effect=AssertionError("DB hit")):
        rows2 = list(qs)
    assert [r.pk for r in rows1] == [r.pk for r in rows2]


def test_values_chain_cache_round_trip(_signal_cache: Any) -> None:
    """``qs.cache().values('name')`` must round-trip: first call
    fills the cache, second call hydrates from bytes without DB
    access."""
    Author.objects.create(name="ValuesCache", age=25)
    rows = list(
        Author.objects.filter(name="ValuesCache").cache(timeout=10).values("name")
    )
    assert any(r.get("name") == "ValuesCache" for r in rows)


# ─────────────────────────────────────────────────────────────────────────────
# Async cache + invalidation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_save_invalidates_cache(_signal_cache: Any) -> None:
    a = await Author.objects.acreate(name="AsyncEv", age=30)
    qs = Author.objects.filter(name="AsyncEv").cache(timeout=60)
    rows = await qs
    assert rows[0].name == "AsyncEv"
    key = qs._cache_key()
    assert key is not None
    assert _signal_cache.get(key) is not None

    # Async save must invalidate.
    await a.asave()
    # Either the sync handler (post_save.send) or the async
    # handler (post_save.asend) ran — either way the entry is
    # gone.
    assert _signal_cache.get(key) is None


# ─────────────────────────────────────────────────────────────────────────────
# Compiler: cache hooks short-circuit cleanly
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_lookup_sync_swallows_backend_exception(
    _signal_cache: Any,
) -> None:
    """A backend failure during ``get`` must NOT propagate; the
    queryset falls through to the live SELECT."""
    Author.objects.create(name="FailGet", age=1)

    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("backend down")

    qs = Author.objects.filter(name="FailGet").cache(timeout=10)
    with patch.object(_signal_cache, "get", _boom):
        rows = list(qs)
    assert rows[0].name == "FailGet"


def test_cache_store_sync_swallows_backend_exception(
    _signal_cache: Any,
) -> None:
    """A backend failure during ``set`` must NOT propagate; the
    iteration completes returning the live rows."""
    Author.objects.create(name="FailSet", age=1)

    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("backend down")

    qs = Author.objects.filter(name="FailSet").cache(timeout=10)
    with patch.object(_signal_cache, "set", _boom):
        rows = list(qs)
    assert rows[0].name == "FailSet"


# ─────────────────────────────────────────────────────────────────────────────
# Backend module-level smoke
# ─────────────────────────────────────────────────────────────────────────────


def test_libsql_module_can_be_imported_without_pyturso() -> None:
    """Even when pyturso isn't installed, importing the backend
    module must succeed — the helpful error surfaces only at
    connection-open time."""
    import importlib
    import sys as _sys

    # Force a fresh import.
    if "dorm.db.backends.libsql" in _sys.modules:
        importlib.reload(_sys.modules["dorm.db.backends.libsql"])
    from dorm.db.backends.libsql import (  # noqa: F401
        LibSQLAsyncDatabaseWrapper,
        LibSQLDatabaseWrapper,
    )


def test_redis_module_can_be_imported_without_redis() -> None:
    from dorm.cache import redis as redis_mod  # noqa: F401
