"""Tests for the bulk-update / m2m-prefetch / pool-check optimizations.

These guard the *behaviour* of each optimization (correctness on the surface
API) and the *number of round-trips* where it's measurable.
"""

from __future__ import annotations

import pytest

import dorm
from tests.models import Article, Author, Book, Publisher, Tag


# ── A: bulk_update / abulk_update execute one query per batch ─────────────────


def _instrument_writes(conn):
    """Count execute_write calls on the given connection wrapper. Returns
    a list to be asserted against, plus the original method to restore."""
    calls: list = []
    original = conn.execute_write

    def wrapper(sql, params=None):
        calls.append((sql, params))
        return original(sql, params)

    conn.execute_write = wrapper
    return calls, original


def test_bulk_update_runs_single_query_per_batch():
    """N objects → ceil(N / batch_size) UPDATE statements, not N."""
    Author.objects.filter(name__startswith="BUOpt").delete()
    objs = Author.objects.bulk_create([
        Author(name=f"BUOpt{i}", age=i, email=f"buopt{i}@x.com") for i in range(5)
    ])

    for o in objs:
        o.age = o.age * 10
        o.is_active = False

    from dorm.db.connection import get_connection
    conn = get_connection()
    calls, original = _instrument_writes(conn)
    try:
        n = Author.objects.filter(name__startswith="BUOpt").bulk_update(
            objs, fields=["age", "is_active"], batch_size=1000
        )
    finally:
        conn.execute_write = original

    assert n == 5
    assert len(calls) == 1, f"expected 1 UPDATE, got {len(calls)}: {calls}"
    sql, _ = calls[0]
    assert "CASE" in sql.upper(), f"expected CASE WHEN form, got: {sql}"

    # Round-trip: values were actually written
    refreshed = list(Author.objects.filter(name__startswith="BUOpt").order_by("name"))
    assert all(a.is_active is False for a in refreshed)
    assert {a.age for a in refreshed} == {0, 10, 20, 30, 40}

    Author.objects.filter(name__startswith="BUOpt").delete()


def test_bulk_update_respects_batch_size():
    """7 objects with batch_size=3 → 3 UPDATE statements (3 + 3 + 1)."""
    Author.objects.filter(name__startswith="BUBatch").delete()
    objs = Author.objects.bulk_create([
        Author(name=f"BUBatch{i}", age=i, email=f"bub{i}@x.com") for i in range(7)
    ])
    for o in objs:
        o.age += 100

    from dorm.db.connection import get_connection
    conn = get_connection()
    calls, original = _instrument_writes(conn)
    try:
        n = Author.objects.bulk_update(objs, fields=["age"], batch_size=3)
    finally:
        conn.execute_write = original

    assert n == 7
    assert len(calls) == 3, f"expected 3 batches, got {len(calls)}"

    Author.objects.filter(name__startswith="BUBatch").delete()


def test_bulk_update_empty_returns_zero():
    assert Author.objects.bulk_update([], fields=["age"]) == 0


def test_bulk_update_skips_objects_without_pk():
    """An object with pk=None is unaddressable; skip it without crashing."""
    a = Author.objects.create(name="BUSkip-A", age=1, email="busk@x.com")
    ghost = Author(name="ghost", age=99)  # never saved → pk is None
    a.age = 11
    n = Author.objects.bulk_update([a, ghost], fields=["age"])
    assert n == 1
    assert Author.objects.get(pk=a.pk).age == 11
    a.delete()


def test_bulk_update_unknown_field_raises():
    a = Author.objects.create(name="BUUnk", age=1, email="buu@x.com")
    with pytest.raises(ValueError, match="Unknown field"):
        Author.objects.bulk_update([a], fields=["nonexistent_field"])
    a.delete()


async def test_abulk_update_runs_single_query_per_batch():
    await Author.objects.filter(name__startswith="ABUOpt").adelete()
    objs = []
    for i in range(5):
        objs.append(
            await Author.objects.acreate(
                name=f"ABUOpt{i}", age=i, email=f"abuopt{i}@x.com"
            )
        )
    for o in objs:
        o.age = o.age * 10

    from dorm.db.connection import get_async_connection
    conn = get_async_connection()

    calls: list = []
    original = conn.execute_write

    async def wrapper(sql, params=None):
        calls.append((sql, params))
        return await original(sql, params)

    conn.execute_write = wrapper
    try:
        n = await Author.objects.abulk_update(objs, fields=["age"], batch_size=1000)
    finally:
        conn.execute_write = original

    assert n == 5
    assert len(calls) == 1, f"expected 1 UPDATE, got {len(calls)}"
    sql, _ = calls[0]
    assert "CASE" in sql.upper()

    await Author.objects.filter(name__startswith="ABUOpt").adelete()


# ── B: M2M prefetch is a single JOIN, not 2 queries ───────────────────────────


def _instrument_reads(conn):
    """Count execute() calls on the connection wrapper."""
    calls: list = []
    original = conn.execute

    def wrapper(sql, params=None):
        calls.append((sql, params))
        return original(sql, params)

    conn.execute = wrapper
    return calls, original


def _cleanup_m2m_articles(prefix: str, tag_prefix: str) -> None:
    for a in Article.objects.filter(title__startswith=prefix):
        a.tags.clear()
    Article.objects.filter(title__startswith=prefix).delete()
    Tag.objects.filter(name__startswith=tag_prefix).delete()


def test_prefetch_m2m_uses_single_join_query():
    """Prefetching an M2M relation should issue 2 queries total: the
    base SELECT + a single JOIN for the M2M. Previously it was 3
    (base + through + targets)."""
    _cleanup_m2m_articles("M2MJ", "m2mj-")

    t1 = Tag.objects.create(name="m2mj-python")
    t2 = Tag.objects.create(name="m2mj-async")
    a1 = Article.objects.create(title="M2MJ-1")
    a2 = Article.objects.create(title="M2MJ-2")
    a1.tags.add(t1, t2)
    a2.tags.add(t1)

    from dorm.db.connection import get_connection
    conn = get_connection()
    calls, original = _instrument_reads(conn)
    try:
        articles = list(
            Article.objects.filter(title__startswith="M2MJ")
            .order_by("title")
            .prefetch_related("tags")
        )
    finally:
        conn.execute = original

    # Base SELECT + single JOIN for tags = 2 reads (not 3).
    select_calls = [c for c in calls if c[0].lstrip().upper().startswith("SELECT")]
    assert len(select_calls) == 2, \
        f"expected 2 SELECTs (base + JOIN), got {len(select_calls)}: " \
        f"{[c[0] for c in select_calls]}"
    join_sql = select_calls[1][0]
    assert " JOIN " in join_sql.upper()

    # Round-trip correctness
    a1_tags = sorted(t.name for t in articles[0].__dict__["_prefetch_tags"])
    a2_tags = sorted(t.name for t in articles[1].__dict__["_prefetch_tags"])
    assert a1_tags == ["m2mj-async", "m2mj-python"]
    assert a2_tags == ["m2mj-python"]

    _cleanup_m2m_articles("M2MJ", "m2mj-")


def test_prefetch_m2m_empty_instances():
    """Empty queryset → no extra queries needed for M2M prefetch."""
    Article.objects.filter(title__startswith="M2MJEmpty").delete()
    list(Article.objects.filter(title__startswith="M2MJEmpty").prefetch_related("tags"))
    # Should not raise.


async def test_aprefetch_m2m_uses_single_join_query():
    """Async path: same single-JOIN optimization as sync."""
    _cleanup_m2m_articles("AM2MJ", "am2mj-")

    t1 = await Tag.objects.acreate(name="am2mj-python")
    t2 = await Tag.objects.acreate(name="am2mj-async")
    a1 = await Article.objects.acreate(title="AM2MJ-1")
    a2 = await Article.objects.acreate(title="AM2MJ-2")
    # M2M setters are sync-only on the descriptor
    a1.tags.add(t1, t2)
    a2.tags.add(t1)

    from dorm.db.connection import get_async_connection
    conn = get_async_connection()
    calls: list = []
    original = conn.execute

    async def wrapper(sql, params=None):
        calls.append((sql, params))
        return await original(sql, params)

    conn.execute = wrapper
    try:
        articles = [
            a async for a in Article.objects.filter(title__startswith="AM2MJ")
            .order_by("title")
            .prefetch_related("tags")
        ]
    finally:
        conn.execute = original

    select_calls = [c for c in calls if c[0].lstrip().upper().startswith("SELECT")]
    assert len(select_calls) == 2, \
        f"expected 2 SELECTs (base + JOIN), got {len(select_calls)}"
    assert " JOIN " in select_calls[1][0].upper()

    a1_tags = sorted(t.name for t in articles[0].__dict__["_prefetch_tags"])
    a2_tags = sorted(t.name for t in articles[1].__dict__["_prefetch_tags"])
    assert a1_tags == ["am2mj-async", "am2mj-python"]
    assert a2_tags == ["am2mj-python"]

    _cleanup_m2m_articles("AM2MJ", "am2mj-")


# ── C: POOL_CHECK toggle ──────────────────────────────────────────────────────


def test_pool_check_default_is_on():
    from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper

    w = PostgreSQLDatabaseWrapper({"NAME": "x", "USER": "u"})
    assert w._pool_check is True


def test_pool_check_can_be_disabled():
    from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper

    w = PostgreSQLDatabaseWrapper({"NAME": "x", "USER": "u", "POOL_CHECK": False})
    assert w._pool_check is False


def test_async_pool_check_default_is_on():
    from dorm.db.backends.postgresql import PostgreSQLAsyncDatabaseWrapper

    w = PostgreSQLAsyncDatabaseWrapper({"NAME": "x", "USER": "u"})
    assert w._pool_check is True


def test_async_pool_check_can_be_disabled():
    from dorm.db.backends.postgresql import PostgreSQLAsyncDatabaseWrapper

    w = PostgreSQLAsyncDatabaseWrapper({"NAME": "x", "USER": "u", "POOL_CHECK": False})
    assert w._pool_check is False


def test_pool_built_without_check_when_disabled():
    """Smoke test: when POOL_CHECK=False, the pool init kwargs do not carry
    a `check` callable. We can't easily assert against a real pool, but we
    can monkey-patch `ConnectionPool` to capture its kwargs."""
    pytest.importorskip("psycopg_pool")
    from dorm.db.backends import postgresql as pg_module
    import psycopg_pool

    captured: dict = {}

    class _FakePool:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)
        def close(self): pass

    real_pool = psycopg_pool.ConnectionPool
    psycopg_pool.ConnectionPool = _FakePool  # type: ignore
    try:
        w = pg_module.PostgreSQLDatabaseWrapper({
            "NAME": "x", "USER": "u", "POOL_CHECK": False,
        })
        try:
            w._get_pool()
        except Exception:
            # We only care about the kwargs captured before any actual usage.
            pass
        assert "check" not in captured, \
            f"check kwarg should be absent when POOL_CHECK=False; got: {captured!r}"
    finally:
        psycopg_pool.ConnectionPool = real_pool


def test_pool_built_with_check_by_default():
    pytest.importorskip("psycopg_pool")
    from dorm.db.backends import postgresql as pg_module
    import psycopg_pool

    captured: dict = {}

    class _FakePool:
        # The dorm code reads ConnectionPool.check_connection as the
        # default check callable, so the fake must expose the attribute.
        check_connection = staticmethod(lambda conn: None)

        def __init__(self, *args, **kwargs):
            captured.update(kwargs)
        def close(self): pass

    real_pool = psycopg_pool.ConnectionPool
    psycopg_pool.ConnectionPool = _FakePool  # type: ignore
    try:
        w = pg_module.PostgreSQLDatabaseWrapper({"NAME": "x", "USER": "u"})
        try:
            w._get_pool()
        except Exception:
            pass
        assert "check" in captured, \
            f"check kwarg should be present by default; got: {captured!r}"
    finally:
        psycopg_pool.ConnectionPool = real_pool
