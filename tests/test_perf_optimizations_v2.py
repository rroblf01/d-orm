"""Regression tests for the round of performance optimizations:

1. ``_to_pyformat`` is cached → repeated calls return the same object
2. ``PREPARE_THRESHOLD`` setting is honoured by both pools
3. M2M ``add()``/``aadd()`` batch into 2 queries (1 SELECT + 1 INSERT)
4. M2M ``remove()``/``aremove()`` batch into 1 DELETE
5. ``__in`` lookup emits ``= ANY(%s)`` on PG, ``IN (?, ?...)`` on SQLite
6. ``bulk_create`` and ``abulk_create`` compute ``fields`` once (hoisted)
7. Async ``prefetch_related`` parallelizes its sub-queries with
   ``asyncio.gather``

Each test asserts the behaviour the optimization promises (no regression
in correctness) plus the side-effect that proves the optimization
happened (query count, SQL shape, cache identity)."""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from dorm.db.backends.postgresql import _to_pyformat
from dorm.lookups import build_lookup_sql
from tests.models import Article, Author, Tag


# ── 1. _to_pyformat cached ────────────────────────────────────────────────────


def test_to_pyformat_returns_same_string_for_equal_input():
    """The function is decorated with lru_cache, so identical inputs go
    through the parser only once. We can't assert "called once" without
    coverage hooks, but we can assert ``cache_info()`` shows hits when
    the same SQL is converted twice."""
    _to_pyformat.cache_clear()
    sql = 'SELECT "a"."id" FROM "tbl" "a" WHERE "a"."x" = $1'
    out1 = _to_pyformat(sql)
    out2 = _to_pyformat(sql)
    info = _to_pyformat.cache_info()
    assert out1 == out2
    assert info.hits >= 1
    assert info.misses >= 1


def test_to_pyformat_handles_quoted_identifiers_and_literals():
    """Cache must not cause stale results for inputs with embedded ``$N``-
    looking content inside quoted regions."""
    _to_pyformat.cache_clear()
    # $1 inside a literal must NOT be rewritten
    sql = "INSERT INTO t (note) VALUES ('cost is $1')"
    assert _to_pyformat(sql) == sql
    # $1 inside a double-quoted identifier must NOT be rewritten
    sql2 = 'SELECT "$1col" FROM t WHERE id = $1'
    assert _to_pyformat(sql2) == 'SELECT "$1col" FROM t WHERE id = %s'


# ── 2. PREPARE_THRESHOLD wiring ───────────────────────────────────────────────


def test_pg_sync_wrapper_stores_prepare_threshold():
    from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper

    w_default = PostgreSQLDatabaseWrapper({"NAME": "x", "USER": "u"})
    w_zero = PostgreSQLDatabaseWrapper(
        {"NAME": "x", "USER": "u", "PREPARE_THRESHOLD": 0}
    )
    w_high = PostgreSQLDatabaseWrapper(
        {"NAME": "x", "USER": "u", "PREPARE_THRESHOLD": 50}
    )
    assert w_default._prepare_threshold is None    # defer to psycopg default
    assert w_zero._prepare_threshold == 0          # always prepare
    assert w_high._prepare_threshold == 50         # custom threshold


def test_pg_async_wrapper_stores_prepare_threshold():
    from dorm.db.backends.postgresql import PostgreSQLAsyncDatabaseWrapper

    w_default = PostgreSQLAsyncDatabaseWrapper({"NAME": "x"})
    w_zero = PostgreSQLAsyncDatabaseWrapper({"NAME": "x", "PREPARE_THRESHOLD": 0})
    assert w_default._prepare_threshold is None
    assert w_zero._prepare_threshold == 0


# ── 3 + 4. M2M add/remove batched ─────────────────────────────────────────────


@contextmanager
def _count_queries():
    """Hook into pre_query to count statements emitted in the block.
    Skips the ones that come from the test setup itself (``CREATE TABLE``,
    ``DROP TABLE``, the autouse fixture's table reset, etc.)."""
    from dorm.signals import pre_query

    seen: list[str] = []

    def listener(sender, sql, params, **kw):
        # Filter out DDL noise so the count reflects user-visible round-trips.
        upper = sql.lstrip().upper()
        if upper.startswith(("CREATE", "DROP", "ALTER", "PRAGMA")):
            return
        seen.append(sql)

    pre_query.connect(listener, weak=False, dispatch_uid="m2m-perf-test")
    try:
        yield seen
    finally:
        pre_query.disconnect(dispatch_uid="m2m-perf-test")


def test_m2m_add_uses_two_queries_for_n_objects():
    """Adding 5 tags should produce: 1 SELECT (existing check) + 1 multi-row
    INSERT. Old behaviour: 5 SELECTs + up to 5 INSERTs = 10 queries."""
    art = Article.objects.create(title="A")
    tags = [Tag.objects.create(name=f"t{i}") for i in range(5)]

    with _count_queries() as seen:
        art.tags.add(*tags)

    selects = [s for s in seen if s.lstrip().upper().startswith("SELECT")]
    inserts = [s for s in seen if s.lstrip().upper().startswith("INSERT")]
    assert len(selects) == 1, f"expected 1 SELECT, got {len(selects)}: {selects}"
    assert len(inserts) == 1, f"expected 1 INSERT, got {len(inserts)}: {inserts}"
    # Sanity: the link rows ended up persisted
    assert art.tags.count() == 5


def test_m2m_add_skips_dupes_within_one_call():
    """add(x, x, x) must result in one row, not three."""
    art = Article.objects.create(title="A")
    t = Tag.objects.create(name="dup")
    art.tags.add(t, t, t)
    assert art.tags.count() == 1


def test_m2m_add_skips_existing_targets():
    """Adding a tag that's already linked is a no-op (no INSERT)."""
    art = Article.objects.create(title="A")
    t = Tag.objects.create(name="t")
    art.tags.add(t)

    with _count_queries() as seen:
        art.tags.add(t)

    inserts = [s for s in seen if s.lstrip().upper().startswith("INSERT")]
    assert inserts == []
    assert art.tags.count() == 1


def test_m2m_add_empty_is_noop():
    """Calling add() with no objects mustn't issue any queries."""
    art = Article.objects.create(title="A")
    with _count_queries() as seen:
        art.tags.add()
    assert seen == []


def test_m2m_remove_uses_one_query():
    """Removing 5 tags should produce a single ``DELETE ... IN (...)``,
    not 5 individual DELETEs."""
    art = Article.objects.create(title="A")
    tags = [Tag.objects.create(name=f"t{i}") for i in range(5)]
    art.tags.add(*tags)

    with _count_queries() as seen:
        art.tags.remove(*tags)

    deletes = [s for s in seen if s.lstrip().upper().startswith("DELETE")]
    assert len(deletes) == 1
    assert art.tags.count() == 0


@pytest.mark.asyncio
async def test_aadd_batches_into_two_queries():
    """Async path mirrors sync: 1 SELECT + 1 INSERT regardless of N."""
    art = await Article.objects.acreate(title="A")
    tags = [await Tag.objects.acreate(name=f"t{i}") for i in range(4)]

    with _count_queries() as seen:
        await art.tags.aadd(*tags)

    selects = [s for s in seen if s.lstrip().upper().startswith("SELECT")]
    inserts = [s for s in seen if s.lstrip().upper().startswith("INSERT")]
    assert len(selects) == 1
    assert len(inserts) == 1


@pytest.mark.asyncio
async def test_aremove_batches_into_one_delete():
    art = await Article.objects.acreate(title="A")
    tags = [await Tag.objects.acreate(name=f"t{i}") for i in range(3)]
    await art.tags.aadd(*tags)

    with _count_queries() as seen:
        await art.tags.aremove(*tags)

    deletes = [s for s in seen if s.lstrip().upper().startswith("DELETE")]
    assert len(deletes) == 1


# ── 5. IN → ANY on PG ─────────────────────────────────────────────────────────


def test_in_lookup_uses_any_on_postgres():
    """PG should produce ``col = ANY(%s)`` with a single array param —
    same SQL shape regardless of list length, so the plan cache hits."""
    sql_short, params_short = build_lookup_sql(
        '"id"', "in", [1, 2, 3], vendor="postgresql"
    )
    sql_long, params_long = build_lookup_sql(
        '"id"', "in", list(range(50)), vendor="postgresql"
    )
    assert sql_short == sql_long == '"id" = ANY(%s)'
    # Single parameter (a list), not 50 separate params.
    assert len(params_short) == 1
    assert len(params_long) == 1
    assert params_short[0] == [1, 2, 3]


def test_in_lookup_uses_classic_in_on_sqlite():
    """SQLite has no array type for ANY — keep ``IN (?, ?...)``."""
    sql, params = build_lookup_sql('"id"', "in", [1, 2, 3], vendor="sqlite")
    assert sql == '"id" IN (%s, %s, %s)'
    assert params == [1, 2, 3]


def test_in_lookup_default_vendor_is_sqlite():
    """Default ``vendor=`` keyword arg keeps backwards compat for any
    callers that haven't been updated."""
    sql, _ = build_lookup_sql('"id"', "in", [1, 2])
    assert "IN (" in sql


def test_in_lookup_empty_list_is_always_false_both_vendors():
    for vendor in ("sqlite", "postgresql"):
        sql, params = build_lookup_sql('"id"', "in", [], vendor=vendor)
        assert sql == "1=0"
        assert params == []


def test_filter_in_round_trip_against_live_db():
    """End-to-end: a real ``filter(pk__in=ids)`` must return the right rows
    on the configured backend (whichever vendor SQL we picked)."""
    a = Author.objects.create(name="A", age=20)
    b = Author.objects.create(name="B", age=30)
    Author.objects.create(name="C", age=40)
    found = list(Author.objects.filter(pk__in=[a.pk, b.pk]).order_by("name"))
    assert [x.name for x in found] == ["A", "B"]


# ── 6. bulk_create field list hoisted ─────────────────────────────────────────


def test_bulk_create_persists_objs_with_small_batch():
    """Hoisting the field list mustn't break correctness with multiple
    batches. Use batch_size=2 so 5 objs split into 3 batches."""
    objs = [Author(name=f"u{i}", age=i) for i in range(5)]
    Author.objects.bulk_create(objs, batch_size=2)
    assert Author.objects.count() == 5
    assert all(o.pk is not None for o in objs)


@pytest.mark.asyncio
async def test_abulk_create_persists_objs_with_small_batch():
    objs = [Author(name=f"x{i}", age=i) for i in range(5)]
    await Author.objects.abulk_create(objs, batch_size=2)
    n = await Author.objects.acount()
    assert n == 5


def test_bulk_create_empty_list_is_noop():
    Author.objects.bulk_create([])
    assert Author.objects.count() == 0


# ── 7. Async prefetch parallelizes ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_prefetch_returns_correct_data():
    """Correctness check: parallelizing with ``asyncio.gather`` mustn't
    swap or lose any of the prefetched relations. The actual concurrency
    is asserted by ``test_async_prefetch_uses_asyncio_gather`` below."""
    art = await Article.objects.acreate(title="A")
    tags = [await Tag.objects.acreate(name=f"t{i}") for i in range(3)]
    await art.tags.aadd(*tags)

    arts = [a async for a in Article.objects.prefetch_related("tags")]
    assert arts
    target = next(a for a in arts if a.pk == art.pk)
    assert len(target.__dict__["_prefetch_tags"]) == 3


@pytest.mark.asyncio
async def test_async_prefetch_uses_asyncio_gather():
    """The implementation collects coroutines and ``await`` them via
    ``asyncio.gather``. Verify by patching gather and counting calls."""
    import asyncio
    from unittest.mock import patch

    art = await Article.objects.acreate(title="A")
    tag = await Tag.objects.acreate(name="t1")
    await art.tags.aadd(tag)

    with patch("dorm.queryset.asyncio.gather", wraps=asyncio.gather) as g:
        _ = [a async for a in Article.objects.prefetch_related("tags")]
        # gather should be called when there's at least one prefetched relation
        assert g.called, "expected asyncio.gather to be used for parallel prefetch"


# ── meta / sanity ─────────────────────────────────────────────────────────────


def test_to_pyformat_cache_does_not_explode_under_unique_inputs():
    """LRU cap is 4096 — keep the cache bounded so a misbehaving caller
    that generates fresh SQL strings can't OOM the process."""
    _to_pyformat.cache_clear()
    for i in range(5000):
        _to_pyformat(f"SELECT $1 FROM t WHERE id = $1 -- {i}")
    info = _to_pyformat.cache_info()
    assert info.maxsize == 4096
    # ``maxsize`` is typed ``int | None`` (None means unbounded) — assert
    # we set a real cap, then compare currsize against it.
    maxsize = info.maxsize
    assert maxsize is not None
    assert info.currsize <= maxsize
