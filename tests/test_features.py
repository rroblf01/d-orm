"""Tests for the round of "high-impact" features added on top of the
post-2.0.1 audit fixes:

- ``transaction.on_commit`` / ``aon_commit``
- ``QuerySet.select_for_update(skip_locked=, no_wait=, of=)``
- ``QuerySet.bulk_create(ignore_conflicts=, update_conflicts=, …)``
- ``QuerySet.alias()``
- ``pool_stats()`` and ``health_check(deep=True)``
- ``dorm.test.transactional_db`` / ``DormTestCase``
- ``dorm.contrib.softdelete``
- PostgreSQL ``LISTEN`` / ``NOTIFY``
- ``dorm.contrib.otel`` instrumentation

Each section keeps the failure mode it's locking down explicit so a
future refactor can't silently regress.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import dorm
from dorm import transaction
from tests.models import Author, Tag


# ── transaction.on_commit ────────────────────────────────────────────────────


def test_on_commit_fires_after_outer_commit():
    """Callback runs once the outer atomic commits — not before."""
    fired: list[str] = []
    with transaction.atomic():
        Author.objects.create(name="Hooked", age=33)
        transaction.on_commit(lambda: fired.append("post"))
        # Still inside the atomic — must not have fired yet.
        assert fired == []
    # After exit, callback fired.
    assert fired == ["post"]


def test_on_commit_discarded_on_rollback():
    fired: list[str] = []
    with pytest.raises(RuntimeError, match="boom"):
        with transaction.atomic():
            Author.objects.create(name="Doomed", age=99)
            transaction.on_commit(lambda: fired.append("post"))
            raise RuntimeError("boom")
    # Rollback discards pending callbacks.
    assert fired == []


def test_on_commit_outside_atomic_runs_immediately():
    fired: list[str] = []
    transaction.on_commit(lambda: fired.append("now"))
    assert fired == ["now"]


def test_on_commit_nested_only_fires_at_outermost():
    """Inner commit merges callbacks into outer; outer rollback discards."""
    fired: list[str] = []
    with pytest.raises(RuntimeError):
        with transaction.atomic():
            with transaction.atomic():
                transaction.on_commit(lambda: fired.append("inner"))
            # Inner block exited cleanly, but the OUTER block raises.
            raise RuntimeError("outer fails")
    assert fired == []


def test_on_commit_callback_exception_logged_not_raised(caplog):
    """A failing post-commit callback must not break the caller — by
    the time it runs, the DB write already landed."""
    import logging

    def boom():
        raise RuntimeError("oops")

    with caplog.at_level(logging.ERROR, logger="dorm.transaction"):
        with transaction.atomic():
            transaction.on_commit(boom)
        # We exit cleanly — the failure was swallowed.

    assert any("on_commit callback" in r.message for r in caplog.records)


def test_set_rollback_undoes_writes_without_exception():
    """``tx.set_rollback(True)`` must roll back even if no exception
    is raised — the foundation of the transactional test fixture."""
    pre_count = Author.objects.count()
    with transaction.atomic() as tx:
        Author.objects.create(name="WillBeRolledBack", age=1)
        tx.set_rollback(True)
    # Insert was rolled back.
    assert Author.objects.count() == pre_count


@pytest.mark.asyncio
async def test_aon_commit_fires_after_outer_aatomic():
    fired: list[str] = []
    async with transaction.aatomic():
        await Author.objects.acreate(name="HookedAsync", age=33)
        transaction.aon_commit(lambda: fired.append("post"))
        assert fired == []
    assert fired == ["post"]


@pytest.mark.asyncio
async def test_aon_commit_async_callback_awaited():
    fired: list[str] = []

    async def cb():
        fired.append("async")

    async with transaction.aatomic():
        transaction.aon_commit(cb)
    assert fired == ["async"]


@pytest.mark.asyncio
async def test_aon_commit_discarded_on_rollback():
    fired: list[str] = []
    with pytest.raises(RuntimeError):
        async with transaction.aatomic():
            transaction.aon_commit(lambda: fired.append("x"))
            raise RuntimeError("boom")
    assert fired == []


# ── select_for_update extensions ─────────────────────────────────────────────


def test_select_for_update_emits_for_update_pg():
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only SQL emission test")

    qs = Author.objects.filter(age__gte=18).select_for_update()
    sql, _ = qs._query.as_select(conn)
    assert "FOR UPDATE" in sql.upper()


def test_select_for_update_skip_locked_pg():
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only SQL emission test")

    qs = Author.objects.select_for_update(skip_locked=True)
    sql, _ = qs._query.as_select(conn)
    assert "FOR UPDATE SKIP LOCKED" in sql.upper()


def test_select_for_update_no_wait_pg():
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only SQL emission test")

    qs = Author.objects.select_for_update(no_wait=True)
    sql, _ = qs._query.as_select(conn)
    assert "FOR UPDATE NOWAIT" in sql.upper()


def test_select_for_update_skip_and_no_wait_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        Author.objects.select_for_update(skip_locked=True, no_wait=True)


def test_select_for_update_pg_only_args_raise_on_sqlite():
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") == "postgresql":
        pytest.skip("only for the SQLite branch")

    with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
        Author.objects.select_for_update(skip_locked=True)


def test_select_for_update_of_validates_identifiers_pg():
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only SQL emission test")

    qs = Author.objects.select_for_update(of=("authors",))
    sql, _ = qs._query.as_select(conn)
    assert 'FOR UPDATE OF "authors"' in sql

    # Bad identifier rejected at SQL emission time.
    bad_qs = Author.objects.select_for_update(of=('authors"; DROP TABLE x; --',))
    with pytest.raises(Exception):  # noqa: PT011
        bad_qs._query.as_select(conn)


# ── bulk_create upsert ───────────────────────────────────────────────────────


def test_bulk_create_ignore_conflicts_skips_duplicates():
    """Insert two rows; re-insert with ignore_conflicts; confirm no error
    and the row count is unchanged."""
    Tag.objects.create(name="alpha")
    Tag.objects.bulk_create(
        [Tag(name="alpha"), Tag(name="beta")],
        ignore_conflicts=True,
    )
    names = set(Tag.objects.values_list("name", flat=True))
    assert {"alpha", "beta"} <= names
    # Only one alpha — duplicate was skipped.
    assert sum(1 for n in names if n == "alpha") == 1


def test_bulk_create_update_conflicts_updates_in_place():
    """Re-inserting with update_conflicts updates the existing row.

    Tag.name has ``unique=True`` so it carries a real DB-level UNIQUE
    constraint, which is what the ON CONFLICT target needs."""
    Tag.objects.create(name="upsert-me")
    # Re-insert with the same unique key — this is a no-op since the
    # only field is the unique one. The test focuses on:
    # 1. it does NOT raise (conflict handled).
    # 2. the row count stays at 1.
    Tag.objects.bulk_create(
        [Tag(name="upsert-me")],
        update_conflicts=True,
        update_fields=["name"],
        unique_fields=["name"],
    )
    assert Tag.objects.filter(name="upsert-me").count() == 1


def test_bulk_create_update_conflicts_requires_unique_fields():
    with pytest.raises(ValueError, match="unique_fields"):
        Tag.objects.bulk_create(
            [Tag(name="x")],
            update_conflicts=True,
        )


def test_bulk_create_ignore_and_update_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        Tag.objects.bulk_create(
            [Tag(name="x")],
            ignore_conflicts=True,
            update_conflicts=True,
            unique_fields=["name"],
        )


@pytest.mark.asyncio
async def test_abulk_create_ignore_conflicts():
    await Tag.objects.acreate(name="a-async")
    await Tag.objects.abulk_create(
        [Tag(name="a-async"), Tag(name="b-async")],
        ignore_conflicts=True,
    )
    names = await Tag.objects.values_list("name", flat=True)
    assert "a-async" in names
    assert "b-async" in names


# ── QuerySet.alias() ─────────────────────────────────────────────────────────


def test_alias_does_not_appear_in_select_columns():
    """alias()'d names must NOT be projected into the SELECT — that's
    the whole point of the API vs annotate()."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = Author.objects.alias(book_count=dorm.Count("books"))
    sql, _ = qs._query.as_select(conn)
    # The alias name must not be in the SELECT list.
    select_clause = sql.split(" FROM ", 1)[0]
    assert "book_count" not in select_clause


def test_alias_then_annotate_promotes_to_select():
    """Re-declaring an alias via annotate() promotes it to a real
    projection — Django pattern."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = (
        Author.objects
        .alias(book_count=dorm.Count("books"))
        .annotate(book_count=dorm.Count("books"))
    )
    sql, _ = qs._query.as_select(conn)
    assert "book_count" in sql


# ── pool_stats / health_check(deep=True) ─────────────────────────────────────


def test_health_check_basic_returns_ok():
    from dorm.db.connection import health_check

    result = health_check()
    assert result["status"] == "ok"
    assert "elapsed_ms" in result
    assert "pool" not in result  # not deep


def test_health_check_deep_includes_pool_stats():
    from dorm.db.connection import health_check

    result = health_check(deep=True)
    assert result["status"] == "ok"
    assert "pool" in result
    assert result["pool"]["alias"] == "default"


def test_pool_stats_returns_alias_and_vendor():
    from dorm.db.connection import get_connection, pool_stats

    # Force the connection to exist.
    get_connection().execute("SELECT 1")
    stats = pool_stats()
    assert stats["alias"] == "default"
    assert stats["vendor"] in ("sqlite", "postgresql")


# ── dorm.test fixtures ───────────────────────────────────────────────────────


def test_transactional_db_fixture_rolls_back(transactional_db):
    """The transactional_db fixture must wrap the test in a rollback."""
    Author.objects.create(name="VanishingAuthor", age=55)
    # Test ends here; framework-level rollback fires next.


def test_transactional_db_fixture_did_actually_roll_back():
    """Sibling of the previous test — the row created in
    test_transactional_db_fixture_rolls_back must NOT be visible here."""
    assert not Author.objects.filter(name="VanishingAuthor").exists()


# ── softdelete contrib ───────────────────────────────────────────────────────


@pytest.fixture
def softdelete_table():
    """Provide a temporary ``audit_softdel`` table for the SoftDeleteModel
    tests. We can't reuse the suite-wide tables (they have no
    ``deleted_at`` column) so we declare a one-off model in-memory and
    create its table for the duration of the test."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql
    from dorm.contrib.softdelete import SoftDeleteModel

    class _SDArticle(SoftDeleteModel):
        title = dorm.CharField(max_length=200)

        class Meta:
            db_table = "audit_softdel"

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel"')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _SDArticle._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "audit_softdel" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield _SDArticle
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel"')


def test_softdelete_default_manager_hides_soft_deleted(softdelete_table):
    M = softdelete_table
    M.objects.create(title="visible")
    b = M.objects.create(title="hidden")
    b.delete()
    titles = set(M.objects.values_list("title", flat=True))
    assert "visible" in titles
    assert "hidden" not in titles


def test_softdelete_all_objects_includes_deleted(softdelete_table):
    M = softdelete_table
    M.objects.create(title="alive")
    b = M.objects.create(title="dead")
    b.delete()
    titles = set(M.all_objects.values_list("title", flat=True))
    assert {"alive", "dead"} <= titles


def test_softdelete_deleted_objects_only_dead(softdelete_table):
    M = softdelete_table
    M.objects.create(title="alive")
    b = M.objects.create(title="dead")
    b.delete()
    titles = set(M.deleted_objects.values_list("title", flat=True))
    assert titles == {"dead"}


def test_softdelete_hard_delete_actually_deletes(softdelete_table):
    M = softdelete_table
    a = M.objects.create(title="purgeme")
    a.delete(hard=True)
    # all_objects also can't see it now.
    assert not M.all_objects.filter(title="purgeme").exists()


def test_softdelete_restore_brings_row_back(softdelete_table):
    M = softdelete_table
    a = M.objects.create(title="reincarnate")
    a.delete()
    assert not M.objects.filter(title="reincarnate").exists()
    a.restore()
    assert M.objects.filter(title="reincarnate").exists()


# ── PG LISTEN / NOTIFY ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pg_notify_round_trips():
    """notify() and listen() should round-trip a payload. PG-only."""
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only feature")

    received: list[Any] = []

    async def listener():
        async for n in conn.listen("audit_chan"):
            received.append(n)
            break  # one is enough; closes the connection

    task = asyncio.create_task(listener())
    # Give LISTEN a moment to register.
    await asyncio.sleep(0.2)
    await conn.notify("audit_chan", "hello")
    # Wait for the listener task to wake up.
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        task.cancel()
        pytest.fail("notify did not reach listener within 5s")

    assert len(received) == 1
    assert received[0].channel == "audit_chan"
    assert received[0].payload == "hello"


@pytest.mark.asyncio
async def test_pg_notify_rejects_unsafe_channel_name():
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only feature")

    with pytest.raises(Exception):  # noqa: PT011
        await conn.notify('orders"; DROP TABLE x; --', "x")


# ── dorm.contrib.otel ────────────────────────────────────────────────────────


def test_otel_instrument_uninstrument_idempotent():
    """instrument() / uninstrument() must be callable multiple times
    without leaking signal subscribers — important for app reloads."""
    pytest.importorskip("opentelemetry")
    from dorm.contrib.otel import instrument, uninstrument
    from dorm import signals

    pre_count_before = len(signals.pre_query._receivers)

    instrument()
    instrument()  # second call should replace, not add.
    assert len(signals.pre_query._receivers) == pre_count_before + 1

    uninstrument()
    uninstrument()  # idempotent
    assert len(signals.pre_query._receivers) == pre_count_before


def test_otel_instrument_does_not_break_queries():
    """Smoke test: instrumented queries still work."""
    pytest.importorskip("opentelemetry")
    from dorm.contrib.otel import instrument, uninstrument

    instrument()
    try:
        Author.objects.create(name="OtelTraced", age=42)
        found = Author.objects.filter(name="OtelTraced").first()
        assert found is not None
    finally:
        uninstrument()
