"""Tests for the DuckDB backend wrapper.

Smoke tests cover the synchronous CRUD path and confirm the async
adapter actually runs awaits to completion.
"""

from __future__ import annotations

import pytest

duckdb = pytest.importorskip("duckdb")

from dorm.db.backends.duckdb import (  # noqa: E402
    DuckDBAsyncDatabaseWrapper,
    DuckDBDatabaseWrapper,
)


@pytest.fixture
def wrapper():
    w = DuckDBDatabaseWrapper({"NAME": ":memory:"})
    w.execute_script(
        "CREATE TABLE widgets ("
        " id INTEGER PRIMARY KEY,"
        " name TEXT NOT NULL,"
        " price INTEGER NOT NULL"
        ")"
    )
    yield w
    w.close()


def test_execute_round_trips_dict_rows(wrapper):
    wrapper.execute_write(
        "INSERT INTO widgets (id, name, price) VALUES (?, ?, ?)",
        [1, "alpha", 100],
    )
    rows = wrapper.execute("SELECT id, name, price FROM widgets")
    assert rows == [{"id": 1, "name": "alpha", "price": 100}]


def test_pg_style_placeholders_are_translated(wrapper):
    wrapper.execute_write(
        "INSERT INTO widgets (id, name, price) VALUES ($1, $2, $3)",
        [2, "beta", 200],
    )
    rows = wrapper.execute("SELECT name FROM widgets WHERE id = $1", [2])
    assert rows[0]["name"] == "beta"


def test_atomic_commits(wrapper):
    with wrapper.atomic():
        wrapper.execute_write(
            "INSERT INTO widgets VALUES (?, ?, ?)", [3, "g", 0]
        )
    rows = wrapper.execute("SELECT id FROM widgets WHERE id = ?", [3])
    assert rows


def test_atomic_rollback(wrapper):
    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        with wrapper.atomic():
            wrapper.execute_write(
                "INSERT INTO widgets VALUES (?, ?, ?)", [4, "x", 0]
            )
            raise _Boom()
    rows = wrapper.execute("SELECT id FROM widgets WHERE id = ?", [4])
    assert rows == []


def test_nested_atomic_no_savepoint_outer_rollback(wrapper):
    """DuckDB has no SAVEPOINT — nested atomic degrades to a no-op
    boundary. Inner failure propagates to the outer block, which
    rolls back the whole transaction (both rows discarded)."""

    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        with wrapper.atomic():
            wrapper.execute_write(
                "INSERT INTO widgets VALUES (?, ?, ?)", [5, "outer", 0]
            )
            with wrapper.atomic():
                wrapper.execute_write(
                    "INSERT INTO widgets VALUES (?, ?, ?)", [6, "inner", 0]
                )
                raise _Boom()

    rows = {r["id"] for r in wrapper.execute("SELECT id FROM widgets")}
    # Both rows reverted — DuckDB doesn't support partial rollback.
    assert 5 not in rows
    assert 6 not in rows


def test_table_exists(wrapper):
    assert wrapper.table_exists("widgets") is True
    assert wrapper.table_exists("does_not") is False


def test_get_table_columns(wrapper):
    cols = wrapper.get_table_columns("widgets")
    names = {c["name"] for c in cols}
    assert {"id", "name", "price"} <= names


def test_streaming_iterator(wrapper):
    for i in range(5):
        wrapper.execute_write(
            "INSERT INTO widgets VALUES (?, ?, ?)", [10 + i, f"w{i}", i]
        )
    rows = list(wrapper.execute_streaming(
        "SELECT id, name FROM widgets ORDER BY id", chunk_size=2
    ))
    assert len(rows) == 5
    assert rows[0]["name"].startswith("w") or rows[0]["name"] == "alpha"


def test_pool_stats_minimal(wrapper):
    s = wrapper.pool_stats()
    assert s["vendor"] == "duckdb"
    assert s["has_pool"] is False


@pytest.mark.asyncio
async def test_async_execute(tmp_path):
    # File-based DB so the connection survives across threads.
    # ``asyncio.to_thread`` may schedule each await in a different
    # worker thread; an in-memory DuckDB DB is per-thread, so the
    # CREATE TABLE issued from one thread isn't visible to the next
    # write. Persisting to disk side-steps the thread isolation.
    db_path = tmp_path / "duck_async.db"
    w = DuckDBAsyncDatabaseWrapper({"NAME": str(db_path)})
    try:
        await w.execute_script(
            "CREATE TABLE t (id INTEGER, v TEXT)"
        )
        await w.execute_write("INSERT INTO t VALUES (?, ?)", [1, "a"])
        rows = await w.execute("SELECT v FROM t WHERE id = ?", [1])
        assert rows[0]["v"] == "a"
    finally:
        w.close()


def test_engine_factory_routes_to_duckdb():
    from dorm.db.connection import _create_sync_connection

    obj = _create_sync_connection("default", {"ENGINE": "duckdb", "NAME": ":memory:"})
    assert obj.vendor == "duckdb"
    obj.close()
