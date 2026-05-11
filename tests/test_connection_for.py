"""Tests for the public connection_for / aconnection_for helpers."""
from __future__ import annotations

import pytest

import dorm


@pytest.fixture(autouse=True)
def fresh_db(tmp_path):
    from dorm.db.connection import _async_connections, _sync_connections

    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "cf.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    yield
    _sync_connections.clear()
    _async_connections.clear()


class TestConnectionFor:
    def test_returns_sync_wrapper(self):
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        conn = dorm.connection_for("default")
        assert isinstance(conn, SQLiteDatabaseWrapper)

    def test_returns_same_singleton_per_alias(self):
        first = dorm.connection_for("default")
        second = dorm.connection_for("default")
        assert first is second

    def test_default_alias_when_omitted(self):
        assert dorm.connection_for() is dorm.connection_for("default")

    def test_executes_sql(self):
        conn = dorm.connection_for()
        rows = conn.execute("SELECT 1 AS n")
        assert rows[0]["n"] == 1


class TestAconnectionFor:
    async def test_returns_async_wrapper(self):
        from dorm.db.backends.sqlite import SQLiteAsyncDatabaseWrapper

        conn = await dorm.aconnection_for("default")
        assert isinstance(conn, SQLiteAsyncDatabaseWrapper)

    async def test_executes_async_sql(self):
        conn = await dorm.aconnection_for()
        rows = await conn.execute("SELECT 1 AS n")
        assert rows[0]["n"] == 1

    async def test_returns_same_singleton(self):
        first = await dorm.aconnection_for()
        second = await dorm.aconnection_for()
        assert first is second


class TestExportSurface:
    def test_dorm_exports_connection_for(self):
        assert "connection_for" in dorm.__all__
        assert "aconnection_for" in dorm.__all__

    def test_connection_for_is_callable(self):
        assert callable(dorm.connection_for)
        assert callable(dorm.aconnection_for)
