"""Tests for the AsyncSchemaEditor — async wrapper around SchemaEditor."""
from __future__ import annotations

import pytest

import dorm
from dorm.migrations.schema import AsyncSchemaEditor, SchemaEditor


class _Widget(dorm.Model):
    name = dorm.CharField(max_length=32)

    class Meta:
        app_label = "tests"


@pytest.fixture(autouse=True)
def fresh_db(tmp_path):
    from dorm.db.connection import _async_connections, _sync_connections

    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "ase.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    yield
    _sync_connections.clear()
    _async_connections.clear()


class TestAsyncSchemaEditor:
    async def test_acreate_model_and_aadd_field(self):
        from dorm.db.connection import get_connection

        conn = get_connection()
        async with AsyncSchemaEditor(conn) as se:
            await se.acreate_model(_Widget)
            await se.aadd_field(
                _Widget, "size", dorm.IntegerField(null=True)
            )
        # Sync verify the table exists with both columns.
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            [_Widget._meta.db_table],
        )
        assert rows
        cols = {c["name"] for c in conn.get_table_columns(_Widget._meta.db_table)}
        assert {"id", "name", "size"}.issubset(cols)

    async def test_aexecute_runs_raw_ddl(self):
        from dorm.db.connection import get_connection

        conn = get_connection()
        async with AsyncSchemaEditor(conn) as se:
            await se.aexecute(
                'CREATE TABLE "ase_raw" ("id" INTEGER PRIMARY KEY, "x" INTEGER)'
            )
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ["ase_raw"],
        )
        assert rows

    async def test_aremove_field_round_trip(self):
        from dorm.db.connection import get_connection

        conn = get_connection()
        # Bootstrap with sync editor; remove via async one.
        with SchemaEditor(conn) as se:
            se.create_model(_Widget)
            se.add_field(_Widget, "tmp", dorm.IntegerField(null=True))
        async with AsyncSchemaEditor(conn) as ase:
            await ase.aremove_field(_Widget, "tmp")
        cols = {c["name"] for c in conn.get_table_columns(_Widget._meta.db_table)}
        assert "tmp" not in cols

    async def test_connection_property_exposed(self):
        from dorm.db.connection import get_connection

        conn = get_connection()
        ase = AsyncSchemaEditor(conn)
        assert ase.connection is conn

    async def test_aalter_field_completes(self):
        # SQLite's alter_field path recreates the table via a temporary
        # copy — exercises the longest DDL sequence the editor can emit.
        from dorm.db.connection import get_connection

        conn = get_connection()
        async with AsyncSchemaEditor(conn) as se:
            await se.acreate_model(_Widget)
            await se.aalter_field(
                _Widget, "name", dorm.CharField(max_length=64)
            )
        cols = {c["name"] for c in conn.get_table_columns(_Widget._meta.db_table)}
        assert "name" in cols

    async def test_aexit_propagates_inner_exception(self):
        """When the body raises, __aexit__ must surface the failure
        (returning False from the underlying SchemaEditor.__exit__)."""
        from dorm.db.connection import get_connection

        conn = get_connection()
        with pytest.raises(RuntimeError, match="boom"):
            async with AsyncSchemaEditor(conn):
                raise RuntimeError("boom")
