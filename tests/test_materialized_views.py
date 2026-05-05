"""Tests for ``CreateMaterializedView`` / ``RefreshMaterializedView`` /
``DropMaterializedView`` migration operations.

PostgreSQL-only — operations raise NotImplementedError on other backends.
The tests bypass the migration runner and call ``database_forwards``
directly so we can verify the SQL behaviour without spinning up a fake
migration graph.
"""

from __future__ import annotations

import pytest

from dorm.db.connection import get_connection
from dorm.migrations.operations import (
    CreateMaterializedView,
    DropMaterializedView,
    RefreshMaterializedView,
)
from tests.models import Author


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


def test_materialized_view_not_supported_on_sqlite(db_config):
    if _is_postgres(db_config):
        pytest.skip("test targets non-PG path")
    op = CreateMaterializedView(
        "active_authors", 'SELECT id FROM "authors"'
    )
    conn = get_connection()
    with pytest.raises(NotImplementedError):
        op.database_forwards("tests", conn, None, None)


def test_create_refresh_drop_materialized_view(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    Author.objects.create(name="active1", age=20, is_active=True)
    Author.objects.create(name="inactive1", age=30, is_active=False)

    conn = get_connection()
    create_op = CreateMaterializedView(
        "active_authors_view",
        'SELECT id, name FROM "authors" WHERE is_active = true',
    )
    create_op.database_forwards("tests", conn, None, None)
    try:
        rows = conn.execute('SELECT name FROM "active_authors_view"')
        assert sorted(r["name"] for r in rows) == ["active1"]

        # Add a new active row — view should still show only the snapshot.
        Author.objects.create(name="active2", age=21, is_active=True)
        rows = conn.execute('SELECT name FROM "active_authors_view"')
        assert sorted(r["name"] for r in rows) == ["active1"]

        # Refresh, then it picks up the new row.
        refresh_op = RefreshMaterializedView("active_authors_view")
        refresh_op.database_forwards("tests", conn, None, None)
        rows = conn.execute('SELECT name FROM "active_authors_view"')
        assert sorted(r["name"] for r in rows) == ["active1", "active2"]

        # Reverse-direction refresh is a no-op (asserts no exception).
        refresh_op.database_backwards("tests", conn, None, None)
    finally:
        DropMaterializedView("active_authors_view").database_forwards(
            "tests", conn, None, None
        )


def test_drop_materialized_view_reverse_recreates(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    sql = 'SELECT id FROM "authors"'
    CreateMaterializedView("v1", sql).database_forwards("tests", conn, None, None)
    drop = DropMaterializedView("v1", reverse_sql=sql)
    drop.database_forwards("tests", conn, None, None)
    drop.database_backwards("tests", conn, None, None)
    # View should exist again.
    rows = conn.execute(
        "SELECT 1 FROM pg_matviews WHERE matviewname = %s", ["v1"]
    )
    assert rows
    DropMaterializedView("v1").database_forwards("tests", conn, None, None)


def test_drop_materialized_view_irreversible_raises(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    CreateMaterializedView("v2", 'SELECT 1 AS x').database_forwards(
        "tests", conn, None, None
    )
    drop = DropMaterializedView("v2", reverse_sql="")
    drop.database_forwards("tests", conn, None, None)
    with pytest.raises(NotImplementedError, match="irreversible"):
        drop.database_backwards("tests", conn, None, None)


def test_create_materialized_view_with_no_data(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    op = CreateMaterializedView(
        "v3",
        'SELECT id FROM "authors"',
        with_data=False,
    )
    op.database_forwards("tests", conn, None, None)
    try:
        # WITH NO DATA → view exists but isn't populated.
        rows = conn.execute(
            "SELECT ispopulated FROM pg_matviews WHERE matviewname = %s",
            ["v3"],
        )
        assert rows and rows[0]["ispopulated"] is False
    finally:
        DropMaterializedView("v3").database_forwards("tests", conn, None, None)
