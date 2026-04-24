"""Tests for Tier-4.5: connection auto-reconnect / health-check."""
from __future__ import annotations

import pytest

import dorm


class ReconnectItem(dorm.Model):
    name = dorm.CharField(max_length=100)

    class Meta:
        db_table = "reconnect_items"


@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "reconnect_items"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in ReconnectItem._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "reconnect_items" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )


def test_reconnect_after_connection_closed():
    """Forcibly close the connection; the next query should reconnect."""
    from dorm.db.connection import get_connection

    conn = get_connection()

    # Only SQLite has a simple close mechanism we can test without killing a server
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("Reconnect test only supported for SQLite")

    ReconnectItem.objects.create(name="before")

    # Force-close the underlying sqlite3 connection
    local = conn._local
    underlying = getattr(local, "conn", None)
    if underlying is not None:
        underlying.close()
        local.conn = None  # simulate stale/dropped connection

    # This should trigger auto-reconnect and succeed
    items = list(ReconnectItem.objects.all())
    assert len(items) == 1
    assert items[0].name == "before"


def test_reconnect_multiple_operations_after_drop():
    """Multiple operations after a forced drop all succeed."""
    from dorm.db.connection import get_connection

    conn = get_connection()

    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("Reconnect test only supported for SQLite")

    ReconnectItem.objects.create(name="alpha")

    # Simulate connection drop
    local = conn._local
    underlying = getattr(local, "conn", None)
    if underlying is not None:
        underlying.close()
        local.conn = None

    # First operation reconnects
    assert ReconnectItem.objects.count() == 1

    # Subsequent operations work normally
    ReconnectItem.objects.create(name="beta")
    assert ReconnectItem.objects.count() == 2


@pytest.mark.asyncio
async def test_async_reconnect_after_connection_closed():
    """Async: forcibly close the aiosqlite connection; next query reconnects."""
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()

    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("Reconnect test only supported for SQLite")

    await ReconnectItem.objects.acreate(name="async_item")

    # Force-close the underlying aiosqlite connection
    underlying = conn._conn
    if underlying is not None:
        await underlying.close()
        conn._conn = None

    # Should trigger auto-reconnect
    items = await ReconnectItem.objects.avalues("name")
    assert len(items) == 1
    assert items[0]["name"] == "async_item"
