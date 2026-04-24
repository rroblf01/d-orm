"""Tests for connection.set_autocommit() / commit() / rollback()."""
from __future__ import annotations

import pytest

import dorm


# ── Model ─────────────────────────────────────────────────────────────────────

class ACItem(dorm.Model):
    name = dorm.CharField(max_length=100)

    class Meta:
        db_table = "ac_items"


# ── Fixture ───────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_table(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "ac_items"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in ACItem._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "ac_items" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )


# ── Sync autocommit ───────────────────────────────────────────────────────────

def test_autocommit_writes_immediately():
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.set_autocommit(True)
    try:
        ACItem.objects.create(name="immediate")
        # In autocommit mode the row should already be visible
        assert ACItem.objects.filter(name="immediate").count() == 1
    finally:
        conn.set_autocommit(False)


def test_autocommit_off_by_default():
    """Without explicit autocommit, normal writes still persist (ORM auto-commits)."""
    ACItem.objects.create(name="normal")
    assert ACItem.objects.filter(name="normal").count() == 1


def test_set_autocommit_toggle():
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.set_autocommit(True)
    ACItem.objects.create(name="ac_on")
    conn.set_autocommit(False)
    ACItem.objects.create(name="ac_off")

    assert ACItem.objects.filter(name="ac_on").count() == 1
    assert ACItem.objects.filter(name="ac_off").count() == 1


def test_commit_method_available():
    from dorm.db.connection import get_connection

    conn = get_connection()
    # commit() should be callable without raising
    conn.commit()


def test_rollback_method_available():
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.rollback()


def test_set_autocommit_false_still_works():
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.set_autocommit(True)
    conn.set_autocommit(False)
    ACItem.objects.create(name="back_to_normal")
    assert ACItem.objects.filter(name="back_to_normal").count() == 1


def test_autocommit_multiple_writes():
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.set_autocommit(True)
    try:
        for i in range(5):
            ACItem.objects.create(name=f"item_{i}")
        assert ACItem.objects.count() == 5
    finally:
        conn.set_autocommit(False)


# ── Async autocommit ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_autocommit_writes_immediately():
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()
    await conn.set_autocommit(True)
    try:
        await ACItem.objects.acreate(name="async_immediate")
        assert await ACItem.objects.filter(name="async_immediate").acount() == 1
    finally:
        await conn.set_autocommit(False)


@pytest.mark.asyncio
async def test_async_autocommit_toggle():
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()
    await conn.set_autocommit(True)
    await ACItem.objects.acreate(name="async_on")
    await conn.set_autocommit(False)
    await ACItem.objects.acreate(name="async_off")

    assert await ACItem.objects.filter(name="async_on").acount() == 1
    assert await ACItem.objects.filter(name="async_off").acount() == 1


@pytest.mark.asyncio
async def test_async_commit_and_rollback_callable():
    from dorm.db.connection import get_async_connection

    conn = get_async_connection()
    await conn.commit()
    await conn.rollback()
