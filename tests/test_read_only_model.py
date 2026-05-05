"""Tests for ``Meta.read_only`` lock."""

from __future__ import annotations

import pytest

import dorm
from dorm import ReadOnlyModelError
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


class _ReadOnlyView(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

    class Meta:
        db_table = "ro_view"
        app_label = "tests"
        read_only = True


@pytest.fixture(autouse=True)
def _table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "ro_view"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _ReadOnlyView._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "ro_view" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    # Seed via raw SQL — bypasses save() so the read_only flag
    # doesn't reject the test setup itself.
    placeholder = "%s" if conn.vendor == "postgresql" else "?"
    conn.execute_write(
        f'INSERT INTO "ro_view" ("name", "age") VALUES ({placeholder}, {placeholder})',
        ["seed", 99],
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "ro_view"{cascade}')


def test_meta_read_only_propagates():
    assert _ReadOnlyView._meta.read_only is True


def test_save_raises():
    obj = _ReadOnlyView(name="x", age=1)
    with pytest.raises(ReadOnlyModelError, match="read_only"):
        obj.save()


def test_delete_raises():
    obj = _ReadOnlyView.objects.first()
    assert obj is not None
    with pytest.raises(ReadOnlyModelError, match="delete"):
        obj.delete()


def test_reads_still_work():
    rows = list(_ReadOnlyView.objects.all())
    assert len(rows) == 1
    assert rows[0].name == "seed"


@pytest.mark.asyncio
async def test_asave_raises():
    obj = _ReadOnlyView(name="async-x", age=1)
    with pytest.raises(ReadOnlyModelError, match="asave"):
        await obj.asave()


@pytest.mark.asyncio
async def test_adelete_raises():
    obj = await _ReadOnlyView.objects.afirst()
    assert obj is not None
    with pytest.raises(ReadOnlyModelError, match="adelete"):
        await obj.adelete()


@pytest.mark.asyncio
async def test_async_reads_work():
    n = await _ReadOnlyView.objects.acount()
    assert n == 1


def test_subclass_of_databaseerror():
    """Generic except DatabaseError should catch the read-only error."""
    obj = _ReadOnlyView(name="x", age=1)
    with pytest.raises(dorm.DatabaseError):
        obj.save()
