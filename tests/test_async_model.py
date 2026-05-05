"""Tests for ``dorm.contrib.asyncmodel.AsyncModel``."""

from __future__ import annotations

import pytest

import dorm
from dorm.contrib.asyncmodel import AsyncModel, AsyncOnlyError
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


class _AsyncAuthor(AsyncModel):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

    class Meta:
        db_table = "_async_authors"
        app_label = "tests"


@pytest.fixture(autouse=True)
def _table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "_async_authors"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _AsyncAuthor._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "_async_authors" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "_async_authors"{cascade}')


def test_sync_create_raises():
    with pytest.raises(AsyncOnlyError, match="create"):
        _AsyncAuthor.objects.create(name="x", age=1)


def test_sync_filter_raises():
    with pytest.raises(AsyncOnlyError, match="filter"):
        _AsyncAuthor.objects.filter(age=1)


def test_sync_get_raises():
    with pytest.raises(AsyncOnlyError, match="get"):
        _AsyncAuthor.objects.get(pk=1)


def test_sync_count_raises():
    with pytest.raises(AsyncOnlyError, match="count"):
        _AsyncAuthor.objects.count()


def test_instance_save_raises():
    obj = _AsyncAuthor(name="x", age=1)
    with pytest.raises(AsyncOnlyError, match="save"):
        obj.save()


def test_instance_delete_raises():
    obj = _AsyncAuthor(name="x", age=1)
    with pytest.raises(AsyncOnlyError, match="delete"):
        obj.delete()


def test_async_only_error_carries_method_name():
    try:
        _AsyncAuthor.objects.create(name="x", age=1)
    except AsyncOnlyError as e:
        assert e.method == "create"
    else:
        pytest.fail("expected AsyncOnlyError")


@pytest.mark.asyncio
async def test_acreate_works():
    obj = await _AsyncAuthor.objects.acreate(name="async-x", age=42)
    assert obj.pk is not None
    assert obj.name == "async-x"


@pytest.mark.asyncio
async def test_aget_and_acount():
    await _AsyncAuthor.objects.acreate(name="y", age=10)
    n = await _AsyncAuthor.objects.acount()
    assert n == 1
    fetched = await _AsyncAuthor.objects.aget(name="y")
    assert fetched.age == 10


@pytest.mark.asyncio
async def test_asave_and_adelete():
    obj = await _AsyncAuthor.objects.acreate(name="zz", age=20)
    obj.age = 21
    await obj.asave()
    await obj.adelete()
    assert await _AsyncAuthor.objects.acount() == 0
