"""Tests for ``dorm.contrib.bulk_copy``.

PostgreSQL paths run end-to-end against the testcontainer; SQLite paths
verify the helper raises a clear NotImplementedError instead of falling
back to a slower ``bulk_create``-shaped INSERT (the call site asked for
COPY explicitly — silently downgrading would mislead capacity planning).
"""

from __future__ import annotations

import pytest

from dorm.contrib.bulk_copy import (
    abulk_copy_from,
    acopy_to,
    bulk_copy_from,
    copy_to,
)
from tests.models import Author, Publisher


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


def test_bulk_copy_from_sqlite_raises(db_config):
    if _is_postgres(db_config):
        pytest.skip("PG path covered separately")
    with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
        bulk_copy_from(Author, [Author(name="x", age=1)])


def test_bulk_copy_from_unknown_column_raises(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    with pytest.raises(ValueError, match="unknown field"):
        bulk_copy_from(Author, [Author(name="x", age=1)], columns=["bogus"])


def test_bulk_copy_from_inserts_rows(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    objs = [Author(name=f"A-{i}", age=20 + i) for i in range(50)]
    n = bulk_copy_from(Author, objs)
    assert n == 50
    assert Author.objects.count() == 50
    sample = Author.objects.filter(name="A-7").first()
    assert sample is not None
    assert sample.age == 27


def test_bulk_copy_from_with_explicit_columns(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    objs = [
        {"name": "Alice", "age": 30, "is_active": True},
        {"name": "Bob", "age": 40, "is_active": False},
    ]
    n = bulk_copy_from(
        Author, objs, columns=["name", "age", "is_active"]
    )
    assert n == 2
    bob = Author.objects.filter(name="Bob").first()
    assert bob is not None
    assert bob.is_active is False


def test_bulk_copy_from_dict_rows(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    rows = [
        {"name": "X", "age": 1, "email": None, "is_active": True, "publisher_id": None},
        {"name": "Y", "age": 2, "email": "y@example.com", "is_active": True, "publisher_id": None},
    ]
    n = bulk_copy_from(Author, rows)
    assert n == 2


def test_copy_to_streams_rows(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    Publisher.objects.create(name="P1")
    Publisher.objects.create(name="P2")
    out = list(copy_to('SELECT name FROM "publishers" ORDER BY id'))
    names = [r[0] for r in out]
    assert names == ["P1", "P2"]


def test_copy_to_inside_atomic_raises(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    from dorm import transaction

    with pytest.raises(RuntimeError, match="atomic"):
        with transaction.atomic():
            list(copy_to('SELECT 1'))


@pytest.mark.asyncio
async def test_abulk_copy_from(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    objs = [Author(name=f"AS-{i}", age=10 + i) for i in range(20)]
    n = await abulk_copy_from(Author, objs)
    assert n == 20
    assert await Author.objects.acount() == 20


@pytest.mark.asyncio
async def test_abulk_copy_from_async_iter(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")

    async def _gen():
        for i in range(10):
            yield Author(name=f"AG-{i}", age=i)

    n = await abulk_copy_from(Author, _gen())
    assert n == 10


@pytest.mark.asyncio
async def test_acopy_to(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")
    await Publisher.objects.acreate(name="async-p")
    rows = []
    async for r in acopy_to('SELECT name FROM "publishers"'):
        rows.append(r)
    assert any(row[0] == "async-p" for row in rows)
