"""Tests for Tier-4.4: Manager.raw() — raw SQL with model hydration."""
from __future__ import annotations

import pytest

import dorm
from dorm.queryset import RawQuerySet


# ── Model definitions ─────────────────────────────────────────────────────────

class RawAuthor(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField(default=0)

    class Meta:
        db_table = "raw_authors"


class RawBook(dorm.Model):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(RawAuthor, on_delete=dorm.CASCADE)

    class Meta:
        db_table = "raw_books"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in ["raw_books", "raw_authors"]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model, tbl in [
        (RawAuthor, "raw_authors"),
        (RawBook, "raw_books"),
    ]:
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{tbl}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )


# ── Basic raw() tests ─────────────────────────────────────────────────────────

def test_raw_returns_rawqueryset():
    qs = RawAuthor.objects.raw('SELECT * FROM "raw_authors"')
    assert isinstance(qs, RawQuerySet)


def test_raw_empty_table():
    results = list(RawAuthor.objects.raw('SELECT * FROM "raw_authors"'))
    assert results == []


def test_raw_hydrates_instances():
    RawAuthor.objects.create(name="Alice", age=30)
    RawAuthor.objects.create(name="Bob", age=25)

    results = list(RawAuthor.objects.raw('SELECT * FROM "raw_authors" ORDER BY "name"'))
    assert len(results) == 2
    assert results[0].name == "Alice"
    assert results[0].age == 30
    assert results[1].name == "Bob"
    assert results[1].age == 25


def test_raw_with_params():
    RawAuthor.objects.create(name="Alice", age=30)
    RawAuthor.objects.create(name="Bob", age=25)

    results = list(
        RawAuthor.objects.raw('SELECT * FROM "raw_authors" WHERE "age" > %s', [26])
    )
    assert len(results) == 1
    assert results[0].name == "Alice"


def test_raw_instances_have_pk():
    author = RawAuthor.objects.create(name="Charlie", age=40)
    results = list(RawAuthor.objects.raw('SELECT * FROM "raw_authors"'))
    assert results[0].pk == author.pk


def test_raw_supports_len():
    RawAuthor.objects.create(name="A", age=1)
    RawAuthor.objects.create(name="B", age=2)
    qs = RawAuthor.objects.raw('SELECT * FROM "raw_authors"')
    assert len(qs) == 2


def test_raw_supports_custom_sql():
    a = RawAuthor.objects.create(name="Alice", age=30)
    RawBook.objects.create(title="Book1", author=a)
    RawBook.objects.create(title="Book2", author=a)

    results = list(
        RawAuthor.objects.raw(
            'SELECT a.* FROM "raw_authors" a '
            'JOIN "raw_books" b ON b."author_id" = a."id" '
            'WHERE b."title" = %s',
            ["Book1"],
        )
    )
    assert len(results) == 1
    assert results[0].name == "Alice"


def test_raw_partial_columns_stored_as_attrs():
    RawAuthor.objects.create(name="Dave", age=35)
    results = list(
        RawAuthor.objects.raw('SELECT "id", "name" FROM "raw_authors"')
    )
    assert len(results) == 1
    assert results[0].name == "Dave"


def test_raw_repr():
    sql = 'SELECT * FROM "raw_authors"'
    qs = RawAuthor.objects.raw(sql)
    assert sql in repr(qs)


# ── Async variants ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_araw_returns_list():
    await RawAuthor.objects.acreate(name="Async Alice", age=20)

    results = await RawAuthor.objects.araw('SELECT * FROM "raw_authors"')
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0].name == "Async Alice"


@pytest.mark.asyncio
async def test_araw_with_params():
    await RawAuthor.objects.acreate(name="A", age=10)
    await RawAuthor.objects.acreate(name="B", age=20)

    results = await RawAuthor.objects.araw(
        'SELECT * FROM "raw_authors" WHERE "age" >= %s', [15]
    )
    assert len(results) == 1
    assert results[0].name == "B"


@pytest.mark.asyncio
async def test_raw_async_iteration():
    await RawAuthor.objects.acreate(name="X", age=1)
    await RawAuthor.objects.acreate(name="Y", age=2)

    qs = RawAuthor.objects.raw('SELECT * FROM "raw_authors" ORDER BY "name"')
    names = [obj.name async for obj in qs]
    assert names == ["X", "Y"]
