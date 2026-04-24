"""Tests for filter(__in=queryset) subquery support."""
from __future__ import annotations

import pytest

import dorm


# ── Model definitions ─────────────────────────────────────────────────────────

class SQAuthor(dorm.Model):
    name = dorm.CharField(max_length=100)
    active = dorm.BooleanField()

    class Meta:
        db_table = "sq_authors"


class SQBook(dorm.Model):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(SQAuthor, on_delete=dorm.CASCADE)

    class Meta:
        db_table = "sq_books"


class SQTag(dorm.Model):
    name = dorm.CharField(max_length=50)

    class Meta:
        db_table = "sq_tags"


class SQBookTag(dorm.Model):
    book = dorm.ForeignKey(SQBook, on_delete=dorm.CASCADE)
    tag = dorm.ForeignKey(SQTag, on_delete=dorm.CASCADE)

    class Meta:
        db_table = "sq_book_tags"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in ["sq_book_tags", "sq_books", "sq_tags", "sq_authors"]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model in [SQAuthor, SQBook, SQTag, SQBookTag]:
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{model._meta.db_table}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )


# ── Basic subquery ─────────────────────────────────────────────────────────────

def test_filter_in_queryset_basic():
    a1 = SQAuthor.objects.create(name="Alice", active=True)
    a2 = SQAuthor.objects.create(name="Bob", active=False)
    SQBook.objects.create(title="Book A", author=a1)
    SQBook.objects.create(title="Book B", author=a2)

    active_authors = SQAuthor.objects.filter(active=True)
    books = SQBook.objects.filter(author__in=active_authors)

    assert books.count() == 1
    assert books.get().title == "Book A"


def test_filter_in_queryset_multiple_results():
    a1 = SQAuthor.objects.create(name="Alice", active=True)
    a2 = SQAuthor.objects.create(name="Bob", active=True)
    a3 = SQAuthor.objects.create(name="Carol", active=False)
    SQBook.objects.create(title="Book 1", author=a1)
    SQBook.objects.create(title="Book 2", author=a2)
    SQBook.objects.create(title="Book 3", author=a3)

    active_qs = SQAuthor.objects.filter(active=True)
    books = SQBook.objects.filter(author__in=active_qs).order_by("title")

    assert books.count() == 2
    titles = [b.title for b in books]
    assert titles == ["Book 1", "Book 2"]


def test_filter_in_queryset_empty_subquery():
    SQAuthor.objects.create(name="Alice", active=False)
    SQBook.objects.create(title="Book A", author=SQAuthor.objects.get(name="Alice"))

    active_qs = SQAuthor.objects.filter(active=True)
    books = SQBook.objects.filter(author__in=active_qs)

    assert books.count() == 0


def test_filter_in_queryset_returns_all_when_all_match():
    a1 = SQAuthor.objects.create(name="X", active=True)
    a2 = SQAuthor.objects.create(name="Y", active=True)
    SQBook.objects.create(title="T1", author=a1)
    SQBook.objects.create(title="T2", author=a2)

    all_authors = SQAuthor.objects.all()
    books = SQBook.objects.filter(author__in=all_authors)

    assert books.count() == 2


def test_filter_in_queryset_direct_model_pk():
    """filter(pk__in=queryset) — subquery on the same model's PK."""
    SQAuthor.objects.create(name="Alice", active=True)
    SQAuthor.objects.create(name="Bob", active=False)
    SQAuthor.objects.create(name="Carol", active=True)

    active_qs = SQAuthor.objects.filter(active=True)
    result = SQAuthor.objects.filter(pk__in=active_qs).order_by("name")

    names = [a.name for a in result]
    assert names == ["Alice", "Carol"]


def test_filter_in_queryset_combined_with_other_filters():
    a1 = SQAuthor.objects.create(name="Alice", active=True)
    a2 = SQAuthor.objects.create(name="Bob", active=True)
    SQBook.objects.create(title="Alpha", author=a1)
    SQBook.objects.create(title="Beta", author=a1)
    SQBook.objects.create(title="Gamma", author=a2)

    active_qs = SQAuthor.objects.filter(active=True)
    books = SQBook.objects.filter(
        author__in=active_qs, title__startswith="A"
    )

    assert books.count() == 1
    assert books.get().title == "Alpha"


def test_filter_in_queryset_with_limit():
    """Subquery with slicing — only the first N authors qualify."""
    a1 = SQAuthor.objects.create(name="A1", active=True)
    a2 = SQAuthor.objects.create(name="A2", active=True)
    SQBook.objects.create(title="B1", author=a1)
    SQBook.objects.create(title="B2", author=a2)

    limited_qs = SQAuthor.objects.filter(active=True).order_by("name")[:1]
    books = SQBook.objects.filter(author__in=limited_qs)

    assert books.count() == 1


def test_filter_in_list_still_works():
    """Ensure list-based __in still functions after subquery change."""
    a1 = SQAuthor.objects.create(name="Alice", active=True)
    a2 = SQAuthor.objects.create(name="Bob", active=False)

    result = SQAuthor.objects.filter(pk__in=[a1.pk, a2.pk]).order_by("name")
    assert [a.name for a in result] == ["Alice", "Bob"]


def test_filter_in_queryset_chained():
    """Nested subqueries: books whose authors are in active_qs, whose tags match."""
    a1 = SQAuthor.objects.create(name="Alice", active=True)
    a2 = SQAuthor.objects.create(name="Bob", active=False)
    b1 = SQBook.objects.create(title="B1", author=a1)
    b2 = SQBook.objects.create(title="B2", author=a2)
    _ = b2

    active_qs = SQAuthor.objects.filter(active=True)
    books_qs = SQBook.objects.filter(author__in=active_qs)

    # books_qs is itself a QuerySet — filter another model using it
    tag = SQTag.objects.create(name="sci-fi")
    SQBookTag.objects.create(book=b1, tag=tag)

    book_tags = SQBookTag.objects.filter(book__in=books_qs)
    assert book_tags.count() == 1


# ── Async variants ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_filter_in_queryset():
    a1 = await SQAuthor.objects.acreate(name="Alice", active=True)
    a2 = await SQAuthor.objects.acreate(name="Bob", active=False)
    await SQBook.objects.acreate(title="Book A", author=a1)
    await SQBook.objects.acreate(title="Book B", author=a2)

    active_qs = SQAuthor.objects.filter(active=True)
    books = SQBook.objects.filter(author__in=active_qs)

    assert await books.acount() == 1
    book = await books.aget()
    assert book.title == "Book A"


@pytest.mark.asyncio
async def test_async_filter_in_queryset_empty():
    await SQAuthor.objects.acreate(name="Alice", active=False)
    a = await SQAuthor.objects.aget(name="Alice")
    await SQBook.objects.acreate(title="B", author=a)

    active_qs = SQAuthor.objects.filter(active=True)
    assert await SQBook.objects.filter(author__in=active_qs).acount() == 0
