"""Tests for Tier-4.2: order_by() with FK traversal."""
from __future__ import annotations

import pytest

import dorm


# ── Model definitions ─────────────────────────────────────────────────────────

class OBPublisher(dorm.Model):
    name = dorm.CharField(max_length=100)

    class Meta:
        db_table = "ob_publishers"


class OBAuthor(dorm.Model):
    name = dorm.CharField(max_length=100)
    publisher = dorm.ForeignKey(OBPublisher, on_delete=dorm.CASCADE, null=True, blank=True)

    class Meta:
        db_table = "ob_authors"


class OBBook(dorm.Model):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(OBAuthor, on_delete=dorm.CASCADE)

    class Meta:
        db_table = "ob_books"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in ["ob_books", "ob_authors", "ob_publishers"]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model, tbl in [
        (OBPublisher, "ob_publishers"),
        (OBAuthor, "ob_authors"),
        (OBBook, "ob_books"),
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


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_order_by_fk_field_ascending():
    a_charlie = OBAuthor.objects.create(name="Charlie")
    a_alice = OBAuthor.objects.create(name="Alice")
    a_bob = OBAuthor.objects.create(name="Bob")
    OBBook.objects.create(title="Book C", author=a_charlie)
    OBBook.objects.create(title="Book A", author=a_alice)
    OBBook.objects.create(title="Book B", author=a_bob)

    books = list(OBBook.objects.order_by("author__name"))
    assert [b.title for b in books] == ["Book A", "Book B", "Book C"]


def test_order_by_fk_field_descending():
    a_charlie = OBAuthor.objects.create(name="Charlie")
    a_alice = OBAuthor.objects.create(name="Alice")
    a_bob = OBAuthor.objects.create(name="Bob")
    OBBook.objects.create(title="Book C", author=a_charlie)
    OBBook.objects.create(title="Book A", author=a_alice)
    OBBook.objects.create(title="Book B", author=a_bob)

    books = list(OBBook.objects.order_by("-author__name"))
    assert [b.title for b in books] == ["Book C", "Book B", "Book A"]


def test_order_by_fk_combined_with_local_field():
    a_z = OBAuthor.objects.create(name="Zelda")
    a_a = OBAuthor.objects.create(name="Alice")
    OBBook.objects.create(title="Z-book2", author=a_z)
    OBBook.objects.create(title="Z-book1", author=a_z)
    OBBook.objects.create(title="A-book", author=a_a)

    books = list(OBBook.objects.order_by("author__name", "title"))
    assert books[0].title == "A-book"
    assert books[1].title == "Z-book1"
    assert books[2].title == "Z-book2"


def test_order_by_fk_with_where_filter():
    a_charlie = OBAuthor.objects.create(name="Charlie")
    a_alice = OBAuthor.objects.create(name="Alice")
    OBBook.objects.create(title="Book C", author=a_charlie)
    OBBook.objects.create(title="Book A", author=a_alice)
    OBBook.objects.create(title="Book A2", author=a_alice)

    books = list(OBBook.objects.filter(author=a_alice).order_by("author__name", "title"))
    assert len(books) == 2
    assert all(b.title.startswith("Book A") for b in books)


def test_order_by_two_hop_fk_traversal():
    pub = OBPublisher.objects.create(name="Penguin")
    author = OBAuthor.objects.create(name="Alice", publisher=pub)
    OBBook.objects.create(title="Book", author=author)

    books = list(OBBook.objects.order_by("author__publisher__name"))
    assert len(books) == 1
    assert books[0].title == "Book"


def test_order_by_simple_field_still_works():
    OBBook.objects.create(title="Zebra", author=OBAuthor.objects.create(name="Z"))
    OBBook.objects.create(title="Apple", author=OBAuthor.objects.create(name="A"))

    books = list(OBBook.objects.order_by("title"))
    assert [b.title for b in books] == ["Apple", "Zebra"]


def test_order_by_fk_no_results():
    books = list(OBBook.objects.order_by("author__name"))
    assert books == []


@pytest.mark.asyncio
async def test_async_order_by_fk():
    a_bob = await OBAuthor.objects.acreate(name="Bob")
    a_alice = await OBAuthor.objects.acreate(name="Alice")
    await OBBook.objects.acreate(title="B", author=a_bob)
    await OBBook.objects.acreate(title="A", author=a_alice)

    books = [b async for b in OBBook.objects.order_by("author__name")]
    assert [b.title for b in books] == ["A", "B"]
