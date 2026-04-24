"""Tests for QuerySet improvements: get_or_none, only/defer, select_related,
prefetch_related, exists optimisation, values_list validation, bulk_create batch."""
from __future__ import annotations

import pytest

from tests.models import Author, Book


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_author(**kw) -> Author:
    return Author.objects.create(name=kw.get("name", "Alice"), age=kw.get("age", 30))


def _make_book(author: Author, **kw) -> Book:
    return Book.objects.create(
        title=kw.get("title", "My Book"),
        author=author,
        pages=kw.get("pages", 100),
    )


# ── get_or_none ───────────────────────────────────────────────────────────────

def test_get_or_none_returns_instance():
    author = _make_author(name="Alice")
    result = Author.objects.get_or_none(pk=author.pk)
    assert result is not None
    assert result.pk == author.pk


def test_get_or_none_returns_none_when_missing():
    result = Author.objects.get_or_none(pk=999999)
    assert result is None


def test_get_or_none_with_filter():
    _make_author(name="Alice")
    assert Author.objects.filter(name="Alice").get_or_none() is not None
    assert Author.objects.filter(name="Bob").get_or_none() is None


# ── values_list flat validation ───────────────────────────────────────────────

def test_values_list_flat_single_field_ok():
    _make_author(name="Alice")
    names = list(Author.objects.values_list("name", flat=True))
    assert "Alice" in names


def test_values_list_flat_requires_single_field():
    with pytest.raises(ValueError, match="flat"):
        Author.objects.values_list("name", "age", flat=True)


def test_values_list_flat_zero_fields_raises():
    with pytest.raises(ValueError, match="flat"):
        Author.objects.values_list(flat=True)


# ── exists() optimisation ─────────────────────────────────────────────────────

def test_exists_true():
    _make_author(name="Alice")
    assert Author.objects.filter(name="Alice").exists() is True


def test_exists_false():
    assert Author.objects.filter(name="NoSuchPerson").exists() is False


def test_exists_empty_qs():
    assert Author.objects.exists() is False


def test_exists_nonempty_qs():
    _make_author()
    assert Author.objects.exists() is True


# ── only() ────────────────────────────────────────────────────────────────────

def test_only_returns_model_instances():
    _make_author(name="Alice", age=30)
    results = list(Author.objects.only("name"))
    assert len(results) == 1
    inst = results[0]
    assert isinstance(inst, Author)
    assert inst.name == "Alice"
    assert inst.pk is not None  # pk always loaded


def test_only_unloaded_fields_are_none():
    _make_author(name="Alice", age=30)
    inst = Author.objects.only("name").get()
    # age and email were not loaded
    assert inst.age is None


def test_only_pk_always_included():
    _make_author(name="Alice", age=30)
    inst = Author.objects.only("name").get()
    assert inst.pk is not None


# ── defer() ───────────────────────────────────────────────────────────────────

def test_defer_returns_model_instances():
    _make_author(name="Bob", age=25)
    results = list(Author.objects.defer("age", "email"))
    assert len(results) == 1
    assert isinstance(results[0], Author)
    assert results[0].name == "Bob"


def test_defer_deferred_fields_are_none():
    _make_author(name="Bob", age=25)
    inst = Author.objects.defer("age").get()
    assert inst.age is None


def test_defer_non_deferred_fields_loaded():
    _make_author(name="Bob", age=25)
    inst = Author.objects.defer("age").get()
    assert inst.name == "Bob"
    assert inst.pk is not None


def test_defer_pk_always_kept():
    _make_author(name="Bob", age=25)
    inst = Author.objects.defer("id").get()
    # pk is forced to remain even if deferred
    assert inst.pk is not None


# ── select_related() ──────────────────────────────────────────────────────────

def test_select_related_loads_fk():
    author = _make_author(name="Carol")
    _make_book(author, title="Carols Book")

    books = list(Book.objects.select_related("author"))
    assert len(books) == 1
    loaded = books[0]
    # Related author should be in cache — no extra query needed
    assert "_cache_author" in loaded.__dict__
    cached = loaded.__dict__["_cache_author"]
    assert cached is not None
    assert cached.name == "Carol"



def test_select_related_multiple_books_same_author():
    author = _make_author(name="Eve")
    _make_book(author, title="Book 1")
    _make_book(author, title="Book 2")

    books = list(Book.objects.select_related("author").order_by("title"))
    assert len(books) == 2
    for b in books:
        assert b.__dict__["_cache_author"].name == "Eve"


# ── prefetch_related() ────────────────────────────────────────────────────────

def test_prefetch_related_loads_fk():
    author = _make_author(name="Frank")
    _make_book(author, title="Franks Book")

    books = list(Book.objects.prefetch_related("author"))
    assert len(books) == 1
    assert books[0].__dict__["_cache_author"].name == "Frank"


def test_prefetch_related_batches_authors():
    a1 = _make_author(name="Author1")
    a2 = _make_author(name="Author2")
    _make_book(a1, title="Book A")
    _make_book(a2, title="Book B")
    _make_book(a1, title="Book C")

    books = list(Book.objects.prefetch_related("author").order_by("title"))
    assert len(books) == 3
    names = {b.__dict__["_cache_author"].name for b in books}
    assert names == {"Author1", "Author2"}


# ── bulk_create batch INSERT ──────────────────────────────────────────────────

def test_bulk_create_assigns_pks():
    objs = [Author(name=f"User{i}", age=i) for i in range(5)]
    result = Author.objects.bulk_create(objs)
    assert all(obj.pk is not None for obj in result)
    pks = [obj.pk for obj in result]
    assert len(set(pks)) == 5  # all distinct


def test_bulk_create_persists_rows():
    objs = [Author(name=f"Bulk{i}", age=i * 10) for i in range(3)]
    Author.objects.bulk_create(objs)
    assert Author.objects.count() == 3


def test_bulk_create_empty_list():
    result = Author.objects.bulk_create([])
    assert result == []


def test_bulk_create_batch_size():
    objs = [Author(name=f"B{i}", age=i) for i in range(10)]
    result = Author.objects.bulk_create(objs, batch_size=3)
    assert Author.objects.count() == 10
    assert all(obj.pk is not None for obj in result)


def test_bulk_create_with_books():
    author = _make_author(name="Bulk Author")
    books = [Book(title=f"Book {i}", author_id=author.pk, pages=i * 10) for i in range(4)]
    Book.objects.bulk_create(books)
    assert Book.objects.count() == 4
    assert all(b.pk is not None for b in books)


# ── async get_or_none ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_aget_or_none_found():
    author = _make_author(name="Async Alice")
    result = await Author.objects.aget_or_none(pk=author.pk)
    assert result is not None
    assert result.pk == author.pk


@pytest.mark.asyncio
async def test_aget_or_none_not_found():
    result = await Author.objects.aget_or_none(pk=999999)
    assert result is None


# ── async exists ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_aexists_true():
    _make_author(name="AsyncBob")
    assert await Author.objects.filter(name="AsyncBob").aexists() is True


@pytest.mark.asyncio
async def test_aexists_false():
    assert await Author.objects.filter(name="NoSuchPerson").aexists() is False


# ── avalues / avalues_list ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_avalues_returns_dicts():
    _make_author(name="Alice", age=30)
    rows = await Author.objects.avalues("name", "age")
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 30


@pytest.mark.asyncio
async def test_avalues_all_fields():
    _make_author(name="Bob")
    rows = await Author.objects.avalues()
    assert len(rows) == 1
    assert "name" in rows[0]
    assert "id" in rows[0]


@pytest.mark.asyncio
async def test_avalues_with_filter():
    _make_author(name="Alice", age=30)
    _make_author(name="Bob", age=25)
    rows = await Author.objects.filter(age__gte=30).avalues("name")
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_avalues_list_tuples():
    _make_author(name="Carol", age=28)
    rows = await Author.objects.avalues_list("name", "age")
    assert len(rows) == 1
    assert rows[0] == ("Carol", 28)


@pytest.mark.asyncio
async def test_avalues_list_flat():
    _make_author(name="Dave")
    _make_author(name="Eve")
    names = await Author.objects.order_by("name").avalues_list("name", flat=True)
    assert names == ["Dave", "Eve"]


@pytest.mark.asyncio
async def test_avalues_list_flat_validation():
    with pytest.raises(ValueError, match="flat"):
        await Author.objects.avalues_list("name", "age", flat=True)
