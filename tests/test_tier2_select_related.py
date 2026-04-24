"""Tests for Tier-2.3: select_related nested depth."""
from __future__ import annotations

from tests.models import Author, Book, Publisher


# ── Single-level (should still work) ─────────────────────────────────────────

def test_select_related_single_level():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="Book 1", author=author)

    books = list(Book.objects.select_related("author"))
    assert len(books) == 1
    assert "_cache_author" in books[0].__dict__
    assert books[0].__dict__["_cache_author"].name == "Alice"


# ── Nested two-level (Book → Author → Publisher) ──────────────────────────────

def test_select_related_two_levels():
    pub = Publisher.objects.create(name="Acme Press")
    author = Author.objects.create(name="Bob", age=40, publisher=pub)
    Book.objects.create(title="Deep Dive", author=author)

    books = list(Book.objects.select_related("author__publisher"))
    assert len(books) == 1
    book = books[0]

    # author should be cached
    cached_author = book.__dict__.get("_cache_author")
    assert cached_author is not None
    assert cached_author.name == "Bob"

    # publisher should be cached on author
    cached_publisher = cached_author.__dict__.get("_cache_publisher")
    assert cached_publisher is not None
    assert cached_publisher.name == "Acme Press"


def test_select_related_nested_null_parent():
    """When author has no publisher, nested cache should be None."""
    author = Author.objects.create(name="Carol", age=25)  # no publisher
    Book.objects.create(title="Solo", author=author)

    books = list(Book.objects.select_related("author__publisher"))
    assert len(books) == 1
    book = books[0]

    cached_author = book.__dict__.get("_cache_author")
    assert cached_author is not None
    assert cached_author.__dict__.get("_cache_publisher") is None


def test_select_related_multiple_paths():
    """select_related with two separate paths: 'author' and 'author__publisher'."""
    pub = Publisher.objects.create(name="Big Books")
    author = Author.objects.create(name="Dan", age=50, publisher=pub)
    Book.objects.create(title="Tome", author=author)

    books = list(Book.objects.select_related("author", "author__publisher"))
    assert len(books) == 1
    book = books[0]

    cached_author = book.__dict__.get("_cache_author")
    assert cached_author is not None
    assert cached_author.name == "Dan"
    assert cached_author.__dict__.get("_cache_publisher") is not None
    assert cached_author.__dict__["_cache_publisher"].name == "Big Books"


def test_select_related_no_duplicate_joins():
    """'author' and 'author__publisher' should not create duplicate JOIN for author."""
    pub = Publisher.objects.create(name="Press")
    author = Author.objects.create(name="Eve", age=35, publisher=pub)
    author2 = Author.objects.create(name="NoPublisher", age=20)
    Book.objects.create(title="Novel", author=author2)
    Book.objects.create(title="Paper", author=author)

    books = list(
        Book.objects.select_related("author", "author__publisher")
        .filter(title="Paper")
    )
    assert len(books) == 1
    assert books[0].__dict__["_cache_author"] is not None
    assert books[0].__dict__["_cache_author"].name == "Eve"
