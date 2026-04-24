"""Tests for Tier-2.2: prefetch_related for forward FK, reverse FK and M2M."""
from __future__ import annotations

from tests.models import Article, Author, Book, Tag


# ── Forward FK (existing, should still work) ──────────────────────────────────

def test_prefetch_forward_fk():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="Book 1", author=author, pages=100)

    books = list(Book.objects.prefetch_related("author"))
    assert len(books) == 1
    assert books[0].__dict__["_cache_author"].name == "Alice"


# ── Reverse FK (getattr(author, "book_set")) ──────────────────────────────────────────────

def test_reverse_fk_accessor():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="Book 1", author=author, pages=100)
    Book.objects.create(title="Book 2", author=author, pages=200)

    titles = {b.title for b in getattr(author, "book_set").all()}
    assert titles == {"Book 1", "Book 2"}


def test_reverse_fk_filter():
    author = Author.objects.create(name="Bob", age=40)
    Book.objects.create(title="Published", author=author, pages=100, published=True)
    Book.objects.create(title="Draft", author=author, pages=50, published=False)

    published = list(getattr(author, "book_set").filter(published=True))
    assert len(published) == 1
    assert published[0].title == "Published"


def test_reverse_fk_count():
    author = Author.objects.create(name="Carol", age=25)
    Book.objects.create(title="B1", author=author)
    Book.objects.create(title="B2", author=author)

    assert getattr(author, "book_set").count() == 2


def test_reverse_fk_empty():
    author = Author.objects.create(name="Dave", age=20)
    assert getattr(author, "book_set").count() == 0
    assert list(getattr(author, "book_set").all()) == []


def test_prefetch_reverse_fk():
    a1 = Author.objects.create(name="Author1", age=30)
    a2 = Author.objects.create(name="Author2", age=35)
    Book.objects.create(title="Book A", author=a1)
    Book.objects.create(title="Book B", author=a1)
    Book.objects.create(title="Book C", author=a2)

    authors = list(Author.objects.prefetch_related("book_set").order_by("name"))
    # Prefetch cache should be set
    for author in authors:
        assert "_prefetch_book_set" in author.__dict__

    books_a1 = {b.title for b in getattr(authors[0], "book_set").all()}
    books_a2 = {b.title for b in getattr(authors[1], "book_set").all()}
    assert books_a1 == {"Book A", "Book B"}
    assert books_a2 == {"Book C"}


def test_prefetch_reverse_fk_reduces_queries():
    a1 = Author.objects.create(name="A1", age=30)
    a2 = Author.objects.create(name="A2", age=35)
    Book.objects.create(title="B1", author=a1)
    Book.objects.create(title="B2", author=a2)

    # With prefetch, accessing book_set.all() should not trigger extra DB calls
    # We verify this by checking the _prefetch_book_set cache exists
    authors = list(Author.objects.prefetch_related("book_set"))
    for author in authors:
        assert "_prefetch_book_set" in author.__dict__


# ── M2M prefetch ──────────────────────────────────────────────────────────────

def test_prefetch_m2m():
    t1 = Tag.objects.create(name="python")
    t2 = Tag.objects.create(name="django")
    t3 = Tag.objects.create(name="flask")
    a1 = Article.objects.create(title="A1")
    a2 = Article.objects.create(title="A2")
    a1.tags.add(t1, t2)
    a2.tags.add(t3)

    articles = list(Article.objects.prefetch_related("tags").order_by("title"))

    # Prefetch cache populated
    for art in articles:
        assert "_prefetch_tags" in art.__dict__

    tags_a1 = {t.name for t in articles[0].tags.all()}
    tags_a2 = {t.name for t in articles[1].tags.all()}
    assert tags_a1 == {"python", "django"}
    assert tags_a2 == {"flask"}


def test_prefetch_m2m_empty():
    Article.objects.create(title="Empty")
    articles = list(Article.objects.prefetch_related("tags"))
    assert articles[0].__dict__["_prefetch_tags"] == []
    assert list(articles[0].tags.all()) == []


def test_prefetch_m2m_multiple_articles():
    tags = [Tag.objects.create(name=f"tag{i}") for i in range(4)]
    arts = [Article.objects.create(title=f"A{i}") for i in range(3)]
    arts[0].tags.add(tags[0], tags[1])
    arts[1].tags.add(tags[2])
    arts[2].tags.add(tags[0], tags[3])

    fetched = list(Article.objects.prefetch_related("tags").order_by("title"))

    assert {t.name for t in fetched[0].tags.all()} == {"tag0", "tag1"}
    assert {t.name for t in fetched[1].tags.all()} == {"tag2"}
    assert {t.name for t in fetched[2].tags.all()} == {"tag0", "tag3"}
