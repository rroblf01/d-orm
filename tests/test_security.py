"""Security tests: LIKE wildcard escaping and identifier injection prevention."""
from __future__ import annotations

import pytest

from tests.models import Author


def _make(name: str, age: int = 30) -> Author:
    return Author.objects.create(name=name, age=age)


# ── LIKE wildcard escaping ────────────────────────────────────────────────────

def test_contains_percent_is_literal():
    _make("100% natural")
    _make("something else")
    results = list(Author.objects.filter(name__contains="100%"))
    assert len(results) == 1
    assert results[0].name == "100% natural"


def test_contains_percent_not_wildcard():
    # Without escaping, "100%" as a LIKE pattern would match "1000 things" too.
    _make("1000 things")
    _make("100% done")
    results = list(Author.objects.filter(name__contains="100%"))
    names = [r.name for r in results]
    assert "1000 things" not in names
    assert "100% done" in names


def test_contains_underscore_is_literal():
    _make("hello_world")
    _make("helloXworld")
    results = list(Author.objects.filter(name__contains="hello_world"))
    names = [r.name for r in results]
    assert "hello_world" in names
    assert "helloXworld" not in names


def test_startswith_percent_is_literal():
    _make("50% off sale")
    _make("50 units")
    results = list(Author.objects.filter(name__startswith="50%"))
    names = [r.name for r in results]
    assert "50% off sale" in names
    assert "50 units" not in names


def test_endswith_percent_is_literal():
    _make("charged 10%")
    _make("charged 10 dollars")
    results = list(Author.objects.filter(name__endswith="10%"))
    names = [r.name for r in results]
    assert "charged 10%" in names
    assert "charged 10 dollars" not in names


def test_icontains_percent_is_literal():
    _make("PRICE: 99%")
    _make("PRICE: 99 EUR")
    results = list(Author.objects.filter(name__icontains="99%"))
    names = [r.name for r in results]
    assert "PRICE: 99%" in names
    assert "PRICE: 99 EUR" not in names


def test_contains_backslash_is_literal():
    _make("path\\to\\file")
    _make("path/to/file")
    results = list(Author.objects.filter(name__contains="path\\to"))
    names = [r.name for r in results]
    assert "path\\to\\file" in names
    assert "path/to/file" not in names


# ── Identifier injection prevention ──────────────────────────────────────────

def test_values_rejects_invalid_field_name():
    with pytest.raises(ValueError, match="Invalid field"):
        list(Author.objects.values('name"; DROP TABLE authors; --'))


def test_only_rejects_invalid_field_name():
    with pytest.raises(ValueError, match="Invalid field"):
        list(Author.objects.only('name" OR 1=1--'))


def test_defer_rejects_invalid_field_name():
    with pytest.raises(ValueError, match="Invalid field"):
        list(Author.objects.defer('age" OR 1=1--'))


def test_annotate_rejects_invalid_alias():
    import dorm
    with pytest.raises(ValueError, match="Invalid annotation alias"):
        list(Author.objects.annotate(**{'count"; DROP TABLE': dorm.Count("id")}))


def test_aggregate_rejects_invalid_alias():
    import dorm
    with pytest.raises(ValueError, match="Invalid aggregate alias"):
        Author.objects.aggregate(**{'total"; DROP TABLE': dorm.Count("id")})
