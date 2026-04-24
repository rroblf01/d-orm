"""Tests for Tier-2.4: union / intersection / difference."""
from __future__ import annotations

import pytest

from tests.models import Author


# ── union ─────────────────────────────────────────────────────────────────────

def test_union_basic():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(name="Alice")
    qs2 = Author.objects.filter(name="Bob")
    result = list(qs1.union(qs2))

    names = {a.name for a in result}
    assert names == {"Alice", "Bob"}


def test_union_deduplication():
    Author.objects.create(name="Alice", age=30)

    qs1 = Author.objects.filter(name="Alice")
    qs2 = Author.objects.filter(name="Alice")
    result = list(qs1.union(qs2))  # UNION deduplicates by default

    assert len(result) == 1


def test_union_all_keeps_duplicates():
    Author.objects.create(name="Alice", age=30)

    qs1 = Author.objects.filter(name="Alice")
    qs2 = Author.objects.filter(name="Alice")
    result = list(qs1.union(qs2, all=True))

    assert len(result) == 2


def test_union_count():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(name="Alice")
    qs2 = Author.objects.filter(name="Bob")
    combined = qs1.union(qs2)

    assert combined.count() == 2


def test_union_order_by():
    Author.objects.create(name="Charlie", age=50)
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs = Author.objects.filter(name="Alice").union(
        Author.objects.filter(name="Bob"),
        Author.objects.filter(name="Charlie"),
    ).order_by("age")

    result = list(qs)
    assert [a.age for a in result] == [30, 40, 50]


def test_union_multiple_querysets():
    Author.objects.create(name="A", age=10)
    Author.objects.create(name="B", age=20)
    Author.objects.create(name="C", age=30)

    result = list(
        Author.objects.filter(name="A").union(
            Author.objects.filter(name="B"),
            Author.objects.filter(name="C"),
        )
    )
    assert len(result) == 3


def test_union_empty():
    Author.objects.create(name="Alice", age=30)

    qs1 = Author.objects.filter(name="Alice")
    qs2 = Author.objects.none()
    result = list(qs1.union(qs2))

    assert len(result) == 1
    assert result[0].name == "Alice"


# ── intersection ──────────────────────────────────────────────────────────────

@pytest.mark.skipif(
    False, reason="INTERSECT is supported by both SQLite and PostgreSQL"
)
def test_intersection_basic():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(age__gte=30)   # Alice, Bob
    qs2 = Author.objects.filter(age__lte=30)   # Alice only
    result = list(qs1.intersection(qs2))

    assert len(result) == 1
    assert result[0].name == "Alice"


def test_intersection_empty_result():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(age=30)  # Alice
    qs2 = Author.objects.filter(age=40)  # Bob
    result = list(qs1.intersection(qs2))

    assert result == []


def test_intersection_count():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(age__gte=30)
    qs2 = Author.objects.filter(age__lte=30)
    assert qs1.intersection(qs2).count() == 1


# ── difference ────────────────────────────────────────────────────────────────

def test_difference_basic():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(age__gte=30)  # Alice, Bob
    qs2 = Author.objects.filter(age=40)       # Bob
    result = list(qs1.difference(qs2))

    assert len(result) == 1
    assert result[0].name == "Alice"


def test_difference_empty_result():
    Author.objects.create(name="Alice", age=30)

    qs1 = Author.objects.filter(name="Alice")
    qs2 = Author.objects.filter(name="Alice")
    result = list(qs1.difference(qs2))

    assert result == []


def test_difference_count():
    Author.objects.create(name="Alice", age=30)
    Author.objects.create(name="Bob", age=40)

    qs1 = Author.objects.filter(age__gte=30)
    qs2 = Author.objects.filter(age=40)
    assert qs1.difference(qs2).count() == 1
