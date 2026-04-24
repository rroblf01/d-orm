"""Tests for Tier-2.5: Meta.ordering default ordering."""
from __future__ import annotations

import pytest

import dorm


class OrderedAuthor(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

    class Meta:
        db_table = "ordered_authors"
        ordering = ["age"]


class ReverseOrderedAuthor(dorm.Model):
    name = dorm.CharField(max_length=100)
    score = dorm.IntegerField()

    class Meta:
        db_table = "reverse_ordered_authors"
        ordering = ["-score"]


@pytest.fixture(autouse=True)
def _create_ordered_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    for model, tbl in [
        (OrderedAuthor, "ordered_authors"),
        (ReverseOrderedAuthor, "reverse_ordered_authors"),
    ]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"')
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


def test_meta_ordering_applied_automatically():
    OrderedAuthor.objects.create(name="Charlie", age=50)
    OrderedAuthor.objects.create(name="Alice", age=10)
    OrderedAuthor.objects.create(name="Bob", age=30)

    result = list(OrderedAuthor.objects.all())
    assert [a.age for a in result] == [10, 30, 50]


def test_meta_ordering_reversed():
    ReverseOrderedAuthor.objects.create(name="A", score=1)
    ReverseOrderedAuthor.objects.create(name="B", score=3)
    ReverseOrderedAuthor.objects.create(name="C", score=2)

    result = list(ReverseOrderedAuthor.objects.all())
    assert [a.score for a in result] == [3, 2, 1]


def test_order_by_overrides_meta_ordering():
    OrderedAuthor.objects.create(name="Charlie", age=50)
    OrderedAuthor.objects.create(name="Alice", age=10)
    OrderedAuthor.objects.create(name="Bob", age=30)

    result = list(OrderedAuthor.objects.order_by("name"))
    assert [a.name for a in result] == ["Alice", "Bob", "Charlie"]


def test_order_by_empty_clears_meta_ordering():
    """Calling .order_by() with no args clears default ordering."""
    OrderedAuthor.objects.create(name="Charlie", age=50)
    OrderedAuthor.objects.create(name="Alice", age=10)

    result = list(OrderedAuthor.objects.order_by())
    ages = [a.age for a in result]
    # No guaranteed order, but query should succeed without error
    assert set(ages) == {10, 50}


def test_meta_ordering_on_filter():
    OrderedAuthor.objects.create(name="Charlie", age=50)
    OrderedAuthor.objects.create(name="Alice", age=10)
    OrderedAuthor.objects.create(name="Bob", age=30)

    result = list(OrderedAuthor.objects.filter(age__gte=10))
    assert [a.age for a in result] == [10, 30, 50]


def test_meta_ordering_with_values():
    OrderedAuthor.objects.create(name="Charlie", age=50)
    OrderedAuthor.objects.create(name="Alice", age=10)

    result = list(OrderedAuthor.objects.values("name"))
    # values() goes through _iterator too — meta ordering should apply
    assert result[0]["name"] == "Alice"
