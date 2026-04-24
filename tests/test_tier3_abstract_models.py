"""Tests for Tier-3.1: Abstract model inheritance."""
from __future__ import annotations

import pytest

import dorm


# ── Model definitions ─────────────────────────────────────────────────────────

class TimestampedMixin(dorm.Model):
    created_at = dorm.DateTimeField(null=True, blank=True)
    updated_at = dorm.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True


class OrderedMixin(dorm.Model):
    rank = dorm.IntegerField(default=0)

    class Meta:
        abstract = True
        ordering = ["rank"]


class Product(TimestampedMixin):
    name = dorm.CharField(max_length=100)
    price = dorm.IntegerField(default=0)

    class Meta:
        db_table = "products"


class Widget(OrderedMixin):
    label = dorm.CharField(max_length=100)

    class Meta:
        db_table = "widgets"


class Gadget(OrderedMixin):
    label = dorm.CharField(max_length=100)

    class Meta:
        db_table = "gadgets"
        ordering = ["-rank"]  # concrete override


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    for model, tbl in [
        (Product, "products"),
        (Widget, "widgets"),
        (Gadget, "gadgets"),
    ]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')
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


# ── Tests: field inheritance ───────────────────────────────────────────────────

def test_abstract_fields_inherited():
    """Concrete model has all fields from abstract parent + its own."""
    field_names = {f.name for f in Product._meta.fields}
    assert "id" in field_names
    assert "created_at" in field_names
    assert "updated_at" in field_names
    assert "name" in field_names
    assert "price" in field_names


def test_abstract_model_is_marked():
    assert TimestampedMixin._meta.abstract is True
    assert Product._meta.abstract is False


def test_abstract_model_has_no_manager():
    assert not hasattr(TimestampedMixin, "objects") or isinstance(
        TimestampedMixin.__dict__.get("objects"), type(None)
    )


def test_concrete_model_crud():
    p = Product.objects.create(name="Widget", price=99)
    assert p.pk is not None

    fetched = Product.objects.get(pk=p.pk)
    assert fetched.name == "Widget"
    assert fetched.price == 99


def test_inherited_fields_queryable():
    Product.objects.create(name="A", price=10)
    Product.objects.create(name="B", price=20)

    result = list(Product.objects.filter(price__gte=10).order_by("price"))
    assert len(result) == 2


# ── Tests: Meta inheritance ────────────────────────────────────────────────────

def test_meta_ordering_inherited_from_abstract():
    Widget.objects.create(label="C", rank=30)
    Widget.objects.create(label="A", rank=10)
    Widget.objects.create(label="B", rank=20)

    result = list(Widget.objects.all())
    assert [w.rank for w in result] == [10, 20, 30]


def test_concrete_meta_overrides_abstract_ordering():
    Gadget.objects.create(label="X", rank=10)
    Gadget.objects.create(label="Y", rank=30)
    Gadget.objects.create(label="Z", rank=20)

    result = list(Gadget.objects.all())
    # Gadget.Meta.ordering = ["-rank"], overrides abstract's ordering = ["rank"]
    assert [g.rank for g in result] == [30, 20, 10]


# ── Tests: multiple concrete models from same abstract ────────────────────────

def test_two_concrete_models_independent():
    """Widget and Gadget are independent — they do NOT share a table."""
    Widget.objects.create(label="only_widget", rank=1)

    assert Widget.objects.count() == 1
    assert Gadget.objects.count() == 0


def test_fields_are_deep_copied():
    """Mutating a field on one concrete model must not affect another."""
    widget_fields = {f.name: f for f in Widget._meta.fields}
    gadget_fields = {f.name: f for f in Gadget._meta.fields}

    assert widget_fields["rank"] is not gadget_fields["rank"]
