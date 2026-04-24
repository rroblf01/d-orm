"""Tests for Tier-3.3: on_delete Python-level enforcement."""
from __future__ import annotations

import pytest

import dorm
from dorm.exceptions import ProtectedError


# ── Model definitions ─────────────────────────────────────────────────────────

class Category(dorm.Model):
    name = dorm.CharField(max_length=100)

    class Meta:
        db_table = "on_del_categories"


class CascadeItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(Category, on_delete=dorm.CASCADE)

    class Meta:
        db_table = "on_del_cascade_items"


class ProtectItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(Category, on_delete=dorm.PROTECT)

    class Meta:
        db_table = "on_del_protect_items"


class SetNullItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(Category, on_delete=dorm.SET_NULL, null=True, blank=True)

    class Meta:
        db_table = "on_del_setnull_items"


class SetDefaultItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    # Use a separate "default" category created per-test
    category = dorm.ForeignKey(Category, on_delete=dorm.SET_DEFAULT, null=True, blank=True)

    class Meta:
        db_table = "on_del_setdefault_items"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    # Drop in reverse dependency order (FK tables first, then referenced tables)
    for tbl in [
        "on_del_setdefault_items",
        "on_del_setnull_items",
        "on_del_protect_items",
        "on_del_cascade_items",
        "on_del_categories",
    ]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model, tbl in [
        (Category, "on_del_categories"),
        (CascadeItem, "on_del_cascade_items"),
        (ProtectItem, "on_del_protect_items"),
        (SetNullItem, "on_del_setnull_items"),
        (SetDefaultItem, "on_del_setdefault_items"),
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


# ── CASCADE ───────────────────────────────────────────────────────────────────

def test_cascade_deletes_related():
    cat = Category.objects.create(name="Electronics")
    item1 = CascadeItem.objects.create(name="Laptop", category=cat)
    item2 = CascadeItem.objects.create(name="Phone", category=cat)

    cat.delete()

    assert CascadeItem.objects.filter(pk=item1.pk).count() == 0
    assert CascadeItem.objects.filter(pk=item2.pk).count() == 0


def test_cascade_no_related_objects():
    cat = Category.objects.create(name="Empty")
    cat.delete()  # Should not raise
    assert Category.objects.count() == 0


# ── PROTECT ───────────────────────────────────────────────────────────────────

def test_protect_raises_if_related_exist():
    cat = Category.objects.create(name="Protected")
    ProtectItem.objects.create(name="Item", category=cat)

    with pytest.raises(ProtectedError):
        cat.delete()

    # Category should still exist
    assert Category.objects.filter(pk=cat.pk).count() == 1


def test_protect_allows_delete_when_no_related():
    cat = Category.objects.create(name="Safe")
    cat.delete()  # Should not raise
    assert Category.objects.count() == 0


# ── SET_NULL ──────────────────────────────────────────────────────────────────

def test_set_null_nullifies_fk():
    cat = Category.objects.create(name="Nullable")
    item = SetNullItem.objects.create(name="Item", category=cat)

    cat.delete()

    refreshed = SetNullItem.objects.get(pk=item.pk)
    assert refreshed.__dict__.get("category_id") is None


def test_set_null_parent_deleted():
    cat = Category.objects.create(name="Gone")
    SetNullItem.objects.create(name="Orphan", category=cat)

    cat.delete()
    assert Category.objects.count() == 0
