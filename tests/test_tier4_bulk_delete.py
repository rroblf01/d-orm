"""Tests for Tier-4.1: QuerySet.delete() respects Python-level on_delete."""
from __future__ import annotations

import pytest

import dorm
from dorm.exceptions import ProtectedError


# ── Model definitions ─────────────────────────────────────────────────────────

class BDCategory(dorm.Model):
    name = dorm.CharField(max_length=100)

    class Meta:
        db_table = "bd_categories"


class BDCascadeItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(BDCategory, on_delete=dorm.CASCADE)

    class Meta:
        db_table = "bd_cascade_items"


class BDProtectItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(BDCategory, on_delete=dorm.PROTECT)

    class Meta:
        db_table = "bd_protect_items"


class BDSetNullItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(BDCategory, on_delete=dorm.SET_NULL, null=True, blank=True)

    class Meta:
        db_table = "bd_setnull_items"


class BDSetDefaultItem(dorm.Model):
    name = dorm.CharField(max_length=100)
    category = dorm.ForeignKey(BDCategory, on_delete=dorm.SET_DEFAULT, null=True, blank=True)

    class Meta:
        db_table = "bd_setdefault_items"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in [
        "bd_setdefault_items",
        "bd_setnull_items",
        "bd_protect_items",
        "bd_cascade_items",
        "bd_categories",
    ]:
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')

    for model, tbl in [
        (BDCategory, "bd_categories"),
        (BDCascadeItem, "bd_cascade_items"),
        (BDProtectItem, "bd_protect_items"),
        (BDSetNullItem, "bd_setnull_items"),
        (BDSetDefaultItem, "bd_setdefault_items"),
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


# ── Bulk CASCADE ──────────────────────────────────────────────────────────────

def test_bulk_cascade_deletes_related():
    cat1 = BDCategory.objects.create(name="A")
    cat2 = BDCategory.objects.create(name="B")
    item1 = BDCascadeItem.objects.create(name="i1", category=cat1)
    item2 = BDCascadeItem.objects.create(name="i2", category=cat2)

    total, counts = BDCategory.objects.all().delete()

    assert BDCategory.objects.count() == 0
    assert BDCascadeItem.objects.count() == 0
    assert total >= 4  # 2 categories + 2 items
    _ = item1, item2


def test_bulk_cascade_empty_queryset():
    total, counts = BDCategory.objects.all().delete()
    assert total == 0


def test_bulk_cascade_filtered_subset():
    cat1 = BDCategory.objects.create(name="Keep")
    cat2 = BDCategory.objects.create(name="Delete")
    BDCascadeItem.objects.create(name="child", category=cat2)

    BDCategory.objects.filter(name="Delete").delete()

    assert BDCategory.objects.count() == 1
    assert BDCategory.objects.get(pk=cat1.pk).name == "Keep"
    assert BDCascadeItem.objects.count() == 0


def test_bulk_cascade_returns_counts():
    cat = BDCategory.objects.create(name="Cat")
    BDCascadeItem.objects.create(name="c1", category=cat)
    BDCascadeItem.objects.create(name="c2", category=cat)

    total, counts = BDCategory.objects.all().delete()

    assert total == 3  # 1 category + 2 items
    assert counts["tests.BDCategory"] == 1
    assert counts["tests.BDCascadeItem"] == 2


# ── Bulk PROTECT ──────────────────────────────────────────────────────────────

def test_bulk_protect_raises_if_related_exist():
    cat = BDCategory.objects.create(name="Protected")
    BDProtectItem.objects.create(name="Item", category=cat)

    with pytest.raises(ProtectedError):
        BDCategory.objects.all().delete()

    assert BDCategory.objects.count() == 1


def test_bulk_protect_allows_delete_when_no_related():
    BDCategory.objects.create(name="Safe")
    total, _ = BDCategory.objects.all().delete()
    assert total == 1
    assert BDCategory.objects.count() == 0


# ── Bulk SET_NULL ─────────────────────────────────────────────────────────────

def test_bulk_set_null_nullifies_fk():
    cat1 = BDCategory.objects.create(name="A")
    cat2 = BDCategory.objects.create(name="B")
    item1 = BDSetNullItem.objects.create(name="i1", category=cat1)
    item2 = BDSetNullItem.objects.create(name="i2", category=cat2)

    BDCategory.objects.all().delete()

    assert BDSetNullItem.objects.filter(pk=item1.pk).get().__dict__.get("category_id") is None
    assert BDSetNullItem.objects.filter(pk=item2.pk).get().__dict__.get("category_id") is None
    assert BDCategory.objects.count() == 0
    _ = item1, item2


def test_bulk_set_null_parent_deleted():
    cat = BDCategory.objects.create(name="Gone")
    BDSetNullItem.objects.create(name="Orphan", category=cat)

    BDCategory.objects.all().delete()
    assert BDCategory.objects.count() == 0
    assert BDSetNullItem.objects.count() == 1


# ── Async variants ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_bulk_cascade_deletes_related():
    cat = await BDCategory.objects.acreate(name="AsyncCat")
    await BDCascadeItem.objects.acreate(name="item", category=cat)

    total, counts = await BDCategory.objects.all().adelete()

    assert await BDCategory.objects.acount() == 0
    assert await BDCascadeItem.objects.acount() == 0
    assert total == 2


@pytest.mark.asyncio
async def test_async_bulk_protect_raises():
    cat = await BDCategory.objects.acreate(name="Protected")
    await BDProtectItem.objects.acreate(name="item", category=cat)

    with pytest.raises(ProtectedError):
        await BDCategory.objects.all().adelete()

    assert await BDCategory.objects.acount() == 1


@pytest.mark.asyncio
async def test_async_bulk_set_null():
    cat = await BDCategory.objects.acreate(name="NullCat")
    item = await BDSetNullItem.objects.acreate(name="item", category=cat)

    await BDCategory.objects.all().adelete()

    refreshed = await BDSetNullItem.objects.aget(pk=item.pk)
    assert refreshed.__dict__.get("category_id") is None
