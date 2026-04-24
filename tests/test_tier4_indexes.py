"""Tests for Tier-4.7: AddIndex/RemoveIndex migration operations and Meta.indexes."""
from __future__ import annotations

import pytest

import dorm
from dorm.indexes import Index
from dorm.migrations.autodetector import MigrationAutodetector
from dorm.migrations.operations import AddIndex, RemoveIndex
from dorm.migrations.state import ProjectState


# ── Model definitions ─────────────────────────────────────────────────────────

class IndexedProduct(dorm.Model):
    name = dorm.CharField(max_length=100)
    sku = dorm.CharField(max_length=20)
    price = dorm.FloatField(default=0.0)

    class Meta:
        db_table = "idx_products"
        indexes = [
            Index(fields=["name"], name="idx_products_name"),
            Index(fields=["sku"], unique=True, name="idx_products_sku_uniq"),
        ]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _create_tables(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "idx_products"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in IndexedProduct._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "idx_products" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )


# ── Index class ───────────────────────────────────────────────────────────────

def test_index_auto_name():
    idx = Index(fields=["name", "sku"])
    assert idx.get_name("Product") == "idx_product_name_sku"


def test_index_auto_name_unique():
    idx = Index(fields=["email"], unique=True)
    assert idx.get_name("User") == "uniq_user_email"


def test_index_explicit_name():
    idx = Index(fields=["name"], name="my_custom_idx")
    assert idx.get_name("Product") == "my_custom_idx"


def test_index_equality():
    a = Index(fields=["x"], name="foo")
    b = Index(fields=["x"], name="foo")
    c = Index(fields=["y"], name="foo")
    assert a == b
    assert a != c


# ── AddIndex / RemoveIndex operations ─────────────────────────────────────────

def test_add_index_creates_index(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.state import ProjectState

    conn = get_connection()

    idx = Index(fields=["name"], name="test_add_idx")
    op = AddIndex(model_name="IndexedProduct", index=idx)

    from_state = ProjectState()
    from_state.models["tests.indexedproduct"] = {
        "name": "IndexedProduct",
        "fields": {},
        "options": {"db_table": "idx_products", "indexes": []},
    }
    to_state = from_state.clone()
    to_state.models["tests.indexedproduct"]["options"]["indexes"] = [idx]

    op.database_forwards("tests", conn, from_state, to_state)

    # Verify by inserting and querying (index should make it work without error)
    IndexedProduct.objects.create(name="Widget", sku="W001", price=9.99)
    assert IndexedProduct.objects.count() == 1

    # Reverse: drop the index
    op.database_backwards("tests", conn, from_state, to_state)


def test_remove_index_drops_index(clean_db):
    from dorm.db.connection import get_connection
    from dorm.migrations.state import ProjectState

    conn = get_connection()

    idx = Index(fields=["sku"], name="test_remove_idx")
    add_op = AddIndex(model_name="IndexedProduct", index=idx)
    remove_op = RemoveIndex(model_name="IndexedProduct", index=idx)

    from_state = ProjectState()
    from_state.models["tests.indexedproduct"] = {
        "name": "IndexedProduct",
        "fields": {},
        "options": {"db_table": "idx_products", "indexes": [idx]},
    }
    to_state = from_state.clone()

    # First create the index
    add_op.database_forwards("tests", conn, from_state, to_state)

    # Then drop it — should not raise
    remove_op.database_forwards("tests", conn, from_state, to_state)


def test_add_unique_index():
    from dorm.db.connection import get_connection
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    idx = Index(fields=["sku"], unique=True, name="test_uniq_idx")
    op = AddIndex(model_name="IndexedProduct", index=idx)

    state = ProjectState()
    state.models["tests.indexedproduct"] = {
        "name": "IndexedProduct",
        "fields": {},
        "options": {"db_table": "idx_products", "indexes": []},
    }

    op.database_forwards("tests", conn, state, state)

    # Unique constraint should be enforced
    IndexedProduct.objects.create(name="A", sku="UNIQUE1", price=1.0)
    with pytest.raises(Exception):  # unique violation
        IndexedProduct.objects.create(name="B", sku="UNIQUE1", price=2.0)

    op.database_backwards("tests", conn, state, state)


# ── Autodetector index change detection ───────────────────────────────────────

def _make_state(models: dict) -> ProjectState:
    state = ProjectState()
    state.models = dict(models)
    return state


def _model_state(name: str, indexes: list, app: str = "myapp") -> tuple[str, dict]:
    key = f"{app}.{name.lower()}"
    return key, {
        "name": name,
        "fields": {"id": dorm.AutoField(primary_key=True)},
        "options": {"db_table": f"{app}_{name.lower()}", "indexes": indexes},
    }


def test_autodetect_add_index():
    idx = Index(fields=["name"], name="idx_thing_name")
    k_from, v_from = _model_state("Thing", [])
    k_to, v_to = _model_state("Thing", [idx])

    from_state = _make_state({k_from: v_from})
    to_state = _make_state({k_to: v_to})

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    add_ops = [op for op in ops if isinstance(op, AddIndex)]
    assert len(add_ops) == 1
    assert add_ops[0].index == idx


def test_autodetect_remove_index():
    idx = Index(fields=["name"], name="idx_thing_name")
    k_from, v_from = _model_state("Thing", [idx])
    k_to, v_to = _model_state("Thing", [])

    from_state = _make_state({k_from: v_from})
    to_state = _make_state({k_to: v_to})

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    remove_ops = [op for op in ops if isinstance(op, RemoveIndex)]
    assert len(remove_ops) == 1
    assert remove_ops[0].index == idx


def test_autodetect_no_index_changes():
    idx = Index(fields=["name"], name="idx_thing_name")
    k1, v1 = _model_state("Thing", [idx])

    from_state = _make_state({k1: v1})
    to_state = _make_state({k1: v1})

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("myapp")
    assert changes == {}


def test_autodetect_add_and_remove_index():
    old_idx = Index(fields=["a"], name="idx_old")
    new_idx = Index(fields=["b"], name="idx_new")

    k_from, v_from = _model_state("Thing", [old_idx])
    k_to, v_to = _model_state("Thing", [new_idx])

    from_state = _make_state({k_from: v_from})
    to_state = _make_state({k_to: v_to})

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    assert any(isinstance(op, AddIndex) for op in ops)
    assert any(isinstance(op, RemoveIndex) for op in ops)


# ── Meta.indexes in model ──────────────────────────────────────────────────────

def test_meta_indexes_attribute():
    assert len(IndexedProduct._meta.indexes) == 2
    names = {i.name for i in IndexedProduct._meta.indexes}
    assert "idx_products_name" in names
    assert "idx_products_sku_uniq" in names


def test_meta_indexes_in_project_state():
    state = ProjectState.from_apps("tests")
    model_state = state.get_model("tests", "IndexedProduct")
    if model_state is None:
        pytest.skip("IndexedProduct not in registry for this test run")
    indexes = model_state.get("options", {}).get("indexes", [])
    assert len(indexes) == 2
