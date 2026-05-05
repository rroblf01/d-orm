"""Tests for the recursive CTE tree helpers."""

from __future__ import annotations

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql
from dorm.tree import ancestors, ancestors_cte, descendants, descendants_cte


class _Cat(dorm.Model):
    name = dorm.CharField(max_length=50)
    # Plain integer column — self-FK isn't required for adjacency-list
    # walking via recursive CTEs, and the dorm test models registry
    # does not yet support ``ForeignKey("self", ...)``.
    parent_id = dorm.IntegerField(null=True, blank=True, db_index=True)

    class Meta:
        db_table = "tree_cats"
        app_label = "tests"


@pytest.fixture(autouse=True)
def _table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "tree_cats"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Cat._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "tree_cats" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "tree_cats"{cascade}')


def _make_tree():
    """Build a small tree:
        root
        ├── a
        │   ├── a1
        │   └── a2
        └── b
    """
    root = _Cat.objects.create(name="root")
    a = _Cat.objects.create(name="a", parent_id=root.pk)
    b = _Cat.objects.create(name="b", parent_id=root.pk)
    a1 = _Cat.objects.create(name="a1", parent_id=a.pk)
    a2 = _Cat.objects.create(name="a2", parent_id=a.pk)
    return root, a, b, a1, a2


def test_descendants_cte_returns_cte_object():
    cte = descendants_cte(_Cat, parent_field="parent_id", root_pk=1)
    assert cte.recursive is True
    assert "UNION ALL" in cte.sql
    assert cte.params == [1]


def test_descendants_returns_all_subtree():
    root, a, b, a1, a2 = _make_tree()
    out = descendants(_Cat, parent_field="parent_id", root_pk=root.pk)
    pks = sorted(r["pk"] for r in out)
    assert pks == sorted([a.pk, b.pk, a1.pk, a2.pk])


def test_descendants_subtree_subset():
    root, a, b, a1, a2 = _make_tree()
    out = descendants(_Cat, parent_field="parent_id", root_pk=a.pk)
    pks = {r["pk"] for r in out}
    assert pks == {a1.pk, a2.pk}


def test_descendants_leaf_returns_empty():
    root, a, b, a1, a2 = _make_tree()
    out = descendants(_Cat, parent_field="parent_id", root_pk=a1.pk)
    assert out == []


def test_ancestors_cte_returns_cte_object():
    cte = ancestors_cte(_Cat, parent_field="parent_id", leaf_pk=1)
    assert cte.recursive is True
    assert "UNION ALL" in cte.sql


def test_ancestors_walks_up_to_root():
    root, a, b, a1, a2 = _make_tree()
    out = ancestors(_Cat, parent_field="parent_id", leaf_pk=a1.pk)
    pks = {r["pk"] for r in out}
    assert pks == {a.pk, root.pk}


def test_ancestors_root_returns_empty():
    root, *_ = _make_tree()
    out = ancestors(_Cat, parent_field="parent_id", leaf_pk=root.pk)
    assert out == []


def test_descendants_cte_validates_identifiers():
    with pytest.raises(dorm.ImproperlyConfigured):
        descendants_cte(_Cat, parent_field="parent;DROP--", root_pk=1)


def test_descendants_cte_with_cycle_field_pg(db_config):
    if db_config.get("ENGINE") != "postgresql":
        pytest.skip("cycle_field uses ARRAY literal — PG-only")
    cte = descendants_cte(
        _Cat,
        parent_field="parent_id",
        root_pk=1,
        cycle_field="path",
    )
    assert "ARRAY[" in cte.sql
    assert "is_cycle" in cte.sql
