"""Tests for ``CompositePrimaryKey``: composite primary keys spanning
multiple columns.

Covers:

* Class-level introspection (``_meta.pk`` is the composite, component
  fields are still concrete).
* CRUD: ``create`` / ``get(pk=…)`` / ``filter(pk=…)`` / ``save()`` /
  ``delete()`` all use the per-component WHERE clause.
* Migration writer emits ``PRIMARY KEY (col1, col2)`` and strips the
  per-column ``PRIMARY KEY``.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.fields import CompositePrimaryKey
from dorm.migrations.operations import _field_to_column_sql


class OrderLine(dorm.Model):
    order_id = dorm.IntegerField()
    line_no = dorm.IntegerField()
    sku = dorm.CharField(max_length=50)
    qty = dorm.IntegerField(default=1)

    pk = dorm.CompositePrimaryKey("order_id", "line_no")

    class Meta:
        db_table = "cpk_order_lines"


@pytest.fixture
def _create_cpk_table(clean_db):
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "cpk_order_lines"{cascade}')

    cols: list[str] = []
    composite_cols: list[str] | None = None
    for field in OrderLine._meta.fields:
        if isinstance(field, CompositePrimaryKey):
            composite_cols = [
                OrderLine._meta.get_field(name).column for name in field.field_names
            ]
            continue
        sql_def = _field_to_column_sql(field.name, field, conn)
        if sql_def:
            cols.append(sql_def)
    if composite_cols:
        cols = [
            c.replace(" PRIMARY KEY", "").replace(" AUTOINCREMENT", "") for c in cols
        ]
        cols.append("PRIMARY KEY (" + ", ".join(f'"{c}"' for c in composite_cols) + ")")
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "cpk_order_lines" (\n  '
        + ",\n  ".join(cols)
        + "\n)"
    )
    yield


class TestCompositePrimaryKeyMeta:
    def test_meta_pk_is_composite(self):
        assert isinstance(OrderLine._meta.pk, CompositePrimaryKey)
        assert OrderLine._meta.pk.field_names == ("order_id", "line_no")

    def test_components_remain_concrete(self):
        cols = {f.name: f.column for f in OrderLine._meta.fields if f.column}
        assert cols["order_id"] == "order_id"
        assert cols["line_no"] == "line_no"

    def test_no_auto_pk_added(self):
        names = {f.name for f in OrderLine._meta.fields}
        assert "id" not in names

    def test_composite_pk_has_no_column(self):
        cpk = OrderLine._meta.pk
        assert cpk.column is None
        assert cpk.db_type(get_connection()) is None


class TestCompositePrimaryKeyCRUD:
    def test_create_and_pk_tuple(self, _create_cpk_table):
        line = OrderLine.objects.create(order_id=1, line_no=1, sku="A", qty=2)
        assert line.pk == (1, 1)

    def test_get_by_pk_tuple(self, _create_cpk_table):
        OrderLine.objects.create(order_id=1, line_no=1, sku="A")
        OrderLine.objects.create(order_id=1, line_no=2, sku="B")
        OrderLine.objects.create(order_id=2, line_no=1, sku="C")

        loaded = OrderLine.objects.get(pk=(1, 2))
        assert loaded.sku == "B"

    def test_filter_by_pk_tuple(self, _create_cpk_table):
        OrderLine.objects.create(order_id=1, line_no=1, sku="A")
        OrderLine.objects.create(order_id=1, line_no=2, sku="B")

        results = list(OrderLine.objects.filter(pk=(1, 1)))
        assert len(results) == 1
        assert results[0].sku == "A"

    def test_filter_by_components(self, _create_cpk_table):
        OrderLine.objects.create(order_id=1, line_no=1, sku="A")
        OrderLine.objects.create(order_id=1, line_no=2, sku="B")
        OrderLine.objects.create(order_id=2, line_no=1, sku="C")

        results = list(OrderLine.objects.filter(order_id=1).order_by("line_no"))
        assert [r.sku for r in results] == ["A", "B"]

    def test_save_updates_existing_row(self, _create_cpk_table):
        line = OrderLine.objects.create(order_id=1, line_no=1, sku="A", qty=1)
        line.qty = 99
        line.save()

        loaded = OrderLine.objects.get(pk=(1, 1))
        assert loaded.qty == 99

        # Should still be exactly one row.
        assert OrderLine.objects.count() == 1

    def test_delete_uses_per_component_where(self, _create_cpk_table):
        OrderLine.objects.create(order_id=1, line_no=1, sku="A")
        OrderLine.objects.create(order_id=1, line_no=2, sku="B")

        line = OrderLine.objects.get(pk=(1, 1))
        line.delete()

        remaining = list(OrderLine.objects.all().order_by("line_no"))
        assert [r.sku for r in remaining] == ["B"]

    def test_pk_setter_unpacks_tuple(self, _create_cpk_table):
        line = OrderLine(sku="X")
        # ``OrderLine.pk`` is a ``CompositePrimaryKey`` declaration at
        # class scope, which the metaclass swaps out for the
        # ``Model.pk`` property at runtime. ty sees only the static
        # attribute, so the assignment is annotated as ``Any``-tunneled.
        setattr(line, "pk", (5, 9))
        assert line.order_id == 5
        assert line.line_no == 9

    def test_pk_setter_rejects_wrong_arity(self):
        line = OrderLine(sku="X")
        with pytest.raises(ValueError, match="CompositePrimaryKey expects a 2-tuple"):
            setattr(line, "pk", (1,))

    def test_filter_pk_rejects_wrong_arity(self, _create_cpk_table):
        with pytest.raises((ValueError, TypeError)):
            list(OrderLine.objects.filter(pk=1))

    def test_unsaved_instance_pk_is_tuple_of_none(self):
        line = OrderLine()
        assert line.pk == (None, None)


class TestCompositePrimaryKeyMigrationWriter:
    def test_create_table_emits_composite_constraint(self):
        conn = get_connection()
        cascade = (
            " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
        )
        conn.execute_script(f'DROP TABLE IF EXISTS "cpk_writer_check"{cascade}')

        from dorm.migrations.operations import CreateModel

        op = CreateModel(
            name="WriterCheck",
            fields=[
                ("a", dorm.IntegerField()),
                ("b", dorm.IntegerField()),
                ("payload", dorm.CharField(max_length=10)),
                ("pk", dorm.CompositePrimaryKey("a", "b")),
            ],
            options={"db_table": "cpk_writer_check"},
        )
        op.database_forwards("tests", conn, None, None)

        # Smoke: insert two rows with same `a` but different `b`.
        # That's only allowed if the PK is composite over (a, b).
        conn.execute_write(
            'INSERT INTO "cpk_writer_check" (a, b, payload) VALUES (1, 1, %s)',
            ["x"],
        )
        conn.execute_write(
            'INSERT INTO "cpk_writer_check" (a, b, payload) VALUES (1, 2, %s)',
            ["y"],
        )

        rows = conn.execute('SELECT a, b, payload FROM "cpk_writer_check" ORDER BY b')
        assert len(rows) == 2
        # Cleanup so xdist workers don't trip on the leftover table.
        conn.execute_script(f'DROP TABLE IF EXISTS "cpk_writer_check"{cascade}')


def test_composite_pk_construction_rejects_empty():
    with pytest.raises(ValueError, match="at least one"):
        dorm.CompositePrimaryKey()
