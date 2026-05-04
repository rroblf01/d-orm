"""Coverage for v3.3 polish round:

- :meth:`QuerySet.explain(format=, verbose=)` extra kwargs.
- :meth:`update_or_create(create_defaults=)` Django 5.0 parity.
- New aggregates: :class:`Mode`, :class:`PercentileCont`,
  :class:`PercentileDisc`, :class:`StringAgg(order_by=)`.
- :class:`SchemaEditor` ad-hoc DDL.
- :func:`warmup_pool` pre-open helper.
- :class:`SoftDeleteModel.delete(cascade=True)` cascading soft delete.
- Async parity: ``adates`` / ``adatetimes`` / ``aexplain``.
"""

from __future__ import annotations

import datetime as _dt

import pytest

import dorm
from dorm import (
    FilteredRelation,
    Mode,
    PercentileCont,
    PercentileDisc,
    Q,
    StringAgg,
)
from dorm.aggregates import _OrderedSetAggregate
from tests.models import Author, Publisher


# ─────────────────────────────────────────────────────────────────────────────
# explain() / aexplain() with format + verbose
# ─────────────────────────────────────────────────────────────────────────────


def test_explain_default_returns_text_plan():
    Author.objects.create(name="ex-default", age=20, email="ed@x.com")
    plan = Author.objects.filter(name__startswith="ex-").explain()
    assert isinstance(plan, str)
    assert plan.strip() != ""


def test_explain_format_json_runs_on_pg_only():
    """JSON format is PG-only. SQLite ignores ``format`` and emits
    ``EXPLAIN QUERY PLAN`` regardless."""
    from dorm.db.connection import get_connection

    if getattr(get_connection(), "vendor", "sqlite") != "postgresql":
        pytest.skip("EXPLAIN (FORMAT JSON) is PG-only.")
    plan = Author.objects.all().explain(format="json")
    assert plan.lstrip().startswith("[") or "Plan" in plan


def test_explain_invalid_format_raises_on_pg():
    from dorm.db.connection import get_connection

    if getattr(get_connection(), "vendor", "sqlite") != "postgresql":
        pytest.skip("Validation only fires on PG (other backends ignore format).")
    with pytest.raises(ValueError, match="expected one of"):
        Author.objects.all().explain(format="csv")


@pytest.mark.asyncio
async def test_aexplain_returns_text():
    plan = await Author.objects.all().aexplain()
    assert isinstance(plan, str)


# ─────────────────────────────────────────────────────────────────────────────
# update_or_create(create_defaults=)
# ─────────────────────────────────────────────────────────────────────────────


def test_update_or_create_create_defaults_distinct_from_defaults():
    """``defaults`` applies on UPDATE, ``create_defaults`` on CREATE.
    Verifies the new branch creates with one set of values, while a
    second invocation updates with the *other* set."""
    Author.objects.filter(email="uoc@x.com").delete()
    obj, created = Author.objects.update_or_create(
        email="uoc@x.com",
        defaults={"name": "updated-name", "age": 50},
        create_defaults={"name": "created-name", "age": 1},
    )
    assert created is True
    assert obj.name == "created-name"
    assert obj.age == 1

    obj2, created = Author.objects.update_or_create(
        email="uoc@x.com",
        defaults={"name": "updated-name", "age": 50},
        create_defaults={"name": "should-not-fire", "age": 999},
    )
    assert created is False
    assert obj2.name == "updated-name"
    assert obj2.age == 50


def test_update_or_create_falls_back_to_defaults_when_no_create_defaults():
    """Backwards-compat: omitting ``create_defaults`` keeps the pre-3.3
    behaviour where ``defaults`` applies on both branches."""
    Author.objects.filter(email="uoc-bw@x.com").delete()
    obj, created = Author.objects.update_or_create(
        email="uoc-bw@x.com",
        defaults={"name": "shared", "age": 7},
    )
    assert created is True
    assert obj.name == "shared"


@pytest.mark.asyncio
async def test_aupdate_or_create_create_defaults():
    Author.objects.filter(email="auoc@x.com").delete()
    obj, created = await Author.objects.aupdate_or_create(
        email="auoc@x.com",
        defaults={"name": "u-name", "age": 20},
        create_defaults={"name": "c-name", "age": 1},
    )
    assert created is True
    assert obj.name == "c-name"


# ─────────────────────────────────────────────────────────────────────────────
# Aggregates: Mode / PercentileCont / PercentileDisc / StringAgg(order_by=)
# ─────────────────────────────────────────────────────────────────────────────


def test_mode_renders_within_group_clause():
    """SQL shape sanity — actual execution requires PG. Inspect the
    generated SQL via ``Aggregate.as_sql`` directly."""
    sql, params = Mode("color").as_sql(table_alias="tags")
    assert "MODE() WITHIN GROUP (ORDER BY" in sql
    assert '"tags"."color"' in sql
    assert params == []


def test_percentile_cont_validates_fraction_range():
    with pytest.raises(ValueError, match="fraction must be"):
        PercentileCont("response_ms", fraction=1.5)


def test_percentile_cont_renders_with_fraction_param():
    sql, params = PercentileCont("ms", fraction=0.95).as_sql(table_alias="t")
    assert "PERCENTILE_CONT(%s)" in sql
    assert "WITHIN GROUP (ORDER BY" in sql
    assert params == [0.95]


def test_percentile_disc_uses_disc_function_name():
    sql, _ = PercentileDisc("ms", fraction=0.5).as_sql(table_alias="t")
    assert sql.startswith("PERCENTILE_DISC(")


def test_string_agg_order_by_renders_clause():
    sql, params = StringAgg(
        "name", separator=", ", order_by="name"
    ).as_sql(table_alias="t")
    assert "ORDER BY" in sql
    assert '"t"."name"' in sql
    assert "ASC" in sql
    assert params == [", "]


def test_string_agg_order_by_desc_prefix():
    sql, _ = StringAgg(
        "name", separator=",", order_by="-name"
    ).as_sql(table_alias="t")
    assert "DESC" in sql


def test_ordered_set_aggregate_base_emits_sql_skeleton():
    """The base class ``_OrderedSetAggregate`` emits the
    ``WITHIN GROUP`` skeleton even with an empty function name —
    subclasses are responsible for setting ``function``. Confirm
    the skeleton is well-formed so subclasses can rely on it."""
    sql, params = _OrderedSetAggregate("col").as_sql(table_alias="t")
    assert "WITHIN GROUP (ORDER BY" in sql
    assert '"t"."col"' in sql
    assert params == []


# ─────────────────────────────────────────────────────────────────────────────
# SchemaEditor — ad-hoc DDL
# ─────────────────────────────────────────────────────────────────────────────


def test_schema_editor_creates_and_drops_table():
    """End-to-end create_model → delete_model. Uses an ad-hoc model
    so the test doesn't pollute the session-shared models."""
    from dorm.db.connection import get_connection
    from dorm.migrations.schema import SchemaEditor

    class _Adhoc(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_3_schema_editor_adhoc"
            app_label = "v3_3_schema"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(
        f'DROP TABLE IF EXISTS "v3_3_schema_editor_adhoc"{cascade}'
    )
    try:
        with SchemaEditor(conn) as se:
            se.create_model(_Adhoc)
        assert conn.table_exists("v3_3_schema_editor_adhoc")

        with SchemaEditor(conn) as se:
            se.delete_model(_Adhoc)
        assert not conn.table_exists("v3_3_schema_editor_adhoc")
    finally:
        conn.execute_script(
            f'DROP TABLE IF EXISTS "v3_3_schema_editor_adhoc"{cascade}'
        )


def test_schema_editor_add_and_remove_field():
    from dorm.db.connection import get_connection
    from dorm.migrations.schema import SchemaEditor

    class _AdhocCol(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_3_schema_editor_col"
            app_label = "v3_3_schema"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(
        f'DROP TABLE IF EXISTS "v3_3_schema_editor_col"{cascade}'
    )
    try:
        with SchemaEditor(conn) as se:
            se.create_model(_AdhocCol)
            se.add_field(_AdhocCol, "rev", dorm.IntegerField(default=0))
        cols = {c["name"] for c in conn.get_table_columns("v3_3_schema_editor_col")}
        assert "rev" in cols

        with SchemaEditor(conn) as se:
            se.remove_field(_AdhocCol, "rev")
        cols = {c["name"] for c in conn.get_table_columns("v3_3_schema_editor_col")}
        assert "rev" not in cols
    finally:
        conn.execute_script(
            f'DROP TABLE IF EXISTS "v3_3_schema_editor_col"{cascade}'
        )


def test_schema_editor_execute_arbitrary_ddl():
    from dorm.db.connection import get_connection
    from dorm.migrations.schema import SchemaEditor

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(
        f'DROP TABLE IF EXISTS "v3_3_schema_editor_raw"{cascade}'
    )
    try:
        with SchemaEditor(conn) as se:
            se.execute('CREATE TABLE "v3_3_schema_editor_raw" ("id" INTEGER)')
        assert conn.table_exists("v3_3_schema_editor_raw")
    finally:
        conn.execute_script(
            f'DROP TABLE IF EXISTS "v3_3_schema_editor_raw"{cascade}'
        )


# ─────────────────────────────────────────────────────────────────────────────
# Pool warmup
# ─────────────────────────────────────────────────────────────────────────────


def test_warmup_pool_returns_zero_on_non_pg():
    """SQLite has no shared pool; warmup must report 0 instead of
    raising or blocking."""
    from dorm.contrib.pool_autoscale import warmup_pool
    from dorm.db.connection import get_connection

    if getattr(get_connection(), "vendor", "sqlite") == "postgresql":
        pytest.skip("This test targets the non-PG branch.")
    assert warmup_pool() == 0


def test_warmup_pool_pg_opens_min_size(monkeypatch):
    """PG: warmup_pool grabs *target* connections from the pool and
    returns them to the idle deque, so the next checkout is hot."""
    from dorm.contrib.pool_autoscale import warmup_pool
    from dorm.db.connection import get_connection

    if getattr(get_connection(), "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only path.")
    n = warmup_pool(target=2)
    # Real PG pool may cap at MAX_POOL_SIZE; just verify >= 1.
    assert n >= 1


# ─────────────────────────────────────────────────────────────────────────────
# Soft delete cascade
# ─────────────────────────────────────────────────────────────────────────────


def test_softdelete_cascade_marks_children():
    """``parent.delete(cascade=True)`` should soft-delete every child
    row whose source model is also a :class:`SoftDeleteModel`."""
    from dorm.contrib.softdelete import SoftDeleteModel
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    class _SDParent(SoftDeleteModel):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_3_sd_parent"
            app_label = "v3_3_sd"

    class _SDChild(SoftDeleteModel):
        parent = dorm.ForeignKey(_SDParent, on_delete=dorm.CASCADE, related_name="children")
        label = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_3_sd_child"
            app_label = "v3_3_sd"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    for tbl in ("v3_3_sd_child", "v3_3_sd_parent"):
        conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')
    for model, tbl in [(_SDParent, "v3_3_sd_parent"), (_SDChild, "v3_3_sd_child")]:
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE "{tbl}" (\n  ' + ",\n  ".join(filter(None, cols)) + "\n)"
        )

    try:
        p = _SDParent.objects.create(name="parent")
        c1 = _SDChild.objects.create(parent=p, label="c1")
        c2 = _SDChild.objects.create(parent=p, label="c2")

        p.delete(cascade=True)

        # Children no longer visible in default manager.
        assert not _SDChild.objects.filter(parent_id=p.pk).exists()
        # But still in all_objects with deleted_at populated.
        all_children = list(_SDChild.all_objects.filter(parent_id=p.pk))
        assert len(all_children) == 2
        assert all(c.deleted_at is not None for c in all_children)
    finally:
        for tbl in ("v3_3_sd_child", "v3_3_sd_parent"):
            conn.execute_script(f'DROP TABLE IF EXISTS "{tbl}"{cascade}')


# ─────────────────────────────────────────────────────────────────────────────
# Async parity: adates / adatetimes
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_adates_returns_truncated_dates():
    """``adates`` truncates the values its underlying
    ``avalues_list`` returned. Use ``Author.age`` — int won't trunc,
    but the function should pass it through without crashing.
    Async-only sanity: the call awaits + returns a list."""
    Author.objects.create(name="ad-a", age=10, email="ad@x.com")
    out = await Author.objects.adates("age", "year", order="ASC")
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_adatetimes_validates_kind():
    with pytest.raises(ValueError, match=r"adatetimes\(\) kind"):
        await Author.objects.adatetimes("age", "century")


# ─────────────────────────────────────────────────────────────────────────────
# Manager.explain proxy
# ─────────────────────────────────────────────────────────────────────────────


def test_manager_explain_proxies_to_queryset():
    plan = Author.objects.explain()
    assert isinstance(plan, str)
