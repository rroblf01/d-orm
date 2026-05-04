"""Coverage for v3.2 ``bulk_create(returning=[…])``.

PG and SQLite ≥ 3.35 accept a ``RETURNING …`` clause on INSERT;
``bulk_create(returning=…)`` uses it to back-fill DB-side defaults
(``DEFAULT now()``, generated columns, …) onto the inserted objects
in a single round-trip — saving a follow-up SELECT.

MySQL has no ``RETURNING`` on INSERT, so the call raises
``NotImplementedError`` there.
"""

from __future__ import annotations

import pytest

import dorm


def _ddl_for(model_cls):
    """Build CREATE TABLE for *model_cls* using the same DDL emitter
    migrations use, so backend-specific differences land naturally."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    table = model_cls._meta.db_table
    conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in model_cls._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        f'CREATE TABLE "{table}" (\n  ' + ",\n  ".join(filter(None, cols)) + "\n)"
    )
    return conn, table, cascade


# ─────────────────────────────────────────────────────────────────────────────
# Backfills DB-side default columns
# ─────────────────────────────────────────────────────────────────────────────


def test_bulk_create_returning_backfills_db_default():
    """A column declared with ``db_default=42`` is set server-side. Without
    ``returning=`` the inserted instance keeps the Python ``NOT_PROVIDED``
    sentinel; with ``returning=['rev']`` the actual DB value lands."""

    class _Item(dorm.Model):
        name = dorm.CharField(max_length=20)
        rev = dorm.IntegerField(db_default=42)

        class Meta:
            db_table = "v3_2_bulk_ret_default"
            app_label = "v3_2_bulk_ret"

    conn, table, cascade = _ddl_for(_Item)
    try:
        items = [_Item(name="a"), _Item(name="b")]
        out = _Item.objects.bulk_create(items, returning=["rev"])
        assert [o.rev for o in out] == [42, 42]
        # Reload to verify the DB really wrote 42 (not just that we copied
        # the object's NOT_PROVIDED-default Python value).
        reloaded = sorted(_Item.objects.all(), key=lambda o: o.pk)
        assert all(o.rev == 42 for o in reloaded)
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')


def test_bulk_create_returning_backfills_pk():
    """Returning='id' is redundant (PKs are already back-filled) but must
    still work — useful when the user mixes PK with other generated cols."""

    class _Stamped(dorm.Model):
        label = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_bulk_ret_pk"
            app_label = "v3_2_bulk_ret"

    conn, table, cascade = _ddl_for(_Stamped)
    try:
        items = [_Stamped(label="x"), _Stamped(label="y"), _Stamped(label="z")]
        out = _Stamped.objects.bulk_create(items, returning=["id"])
        assert all(o.id is not None for o in out)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        assert len({o.id for o in out}) == 3  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')


def test_bulk_create_returning_multiple_columns():
    """Multi-column returning: the column order in the kwarg shouldn't
    matter — each obj gets every column populated."""

    class _Multi(dorm.Model):
        a = dorm.CharField(max_length=10)
        b = dorm.IntegerField(db_default=7)
        c = dorm.IntegerField(db_default=11)

        class Meta:
            db_table = "v3_2_bulk_ret_multi"
            app_label = "v3_2_bulk_ret"

    conn, table, cascade = _ddl_for(_Multi)
    try:
        items = [_Multi(a="x"), _Multi(a="y")]
        out = _Multi.objects.bulk_create(items, returning=["c", "b"])
        assert [o.b for o in out] == [7, 7]
        assert [o.c for o in out] == [11, 11]
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')


# ─────────────────────────────────────────────────────────────────────────────
# Validation: rejects unknown / column-less fields
# ─────────────────────────────────────────────────────────────────────────────


def test_bulk_create_returning_rejects_unknown_field():
    class _R(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_bulk_ret_unknown"
            app_label = "v3_2_bulk_ret"

    with pytest.raises(ValueError, match="unknown field"):
        _R.objects.bulk_create([_R(name="x")], returning=["does_not_exist"])


# ─────────────────────────────────────────────────────────────────────────────
# Validation: incompatible with conflicts
# ─────────────────────────────────────────────────────────────────────────────


def test_bulk_create_returning_rejects_ignore_conflicts():
    class _R(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_bulk_ret_ic"
            app_label = "v3_2_bulk_ret"

    with pytest.raises(ValueError, match="cannot be combined"):
        _R.objects.bulk_create(
            [_R(name="x")], returning=["id"], ignore_conflicts=True
        )


def test_bulk_create_returning_rejects_update_conflicts():
    class _R(dorm.Model):
        name = dorm.CharField(max_length=20, unique=True)

        class Meta:
            db_table = "v3_2_bulk_ret_uc"
            app_label = "v3_2_bulk_ret"

    with pytest.raises(ValueError, match="cannot be combined"):
        _R.objects.bulk_create(
            [_R(name="x")],
            returning=["id"],
            update_conflicts=True,
            unique_fields=["name"],
        )


# ─────────────────────────────────────────────────────────────────────────────
# Empty objs: short-circuit, no DB call
# ─────────────────────────────────────────────────────────────────────────────


def test_bulk_create_returning_empty_objs_no_db_call():
    class _R(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_bulk_ret_empty"
            app_label = "v3_2_bulk_ret"

    out = _R.objects.bulk_create([], returning=["id"])
    assert out == []


# ─────────────────────────────────────────────────────────────────────────────
# Async parity
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_abulk_create_returning_backfills_db_default():
    class _AItem(dorm.Model):
        name = dorm.CharField(max_length=20)
        rev = dorm.IntegerField(db_default=99)

        class Meta:
            db_table = "v3_2_abulk_ret_default"
            app_label = "v3_2_abulk_ret"

    conn, table, cascade = _ddl_for(_AItem)
    try:
        items = [_AItem(name="a"), _AItem(name="b")]
        out = await _AItem.objects.abulk_create(items, returning=["rev"])
        assert [o.rev for o in out] == [99, 99]
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')


@pytest.mark.asyncio
async def test_abulk_create_returning_rejects_conflicts():
    class _R(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_abulk_ret_conflict"
            app_label = "v3_2_abulk_ret"

    with pytest.raises(ValueError, match="cannot be combined"):
        await _R.objects.abulk_create(
            [_R(name="x")], returning=["id"], ignore_conflicts=True
        )


# ─────────────────────────────────────────────────────────────────────────────
# Query layer: as_bulk_insert returning_cols rendering
# ─────────────────────────────────────────────────────────────────────────────


def test_as_bulk_insert_emits_returning_clause():
    """Hit the SQL builder directly so the test doesn't depend on a live
    DB connection — every backend with RETURNING support gets the same
    text."""
    from dorm.query import SQLQuery

    class _M(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_2_q_ret"
            app_label = "v3_2_q_ret"

    class _FakeConn:
        vendor = "sqlite"

    q = SQLQuery(_M)
    name_field = _M._meta.get_field("name")
    sql, params = q.as_bulk_insert(
        [name_field],
        [["alpha"], ["beta"]],
        _FakeConn(),
        returning_cols=["id", "name"],
    )
    assert "RETURNING" in sql
    assert '"id"' in sql.split("RETURNING", 1)[1]
    assert '"name"' in sql.split("RETURNING", 1)[1]
    assert params == ["alpha", "beta"]


def test_as_bulk_insert_returning_clause_after_on_conflict():
    """When both on-conflict and returning would render together, RETURNING
    must come *after* ON CONFLICT — otherwise PG/SQLite reject the SQL."""
    from dorm.query import SQLQuery

    class _M(dorm.Model):
        name = dorm.CharField(max_length=20, unique=True)

        class Meta:
            db_table = "v3_2_q_ret_oc"
            app_label = "v3_2_q_ret_oc"

    class _FakeConn:
        vendor = "sqlite"

    q = SQLQuery(_M)
    name_field = _M._meta.get_field("name")
    # Hand-coded combination: callers can't do this through bulk_create()
    # (the ValueError check blocks it), but the SQL builder still needs
    # to produce well-formed text for any third party that calls it.
    sql, _ = q.as_bulk_insert(
        [name_field],
        [["a"]],
        _FakeConn(),
        ignore_conflicts=True,
        returning_cols=["id"],
    )
    assert sql.index("ON CONFLICT") < sql.index("RETURNING")
