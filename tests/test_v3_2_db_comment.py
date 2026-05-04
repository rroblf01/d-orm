"""Coverage for v3.2 ``Field(db_comment=…)`` + ``Meta.db_table_comment``
DDL emission. Field stored the value in 3.1; 3.2 emits the
matching ``COMMENT ON COLUMN`` (PostgreSQL) and inline
``COMMENT '...'`` (MySQL) so DBA tooling sees the docstring.
SQLite has no comment syntax — verified silent skip.
"""

from __future__ import annotations

import dorm


# ──────────────────────────────────────────────────────────────────────────────
# Field stores db_comment
# ──────────────────────────────────────────────────────────────────────────────


def test_field_stores_db_comment_kwarg():
    f = dorm.IntegerField(db_comment="rev count")
    assert f.db_comment == "rev count"


def test_field_db_comment_default_none():
    f = dorm.IntegerField()
    assert f.db_comment is None


# ──────────────────────────────────────────────────────────────────────────────
# Field.deconstruct surfaces db_comment
# ──────────────────────────────────────────────────────────────────────────────


def test_field_deconstruct_emits_db_comment():
    f = dorm.IntegerField(db_comment="why this column exists")
    name, path, args, kwargs = f.deconstruct()
    assert kwargs.get("db_comment") == "why this column exists"


def test_field_deconstruct_skips_db_comment_when_default():
    f = dorm.IntegerField()
    _, _, _, kwargs = f.deconstruct()
    assert "db_comment" not in kwargs


# ──────────────────────────────────────────────────────────────────────────────
# DDL: MySQL inline COMMENT
# ──────────────────────────────────────────────────────────────────────────────


def test_field_to_column_sql_mysql_emits_inline_comment():
    """MySQL puts the comment directly on the column DDL."""
    from dorm.migrations.operations import _field_to_column_sql

    class _MySQLConn:
        vendor = "mysql"

    f = dorm.IntegerField(db_comment="row revision counter")
    f.column = "rev"
    f.name = "rev"
    sql = _field_to_column_sql("rev", f, _MySQLConn())
    assert "COMMENT 'row revision counter'" in sql


def test_field_to_column_sql_mysql_escapes_single_quote():
    from dorm.migrations.operations import _field_to_column_sql

    class _MySQLConn:
        vendor = "mysql"

    f = dorm.CharField(max_length=10, db_comment="user's note")
    f.column = "note"
    f.name = "note"
    sql = _field_to_column_sql("note", f, _MySQLConn())
    assert "COMMENT 'user''s note'" in sql


def test_field_to_column_sql_postgres_does_not_inline_comment():
    """PG uses ``COMMENT ON COLUMN`` (separate stmt, emitted by
    ``CreateModel``). The inline form is MySQL-only."""
    from dorm.migrations.operations import _field_to_column_sql

    class _PGConn:
        vendor = "postgresql"

    f = dorm.IntegerField(db_comment="should not inline")
    f.column = "x"
    f.name = "x"
    sql = _field_to_column_sql("x", f, _PGConn())
    assert "COMMENT" not in sql


def test_field_to_column_sql_sqlite_ignores_comment():
    from dorm.migrations.operations import _field_to_column_sql

    class _SQLiteConn:
        vendor = "sqlite"

    f = dorm.IntegerField(db_comment="ignored on sqlite")
    f.column = "x"
    f.name = "x"
    sql = _field_to_column_sql("x", f, _SQLiteConn())
    assert "COMMENT" not in sql


# ──────────────────────────────────────────────────────────────────────────────
# CreateModel emits PG COMMENT ON COLUMN / COMMENT ON TABLE
# ──────────────────────────────────────────────────────────────────────────────


def test_create_model_emits_pg_column_comment():
    """Capture statements via a fake connection and assert the
    ``COMMENT ON COLUMN`` form lands."""
    from dorm.migrations.operations import CreateModel
    from dorm.migrations.state import ProjectState

    captured: list[str] = []

    class _FakePGConn:
        vendor = "postgresql"

        def execute_script(self, sql, params=None):
            captured.append(sql)

    op = CreateModel(
        name="Item",
        fields=[
            ("id", dorm.BigAutoField(primary_key=True)),
            ("rev", dorm.IntegerField(db_comment="row revision counter")),
        ],
        options={"db_table": "v3_2_items"},
    )
    op.database_forwards("v3_2", _FakePGConn(), ProjectState(), ProjectState())
    joined = "\n".join(captured)
    assert 'COMMENT ON COLUMN "v3_2_items"."rev" IS \'row revision counter\'' in joined


def test_create_model_emits_pg_table_comment():
    from dorm.migrations.operations import CreateModel
    from dorm.migrations.state import ProjectState

    captured: list[str] = []

    class _FakePGConn:
        vendor = "postgresql"

        def execute_script(self, sql, params=None):
            captured.append(sql)

    op = CreateModel(
        name="Stamped",
        fields=[("id", dorm.BigAutoField(primary_key=True))],
        options={
            "db_table": "v3_2_stamped",
            "db_table_comment": "Audit ledger — append-only.",
        },
    )
    op.database_forwards("v3_2", _FakePGConn(), ProjectState(), ProjectState())
    joined = "\n".join(captured)
    assert 'COMMENT ON TABLE "v3_2_stamped" IS \'Audit ledger — append-only.\'' in joined


def test_create_model_skips_pg_comment_emit_for_other_vendors():
    """SQLite + MySQL must NOT see the ``COMMENT ON …`` separate
    statement. MySQL relies on the inline form via
    ``_field_to_column_sql``; SQLite has no comments at all."""
    from dorm.migrations.operations import CreateModel
    from dorm.migrations.state import ProjectState

    for vendor in ("sqlite", "mysql"):
        captured: list[str] = []

        class _FakeConn:
            pass

        conn = _FakeConn()
            # ``vendor`` set per loop so the closure binds correctly.
        conn.vendor = vendor  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        conn.execute_script = lambda s, params=None, _cap=captured: _cap.append(s)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

        op = CreateModel(
            name="Skip",
            fields=[
                ("id", dorm.BigAutoField(primary_key=True)),
                ("rev", dorm.IntegerField(db_comment="ignored here")),
            ],
            options={
                "db_table": f"v3_2_skip_{vendor}",
                "db_table_comment": "should not emit",
            },
        )
        op.database_forwards("v3_2", conn, ProjectState(), ProjectState())
        joined = "\n".join(captured)
        assert "COMMENT ON COLUMN" not in joined
        assert "COMMENT ON TABLE" not in joined


# ──────────────────────────────────────────────────────────────────────────────
# Meta.db_table_comment lands in Options
# ──────────────────────────────────────────────────────────────────────────────


def test_meta_db_table_comment_propagates_to_meta():
    class V32WithComment(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            db_table = "v3_2_with_comment"
            db_table_comment = "Tracked by the audit subsystem."
            app_label = "v3_2_meta"

    assert V32WithComment._meta.db_table_comment == "Tracked by the audit subsystem."


def test_meta_db_table_comment_default_empty_string():
    class V32NoComment(dorm.Model):
        name = dorm.CharField(max_length=10)

        class Meta:
            db_table = "v3_2_no_comment"
            app_label = "v3_2_meta"

    assert V32NoComment._meta.db_table_comment == ""
