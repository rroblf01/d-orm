"""Tests for individual migration Operation classes — describe(), repr(),
state_forwards / state_backwards parity, and database_backwards round-trip
where it can run on the live test connection.

These complement the higher-level test_migrations.py / test_tier4_squash.py
suites by hitting each operation directly so coverage tracks every code
path."""

from __future__ import annotations


import dorm
from dorm.indexes import Index
from dorm.migrations.operations import (
    AddField,
    AddIndex,
    AlterField,
    CreateModel,
    DeleteModel,
    RemoveField,
    RemoveIndex,
    RenameField,
    RenameModel,
    RunPython,
    RunSQL,
    _field_to_column_sql,
)
from dorm.migrations.state import ProjectState


# ── describe() / __repr__ ─────────────────────────────────────────────────────


def test_describe_text_for_each_operation():
    op_create = CreateModel("Author", [("name", dorm.CharField(max_length=10))])
    assert "Author" in op_create.describe()
    assert "Author" in repr(op_create)

    assert "Author" in DeleteModel("Author").describe()
    assert "Author" in repr(DeleteModel("Author"))

    add = AddField("Author", "age", dorm.IntegerField(null=True))
    assert "age" in add.describe() and "Author" in add.describe()
    assert "age" in repr(add)

    rem = RemoveField("Author", "age")
    assert "age" in rem.describe()
    assert "age" in repr(rem)

    alt = AlterField("Author", "age", dorm.IntegerField())
    assert "age" in alt.describe()
    assert "age" in repr(alt)

    ren_field = RenameField("Author", "age", "years")
    assert "years" in ren_field.describe()
    assert "age" in repr(ren_field)

    ren_model = RenameModel("Author", "Writer")
    assert "Writer" in ren_model.describe()
    assert "Author" in repr(ren_model)

    idx = Index(fields=["name"], name="ix_author_name")
    add_ix = AddIndex("Author", idx)
    assert "Author" in add_ix.describe()
    assert "ix_author_name" in repr(idx)

    rem_ix = RemoveIndex("Author", idx)
    assert "Author" in rem_ix.describe()
    assert "Author" in repr(rem_ix)

    rs = RunSQL("CREATE TABLE x (id INT)")
    assert "Run SQL" in rs.describe()
    assert "CREATE TABLE" in repr(rs)

    rp = RunPython(lambda app, reg: None)
    assert "Run Python" in rp.describe()
    assert "RunPython" in repr(rp)


# ── state_forwards updates the in-memory ProjectState ─────────────────────────


def test_create_model_state_forwards():
    state = ProjectState()
    op = CreateModel("Book", [("title", dorm.CharField(max_length=80))])
    op.state_forwards("blog", state)
    key = "blog.book"
    assert key in state.models
    assert state.models[key]["name"] == "Book"
    assert "title" in state.models[key]["fields"]


def test_delete_model_state_forwards_removes_entry():
    state = ProjectState()
    state.models["blog.book"] = {"name": "Book", "fields": {}, "options": {}}
    DeleteModel("Book").state_forwards("blog", state)
    assert "blog.book" not in state.models


def test_add_field_state_forwards_adds_field():
    state = ProjectState()
    state.models["blog.book"] = {"name": "Book", "fields": {}, "options": {}}
    AddField("Book", "pages", dorm.IntegerField()).state_forwards("blog", state)
    assert "pages" in state.models["blog.book"]["fields"]


def test_remove_field_state_forwards_removes_field():
    state = ProjectState()
    state.models["blog.book"] = {
        "name": "Book", "fields": {"pages": dorm.IntegerField()}, "options": {},
    }
    RemoveField("Book", "pages").state_forwards("blog", state)
    assert "pages" not in state.models["blog.book"]["fields"]


def test_alter_field_state_forwards_replaces_field():
    state = ProjectState()
    old = dorm.IntegerField()
    new = dorm.BigIntegerField()
    state.models["blog.book"] = {"name": "Book", "fields": {"x": old}, "options": {}}
    AlterField("Book", "x", new).state_forwards("blog", state)
    assert state.models["blog.book"]["fields"]["x"] is new


def test_rename_field_state_forwards():
    state = ProjectState()
    field = dorm.CharField(max_length=10)
    state.models["blog.book"] = {"name": "Book", "fields": {"title": field}, "options": {}}
    RenameField("Book", "title", "name").state_forwards("blog", state)
    assert "title" not in state.models["blog.book"]["fields"]
    assert state.models["blog.book"]["fields"]["name"] is field


def test_rename_model_state_forwards():
    state = ProjectState()
    state.models["blog.book"] = {"name": "Book", "fields": {}, "options": {}}
    RenameModel("Book", "Tome").state_forwards("blog", state)
    assert "blog.book" not in state.models
    assert state.models["blog.tome"]["name"] == "Tome"


def test_add_index_state_forwards():
    state = ProjectState()
    state.models["blog.book"] = {"name": "Book", "fields": {}, "options": {}}
    idx = Index(fields=["title"], name="ix_book_title")
    AddIndex("Book", idx).state_forwards("blog", state)
    assert idx in state.models["blog.book"]["options"]["indexes"]


def test_remove_index_state_forwards():
    state = ProjectState()
    idx = Index(fields=["title"], name="ix_book_title")
    state.models["blog.book"] = {
        "name": "Book", "fields": {}, "options": {"indexes": [idx]},
    }
    RemoveIndex("Book", idx).state_forwards("blog", state)
    assert idx not in state.models["blog.book"]["options"]["indexes"]


def test_run_sql_and_run_python_state_forwards_are_no_op():
    """RunSQL / RunPython only do DB work; state must not change."""
    state = ProjectState()
    state.models["blog.book"] = {"name": "Book", "fields": {}, "options": {}}
    snapshot = dict(state.models)
    RunSQL("SELECT 1").state_forwards("blog", state)
    RunPython(lambda app, reg: None).state_forwards("blog", state)
    assert state.models == snapshot


# ── _field_to_column_sql ──────────────────────────────────────────────────────


class _Conn:
    def __init__(self, vendor: str = "sqlite"):
        self.vendor = vendor


def test_field_to_column_sql_string_field():
    sql = _field_to_column_sql("name", dorm.CharField(max_length=20), _Conn("sqlite"))
    assert '"name"' in sql
    assert "VARCHAR(20)" in sql


def test_field_to_column_sql_includes_not_null_when_not_nullable():
    sql = _field_to_column_sql("age", dorm.IntegerField(), _Conn("sqlite"))
    assert "NOT NULL" in sql


def test_field_to_column_sql_skips_not_null_when_null_true():
    sql = _field_to_column_sql("age", dorm.IntegerField(null=True), _Conn("sqlite"))
    assert "NOT NULL" not in sql


def test_field_to_column_sql_unique():
    sql = _field_to_column_sql("email", dorm.EmailField(unique=True), _Conn("sqlite"))
    assert "UNIQUE" in sql


def test_field_to_column_sql_default_string():
    sql = _field_to_column_sql(
        "name", dorm.CharField(max_length=10, default="anon"), _Conn("sqlite")
    )
    assert "DEFAULT 'anon'" in sql


def test_field_to_column_sql_default_int():
    sql = _field_to_column_sql("count", dorm.IntegerField(default=0), _Conn("sqlite"))
    assert "DEFAULT" in sql and "0" in sql


def test_field_to_column_sql_default_bool_per_vendor():
    pg_sql = _field_to_column_sql(
        "active", dorm.BooleanField(default=True), _Conn("postgresql")
    )
    sqlite_sql = _field_to_column_sql(
        "active", dorm.BooleanField(default=True), _Conn("sqlite")
    )
    assert "DEFAULT TRUE" in pg_sql
    assert "DEFAULT 1" in sqlite_sql


def test_field_to_column_sql_callable_default_emits_no_default():
    """A callable default is resolved at insert time, not embedded in DDL."""
    sql = _field_to_column_sql(
        "ts", dorm.IntegerField(default=lambda: 0), _Conn("sqlite")
    )
    assert "DEFAULT" not in sql


def test_field_to_column_sql_fk_uses_id_suffix():
    """An FK reconstructed from a migration file has column=None — the
    helper has to derive `<name>_id` from the field name. This is the
    exact path that broke dbcheck before."""
    # Use one of the test app's already-registered models so the FK resolves
    fk = dorm.ForeignKey("Author", on_delete=dorm.CASCADE, null=True)
    sql = _field_to_column_sql("author", fk, _Conn("sqlite"))
    assert '"author_id"' in sql


def test_field_to_column_sql_db_column_override():
    """Explicit db_column wins over the auto-derived name."""
    f = dorm.CharField(max_length=10, db_column="legacy_name")
    sql = _field_to_column_sql("name", f, _Conn("sqlite"))
    assert '"legacy_name"' in sql


# ── RunSQL forward + backward against the live test connection ───────────────


def test_run_sql_forward_and_reverse_round_trip():
    """RunSQL.database_forwards executes the script; database_backwards runs
    the reverse_sql when present."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    table = "test_runsql_op_table"
    conn.execute_script(f'DROP TABLE IF EXISTS "{table}"')
    op = RunSQL(
        sql=f'CREATE TABLE "{table}" (id INTEGER)',
        reverse_sql=f'DROP TABLE IF EXISTS "{table}"',
    )

    op.database_forwards("any_app", conn, ProjectState(), ProjectState())
    assert conn.table_exists(table)

    op.database_backwards("any_app", conn, ProjectState(), ProjectState())
    assert not conn.table_exists(table)


def test_run_sql_backwards_no_reverse_is_noop():
    """No reverse_sql ⇒ backwards is a no-op (no exception, no change)."""
    from dorm.db.connection import get_connection
    op = RunSQL("SELECT 1")
    # Just must not raise
    op.database_backwards("any_app", get_connection(), ProjectState(), ProjectState())


# ── RunPython forward + backward ─────────────────────────────────────────────


def test_run_python_runs_the_callable():
    seen = []

    def go(app_label, registry):
        seen.append((app_label, type(registry).__name__))

    from dorm.db.connection import get_connection
    op = RunPython(go)
    op.database_forwards("blog", get_connection(), ProjectState(), ProjectState())
    assert seen and seen[0][0] == "blog"


def test_run_python_reverse_runs_when_provided():
    forward, reverse = [], []

    def go(app_label, registry):
        forward.append(app_label)

    def undo(app_label, registry):
        reverse.append(app_label)

    from dorm.db.connection import get_connection
    op = RunPython(go, reverse_code=undo)
    conn = get_connection()
    op.database_forwards("blog", conn, ProjectState(), ProjectState())
    op.database_backwards("blog", conn, ProjectState(), ProjectState())
    assert forward == ["blog"] and reverse == ["blog"]


def test_run_python_reverse_no_op_when_missing():
    """No reverse_code ⇒ database_backwards must be a silent no-op."""
    from dorm.db.connection import get_connection
    op = RunPython(lambda *a, **k: None)
    op.database_backwards("blog", get_connection(), ProjectState(), ProjectState())


# ── CreateModel + DeleteModel round-trip on the live test connection ─────────


def test_create_model_forward_creates_table_and_backward_drops_it():
    from dorm.db.connection import get_connection

    conn = get_connection()
    op = CreateModel(
        "TempThing",
        [
            ("id", dorm.IntegerField(primary_key=True)),
            ("name", dorm.CharField(max_length=30)),
        ],
    )
    state = ProjectState()
    op.state_forwards("temp", state)
    op.database_forwards("temp", conn, ProjectState(), state)
    try:
        assert conn.table_exists("temp_tempthing")
    finally:
        op.database_backwards("temp", conn, state, ProjectState())
    assert not conn.table_exists("temp_tempthing")
