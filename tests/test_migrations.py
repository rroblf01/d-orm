"""Migration system tests."""
import tempfile
from pathlib import Path
from types import SimpleNamespace

import dorm
from dorm.migrations.autodetector import MigrationAutodetector
from dorm.migrations.executor import MigrationExecutor
from dorm.migrations.operations import (
    AddField,
    CreateModel,
    DeleteModel,
    RemoveField,
    RunPython,
    RunSQL,
)
from dorm.migrations.recorder import MigrationRecorder
from dorm.migrations.state import ProjectState
from dorm.migrations.writer import write_migration
from tests.models import Author


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_migration_module(operations: list) -> SimpleNamespace:
    """Return a minimal migration module object with the given operations."""
    return SimpleNamespace(dependencies=[], operations=operations)


def _write_and_apply(conn, mig_dir, app, num, ops, name_suffix="auto"):
    """Write a migration file and apply it; return the migration name."""
    write_migration(app, mig_dir, num, ops)
    executor = MigrationExecutor(conn, verbosity=0)
    executor.migrate(app, mig_dir)
    return f"{str(num).zfill(4)}_{name_suffix}" if num > 1 else "0001_initial"


# ── ProjectState ──────────────────────────────────────────────────────────────

def test_project_state_add_remove():
    state = ProjectState()
    state.add_model("myapp", "Author", {"name": dorm.CharField(max_length=100)})
    assert "myapp.author" in state.models
    state.remove_model("myapp", "Author")
    assert "myapp.author" not in state.models


def test_project_state_clone():
    state = ProjectState()
    state.add_model("myapp", "Author", {"name": dorm.CharField(max_length=100)})
    clone = state.clone()
    clone.remove_model("myapp", "Author")
    assert "myapp.author" in state.models  # original untouched


# ── Autodetector ──────────────────────────────────────────────────────────────

def test_autodetector_create_model():
    from_state = ProjectState()
    to_state = ProjectState()
    to_state.add_model("app", "Post", {"title": dorm.CharField(max_length=200)})

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("app")
    assert "app" in changes
    ops = changes["app"]
    assert any(isinstance(op, CreateModel) for op in ops)


def test_autodetector_delete_model():
    from_state = ProjectState()
    from_state.add_model("app", "Post", {"title": dorm.CharField(max_length=200)})
    to_state = ProjectState()

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("app")
    ops = changes.get("app", [])
    assert any(isinstance(op, DeleteModel) for op in ops)


def test_autodetector_add_field():
    from_state = ProjectState()
    from_state.add_model("app", "Post", {"title": dorm.CharField(max_length=200)})
    to_state = from_state.clone()
    to_state.models["app.post"]["fields"]["body"] = dorm.TextField(null=True)

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("app")
    ops = changes.get("app", [])
    assert any(isinstance(op, AddField) and op.name == "body" for op in ops)


def test_autodetector_remove_field():
    from_state = ProjectState()
    from_state.add_model(
        "app", "Post",
        {"title": dorm.CharField(max_length=200), "body": dorm.TextField(null=True)}
    )
    to_state = from_state.clone()
    del to_state.models["app.post"]["fields"]["body"]

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("app")
    ops = changes.get("app", [])
    assert any(isinstance(op, RemoveField) and op.name == "body" for op in ops)


def test_autodetector_no_changes():
    state = ProjectState()
    state.add_model("app", "Post", {"title": dorm.CharField(max_length=200)})
    detector = MigrationAutodetector(state, state.clone())
    changes = detector.changes("app")
    assert not changes.get("app")


# ── Writer ────────────────────────────────────────────────────────────────────

def test_write_migration_creates_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        fields = [(f.name, f) for f in Author._meta.fields]
        ops = [CreateModel(name="Author", fields=fields, options={"db_table": "authors"})]
        path = write_migration("myapp", mig_dir, 1, ops)

        assert path.exists()
        assert path.name == "0001_initial.py"
        content = path.read_text()
        assert "CreateModel" in content
        assert "Author" in content


def test_write_migration_init_created():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [CreateModel(name="Post", fields=[("title", dorm.CharField(max_length=200))], options={})]
        write_migration("myapp", mig_dir, 1, ops)
        assert (mig_dir / "__init__.py").exists()


# ── Executor & Recorder ───────────────────────────────────────────────────────

def test_recorder_tracks_applied():
    from dorm.db.connection import get_connection
    conn = get_connection()
    recorder = MigrationRecorder(conn)
    recorder.ensure_table()

    recorder.record_applied("myapp", "0001_initial")
    applied = recorder.applied_migrations()
    assert ("myapp", "0001_initial") in applied

    recorder.record_unapplied("myapp", "0001_initial")
    applied = recorder.applied_migrations()
    assert ("myapp", "0001_initial") not in applied


def test_executor_applies_migration():
    from dorm.db.connection import get_connection
    conn = get_connection()

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        fields = [(f.name, f) for f in Author._meta.fields]
        ops = [CreateModel(name="TempModel", fields=fields, options={"db_table": "temp_model_exec"})]
        write_migration("myapp", mig_dir, 1, ops)

        executor = MigrationExecutor(conn, verbosity=0)
        executor.migrate("myapp", mig_dir)

        # Should be recorded
        recorder = MigrationRecorder(conn)
        applied = recorder.applied_migrations()
        assert ("myapp", "0001_initial") in applied

        # Calling again should be a no-op
        executor.migrate("myapp", mig_dir)


# ── Operations ────────────────────────────────────────────────────────────────

def test_run_sql_operation():
    from dorm.db.connection import get_connection
    conn = get_connection()
    state = ProjectState()

    op = RunSQL("SELECT 1")
    op.database_forwards("myapp", conn, state, state)  # should not raise


def test_create_model_operation():
    from dorm.db.connection import get_connection
    conn = get_connection()

    from_state = ProjectState()
    to_state = ProjectState()
    to_state.add_model("myapp", "TmpOpModel", {}, {"db_table": "tmp_op_model"})

    op = CreateModel(
        name="TmpOpModel",
        fields=[("id", dorm.BigAutoField(primary_key=True)), ("name", dorm.CharField(max_length=50))],
        options={"db_table": "tmp_op_model"},
    )
    op.database_forwards("myapp", conn, from_state, to_state)
    assert conn.table_exists("tmp_op_model")

    op.database_backwards("myapp", conn, from_state, to_state)
    assert not conn.table_exists("tmp_op_model")


# ── Rollback ──────────────────────────────────────────────────────────────────

def test_rollback_create_model():
    """Rolling back a CreateModel migration drops the table."""
    from dorm.db.connection import get_connection
    conn = get_connection()

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        app = "rbtest"
        ops = [CreateModel(
            name="RbTable",
            fields=[("id", dorm.BigAutoField(primary_key=True)), ("val", dorm.IntegerField())],
            options={"db_table": "rb_table"},
        )]
        write_migration(app, mig_dir, 1, ops)

        executor = MigrationExecutor(conn, verbosity=0)
        executor.migrate(app, mig_dir)
        assert conn.table_exists("rb_table")

        executor2 = MigrationExecutor(conn, verbosity=0)
        executor2.rollback(app, mig_dir, "zero")
        assert not conn.table_exists("rb_table")

        recorder = MigrationRecorder(conn)
        assert (app, "0001_initial") not in recorder.applied_migrations()


def test_rollback_add_field():
    """Rolling back AddField drops the column (state is correctly rebuilt)."""
    from dorm.db.connection import get_connection
    conn = get_connection()

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        app = "rbfield"

        # Migration 1: create table
        ops1 = [CreateModel(
            name="RbFieldTable",
            fields=[("id", dorm.BigAutoField(primary_key=True))],
            options={"db_table": "rb_field_table"},
        )]
        write_migration(app, mig_dir, 1, ops1)

        # Migration 2: add column
        ops2 = [AddField(
            model_name="RbFieldTable",
            name="extra",
            field=dorm.IntegerField(null=True),
        )]
        write_migration(app, mig_dir, 2, ops2)

        executor = MigrationExecutor(conn, verbosity=0)
        executor.migrate(app, mig_dir)

        cols_before = [c["name"] for c in conn.get_table_columns("rb_field_table")]
        assert "extra" in cols_before

        # Roll back only migration 2 (keep migration 1 applied)
        executor2 = MigrationExecutor(conn, verbosity=0)
        executor2.rollback(app, mig_dir, "0001")

        cols_after = [c["name"] for c in conn.get_table_columns("rb_field_table")]
        assert "extra" not in cols_after
        assert conn.table_exists("rb_field_table")  # table still exists

        recorder = MigrationRecorder(conn)
        applied = recorder.applied_migrations()
        assert (app, "0001_initial") in applied
        assert (app, "0002_auto") not in applied

        # Cleanup
        executor3 = MigrationExecutor(conn, verbosity=0)
        executor3.rollback(app, mig_dir, "zero")


def test_migrate_to_goes_backward():
    """migrate_to() detects that target is before current state and rolls back."""
    from dorm.db.connection import get_connection
    conn = get_connection()

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        app = "migto"

        ops1 = [CreateModel(
            name="MigToTable",
            fields=[("id", dorm.BigAutoField(primary_key=True))],
            options={"db_table": "mig_to_table"},
        )]
        ops2 = [AddField(
            model_name="MigToTable",
            name="score",
            field=dorm.IntegerField(null=True),
        )]
        write_migration(app, mig_dir, 1, ops1)
        write_migration(app, mig_dir, 2, ops2)

        executor = MigrationExecutor(conn, verbosity=0)
        executor.migrate(app, mig_dir)

        # Go back to just migration 1
        executor2 = MigrationExecutor(conn, verbosity=0)
        executor2.migrate_to(app, mig_dir, "0001")

        cols = [c["name"] for c in conn.get_table_columns("mig_to_table")]
        assert "score" not in cols

        # Clean up
        executor3 = MigrationExecutor(conn, verbosity=0)
        executor3.rollback(app, mig_dir, "zero")


# ── RunPython / RunSQL with reverse ──────────────────────────────────────────

def test_run_python_forward_and_backward():
    """RunPython calls code on forward and reverse_code on backward."""
    log: list[str] = []

    def forward(app_label, registry):
        log.append("forward")

    def backward(app_label, registry):
        log.append("backward")

    from dorm.db.connection import get_connection
    conn = get_connection()
    state = ProjectState()

    op = RunPython(code=forward, reverse_code=backward)
    op.database_forwards("myapp", conn, state, state)
    assert log == ["forward"]

    op.database_backwards("myapp", conn, state, state)
    assert log == ["forward", "backward"]


def test_run_python_backward_no_reverse_code():
    """RunPython.database_backwards is a no-op when reverse_code is None."""
    called: list[bool] = []

    def forward(app_label, registry):
        called.append(True)

    from dorm.db.connection import get_connection
    conn = get_connection()
    state = ProjectState()

    op = RunPython(code=forward)
    op.database_backwards("myapp", conn, state, state)  # must not raise
    assert called == []


def test_run_sql_forward_and_backward():
    """RunSQL executes sql forward and reverse_sql backward."""
    from dorm.db.connection import get_connection
    conn = get_connection()

    # Use a temp table to verify execution
    conn.execute_script('CREATE TABLE IF NOT EXISTS "runsql_test" ("id" INTEGER PRIMARY KEY)')
    try:
        state = ProjectState()
        op = RunSQL(
            sql='INSERT INTO "runsql_test" ("id") VALUES (42)',
            reverse_sql='DELETE FROM "runsql_test" WHERE "id" = 42',
        )
        op.database_forwards("myapp", conn, state, state)
        rows = conn.execute('SELECT "id" FROM "runsql_test" WHERE "id" = 42')
        assert len(rows) == 1

        op.database_backwards("myapp", conn, state, state)
        rows = conn.execute('SELECT "id" FROM "runsql_test" WHERE "id" = 42')
        assert len(rows) == 0
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "runsql_test"')


def test_run_python_in_migration_rollback():
    """RunPython reverse_code is called when the migration is rolled back."""
    log: list[str] = []

    from dorm.db.connection import get_connection
    conn = get_connection()

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        app = "runpyapp"

        def fwd(app_label, registry):
            log.append("applied")

        def bwd(app_label, registry):
            log.append("rolled_back")

        ops = [RunPython(code=fwd, reverse_code=bwd)]

        # Write migration manually (writer doesn't serialize lambdas)
        mig_dir.mkdir(parents=True, exist_ok=True)
        (mig_dir / "__init__.py").touch()
        mod_path = mig_dir / "0001_initial.py"
        mod_path.write_text("")  # placeholder — we inject via loader

        # Inject the module directly into a fresh executor's loader
        fake_mod = SimpleNamespace(dependencies=[], operations=ops)

        executor = MigrationExecutor(conn, verbosity=0)
        executor.loader.migrations[app] = [(1, "0001_initial", fake_mod)]
        executor.loader.load_applied(executor.recorder)
        executor.loader.applied.discard((app, "0001_initial"))

        # Apply
        ProjectState()
        executor._apply_forward(app, [(1, "0001_initial", fake_mod)], set())
        assert "applied" in log

        # Rollback
        executor2 = MigrationExecutor(conn, verbosity=0)
        executor2.loader.migrations[app] = [(1, "0001_initial", fake_mod)]
        executor2.loader.load_applied(executor2.recorder)

        applied = executor2._applied_names(app)
        executor2._rollback_to(app, [(1, "0001_initial", fake_mod)], applied, -1)
        assert "rolled_back" in log
