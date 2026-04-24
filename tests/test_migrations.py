"""Migration system tests."""
import tempfile
from pathlib import Path

import pytest

import dorm
from dorm.migrations.autodetector import MigrationAutodetector
from dorm.migrations.executor import MigrationExecutor
from dorm.migrations.operations import (
    AddField,
    CreateModel,
    DeleteModel,
    RemoveField,
    RunSQL,
)
from dorm.migrations.recorder import MigrationRecorder
from dorm.migrations.state import ProjectState
from dorm.migrations.writer import write_migration
from tests.models import Author


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
