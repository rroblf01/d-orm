"""Tests for squashmigrations: squasher, writer, and executor integration."""
from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace

import dorm
from dorm.migrations.operations import (
    AddField,
    AlterField,
    CreateModel,
    DeleteModel,
    RemoveField,
)
from dorm.migrations.squasher import squash_operations
from dorm.migrations.writer import write_squashed_migration


# ── squash_operations ─────────────────────────────────────────────────────────


def test_squash_create_plus_add_field():
    """CreateModel(X) + AddField(X, f) → CreateModel with field merged in."""
    cm = CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {})
    af = AddField("Post", "title", dorm.CharField(max_length=200))

    result = squash_operations([cm, af])

    assert len(result) == 1
    assert isinstance(result[0], CreateModel)
    field_names = [name for name, _ in result[0].fields]
    assert "id" in field_names
    assert "title" in field_names


def test_squash_add_remove_cancel():
    """AddField(X, f) + RemoveField(X, f) → both eliminated."""
    af = AddField("Post", "temp", dorm.IntegerField(null=True))
    rf = RemoveField("Post", "temp")

    result = squash_operations([af, rf])

    assert result == []


def test_squash_add_alter_merges():
    """AddField(X, f) + AlterField(X, f, new_type) → AddField with new_type."""
    af = AddField("Post", "score", dorm.IntegerField(null=True))
    alt = AlterField("Post", "score", dorm.BigIntegerField(null=True))

    result = squash_operations([af, alt])

    assert len(result) == 1
    assert isinstance(result[0], AddField)
    assert result[0].field.__class__.__name__ == "BigIntegerField"


def test_squash_create_delete_cancel():
    """CreateModel(X) + DeleteModel(X) → both eliminated (along with any X ops in between)."""
    cm = CreateModel("Tmp", [("id", dorm.BigAutoField(primary_key=True))], {})
    af = AddField("Tmp", "extra", dorm.IntegerField(null=True))
    dm = DeleteModel("Tmp")

    result = squash_operations([cm, af, dm])

    assert result == []


def test_squash_unrelated_ops_preserved():
    """Operations on different models are not touched."""
    af1 = AddField("Post", "title", dorm.CharField(max_length=100))
    af2 = AddField("Comment", "body", dorm.TextField(null=True))

    result = squash_operations([af1, af2])

    assert len(result) == 2


def test_squash_no_match_preserved():
    """A lone CreateModel with no matching ops is kept as-is."""
    cm = CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {})
    result = squash_operations([cm])
    assert len(result) == 1
    assert isinstance(result[0], CreateModel)


def test_squash_only_cancels_matching_field():
    """AddField + RemoveField only cancel when model AND field name match."""
    af = AddField("Post", "score", dorm.IntegerField(null=True))
    rf = RemoveField("Post", "title")  # different field

    result = squash_operations([af, rf])

    assert len(result) == 2


# ── write_squashed_migration ──────────────────────────────────────────────────


def test_write_squashed_migration_file_exists():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {})]
        replaces = [("myapp", "0001_initial"), ("myapp", "0002_auto")]

        path = write_squashed_migration("myapp", mig_dir, 3, ops, replaces)

        assert path.exists()
        assert path.name == "0003_squashed.py"


def test_write_squashed_migration_contains_replaces():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {})]
        replaces = [("myapp", "0001_initial"), ("myapp", "0002_auto")]

        path = write_squashed_migration("myapp", mig_dir, 3, ops, replaces)
        content = path.read_text()

        assert "replaces" in content
        assert "0001_initial" in content
        assert "0002_auto" in content


def test_write_squashed_migration_custom_name():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {})]

        path = write_squashed_migration("myapp", mig_dir, 1, ops, [], name="my_squash")

        assert path.name == "0001_my_squash.py"


def test_write_squashed_migration_creates_init():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {})]

        write_squashed_migration("myapp", mig_dir, 1, ops, [])

        assert (mig_dir / "__init__.py").exists()


def test_write_squashed_migration_ops_in_content():
    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [
            CreateModel("Post", [("id", dorm.BigAutoField(primary_key=True))], {}),
            AddField("Post", "title", dorm.CharField(max_length=100)),
        ]
        path = write_squashed_migration("myapp", mig_dir, 1, ops, [])
        content = path.read_text()

        assert "CreateModel" in content
        assert "AddField" in content


# ── MigrationExecutor squash integration ─────────────────────────────────────


def _make_module(**kwargs) -> SimpleNamespace:
    defaults = {"dependencies": [], "operations": []}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_executor_marks_replaces_on_apply():
    """Applying a squashed migration also records its replaces as applied."""
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor
    from dorm.migrations.recorder import MigrationRecorder

    conn = get_connection()

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        mig_dir.mkdir(parents=True, exist_ok=True)
        (mig_dir / "__init__.py").touch()

        app = "squashapp"
        table = "squash_test_table"

        ops = [CreateModel(
            "SquashTable",
            [("id", dorm.BigAutoField(primary_key=True)), ("val", dorm.IntegerField(null=True))],
            {"db_table": table},
        )]
        squashed_mod = _make_module(
            operations=ops,
            replaces=[(app, "0001_initial"), (app, "0002_auto")],
        )

        executor = MigrationExecutor(conn, verbosity=0)
        executor.loader.migrations[app] = [(3, "0003_squashed", squashed_mod)]
        executor.loader.load_applied(executor.recorder)

        executor._apply_forward(app, [(3, "0003_squashed", squashed_mod)], set())

        recorder = MigrationRecorder(conn)
        applied = recorder.applied_migrations()

        assert (app, "0003_squashed") in applied
        assert (app, "0001_initial") in applied
        assert (app, "0002_auto") in applied

        # Cleanup
        conn.execute_script(f'DROP TABLE IF EXISTS "{table}"')
        for name in ["0001_initial", "0002_auto", "0003_squashed"]:
            recorder.record_unapplied(app, name)


def test_executor_sync_squashed_auto_marks_applied():
    """_sync_squashed marks squashed as applied when all replaces are already applied."""
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor
    from dorm.migrations.recorder import MigrationRecorder

    conn = get_connection()
    app = "syncapp"

    recorder = MigrationRecorder(conn)
    recorder.ensure_table()

    # Pre-mark the two replaced migrations as applied
    recorder.record_applied(app, "0001_initial")
    recorder.record_applied(app, "0002_auto")

    squashed_mod = _make_module(replaces=[(app, "0001_initial"), (app, "0002_auto")])

    executor = MigrationExecutor(conn, verbosity=0)
    executor.loader.migrations[app] = [
        (1, "0001_initial", _make_module()),
        (2, "0002_auto", _make_module()),
        (3, "0003_squashed", squashed_mod),
    ]
    executor.loader.load_applied(recorder)

    all_migs = executor._sorted(app)
    executor._sync_squashed(app, all_migs)

    applied = recorder.applied_migrations()
    assert (app, "0003_squashed") in applied

    # Cleanup
    for name in ["0001_initial", "0002_auto", "0003_squashed"]:
        recorder.record_unapplied(app, name)


def test_executor_partial_replaces_not_auto_marked():
    """_sync_squashed does NOT mark squashed if only some replaces are applied."""
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor
    from dorm.migrations.recorder import MigrationRecorder

    conn = get_connection()
    app = "partialapp"

    recorder = MigrationRecorder(conn)
    recorder.ensure_table()

    # Only one of the two is applied
    recorder.record_applied(app, "0001_initial")

    squashed_mod = _make_module(replaces=[(app, "0001_initial"), (app, "0002_auto")])

    executor = MigrationExecutor(conn, verbosity=0)
    executor.loader.migrations[app] = [(3, "0003_squashed", squashed_mod)]
    executor.loader.load_applied(recorder)

    all_migs = executor._sorted(app)
    executor._sync_squashed(app, all_migs)

    applied = recorder.applied_migrations()
    assert (app, "0003_squashed") not in applied

    # Cleanup
    recorder.record_unapplied(app, "0001_initial")
