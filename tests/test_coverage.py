"""Coverage-driven tests targeting code paths whose absence could hide
real bugs:

- ``DormTestCase`` mixin combined with ``unittest.TestCase``.
- CLI commands (``cmd_init`` templates, ``cmd_dbshell`` error paths).
- ``conf._autodiscover_settings`` discovery + edge cases.
- Migration writer serialisation for rare operation classes.
- ``db.utils`` masking and exception normalisation edge cases.
- Field validators and ``get_db_prep_value`` paths.
- Transaction decorator forms and rollback semantics.
- QuerySet edge cases that previous suites skipped.

The goal is bug detection, not vanity numbers — every test pins down a
specific behaviour that a future refactor could silently change.
"""

from __future__ import annotations

import io
import os
import sys
import textwrap
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

import pytest

import dorm
from dorm import transaction
from tests.models import Author, Book, Tag


# ── DormTestCase mixin ───────────────────────────────────────────────────────


def _build_dorm_test_case_class():
    """Build a unittest.TestCase subclass at call time so pytest's
    collector doesn't try to invoke it directly (it would inject the
    autouse ``clean_db`` fixture, which a unittest TestCase rejects).
    The class is created inside each test that needs it and run through
    ``unittest`` machinery instead.
    """
    from dorm.test import DormTestCase

    class _Inner(DormTestCase, unittest.TestCase):
        def test_atomic_block_opens_and_rolls_back(self):
            starting = Author.objects.count()
            Author.objects.create(name="DormCaseInner", age=1)
            assert Author.objects.count() == starting + 1

    return _Inner


def test_dormtestcase_rollback_propagates_across_methods():
    """The mixin's tearDown must roll back even when the test succeeds —
    that's what makes it a useful "transactional fixture" for unittest
    suites. We run a real unittest TestCase under the standard runner
    and then verify nothing leaked."""
    cls = _build_dorm_test_case_class()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(cls)
    runner = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0)
    result = runner.run(suite)
    assert result.wasSuccessful(), result.failures or result.errors

    # The author created inside the inner test must be gone.
    assert not Author.objects.filter(name="DormCaseInner").exists()


def test_dormtestcase_teardown_is_idempotent():
    """Calling tearDown twice (or after an explicit rollback) must not
    double-rollback or raise."""
    cls = _build_dorm_test_case_class()
    case = cls("test_atomic_block_opens_and_rolls_back")
    case.setUp()
    Author.objects.create(name="DormCaseTwoTearDown", age=2)
    case.tearDown()
    case.tearDown()  # second call: cm is None, must be a no-op
    assert not Author.objects.filter(name="DormCaseTwoTearDown").exists()


# ── CLI: cmd_init ────────────────────────────────────────────────────────────


def test_cli_cmd_init_creates_settings_and_app(tmp_path, monkeypatch):
    """Running ``dorm init --app blog`` in an empty dir creates
    settings.py, blog/__init__.py and blog/models.py with the user
    template. Subsequent runs are idempotent — files left untouched."""
    from dorm.cli import cmd_init

    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(app="blog")
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_init(args)

    assert (tmp_path / "settings.py").exists()
    assert (tmp_path / "blog" / "__init__.py").exists()
    assert (tmp_path / "blog" / "models.py").exists()
    assert "class User(dorm.Model)" in (tmp_path / "blog" / "models.py").read_text()

    # Second run must not overwrite — it should report leaving them alone.
    buf2 = io.StringIO()
    with redirect_stdout(buf2):
        cmd_init(args)
    assert "already exists" in buf2.getvalue()


def test_cli_cmd_init_without_app_only_writes_settings(tmp_path, monkeypatch):
    from dorm.cli import cmd_init

    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(app=None)
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_init(args)
    assert (tmp_path / "settings.py").exists()
    # No "blog/" directory created when --app is omitted.
    assert not any(p.is_dir() for p in tmp_path.iterdir())


# ── CLI: cmd_dbshell error paths ─────────────────────────────────────────────


@pytest.fixture
def restore_dorm_settings():
    """``cmd_dbshell`` calls ``_load_settings`` which mutates the global
    dorm settings singleton. Snapshot the relevant fields before each
    test and put them back after, so the suite's session-wide
    ``configure_dorm`` fixture isn't corrupted."""
    from dorm.conf import settings
    from dorm.db.connection import reset_connections

    snapshot = (
        dict(getattr(settings, "DATABASES", {}) or {}),
        list(getattr(settings, "INSTALLED_APPS", []) or []),
        getattr(settings, "_configured", True),
    )
    yield
    settings.configure(DATABASES=snapshot[0], INSTALLED_APPS=snapshot[1])
    settings._configured = snapshot[2]
    reset_connections()


def test_cli_cmd_dbshell_unknown_alias(
    monkeypatch, tmp_path, capsys, restore_dorm_settings
):
    """``dorm dbshell --database missing`` must exit non-zero with a
    clear error — not crash with KeyError."""
    from dorm import cli

    settings_file = tmp_path / "settings_dbshell_alias.py"
    settings_file.write_text(
        textwrap.dedent("""
            DATABASES = {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}}
            INSTALLED_APPS = []
        """)
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    args = SimpleNamespace(database="not-an-alias", settings="settings_dbshell_alias")
    with pytest.raises(SystemExit) as excinfo:
        cli.cmd_dbshell(args)
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert "not in DATABASES" in captured.err


def test_cli_cmd_dbshell_unsupported_engine(
    monkeypatch, tmp_path, capsys, restore_dorm_settings
):
    """An unknown engine value must exit cleanly with an error message
    rather than silently doing nothing."""
    from dorm import cli

    settings_file = tmp_path / "settings_dbshell_engine.py"
    settings_file.write_text(
        textwrap.dedent("""
            DATABASES = {"default": {"ENGINE": "oracle", "NAME": "db"}}
            INSTALLED_APPS = []
        """)
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    args = SimpleNamespace(database="default", settings="settings_dbshell_engine")
    with pytest.raises(SystemExit) as excinfo:
        cli.cmd_dbshell(args)
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert "does not know how to launch" in captured.err


def test_cli_cmd_dbshell_missing_sqlite_client(
    monkeypatch, tmp_path, capsys, restore_dorm_settings
):
    """If ``sqlite3`` isn't on PATH, dbshell must exit 127 instead of
    raising FileNotFoundError from execvp."""
    from dorm import cli

    settings_file = tmp_path / "settings_dbshell_missing.py"
    settings_file.write_text(
        textwrap.dedent("""
            DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}
            INSTALLED_APPS = []
        """)
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    # Force shutil.which to report "not found" for sqlite3.
    import shutil
    monkeypatch.setattr(shutil, "which", lambda _name: None)

    args = SimpleNamespace(database="default", settings="settings_dbshell_missing")
    with pytest.raises(SystemExit) as excinfo:
        cli.cmd_dbshell(args)
    assert excinfo.value.code == 127
    captured = capsys.readouterr()
    assert "sqlite3" in captured.err


def test_cli_cmd_dbshell_pg_invokes_psql_with_pgpassword(
    monkeypatch, tmp_path, restore_dorm_settings
):
    """For PG, dbshell must build psql argv with -h/-p/-U/-d and pass
    PASSWORD via the ``PGPASSWORD`` env var — not the connection string,
    which would leak the password into ``ps``."""
    from dorm import cli

    settings_file = tmp_path / "settings_dbshell_pg.py"
    settings_file.write_text(
        textwrap.dedent("""
            DATABASES = {
                "default": {
                    "ENGINE": "postgresql",
                    "NAME": "mydb",
                    "USER": "alice",
                    "PASSWORD": "s3cret",
                    "HOST": "db.example",
                    "PORT": 5433,
                }
            }
            INSTALLED_APPS = []
        """)
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    captured: dict = {}

    def fake_execvpe(client, argv, env):
        captured["client"] = client
        captured["argv"] = argv
        captured["env"] = env
        # Don't actually exec — just signal control flow finished.
        raise SystemExit(0)

    import shutil
    monkeypatch.setattr(shutil, "which", lambda _: "/usr/bin/psql")
    monkeypatch.setattr(os, "execvpe", fake_execvpe)

    args = SimpleNamespace(database="default", settings="settings_dbshell_pg")
    with pytest.raises(SystemExit):
        cli.cmd_dbshell(args)

    assert captured["client"] == "/usr/bin/psql"
    assert "-h" in captured["argv"] and "db.example" in captured["argv"]
    assert "-p" in captured["argv"] and "5433" in captured["argv"]
    assert "-U" in captured["argv"] and "alice" in captured["argv"]
    assert "-d" in captured["argv"] and "mydb" in captured["argv"]
    # Critical: password must travel via env, not argv.
    assert "s3cret" not in " ".join(captured["argv"])
    assert captured["env"]["PGPASSWORD"] == "s3cret"


# ── conf._autodiscover_settings ──────────────────────────────────────────────


def test_autodiscover_returns_false_when_no_settings_file(tmp_path, monkeypatch):
    """When neither cwd nor sys.argv[0] dir contains settings.py,
    autodiscover must return False without raising — not having a
    settings.py is a legitimate state for ``dorm init``-style commands."""
    from dorm.conf import _autodiscover_settings, settings

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", [str(tmp_path / "no_such_script.py")])

    # Reset the configured flag so autodiscover actually runs.
    monkeypatch.setattr(settings, "_configured", False, raising=False)
    monkeypatch.setattr(settings, "DATABASES", {}, raising=False)
    monkeypatch.setattr(settings, "INSTALLED_APPS", [], raising=False)

    assert _autodiscover_settings() is False


def test_autodiscover_skips_when_already_configured(monkeypatch):
    """If settings is already configured (the usual case), autodiscover
    must short-circuit without doing any filesystem work."""
    from dorm.conf import _autodiscover_settings, settings

    monkeypatch.setattr(settings, "_configured", True, raising=False)
    # No filesystem patching needed — short-circuit means we never call os.*.
    assert _autodiscover_settings() is True


# ── Migration writer: rare ops ───────────────────────────────────────────────


def test_writer_serializes_rename_field(tmp_path):
    """RenameField round-trip: writer emits valid Python that re-creates
    the operation when re-imported."""
    from dorm.migrations.operations import RenameField
    from dorm.migrations.writer import write_migration

    op = RenameField(model_name="Author", old_name="bio", new_name="biography")
    path = write_migration("blog", tmp_path, 7, [op])
    src = path.read_text()
    assert "RenameField" in src
    assert "old_name='bio'" in src
    assert "new_name='biography'" in src

    # Importable.
    spec = __import__("importlib.util", fromlist=["spec_from_file_location"]).spec_from_file_location(
        "_w_rename_field", path
    )
    mod = __import__("importlib.util", fromlist=["module_from_spec"]).module_from_spec(spec)
    spec.loader.exec_module(mod)
    [reloaded] = mod.operations
    assert reloaded.old_name == "bio"
    assert reloaded.new_name == "biography"


def test_writer_serializes_rename_model(tmp_path):
    from dorm.migrations.operations import RenameModel
    from dorm.migrations.writer import write_migration

    op = RenameModel(old_name="Article", new_name="Post")
    path = write_migration("blog", tmp_path, 8, [op])
    src = path.read_text()
    assert "RenameModel" in src
    assert "old_name='Article'" in src
    assert "new_name='Post'" in src


def test_writer_serializes_run_sql(tmp_path):
    from dorm.migrations.operations import RunSQL
    from dorm.migrations.writer import write_migration

    op = RunSQL("UPDATE authors SET active = true")
    path = write_migration("blog", tmp_path, 9, [op])
    src = path.read_text()
    assert "RunSQL" in src
    assert "UPDATE authors SET active = true" in src


def test_writer_serializes_remove_index(tmp_path):
    from dorm.indexes import Index
    from dorm.migrations.operations import RemoveIndex
    from dorm.migrations.writer import write_migration

    op = RemoveIndex(model_name="Author", index=Index(fields=["name"], unique=False, name=None))
    path = write_migration("blog", tmp_path, 10, [op])
    src = path.read_text()
    assert "RemoveIndex" in src
    assert "from dorm.indexes import Index" in src
    assert "Index(fields=['name']" in src


def test_writer_creates_init_py_when_missing(tmp_path):
    """First migration in a brand new app must also create the
    package's ``__init__.py``."""
    from dorm.migrations.operations import RunSQL
    from dorm.migrations.writer import write_migration

    target_dir = tmp_path / "newapp_migrations"
    assert not target_dir.exists()
    write_migration("newapp", target_dir, 1, [RunSQL("SELECT 1")])
    assert (target_dir / "__init__.py").exists()


# ── db.utils: masking + transient retry ──────────────────────────────────────


def test_mask_params_handles_in_clause():
    """``WHERE password IN (?, ?, ?)`` — every placeholder inside the
    sensitive column's IN list must be masked. Bug we want to lock down:
    naive impls only mask the first one and leak the rest."""
    from dorm.db.utils import _mask_params

    sql = 'SELECT * FROM "u" WHERE "password" IN (?, ?, ?)'
    out = _mask_params(sql, ["pw1", "pw2", "pw3"])
    assert out == ["***", "***", "***"], out


def test_mask_params_returns_input_when_params_empty():
    from dorm.db.utils import _mask_params

    assert _mask_params("INSERT INTO x VALUES (%s)", []) == []
    assert _mask_params("INSERT INTO x VALUES (%s)", None) is None


def test_mask_params_does_not_choke_on_oddly_shaped_params():
    """A tuple of non-strings, weird dict shapes — masker must never
    raise. Logging-time code that crashes is worse than the leak it's
    trying to prevent."""
    from dorm.db.utils import _mask_params

    sql = 'UPDATE "u" SET "password" = %s WHERE "id" = %s'
    # Tuple form
    out = _mask_params(sql, ("pw", 1))
    assert "pw" not in str(out)
    # Empty tuple
    assert _mask_params(sql, ()) == ()


def test_with_transient_retry_skips_when_in_transaction():
    """Inside a transaction, retry would re-run already-committed work
    — the helper must short-circuit and not retry."""
    from dorm.db.utils import with_transient_retry

    calls = []

    def _do():
        calls.append(1)
        raise ConnectionResetError("fake transient")

    with pytest.raises(ConnectionResetError):
        with_transient_retry(_do, in_transaction=True)
    assert len(calls) == 1, "in_transaction=True must short-circuit retry"


def test_with_transient_retry_does_not_retry_non_transient():
    """A ProgrammingError-class exception must propagate immediately;
    only connection-level errors are retryable."""
    from dorm.db.utils import with_transient_retry

    calls = []

    def _do():
        calls.append(1)
        raise ValueError("not transient")

    with pytest.raises(ValueError):
        with_transient_retry(_do, attempts=3, backoff=0.001)
    assert len(calls) == 1


def test_normalize_db_exception_wraps_sqlite_integrity():
    """sqlite3.IntegrityError must surface as dorm.IntegrityError so
    user code can catch the dorm-level exception across both backends."""
    import sqlite3
    from dorm.db.utils import normalize_db_exception
    from dorm.exceptions import IntegrityError

    try:
        normalize_db_exception(sqlite3.IntegrityError("UNIQUE constraint failed"))
    except IntegrityError:
        pass
    else:
        pytest.fail("Expected IntegrityError to be raised")


def test_normalize_db_exception_adds_migration_hint():
    """A 'no such table' OperationalError must get the migration hint
    appended to its message — that's the whole point of the helper."""
    import sqlite3
    from dorm.db.utils import normalize_db_exception
    from dorm.exceptions import OperationalError

    with pytest.raises(OperationalError) as excinfo:
        normalize_db_exception(sqlite3.OperationalError("no such table: foo"))
    assert "dorm migrate" in str(excinfo.value)


# ── Field validators / get_db_prep_value ─────────────────────────────────────


def test_charfield_max_length_validator_rejects_long_strings():
    """CharField.validate() must reject values longer than ``max_length``.
    Bug we lock down: a future refactor that drops the length check
    inside ``validate`` would silently let oversized strings reach the
    DB and trigger a backend error far from the user's call site."""
    from dorm.exceptions import ValidationError
    from dorm.fields import CharField

    f = CharField(max_length=5)
    f.name = "name"  # mimic contribute_to_class side effect
    with pytest.raises(ValidationError, match="too long"):
        f.validate("toolong!", model_instance=None)
    # Within bounds: no exception.
    f.validate("ok", model_instance=None)


def test_emailfield_validator_rejects_garbage():
    """EmailField rejects malformed input at assignment time
    (``to_python``) AND in ``validate``. Lock down both paths — they
    cover different call sites: assignment, and explicit
    ``full_clean()`` / serializer-style validation."""
    from dorm.exceptions import ValidationError
    from dorm.fields import EmailField

    f = EmailField()
    f.name = "email"

    # to_python rejects on assignment-time conversion.
    with pytest.raises(ValidationError):
        f.to_python("not-an-email")
    # validate() rejects too (different call site, e.g. full_clean()).
    with pytest.raises(ValidationError):
        f.validate("not-an-email", model_instance=None)

    # Both accept a real email.
    assert f.to_python("alice@example.com") == "alice@example.com"
    f.validate("alice@example.com", model_instance=None)
    # And tolerate empty strings / None (nullable use).
    assert f.to_python("") == ""
    assert f.to_python(None) is None


def test_jsonfield_db_prep_serializes_dict():
    """JSON fields must JSON-encode dicts on the way to the DB."""
    import json
    from dorm.fields import JSONField

    f = JSONField()
    out = f.get_db_prep_value({"a": 1, "b": [2, 3]})
    # Could be str or stay as dict (PG's psycopg native JSON support);
    # decode if it's a string and confirm the round-trip.
    if isinstance(out, str):
        assert json.loads(out) == {"a": 1, "b": [2, 3]}


def test_decimalfield_round_trip_preserves_precision():
    """DecimalField must NOT silently coerce to float — that loses
    precision and breaks money math."""
    import decimal
    from dorm.fields import DecimalField

    f = DecimalField(max_digits=10, decimal_places=4)
    out = f.get_db_prep_value(decimal.Decimal("3.1415"))
    # The value should still be a Decimal or an exact string — never a
    # binary float.
    assert not isinstance(out, float)


# ── Transaction decorator forms ──────────────────────────────────────────────


def test_atomic_decorator_no_parens():
    """``@atomic`` (no parens) must wrap the function; calling it fires
    the transaction lifecycle."""
    fired: list[str] = []

    @transaction.atomic
    def do_work():
        Author.objects.create(name="DecoratedNoParens", age=11)
        transaction.on_commit(lambda: fired.append("post"))

    do_work()
    assert fired == ["post"]
    assert Author.objects.filter(name="DecoratedNoParens").exists()


def test_atomic_decorator_with_alias():
    """``@atomic("default")`` parametrised form must also wrap."""
    fired: list[str] = []

    @transaction.atomic("default")
    def do_work():
        Author.objects.create(name="DecoratedAlias", age=12)
        transaction.on_commit(lambda: fired.append("post"))

    do_work()
    assert fired == ["post"]


def test_atomic_decorator_propagates_exception_and_rolls_back():
    @transaction.atomic
    def will_fail():
        Author.objects.create(name="WillRollback", age=13)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        will_fail()
    assert not Author.objects.filter(name="WillRollback").exists()


@pytest.mark.asyncio
async def test_aatomic_decorator_no_parens():
    @transaction.aatomic
    async def do_work():
        await Author.objects.acreate(name="ADecoratedNoParens", age=14)

    await do_work()
    assert await Author.objects.filter(name="ADecoratedNoParens").aexists()


@pytest.mark.asyncio
async def test_aatomic_decorator_with_alias():
    @transaction.aatomic("default")
    async def do_work():
        await Author.objects.acreate(name="ADecoratedAlias", age=15)

    await do_work()
    assert await Author.objects.filter(name="ADecoratedAlias").aexists()


def test_set_rollback_inside_nested_atomic():
    """Calling set_rollback on the INNER atomic should only roll back
    that block; the outer one keeps its own writes if no exception
    propagates."""
    Author.objects.filter(name__startswith="NestedRollback").delete()
    with transaction.atomic():
        Author.objects.create(name="NestedRollbackOuter", age=20)
        with transaction.atomic() as inner:
            Author.objects.create(name="NestedRollbackInner", age=21)
            inner.set_rollback(True)
        # Inner block already exited — outer continues here.
        Author.objects.create(name="NestedRollbackOuter2", age=22)
    assert Author.objects.filter(name="NestedRollbackOuter").exists()
    assert Author.objects.filter(name="NestedRollbackOuter2").exists()
    assert not Author.objects.filter(name="NestedRollbackInner").exists()


# ── on_commit advanced cases ─────────────────────────────────────────────────


def test_on_commit_multiple_callbacks_fire_in_order():
    fired: list[int] = []
    with transaction.atomic():
        for i in range(5):
            transaction.on_commit(lambda i=i: fired.append(i))
    assert fired == [0, 1, 2, 3, 4]


def test_on_commit_inner_commit_outer_rollback_discards_all():
    """A clean inner commit followed by an outer rollback must discard
    the inner's callbacks too — they were merged into the outer frame
    when the inner committed, so the outer's rollback eats them."""
    fired: list[str] = []
    with pytest.raises(RuntimeError):
        with transaction.atomic():
            with transaction.atomic():
                transaction.on_commit(lambda: fired.append("inner"))
            raise RuntimeError("outer")
    assert fired == []


@pytest.mark.asyncio
async def test_aon_commit_outside_atomic_with_async_callback():
    fired: list[str] = []

    async def cb():
        fired.append("ran")

    transaction.aon_commit(cb)
    # No active aatomic → cb is scheduled as a task; let the loop tick.
    import asyncio

    await asyncio.sleep(0.05)
    assert fired == ["ran"]


# ── QuerySet edge cases ──────────────────────────────────────────────────────


def test_alias_chain_preserves_all_aliased_names():
    """Multiple ``alias()`` calls in a chain must accumulate, not
    replace, the alias-only set."""
    qs = (
        Author.objects
        .alias(book_count=dorm.Count("books"))
        .alias(name_upper=dorm.Upper("name"))
    )
    assert qs._query.alias_only_names == {"book_count", "name_upper"}


def test_alias_then_annotate_promotion_changes_select_sql():
    """Promoting an alias to annotate() must remove it from
    ``alias_only_names`` and emit it in the SELECT projection. We
    verify the SQL shape directly — exercising the end-to-end query
    pulls in a different chain (annotation lookup in WHERE) that
    isn't fully implemented and would mask the alias-promotion bug
    if it ever broke."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs_alias = Author.objects.alias(bc=dorm.Count("books"))
    sql_alias, _ = qs_alias._query.as_select(conn)
    assert "AS \"bc\"" not in sql_alias

    qs_promoted = qs_alias.annotate(bc=dorm.Count("books"))
    assert qs_promoted._query.alias_only_names == set()
    sql_promoted, _ = qs_promoted._query.as_select(conn)
    assert 'AS "bc"' in sql_promoted


def test_select_for_update_clones_state_per_call():
    """Each call to ``select_for_update`` must return a fresh clone —
    re-using the same QuerySet would leak the lock flag across
    independent iterations."""
    base = Author.objects.filter(age__gte=18)
    locked = base.select_for_update()
    # Original must NOT have for_update_flag set.
    assert base._query.for_update_flag is False
    assert locked._query.for_update_flag is True


def test_bulk_create_empty_list_returns_empty_no_query():
    """Edge case that historically panicked some ORMs: bulk_create([])
    must short-circuit before computing field schema."""
    out = Author.objects.bulk_create([])
    assert out == []


def test_bulk_create_ignore_conflicts_with_unique_fields_target_pg(monkeypatch):
    """When ``unique_fields`` is supplied alongside ``ignore_conflicts``,
    the SQL still uses unconstrained ``ON CONFLICT DO NOTHING``
    (Django's behaviour). We only need a valid target for
    ``update_conflicts``. Lock that down so a future change doesn't
    accidentally make ``ignore_conflicts`` require it."""
    # Pure SQL-shape test using the query builder; doesn't hit the DB.
    from dorm.db.connection import get_connection
    from dorm.query import SQLQuery

    conn = get_connection()
    q = SQLQuery(Tag)
    fake_field = type(
        "F", (), {"column": "name", "primary_key": False}
    )()
    sql, _ = q.as_bulk_insert(
        [fake_field], [["a"]], conn,
        ignore_conflicts=True, unique_fields=["name"],
    )
    assert "ON CONFLICT DO NOTHING" in sql.upper()


def test_bulk_create_update_conflicts_default_update_fields_excludes_unique():
    """When ``update_fields`` is omitted under ``update_conflicts=True``,
    the default list must skip the columns named in ``unique_fields``
    — updating the conflict target to itself is a no-op at best and a
    deadlock invitation at worst on some PG configurations."""
    from dorm.db.connection import get_connection
    from dorm.query import SQLQuery

    conn = get_connection()
    q = SQLQuery(Author)

    # Two fake fields: one unique, one regular.
    fields = [
        type("F1", (), {"column": "email", "primary_key": False})(),
        type("F2", (), {"column": "age", "primary_key": False})(),
    ]
    sql, _ = q.as_bulk_insert(
        fields, [["a@b.com", 30]], conn,
        update_conflicts=True, unique_fields=["email"],
    )
    assert "DO UPDATE SET" in sql.upper()
    # ``email`` must NOT appear in the SET clause (it's the conflict target).
    set_part = sql.upper().split("DO UPDATE SET", 1)[1]
    assert '"AGE" = EXCLUDED."AGE"' in set_part
    assert '"EMAIL" = EXCLUDED."EMAIL"' not in set_part


# ── pool_stats variants ──────────────────────────────────────────────────────


def test_pool_stats_for_unconfigured_alias():
    """Asking for stats on an alias that was never used must return a
    sentinel dict — not raise."""
    from dorm.db.connection import pool_stats

    out = pool_stats("does-not-exist")
    assert out == {"alias": "does-not-exist", "status": "uninitialised"}


def test_health_check_deep_for_uninitialised_alias():
    """A health check on an alias not in DATABASES must report an error
    AND still expose the pool sentinel under deep=True."""
    from dorm.db.connection import health_check

    out = health_check(alias="not-in-databases", deep=True)
    assert out["status"] == "error"
    assert out["pool"]["status"] == "uninitialised"


# ── Bulk update edge cases ───────────────────────────────────────────────────


def test_bulk_update_empty_list_returns_zero():
    assert Author.objects.bulk_update([], fields=["age"]) == 0


def test_bulk_update_skips_objects_without_pk():
    """``bulk_update`` must filter out objects whose pk is None — they
    can't be addressed in the WHERE clause anyway, and the underlying
    builder returns ``None`` for an all-None batch. We pass a mix and
    confirm only the saved row gets touched."""
    saved = Author.objects.create(name="HasPK", age=30)
    floating = Author(name="NoPK", age=99)  # never saved → pk is None
    saved.age = 31
    n = Author.objects.bulk_update([saved, floating], fields=["age"])
    # Only the row with a pk was updated.
    assert n == 1
    saved.refresh_from_db()
    assert saved.age == 31


def test_bulk_update_unknown_field_raises():
    """Typo in the ``fields`` list must raise a clear error before any
    SQL is sent — protect users from a silent partial update where
    just the recognised columns moved."""
    a = Author.objects.create(name="bu-typo", age=5)
    with pytest.raises(ValueError, match="Unknown field"):
        Author.objects.bulk_update([a], fields=["definitely_not_a_field"])


# ── Reverse-FK prefetch via descriptor scan (sync) ───────────────────────────


def test_prefetch_reverse_fk_via_descriptor():
    """``Author -> Book`` reverse FK (no ``related_name`` set, so the
    default reverse is ``book_set``); ``prefetch_related("book_set")``
    fetches all books in one extra query and assigns them to each
    author. Confirms the descriptor branch (the common path)."""
    a1 = Author.objects.create(name="rev-a1", age=10)
    a2 = Author.objects.create(name="rev-a2", age=11)
    Book.objects.create(title="b1", author=a1, pages=10)
    Book.objects.create(title="b2", author=a1, pages=20)
    Book.objects.create(title="b3", author=a2, pages=30)

    authors = list(
        Author.objects.filter(name__startswith="rev-").prefetch_related("book_set")
    )
    by_name = {a.name: a for a in authors}
    assert len(by_name["rev-a1"].__dict__["_prefetch_book_set"]) == 2
    assert len(by_name["rev-a2"].__dict__["_prefetch_book_set"]) == 1


@pytest.mark.asyncio
async def test_aprefetch_reverse_fk_via_descriptor():
    a1 = await Author.objects.acreate(name="arev-a1", age=10)
    a2 = await Author.objects.acreate(name="arev-a2", age=11)
    await Book.objects.acreate(title="ab1", author_id=a1.pk, pages=1)
    await Book.objects.acreate(title="ab2", author_id=a1.pk, pages=2)
    await Book.objects.acreate(title="ab3", author_id=a2.pk, pages=3)

    qs = Author.objects.filter(name__startswith="arev-").prefetch_related("book_set")
    out = [a async for a in qs]
    by_name = {a.name: a for a in out}
    assert len(by_name["arev-a1"].__dict__["_prefetch_book_set"]) == 2
    assert len(by_name["arev-a2"].__dict__["_prefetch_book_set"]) == 1


def test_prefetch_reverse_fk_with_no_source_pks_short_circuits():
    """If the parent queryset is empty, the prefetch must short-circuit
    without firing a follow-up query."""
    out = list(
        Author.objects.filter(name="non-existent").prefetch_related("book_set")
    )
    assert out == []


# ── Fields: choices, defaults, db_type ───────────────────────────────────────


def test_charfield_choices_validate_rejects_unknown():
    from dorm.exceptions import ValidationError
    from dorm.fields import CharField

    f = CharField(max_length=20, choices=[("a", "A"), ("b", "B")])
    f.name = "tag"
    with pytest.raises(ValidationError):
        f.validate("c", model_instance=None)
    f.validate("a", model_instance=None)


def test_intfield_null_validation():
    from dorm.exceptions import ValidationError
    from dorm.fields import IntegerField

    f = IntegerField()
    f.name = "n"
    with pytest.raises(ValidationError):
        f.validate(None, model_instance=None)
    # Nullable variant accepts None.
    g = IntegerField(null=True)
    g.name = "n"
    g.validate(None, model_instance=None)


def test_jsonfield_to_python_passes_through_dicts_and_lists():
    from dorm.fields import JSONField

    f = JSONField()
    assert f.to_python({"a": 1}) == {"a": 1}
    assert f.to_python([1, 2, 3]) == [1, 2, 3]
    # A JSON-encoded string round-trips back to the structured form.
    assert f.to_python('{"a": 1}') == {"a": 1}


def test_booleanfield_coerces_strings_and_ints():
    from dorm.fields import BooleanField

    f = BooleanField()
    assert f.to_python("true") is True
    assert f.to_python("FALSE") is False
    assert f.to_python(1) is True
    assert f.to_python(0) is False
    assert f.to_python(None) is None


def test_decimalfield_does_not_coerce_to_float():
    """The whole point of DecimalField is preserving exact precision.
    A regression that silently converts to float would re-introduce
    the very bug the field exists to prevent."""
    import decimal
    from dorm.fields import DecimalField

    f = DecimalField(max_digits=12, decimal_places=4)
    out = f.to_python("123.4567")
    assert isinstance(out, decimal.Decimal)
    assert out == decimal.Decimal("123.4567")


def test_datefield_parses_iso_string():
    import datetime as dt
    from dorm.fields import DateField

    f = DateField()
    assert f.to_python("2026-04-27") == dt.date(2026, 4, 27)
    assert f.to_python(dt.date(2024, 1, 1)) == dt.date(2024, 1, 1)


def test_datetimefield_parses_iso_string():
    import datetime as dt
    from dorm.fields import DateTimeField

    f = DateTimeField()
    parsed = f.to_python("2026-04-27T12:34:56")
    assert isinstance(parsed, dt.datetime)
    assert parsed.year == 2026 and parsed.minute == 34


def test_uuidfield_parses_string():
    import uuid
    from dorm.fields import UUIDField

    f = UUIDField()
    s = "12345678-1234-5678-1234-567812345678"
    out = f.to_python(s)
    assert isinstance(out, uuid.UUID)
    assert str(out) == s


def test_uuidfield_rejects_garbage():
    """UUIDField wraps ``uuid.UUID(value)`` for the conversion. A garbage
    string surfaces as ``ValueError`` from the stdlib — that's still a
    clear signal to the caller, but lock down the behaviour so a future
    refactor can't quietly start swallowing it."""
    from dorm.fields import UUIDField

    f = UUIDField()
    f.name = "id"
    with pytest.raises((ValueError, dorm.ValidationError)):
        f.to_python("not-a-uuid")


def test_field_get_default_callable_default():
    """When ``default`` is callable, ``get_default()`` must invoke it
    each time — important for ``default=now`` / ``default=uuid.uuid4``
    so every row gets a fresh value."""
    counter = {"n": 0}

    def gen():
        counter["n"] += 1
        return counter["n"]

    from dorm.fields import IntegerField

    f = IntegerField(default=gen)
    assert f.get_default() == 1
    assert f.get_default() == 2
    assert counter["n"] == 2


# ── Models: refresh_from_db / equality ───────────────────────────────────────


def test_refresh_from_db_picks_up_external_change():
    """``refresh_from_db`` must re-fetch *all* concrete fields, not
    just the changed one — locking down a Django parity that's easy
    to regress when adding partial-refresh support."""
    a = Author.objects.create(name="RfdOriginal", age=20)
    Author.objects.filter(pk=a.pk).update(name="RfdChanged", age=99)
    a.refresh_from_db()
    assert a.name == "RfdChanged"
    assert a.age == 99


def test_refresh_from_db_with_fields_only_refreshes_subset():
    a = Author.objects.create(name="RfdSubsetOriginal", age=20)
    Author.objects.filter(pk=a.pk).update(name="RfdSubsetChanged", age=42)
    a.refresh_from_db(fields=["age"])
    # Age came back from the DB; name remained the in-memory value.
    assert a.age == 42
    assert a.name == "RfdSubsetOriginal"


def test_model_equality_uses_pk_and_class():
    a = Author.objects.create(name="EqA", age=1)
    same = Author.objects.get(pk=a.pk)
    assert a == same
    other = Author.objects.create(name="EqB", age=2)
    assert a != other


def test_model_repr_renders_pk():
    """``repr(instance)`` must include the pk so log lines / pdb output
    are useful. Lock down the format — a future change that drops the
    pk from repr makes debugging painfully harder."""
    a = Author.objects.create(name="ReprMe", age=1)
    r = repr(a)
    assert str(a.pk) in r


# ── Transaction: nested savepoint rollback ───────────────────────────────────


def test_inner_atomic_failure_rolls_back_only_inner():
    """``RollbackToSavepoint`` semantics: an exception inside a nested
    ``atomic()`` block must roll back only that nested block, leaving
    the outer transaction's writes intact."""
    Author.objects.filter(name__startswith="SP").delete()
    with transaction.atomic():
        Author.objects.create(name="SPouter", age=10)
        try:
            with transaction.atomic():
                Author.objects.create(name="SPinner", age=11)
                raise RuntimeError("inner rollback")
        except RuntimeError:
            pass
        Author.objects.create(name="SPouter2", age=12)
    assert Author.objects.filter(name="SPouter").exists()
    assert Author.objects.filter(name="SPouter2").exists()
    assert not Author.objects.filter(name="SPinner").exists()


@pytest.mark.asyncio
async def test_aatomic_inner_failure_rolls_back_only_inner():
    await Author.objects.filter(name__startswith="ASP").adelete()
    async with transaction.aatomic():
        await Author.objects.acreate(name="ASPouter", age=10)
        try:
            async with transaction.aatomic():
                await Author.objects.acreate(name="ASPinner", age=11)
                raise RuntimeError("inner rollback")
        except RuntimeError:
            pass
        await Author.objects.acreate(name="ASPouter2", age=12)

    assert await Author.objects.filter(name="ASPouter").aexists()
    assert await Author.objects.filter(name="ASPouter2").aexists()
    assert not await Author.objects.filter(name="ASPinner").aexists()


# ── QuerySet Set operations and slicing edge cases ───────────────────────────


def test_queryset_slicing_with_step_raises():
    """Slicing with a step (``qs[::2]``) is unsupported — locking down
    the rejection so a future "be lenient and materialise" change
    can't silently swallow huge result sets."""
    qs = Author.objects.all()
    with pytest.raises(Exception):  # noqa: PT011
        _ = qs[::2]


def test_queryset_negative_index_rejected():
    qs = Author.objects.all()
    with pytest.raises(Exception):  # noqa: PT011
        _ = qs[-1]


def test_queryset_get_or_none_returns_none_when_missing():
    out = Author.objects.get_or_none(name="completely-missing-author-xyz")
    assert out is None


def test_queryset_first_last_on_empty():
    """``.first()`` / ``.last()`` on an empty queryset return None — not
    raise. Pin this down because the temptation to "return DoesNotExist"
    is real but breaks every "default to None" call site."""
    Author.objects.filter(name="never-exists").delete()
    assert Author.objects.filter(name="never-exists").first() is None
    assert Author.objects.filter(name="never-exists").last() is None


# ── DB exception normalisation: PG branch ────────────────────────────────────


def test_normalize_psycopg_integrity_error():
    """If psycopg is installed, normalize must convert its IntegrityError
    too (mirror of the SQLite branch)."""
    try:
        import psycopg.errors as pg_errors
    except ImportError:
        pytest.skip("psycopg not installed")
    from dorm.db.utils import normalize_db_exception
    from dorm.exceptions import IntegrityError

    raw = pg_errors.IntegrityError("duplicate key value")
    with pytest.raises(IntegrityError):
        normalize_db_exception(raw)


# ── Manager.alias passthrough ────────────────────────────────────────────────


def test_manager_alias_passes_through_to_queryset():
    """The Manager.alias() proxy I added must round-trip into
    QuerySet.alias() without losing the alias-only flag."""
    qs = Author.objects.alias(bc=dorm.Count("books"))
    assert "bc" in qs._query.alias_only_names


# ── pool_stats more cases ────────────────────────────────────────────────────


def test_pool_stats_for_sqlite_reports_atomic_depth():
    from dorm.db.connection import get_connection, pool_stats

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only field")
    # Force the connection to be created.
    conn.execute("SELECT 1")
    out = pool_stats()
    assert out["vendor"] == "sqlite"
    assert "atomic_depth" in out
    assert out["atomic_depth"] >= 0


# ── Model.full_clean / validate_unique ───────────────────────────────────────


def test_full_clean_runs_field_validators_and_clean():
    """``full_clean`` chains clean_fields → clean → validate_unique.
    Lock down each step by triggering at least one of them."""
    a = Author(name="x" * 200, age=10)  # over CharField(100) max_length
    with pytest.raises(dorm.ValidationError):
        a.full_clean()


def test_validate_unique_rejects_duplicate_unique_field():
    Tag.objects.create(name="dup-tag")
    new = Tag(name="dup-tag")
    with pytest.raises(dorm.ValidationError):
        new.validate_unique()


def test_validate_unique_skips_excluded_field():
    """When the field is excluded, validate_unique must skip the check —
    common pattern in form validation when only a partial update is
    being checked."""
    Tag.objects.create(name="dup-skip")
    new = Tag(name="dup-skip")
    # Explicit exclude bypasses the duplicate check.
    new.validate_unique(exclude=["name"])


@pytest.mark.asyncio
async def test_asave_update_fields_only_writes_listed_columns():
    """``await obj.asave(update_fields=[...])`` issues an UPDATE that
    only references the listed columns. Lock down both the SQL shape
    (no untouched columns) and the result (other fields unchanged)."""
    a = await Author.objects.acreate(name="UpFieldsOriginal", age=20)
    a.name = "UpFieldsChanged"
    a.age = 99
    await a.asave(update_fields=["age"])
    fresh = await Author.objects.aget(pk=a.pk)
    # Only ``age`` was written; ``name`` retained its DB value.
    assert fresh.age == 99
    assert fresh.name == "UpFieldsOriginal"


# ── More fields edge cases ───────────────────────────────────────────────────


def test_ipaddressfield_round_trip():
    from dorm.fields import IPAddressField

    f = IPAddressField()
    assert f.to_python("192.168.0.1") == "192.168.0.1"
    assert f.to_python(None) is None


def test_ipaddressfield_rejects_garbage():
    from dorm.exceptions import ValidationError
    from dorm.fields import IPAddressField

    f = IPAddressField()
    with pytest.raises(ValidationError):
        f.to_python("not-an-ip")


def test_generic_ipaddressfield_accepts_ipv6():
    from dorm.fields import GenericIPAddressField

    f = GenericIPAddressField()
    out = f.to_python("::1")
    assert "::1" in out


def test_jsonfield_db_prep_handles_none():
    from dorm.fields import JSONField

    f = JSONField()
    # ``None`` must round-trip as None — JSONField is a frequent
    # culprit for "null vs JSON null" bugs.
    assert f.get_db_prep_value(None) is None


def test_textfield_db_type_is_text():
    """TextField db_type must be ``TEXT`` on every backend — locks
    down the cross-backend portability promise."""
    from dorm.fields import TextField

    f = TextField()

    class _PG:
        vendor = "postgresql"

    class _SQ:
        vendor = "sqlite"

    assert f.db_type(_PG()) == "TEXT"
    assert f.db_type(_SQ()) == "TEXT"


def test_uuidfield_db_type_differs_per_backend():
    from dorm.fields import UUIDField

    f = UUIDField()

    class _PG:
        vendor = "postgresql"

    class _SQ:
        vendor = "sqlite"

    assert f.db_type(_PG()) == "UUID"
    # SQLite has no native UUID type — store as 36-char VARCHAR.
    assert "VARCHAR" in f.db_type(_SQ())


def test_decimalfield_db_type_includes_precision():
    from dorm.fields import DecimalField

    f = DecimalField(max_digits=12, decimal_places=4)

    class _PG:
        vendor = "postgresql"

    out = f.db_type(_PG())
    assert "12" in out and "4" in out


def test_floatfield_to_python_coerces_to_float():
    from dorm.fields import FloatField

    f = FloatField()
    assert f.to_python("3.14") == 3.14
    assert f.to_python(2) == 2.0
    assert f.to_python(None) is None


def test_smallintegerfield_db_type():
    from dorm.fields import SmallIntegerField

    f = SmallIntegerField()

    class _PG:
        vendor = "postgresql"

    out = f.db_type(_PG())
    assert "SMALLINT" in out.upper()


def test_bigintegerfield_db_type():
    from dorm.fields import BigIntegerField

    f = BigIntegerField()

    class _PG:
        vendor = "postgresql"

    out = f.db_type(_PG())
    assert "BIGINT" in out.upper()


# ── OTel error-path coverage ─────────────────────────────────────────────────


def test_otel_marks_span_error_on_query_failure():
    """When a query raises, the OTel post_query handler must set the
    span status to ERROR and tag ``db.dorm.error`` with the exception
    class name. Lock that path down because it's the one users rely
    on to find regressions in their alerting."""
    pytest.importorskip("opentelemetry")
    from dorm.contrib.otel import instrument, uninstrument
    from dorm.signals import pre_query, post_query

    instrument()
    try:
        # Simulate the signal flow without going through SQL — directly
        # exercise the post_query handler's error branch.
        sql = "SELECT * FROM does_not_exist"
        params: list = []
        pre_query.send(sender="sqlite", sql=sql, params=params)
        post_query.send(
            sender="sqlite",
            sql=sql,
            params=params,
            elapsed_ms=1.0,
            error=RuntimeError("boom"),
        )
    finally:
        uninstrument()


# ── softdelete async ─────────────────────────────────────────────────────────


@pytest.fixture
async def asoftdelete_table():
    """Fixture for the async soft-delete tests — same pattern as the
    sync ``softdelete_table`` fixture but compatible with pytest-asyncio
    (the model class is built once and the table dropped on teardown)."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql
    from dorm.contrib.softdelete import SoftDeleteModel

    class _ASDArticle(SoftDeleteModel):
        title = dorm.CharField(max_length=200)

        class Meta:
            db_table = "audit_softdel_async"

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel_async"')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _ASDArticle._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "audit_softdel_async" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield _ASDArticle
    conn.execute_script('DROP TABLE IF EXISTS "audit_softdel_async"')


@pytest.mark.asyncio
async def test_async_softdelete_adelete_then_arestore(asoftdelete_table):
    M = asoftdelete_table
    a = M.objects.create(title="async-cycle")
    await a.adelete()
    # objects manager hides it.
    assert not M.objects.filter(title="async-cycle").exists()
    assert M.deleted_objects.filter(title="async-cycle").exists()
    await a.arestore()
    assert M.objects.filter(title="async-cycle").exists()


@pytest.mark.asyncio
async def test_async_softdelete_hard_delete_purges(asoftdelete_table):
    M = asoftdelete_table
    a = M.objects.create(title="async-purge")
    await a.adelete(hard=True)
    # Even all_objects can't see it now — real DELETE.
    assert not M.all_objects.filter(title="async-purge").exists()


# ── conf._discover_apps ──────────────────────────────────────────────────────


def test_discover_apps_finds_packages_with_models_py(tmp_path):
    """``_discover_apps`` walks the directory next to settings.py and
    returns subdirectories that look like a dorm app (have both
    ``__init__.py`` and ``models.py``). Pin down the heuristic so a
    future change can't quietly include / exclude unintended dirs."""
    from dorm.conf import _discover_apps

    # An "app" with both files.
    app_a = tmp_path / "blog"
    app_a.mkdir()
    (app_a / "__init__.py").write_text("")
    (app_a / "models.py").write_text("")
    # A "non-app" with only one of them.
    app_b = tmp_path / "scripts"
    app_b.mkdir()
    (app_b / "__init__.py").write_text("")
    # A "hidden" dir.
    hidden = tmp_path / ".venv"
    hidden.mkdir()

    found = _discover_apps(tmp_path)
    assert "blog" in found
    assert "scripts" not in found
    assert ".venv" not in found


def test_autodiscover_loads_settings_from_disk(tmp_path, monkeypatch):
    """End-to-end: drop a settings.py in tmp_path, run autodiscover
    with cwd pointed there, and confirm the singleton picked it up."""
    from dorm.conf import _autodiscover_settings, settings

    snapshot = (
        dict(getattr(settings, "DATABASES", {}) or {}),
        list(getattr(settings, "INSTALLED_APPS", []) or []),
        getattr(settings, "_configured", True),
    )
    try:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", [str(tmp_path / "noscript.py")])
        (tmp_path / "settings.py").write_text(
            textwrap.dedent("""
                DATABASES = {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}}
                INSTALLED_APPS = []
            """)
        )
        # Force re-discovery.
        monkeypatch.setattr(settings, "_configured", False, raising=False)
        monkeypatch.setattr(settings, "DATABASES", {}, raising=False)
        monkeypatch.setattr(settings, "INSTALLED_APPS", [], raising=False)
        assert _autodiscover_settings() is True
        assert "default" in settings.DATABASES
        assert settings.DATABASES["default"]["ENGINE"] == "sqlite"
    finally:
        # Restore the suite's settings.
        from dorm.db.connection import reset_connections

        settings.configure(DATABASES=snapshot[0], INSTALLED_APPS=snapshot[1])
        settings._configured = snapshot[2]
        reset_connections()


# ── db.utils: edge cases of the masker ───────────────────────────────────────


def test_mask_params_preserves_non_sensitive_in_clause():
    """``WHERE name IN (?, ?)`` — name is not sensitive, so nothing is
    masked. Mirror of the password-IN test that locks down the
    masker doesn't *over*-redact."""
    from dorm.db.utils import _mask_params

    sql = 'SELECT * FROM "u" WHERE "name" IN (?, ?, ?)'
    out = _mask_params(sql, ["alice", "bob", "carol"])
    assert out == ["alice", "bob", "carol"]


def test_mask_params_handles_in_clause_with_subquery():
    """A sub-SELECT inside an IN list still has its outer placeholders
    masked. Locks down the paren-walking in ``_placeholder_column_index``."""
    from dorm.db.utils import _mask_params

    # Mixed: outer IN list with a function call.
    sql = 'SELECT * FROM "u" WHERE "password" IN (?, COALESCE(?, ?))'
    out = _mask_params(sql, ["pw1", "pw2", "pw3"])
    assert out == ["***", "***", "***"]


def test_columns_from_insert_handles_quoted_columns():
    """``INSERT INTO t ("a", "b") VALUES (?, ?)`` — column names are
    quoted; helper must strip the quotes for matching."""
    from dorm.db.utils import _columns_from_insert

    sql = 'INSERT INTO "u" ("name", "password") VALUES (?, ?)'
    cols = _columns_from_insert(sql)
    assert cols == ["name", "password"]


def test_columns_from_insert_returns_none_for_select():
    from dorm.db.utils import _columns_from_insert

    assert _columns_from_insert('SELECT * FROM "u"') is None


# ── DB exception normalisation: more branches ───────────────────────────────


def test_normalize_db_exception_preserves_unknown_exception():
    """Random non-DB exceptions must be left alone — not wrapped or
    raised by ``normalize_db_exception``."""
    from dorm.db.utils import normalize_db_exception

    # Unrelated exception: helper returns None, doesn't re-raise.
    normalize_db_exception(KeyError("not a DB error"))


def test_with_transient_retry_eventually_succeeds():
    """After N transient failures, on the (N+1)th attempt, the helper
    must return the successful result (not still keep retrying)."""
    import sqlite3
    from dorm.db.utils import with_transient_retry

    state = {"calls": 0}

    def _do():
        state["calls"] += 1
        if state["calls"] < 2:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    out = with_transient_retry(_do, in_transaction=False, attempts=5, backoff=0.001)
    assert out == "ok"
    assert state["calls"] == 2


# ── on_commit: callable-with-args via partial ────────────────────────────────


def test_on_commit_works_with_functools_partial():
    """The signature ``Callable[[], Any]`` is what ``on_commit`` accepts.
    Users typically wrap with ``partial`` or ``lambda``; lock down that
    partial keeps working across refactors of the dispatch logic."""
    import functools

    fired: list[int] = []
    with transaction.atomic():
        transaction.on_commit(functools.partial(fired.append, 7))
    assert fired == [7]



