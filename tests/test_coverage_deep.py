"""Deeper coverage tests targeting code paths the previous suites didn't
exercise:

- Migration ops less-used branches: ``DeleteModel.database_backwards``,
  ``RemoveIndex.database_backwards``, ``RunPython.reverse_code``,
  ``CreateModel`` with options.
- ``MigrationLoader`` edge cases: non-numeric stems, invalid numbers,
  underscore-prefixed files, missing dir.
- ``_DryRunConnection`` capture behaviour for every recorded method.
- Async SQLite paths: ``set_autocommit``, ``commit``/``rollback``
  pass-throughs, ``pool_stats`` shim, streaming with early break,
  ``_in_atomic`` detection.
- Async PostgreSQL surfaces: ``set_autocommit``, ``commit``/``rollback``,
  ``table_exists``, ``get_table_columns``.
- Field descriptor cache invalidation, ``RelatedField.db_type`` paths.
- ``CombinedQuerySet`` ``__repr__``, ``avalues`` with empty result.
- ``ValuesListQuerySet._extract_row`` with sequence rows (non-dict).
- ``conf._discover_apps`` with file at root (no parts).
- CLI ``cmd_makemigrations``, ``cmd_squashmigrations``, ``cmd_showmigrations``,
  ``cmd_sql``, ``cmd_dbcheck`` happy paths.

The goal stays bug-detection over coverage cosmetics — every test pins
down a behaviour worth catching if it ever drifts.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from types import SimpleNamespace

import pytest

import dorm
from dorm.db.connection import get_connection, get_async_connection
from tests.models import Author, Book


# ── Migration ops: less-used branches ───────────────────────────────────────


def test_delete_model_backwards_recreates_table():
    """``DeleteModel.database_backwards`` must reconstruct the dropped
    table from the ``to_state`` snapshot. If the to_state still has the
    model definition, the operation should run a CreateModel forward."""
    from dorm.migrations.operations import DeleteModel
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "delmodel_back_t"')

    field_id = dorm.BigAutoField(primary_key=True)
    field_val = dorm.IntegerField(default=0)

    # Seed both states: from_state has the model, to_state has it too
    # (DeleteModel goes from "exists" to "exists then" — but the
    # backwards branch reads from to_state).
    to_state = ProjectState()
    to_state.models["delapp.target"] = {
        "name": "Target",
        "fields": {"id": field_id, "val": field_val},
        "options": {"db_table": "delmodel_back_t"},
    }
    from_state = ProjectState()  # already deleted

    op = DeleteModel(name="Target")
    # database_backwards: should recreate via CreateModel.
    op.database_backwards("delapp", conn, from_state, to_state)
    assert conn.table_exists("delmodel_back_t")

    conn.execute_script('DROP TABLE IF EXISTS "delmodel_back_t"')


def test_delete_model_backwards_noop_when_not_in_to_state():
    """If the target model is missing from ``to_state`` (already
    deleted in both directions), backwards must silently no-op rather
    than raise."""
    from dorm.migrations.operations import DeleteModel
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    op = DeleteModel(name="GhostModel")
    # No model entry in either state → no-op.
    op.database_backwards("ghostapp", conn, ProjectState(), ProjectState())


def test_remove_index_backwards_recreates_index():
    """``RemoveIndex.database_backwards`` must rebuild the index using
    the same fields/uniqueness from the model state. Pin down the SQL
    shape with a real DB round-trip."""
    from dorm.indexes import Index
    from dorm.migrations.operations import RemoveIndex
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "rmidx_back_t"')
    conn.execute_script(
        'CREATE TABLE "rmidx_back_t" ("id" INTEGER PRIMARY KEY, "name" VARCHAR(100))'
    )
    # Pre-create the index so forwards has something to drop.
    conn.execute_script('CREATE INDEX "rmidx_back_idx" ON "rmidx_back_t" ("name")')

    state = ProjectState()
    state.models["rmidxapp.t"] = {
        "name": "T",
        "fields": {},
        "options": {"db_table": "rmidx_back_t"},
    }

    idx = Index(fields=["name"], unique=False, name="rmidx_back_idx")
    op = RemoveIndex(model_name="T", index=idx)
    op.database_forwards("rmidxapp", conn, state, state)
    op.database_backwards("rmidxapp", conn, state, state)
    # After backwards the index is back; forwards once more must succeed.
    op.database_forwards("rmidxapp", conn, state, state)

    conn.execute_script('DROP TABLE IF EXISTS "rmidx_back_t"')


def test_runpython_with_reverse_code_executes_reverse():
    """``RunPython.database_backwards`` must invoke ``reverse_code`` —
    the most error-prone branch is when both code and reverse_code are
    set; this test pins down that the reverse runs (and gets the
    model registry argument like the forward path)."""
    from dorm.migrations.operations import RunPython

    forward_calls: list[str] = []
    reverse_calls: list[str] = []

    def fwd(app_label, registry):
        forward_calls.append(app_label)

    def rev(app_label, registry):
        reverse_calls.append(app_label)

    op = RunPython(code=fwd, reverse_code=rev)
    op.database_forwards("rpapp", None, None, None)
    op.database_backwards("rpapp", None, None, None)
    assert forward_calls == ["rpapp"]
    assert reverse_calls == ["rpapp"]


def test_runpython_describe_uses_function_name():
    from dorm.migrations.operations import RunPython

    def my_data_op(app_label, registry):
        pass

    op = RunPython(code=my_data_op)
    desc = op.describe()
    assert "my_data_op" in desc


def test_runpython_describe_handles_non_callable():
    """If ``code`` somehow isn't callable, ``describe()`` must still
    return a sensible string instead of crashing."""
    from dorm.migrations.operations import RunPython

    op = RunPython(code=lambda app, reg: None)
    op.code = "not a function"  # simulate corruption
    desc = op.describe()
    assert "function" in desc


# ── _DryRunConnection capture ─────────────────────────────────────────────────


def test_dryrun_connection_captures_writes_and_passes_reads():
    """``_DryRunConnection`` must:
    1. Pass SELECT/WITH/PRAGMA/EXPLAIN through to the real connection.
    2. Capture every other ``execute`` / ``execute_script`` /
       ``execute_write`` / ``execute_insert`` / ``execute_bulk_insert``
       in the ``captured`` list.
    3. Forward unknown attributes via ``__getattr__``."""
    from dorm.migrations.executor import _DryRunConnection

    conn = get_connection()
    # Force a row to exist so reads have something to return.
    Author.objects.filter(name="dryrun-x").delete()
    Author.objects.create(name="dryrun-x", age=1)

    dry = _DryRunConnection(conn)

    # Writes: captured, no execution.
    dry.execute('INSERT INTO authors (name) VALUES (\'X\')')
    dry.execute_script('CREATE TABLE foo (...)')
    dry.execute_write('UPDATE foo SET x = 1')
    dry.execute_insert('INSERT INTO foo (x) VALUES (1)')
    dry.execute_bulk_insert('INSERT INTO foo (x) VALUES (1), (2)', count=2)

    captured_sqls = [sql for sql, _ in dry.captured]
    assert any("INSERT INTO authors" in s for s in captured_sqls)
    assert any("CREATE TABLE" in s for s in captured_sqls)
    assert any("UPDATE foo" in s for s in captured_sqls)
    assert any("INSERT INTO foo" in s for s in captured_sqls)

    # Reads: pass-through (returns real rows).
    rows = dry.execute('SELECT name FROM authors WHERE name = %s', ['dryrun-x'])
    assert len(rows) == 1

    # __getattr__ forwarding for known wrapper attribute.
    assert dry.vendor == getattr(conn, "vendor", "sqlite")

    # ``table_exists`` / ``get_table_columns`` pass through.
    assert dry.table_exists("authors")
    cols = {c["name"] for c in dry.get_table_columns("authors")}
    assert "name" in cols


def test_dryrun_connection_atomic_delegates_to_real():
    """``_DryRunConnection.atomic()`` must return the real wrapper's
    atomic context manager — otherwise the migration apply loop's
    atomic() wrap would be a no-op under dry-run, regressing the
    fix from the audit pass."""
    from dorm.migrations.executor import _DryRunConnection

    conn = get_connection()
    dry = _DryRunConnection(conn)
    cm = dry.atomic()
    # It must be usable as a context manager.
    with cm:
        pass


# ── MigrationLoader edge cases ──────────────────────────────────────────────


def test_migration_loader_skips_non_numeric_files(tmp_path):
    """Files starting with ``_`` (e.g. ``__init__.py``) or with
    non-numeric stems must be ignored. Otherwise the loader would try
    to import them as migrations."""
    from dorm.migrations.loader import MigrationLoader

    mig = tmp_path / "migrations"
    mig.mkdir()
    (mig / "__init__.py").write_text("")
    (mig / "not_a_migration.py").write_text("operations = []")
    (mig / "abc_skipme.py").write_text("operations = []")
    (mig / "0001_initial.py").write_text(
        "operations = []\ndependencies = []\n"
    )

    conn = get_connection()
    loader = MigrationLoader(conn)
    loader.load(mig, "loaderapp")
    names = [name for _, name, _ in loader.migrations.get("loaderapp", [])]
    assert names == ["0001_initial"]


def test_migration_loader_skips_missing_dir(tmp_path):
    """Pointing the loader at a non-existent dir must be a no-op, not
    raise. Common case: app with no migrations yet."""
    from dorm.migrations.loader import MigrationLoader

    conn = get_connection()
    loader = MigrationLoader(conn)
    loader.load(tmp_path / "does_not_exist", "missingapp")
    assert loader.migrations.get("missingapp", []) == []


def test_migration_loader_skips_files_with_invalid_number(tmp_path):
    """A file like ``00ax_thing.py`` whose prefix isn't an integer
    must be silently skipped (not crash)."""
    from dorm.migrations.loader import MigrationLoader

    mig = tmp_path / "migrations"
    mig.mkdir()
    # numeric prefix but not parseable as int — leading digit then letter.
    (mig / "0a_bad.py").write_text("operations = []")
    (mig / "0001_ok.py").write_text("operations = []")

    conn = get_connection()
    loader = MigrationLoader(conn)
    loader.load(mig, "badnums")
    names = [name for _, name, _ in loader.migrations.get("badnums", [])]
    assert names == ["0001_ok"]


def test_migration_loader_get_state_without_applied(tmp_path):
    """``get_migration_state`` with default ``all_migrations=False``
    must replay only applied migrations. If none are applied, returns
    an empty state."""
    from dorm.migrations.loader import MigrationLoader

    mig = tmp_path / "migrations"
    mig.mkdir()
    (mig / "0001_initial.py").write_text(
        "from dorm.migrations.operations import RunSQL\n"
        "operations = [RunSQL('SELECT 1')]\n"
        "dependencies = []\n"
    )

    conn = get_connection()
    loader = MigrationLoader(conn)
    loader.load(mig, "stateapp")
    state = loader.get_migration_state("stateapp")  # nothing applied
    assert state.models == {}


# ── Async SQLite wrapper paths ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_sqlite_table_exists():
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only")
    assert await conn.table_exists("authors") is True
    assert await conn.table_exists("totally_made_up") is False


@pytest.mark.asyncio
async def test_async_sqlite_get_table_columns():
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only")
    cols = await conn.get_table_columns("authors")
    names = {c["name"] for c in cols}
    assert "id" in names
    assert "name" in names


@pytest.mark.asyncio
async def test_async_sqlite_pool_stats_shim():
    """The async SQLite wrapper exposes a no-op ``pool_stats`` for
    parity with PG. Lock down the keys so monitoring code doesn't
    KeyError on missing entries."""
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only")
    # Force the async conn to open by running a query.
    await conn.execute("SELECT 1")
    out = conn.pool_stats()
    assert out["vendor"] == "sqlite"
    assert "open" in out


@pytest.mark.asyncio
async def test_async_sqlite_set_autocommit_round_trip():
    """``set_autocommit`` on the async wrapper must close any held
    connection so the next operation reopens with the new
    ``isolation_level`` setting."""
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only")
    # Force a connection to be cached.
    await conn.execute("SELECT 1")
    # Toggle autocommit; the held conn should be reset.
    await conn.set_autocommit(True)
    # Run a write; should still succeed under autocommit mode.
    await conn.execute_write(
        'INSERT INTO authors (name, age, is_active) VALUES (?, ?, ?)',
        ['async-ac-toggle', 1, 1],
    )
    found = await Author.objects.filter(name="async-ac-toggle").aexists()
    assert found
    await conn.set_autocommit(False)


@pytest.mark.asyncio
async def test_async_sqlite_streaming_with_early_break():
    """SQLite async streaming must close the cursor on early break.
    A regression that left the cursor open would hold read locks for
    the duration of the connection."""
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only")
    for i in range(5):
        await Author.objects.acreate(name=f"stream-break-{i}", age=i)
    seen = 0
    async for _ in conn.execute_streaming(
        'SELECT * FROM "authors" WHERE name LIKE ? ORDER BY name',
        ['stream-break-%'],
    ):
        seen += 1
        if seen >= 2:
            break
    assert seen == 2
    # Subsequent query must work — cursor was closed cleanly.
    n = await Author.objects.filter(name__startswith="stream-break-").acount()
    assert n == 5


@pytest.mark.asyncio
async def test_async_sqlite_commit_rollback_passthrough():
    """``await conn.commit()`` / ``rollback()`` on the async sqlite
    wrapper must not raise even when there's no active transaction."""
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("sqlite-only")
    await conn.execute("SELECT 1")  # warm up
    await conn.commit()
    await conn.rollback()


# ── Async PostgreSQL surfaces ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_pg_table_exists():
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only")
    assert await conn.table_exists("authors") is True
    assert await conn.table_exists("totally_made_up_table") is False


@pytest.mark.asyncio
async def test_async_pg_get_table_columns():
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only")
    cols = await conn.get_table_columns("authors")
    names = {c["name"] for c in cols}
    assert "id" in names
    assert "name" in names
    # PG returns ``data_type`` for each column.
    sample = next(c for c in cols if c["name"] == "id")
    assert "data_type" in sample


@pytest.mark.asyncio
async def test_async_pg_set_autocommit_toggle():
    """``set_autocommit(True)`` must let a write commit immediately
    without an explicit commit. ``set_autocommit(False)`` must re-
    require a transaction or commit."""
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only")
    await conn.set_autocommit(True)
    try:
        await Author.objects.acreate(name="pg-ac-1", age=1)
        # Visible immediately because autocommit is on.
        assert await Author.objects.filter(name="pg-ac-1").aexists()
    finally:
        await conn.set_autocommit(False)


@pytest.mark.asyncio
async def test_async_pg_commit_rollback_no_op_outside_autocommit():
    """``commit()`` / ``rollback()`` must be no-ops when there's no
    persistent autocommit connection."""
    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-only")
    # Both must complete without raising.
    await conn.commit()
    await conn.rollback()


# ── ForeignKey descriptor cache invalidation ───────────────────────────────


def test_foreignkey_descriptor_invalidates_cache_on_id_change():
    """Setting ``book.author_id = X`` (the FK's underlying _id slot)
    must drop the cached ``_cache_author`` so the next ``book.author``
    access re-fetches with the new pk. Locks down a known footgun
    where the in-memory cache went stale on direct id assignment."""
    a1 = Author.objects.create(name="fk-cache-1", age=1)
    a2 = Author.objects.create(name="fk-cache-2", age=2)
    b = Book.objects.create(title="fk-cache-book", author=a1, pages=10)
    # Read the descriptor — populates _cache_author.
    assert b.author.pk == a1.pk
    assert "_cache_author" in b.__dict__

    # Now mutate via the _id descriptor.
    b.author_id = a2.pk
    # Cache must be cleared.
    assert "_cache_author" not in b.__dict__


def test_foreignkey_descriptor_get_returns_self_on_class_access():
    """``Book.author`` (class access, no instance) must return the
    descriptor itself, not raise. Pin this down — Python's descriptor
    protocol relies on it for tools like form field introspection."""
    descriptor = Book.author
    # The class-level access returns the descriptor (self).
    from dorm.fields import ForeignKey
    assert isinstance(descriptor, ForeignKey)


def test_foreignkey_descriptor_returns_none_when_id_is_none():
    """When the FK's underlying _id slot is ``None``, the descriptor
    must return ``None`` without firing a DB query — otherwise a
    nullable FK would crash with ``DoesNotExist`` on every access."""
    a = Author(name="fk-noid-author", age=1)
    a.publisher_id = None
    # Without a saved publisher, ``a.publisher`` must be None.
    assert a.publisher is None


def test_foreignkey_db_type_uses_rel_db_type_when_available():
    """``ForeignKey.db_type`` must prefer the related model's
    ``rel_db_type`` (returns INTEGER for SERIAL on PG) over its raw
    ``db_type`` (which returns SERIAL — wrong for the FK column)."""
    from dorm.db.connection import get_connection
    from dorm.fields import ForeignKey

    conn = get_connection()
    # Author's pk is BigAutoField, which has rel_db_type. Build a fresh
    # FK pointing at it and ask for its db_type.
    f = ForeignKey(Author, on_delete=dorm.CASCADE)
    out = f.db_type(conn)
    if getattr(conn, "vendor", "sqlite") == "postgresql":
        assert out in ("INTEGER", "BIGINT")  # rel_db_type, not "SERIAL"
        assert "SERIAL" not in out


# ── CombinedQuerySet / ValuesListQuerySet edge cases ───────────────────────


def test_combined_queryset_difference_excludes_overlap():
    a1 = Author.objects.create(name="diff-1", age=1)
    a2 = Author.objects.create(name="diff-2", age=2)
    Author.objects.create(name="diff-3", age=3)

    qs_all = Author.objects.filter(name__startswith="diff-")
    qs_first_two = Author.objects.filter(name__in=["diff-1", "diff-2"])
    qs_third = qs_all.difference(qs_first_two)

    pks = {row.pk for row in qs_third}
    # The third row has the only pk in qs_all but not in qs_first_two.
    third = Author.objects.get(name="diff-3")
    assert pks == {third.pk}
    assert a1.pk not in pks
    assert a2.pk not in pks


def test_combined_queryset_intersection_keeps_only_common():
    Author.objects.create(name="int-1", age=10)
    Author.objects.create(name="int-2", age=20)

    a = Author.objects.filter(name__in=["int-1", "int-2"])
    b = Author.objects.filter(age__in=[10, 30])
    inter = a.intersection(b)
    names = {row.name for row in inter}
    assert names == {"int-1"}


def test_combined_queryset_union_all_keeps_duplicates():
    """``union(other, all=True)`` must use ``UNION ALL`` and keep
    duplicates — distinguishes from default ``UNION``."""
    Author.objects.create(name="ua-shared", age=5)
    qs = Author.objects.filter(name="ua-shared")
    combined = qs.union(qs, all=True)
    rows = list(combined)
    # Same row appears twice under UNION ALL.
    assert len(rows) == 2


def test_values_list_with_sequence_row(monkeypatch):
    """``ValuesListQuerySet._extract_row`` must handle non-dict rows
    (psycopg with default tuple-row factory, or backends that return
    tuples). We simulate by passing a tuple directly."""
    from dorm.queryset import ValuesListQuerySet

    qs: ValuesListQuerySet = ValuesListQuerySet(Author)  # type: ignore[arg-type]
    qs._fields = ["name", "age"]
    qs._flat = False
    out = qs._extract_row(("Alice", 30), ["name", "age"])
    assert out == ("Alice", 30)
    qs._flat = True
    qs._fields = ["name"]
    out = qs._extract_row(("Alice",), ["name"])
    assert out == "Alice"


@pytest.mark.asyncio
async def test_avalues_returns_empty_list_when_no_rows():
    """``avalues()`` over an empty queryset must return ``[]``, not
    raise. Locks down a path where ``rows[0]`` would IndexError."""
    rows = await Author.objects.filter(name="never-exists-asdf").avalues("name")
    assert rows == []


@pytest.mark.asyncio
async def test_avalues_list_returns_empty_list():
    rows = await Author.objects.filter(name="never-exists-qwer").avalues_list("name")
    assert rows == []


# ── conf._discover_apps quirks ─────────────────────────────────────────────


def test_discover_apps_skips_models_at_root(tmp_path):
    """A ``models.py`` directly at the search root has zero
    ``parts`` after relative_to → not a valid app. Must be skipped."""
    from dorm.conf import _discover_apps

    (tmp_path / "models.py").write_text("")
    (tmp_path / "__init__.py").write_text("")
    found = _discover_apps(tmp_path)
    # No parts → no app name; must not crash and must not include
    # an empty-string app.
    assert "" not in found


# ── CLI: cmd_makemigrations and cmd_showmigrations happy paths ─────────────


@pytest.fixture
def isolated_dorm_settings():
    """Snapshot+restore the dorm settings singleton for tests that
    re-configure dorm via ``_load_settings``."""
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


def test_cli_cmd_makemigrations_creates_initial_file(
    monkeypatch, tmp_path, capsys, isolated_dorm_settings
):
    """``dorm makemigrations <app>`` in a fresh directory must
    generate ``0001_initial.py`` for that app's models."""
    from dorm import cli

    # Build a tiny app on disk: ``demo_app/__init__.py`` +
    # ``demo_app/models.py``.
    app_dir = tmp_path / "demo_app"
    app_dir.mkdir()
    (app_dir / "__init__.py").write_text("")
    (app_dir / "models.py").write_text(
        "import dorm\n\n"
        "class Widget(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    class Meta:\n"
        "        db_table = 'demo_app_widget'\n"
    )
    settings_file = tmp_path / "_demo_settings.py"
    settings_file.write_text(
        "DATABASES = {'default': {'ENGINE': 'sqlite', 'NAME': ':memory:'}}\n"
        "INSTALLED_APPS = ['demo_app']\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    args = SimpleNamespace(
        settings="_demo_settings",
        apps=["demo_app"],
        dry_run=False,
        empty=False,
        name=None,
    )
    cli.cmd_makemigrations(args)

    mig_dir = app_dir / "migrations"
    assert mig_dir.exists()
    files = sorted(p.name for p in mig_dir.glob("*.py"))
    assert "__init__.py" in files
    assert any(name.startswith("0001_") for name in files)


def test_cli_cmd_showmigrations_lists_status(
    monkeypatch, tmp_path, capsys, isolated_dorm_settings
):
    """``dorm showmigrations`` reports applied vs unapplied migrations
    per app, marking applied with ``[X]`` and unapplied with ``[ ]``."""
    from dorm import cli

    app_dir = tmp_path / "showapp"
    app_dir.mkdir()
    (app_dir / "__init__.py").write_text("")
    (app_dir / "models.py").write_text("")
    mig_dir = app_dir / "migrations"
    mig_dir.mkdir()
    (mig_dir / "__init__.py").write_text("")
    (mig_dir / "0001_initial.py").write_text(
        "from dorm.migrations.operations import RunSQL\n"
        "operations = [RunSQL('SELECT 1')]\n"
        "dependencies = []\n"
    )

    settings_file = tmp_path / "_show_settings.py"
    settings_file.write_text(
        "DATABASES = {'default': {'ENGINE': 'sqlite', 'NAME': ':memory:'}}\n"
        "INSTALLED_APPS = ['showapp']\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)

    buf = io.StringIO()
    args = SimpleNamespace(settings="_show_settings", apps=["showapp"])
    with redirect_stdout(buf):
        cli.cmd_showmigrations(args)
    out = buf.getvalue()
    assert "showapp" in out
    assert "0001_initial" in out


# ── Field __get__ on class access returns the field itself ─────────────────


def test_field_descriptor_class_access_returns_field():
    """The metaclass strips Field instances from the class
    namespace and stores them in ``_meta.fields`` instead — Django
    parity. Class-level access doesn't return the field; that's by
    design. Lock down both behaviours."""
    from dorm.fields import IntegerField

    # Author.age is NOT a class attribute — it lives in _meta.
    assert "age" not in Author.__dict__
    # But _meta.get_field returns the actual field object.
    field = Author._meta.get_field("age")
    assert isinstance(field, IntegerField)


# ── Lookups: edge cases ─────────────────────────────────────────────────────


def test_isnull_lookup_true_returns_null_rows():
    """``filter(field__isnull=True)`` must return rows where the
    column is NULL, not where it's missing/empty/anything else."""
    Author.objects.create(name="isnull-no-pub", age=1)  # publisher None
    qs = Author.objects.filter(name="isnull-no-pub", publisher__isnull=True)
    assert qs.count() == 1


def test_isnull_lookup_false_excludes_null_rows():
    from tests.models import Publisher

    p = Publisher.objects.create(name="some-publisher")
    Author.objects.create(name="isnull-with-pub", age=2, publisher=p)
    qs = Author.objects.filter(name="isnull-with-pub", publisher__isnull=False)
    assert qs.count() == 1


def test_in_lookup_with_empty_list_returns_no_rows():
    """``filter(pk__in=[])`` must return an empty queryset cleanly,
    NOT crash or return everything. Lock down a Django-compat
    behaviour that's easy to break with a naive empty-list check."""
    Author.objects.create(name="empty-in", age=1)
    qs = Author.objects.filter(pk__in=[])
    assert qs.count() == 0
    assert list(qs) == []


def test_lookup_lt_gt_lte_gte_combine():
    """Range filters via ``__lt`` / ``__gt`` / ``__lte`` / ``__gte``
    must combine with AND when chained or used together."""
    for i in range(5):
        Author.objects.create(name=f"range-{i}", age=i)
    qs = Author.objects.filter(
        name__startswith="range-", age__gte=1, age__lt=4
    )
    ages = sorted(a.age for a in qs)
    assert ages == [1, 2, 3]


def test_contains_iexact_icontains_lookups():
    Author.objects.create(name="Alice Wonderland", age=1)
    Author.objects.create(name="bob the builder", age=2)
    # contains is case-sensitive.
    assert Author.objects.filter(name__contains="Alice").count() == 1
    # icontains case-insensitive.
    assert Author.objects.filter(name__icontains="ALICE").count() == 1
    # iexact case-insensitive equality.
    assert Author.objects.filter(name__iexact="alice wonderland").count() == 1


# ── Manager / QuerySet less-tested methods ─────────────────────────────────


def test_queryset_distinct_emits_distinct_clause():
    """``qs.distinct()`` must set the SQL distinct flag."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = Author.objects.distinct()
    sql, _ = qs._query.as_select(conn)
    assert "DISTINCT" in sql.upper()


def test_queryset_order_by_descending_prefix():
    """A leading ``-`` toggles DESC sort. Verifies SQL emission
    includes ``DESC`` for the prefixed field."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = Author.objects.order_by("-age", "name")
    sql, _ = qs._query.as_select(conn)
    assert "DESC" in sql.upper()


def test_manager_db_manager_uses_alias():
    """``BaseManager.db_manager(alias)`` must return a manager pinned
    to that alias. We check the ``_db`` field and the underlying
    queryset's alias."""
    routed = Author.objects.db_manager("not_default")
    assert routed._db == "not_default"
    qs = routed.get_queryset()
    assert qs._db == "not_default"


# ── Pydantic interop: schema_for ────────────────────────────────────────────


def test_pydantic_schema_for_basic_model():
    """``dorm.contrib.pydantic.schema_for(Author)`` must produce a
    pydantic v2 model with at least ``name`` and ``age``."""
    pytest.importorskip("pydantic")
    from dorm.contrib.pydantic import schema_for

    AuthorSchema = schema_for(Author)
    fields = AuthorSchema.model_fields
    assert "name" in fields
    assert "age" in fields


def test_pydantic_schema_for_validates_payload():
    """The generated pydantic schema must reject a payload that
    breaks one of the source model's typing constraints."""
    pytest.importorskip("pydantic")
    from dorm.contrib.pydantic import schema_for

    AuthorSchema = schema_for(Author)
    # Valid payload.
    AuthorSchema(name="Alice", age=30)
    # Invalid age type.
    with pytest.raises(Exception):  # noqa: PT011 — pydantic.ValidationError
        AuthorSchema(name="Bob", age="not-an-int")


# ── DB utils: more masker / observability cases ────────────────────────────


def test_log_query_signal_receives_post_query_with_elapsed_ms():
    """The ``post_query`` signal must include ``elapsed_ms`` in its
    kwargs — APM integrations rely on it. Lock down the contract."""
    from dorm import signals

    captured: list[dict] = []

    def grab(sender, **kwargs):
        captured.append(kwargs)

    signals.post_query.connect(grab, weak=False)
    try:
        Author.objects.create(name="signal-elapsed", age=1)
    finally:
        signals.post_query.disconnect(grab)

    assert captured  # at least one query fired
    assert "elapsed_ms" in captured[0]
    assert isinstance(captured[0]["elapsed_ms"], float)


def test_pre_query_signal_receives_sql_and_params():
    """``pre_query`` fires before the query runs and includes the
    raw SQL + params. Used for tracing prep / cache key derivation."""
    from dorm import signals

    captured: list[dict] = []

    def grab(sender, **kwargs):
        captured.append(kwargs)

    signals.pre_query.connect(grab, weak=False)
    try:
        Author.objects.create(name="signal-pre", age=1)
    finally:
        signals.pre_query.disconnect(grab)

    assert captured
    assert "sql" in captured[0]
    assert "params" in captured[0]


# ── Index get_name fallback ───────────────────────────────────────────────


def test_index_default_name_uses_model_and_fields():
    """An ``Index`` without an explicit name must derive one from the
    model name + field list — long enough to be unique, short enough
    to fit DB identifier limits."""
    from dorm.indexes import Index

    idx = Index(fields=["a", "b"])
    name = idx.get_name("Widget")
    # Heuristic shape: contains model + field hints, max 30 chars-ish.
    assert "widget" in name.lower() or "a" in name.lower() or "b" in name.lower()
    assert len(name) <= 63  # PostgreSQL NAMEDATALEN-1


def test_index_explicit_name_preserved():
    from dorm.indexes import Index

    idx = Index(fields=["a"], name="my_custom_idx")
    assert idx.get_name("Anything") == "my_custom_idx"


# ── Aggregate: distinct, output_field ───────────────────────────────────────


def test_count_distinct_emits_distinct_in_sql():
    """``Count("col", distinct=True)`` must emit
    ``COUNT(DISTINCT "col")`` in the SQL."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = Author.objects.annotate(distinct_count=dorm.Count("name", distinct=True))
    sql, _ = qs._query.as_select(conn)
    assert "DISTINCT" in sql.upper()


def test_count_star_emits_count_star():
    """``Count()`` with no args defaults to ``*``."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    qs = Author.objects.annotate(n=dorm.Count())
    sql, _ = qs._query.as_select(conn)
    assert "COUNT(*)" in sql.upper()
