"""Tests for the `dorm` CLI: __main__ entry point, settings loading,
app autodiscovery warnings, import-error surfacing, and the daemon-thread
defensive warning for aiosqlite."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


def _run(args: list[str], cwd: Path, env_extra: dict[str, str] | None = None):
    """Run a subprocess with the repo importable, capturing stdout+stderr."""
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + pp if pp else "")
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        args,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


# ── Fix 1: `python -m dorm` works ─────────────────────────────────────────────


def test_python_m_dorm_runs_help(tmp_path: Path):
    """`python -m dorm help` should exit 0 and print the command list."""
    result = _run([sys.executable, "-m", "dorm", "help"], cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "makemigrations" in result.stdout
    assert "migrate" in result.stdout
    assert "init" in result.stdout


# ── Fix 2: warning when autodiscovery finds no apps ───────────────────────────


def test_warns_when_no_apps_autodiscovered(tmp_path: Path):
    """Running a CLI command in a settings dir with no apps should warn,
    not fail silently."""
    (tmp_path / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
    )
    result = _run(
        [sys.executable, "-m", "dorm", "showmigrations"],
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "no apps detected" in result.stderr.lower()


def test_no_warning_when_apps_present(tmp_path: Path):
    """If at least one app is autodiscovered, no warning should be printed."""
    (tmp_path / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
    )
    app_dir = tmp_path / "blog"
    app_dir.mkdir()
    (app_dir / "__init__.py").touch()
    (app_dir / "models.py").write_text(
        "import dorm\n\nclass Post(dorm.Model):\n    title = dorm.CharField(max_length=100)\n"
    )
    result = _run(
        [sys.executable, "-m", "dorm", "showmigrations"],
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "no apps detected" not in result.stderr.lower()


# ── Fix 3: settings_dir + parent both go on sys.path ──────────────────────────


def test_load_settings_path_for_dotted_module(tmp_path: Path):
    """When settings is imported via a dotted module path (myproj.settings)
    and we run dorm from the parent, both layouts must work."""
    pkg = tmp_path / "myproj"
    pkg.mkdir()
    (pkg / "__init__.py").touch()
    (pkg / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
        'INSTALLED_APPS = ["myproj.shop"]\n'
    )
    shop = pkg / "shop"
    shop.mkdir()
    (shop / "__init__.py").touch()
    (shop / "models.py").write_text(
        "import dorm\n\nclass Item(dorm.Model):\n    name = dorm.CharField(max_length=50)\n"
    )

    result = _run(
        [sys.executable, "-m", "dorm", "makemigrations", "--settings", "myproj.settings"],
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert (shop / "migrations" / "0001_initial.py").exists()


def test_load_settings_path_for_flat_layout(tmp_path: Path):
    """Flat layout (settings.py + apps as siblings) must also work when
    the user runs dorm from a directory other than cwd via --settings."""
    proj = tmp_path / "flat"
    proj.mkdir()
    (proj / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
        'INSTALLED_APPS = ["catalog"]\n'
    )
    catalog = proj / "catalog"
    catalog.mkdir()
    (catalog / "__init__.py").touch()
    (catalog / "models.py").write_text(
        "import dorm\n\nclass Product(dorm.Model):\n    name = dorm.CharField(max_length=50)\n"
    )

    # Run with cwd = proj (flat layout's normal usage). --settings = "settings".
    result = _run(
        [sys.executable, "-m", "dorm", "makemigrations"],
        cwd=proj,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert (catalog / "migrations" / "0001_initial.py").exists()


# ── Fix 4: import errors are surfaced, not swallowed ──────────────────────────


def test_broken_models_import_warns(tmp_path: Path):
    """A models.py with a real import error should produce a visible warning."""
    (tmp_path / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
        'INSTALLED_APPS = ["broken"]\n'
    )
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "__init__.py").touch()
    (broken / "models.py").write_text(
        "import this_module_definitely_does_not_exist_xyz\n"
    )

    result = _run(
        [sys.executable, "-m", "dorm", "makemigrations"],
        cwd=tmp_path,
    )
    assert "this_module_definitely_does_not_exist_xyz" in result.stderr or \
           "failed to import" in result.stderr.lower(), \
           f"stderr was: {result.stderr!r}"


def test_typo_in_installed_apps_warns(tmp_path: Path):
    """A typo'd app name in INSTALLED_APPS should produce a visible warning."""
    (tmp_path / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
        'INSTALLED_APPS = ["nonexistent_app_zzz"]\n'
    )
    result = _run(
        [sys.executable, "-m", "dorm", "makemigrations"],
        cwd=tmp_path,
    )
    assert "nonexistent_app_zzz" in result.stderr, \
        f"stderr was: {result.stderr!r}"


def test_app_without_models_py_does_not_warn(tmp_path: Path):
    """An app with __init__.py but no models.py must not produce a warning
    (it's a valid configuration)."""
    (tmp_path / "settings.py").write_text(
        'DATABASES = {"default": {"ENGINE": "sqlite", "NAME": "db.sqlite3"}}\n'
        'INSTALLED_APPS = ["empty_app"]\n'
    )
    empty = tmp_path / "empty_app"
    empty.mkdir()
    (empty / "__init__.py").touch()

    result = _run(
        [sys.executable, "-m", "dorm", "makemigrations"],
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "failed to import" not in result.stderr.lower()
    assert "could not import" not in result.stderr.lower()


# ── Fix 5: defensive warning when aiosqlite shape changes ─────────────────────


def test_daemon_warning_when_thread_attr_missing(tmp_path: Path, monkeypatch):
    """If a future aiosqlite version drops `_thread`, we should emit a
    RuntimeWarning instead of silently falling back to non-daemon behavior."""
    import asyncio
    import warnings

    from dorm.db.backends.sqlite import SQLiteAsyncDatabaseWrapper

    class FakePending:
        # No `_thread` attribute on purpose.
        def __await__(self):
            async def _coro():
                return FakeConn()
            return _coro().__await__()

    class FakeRow:
        pass

    class FakeConn:
        row_factory = None

        async def execute(self, sql, params=None):
            class _Cursor:
                async def fetchall(self_inner):
                    return []
                rowcount = 0
                lastrowid = 0
            return _Cursor()

        async def close(self):
            pass

    def fake_connect(*args, **kwargs):
        return FakePending()

    fake_aiosqlite = type(sys)("aiosqlite")
    fake_aiosqlite.connect = fake_connect  # type: ignore
    fake_aiosqlite.Row = FakeRow  # type: ignore
    monkeypatch.setitem(sys.modules, "aiosqlite", fake_aiosqlite)

    wrapper = SQLiteAsyncDatabaseWrapper({"NAME": ":memory:"})

    async def go():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await wrapper._new_connection()
        return caught

    caught = asyncio.run(go())
    assert any(
        issubclass(w.category, RuntimeWarning) and "_thread" in str(w.message)
        for w in caught
    ), f"expected RuntimeWarning about _thread, got: {[str(w.message) for w in caught]}"


# ── Fix 5 (positive case): real aiosqlite path stays warning-free ─────────────


def test_daemon_no_warning_with_real_aiosqlite(tmp_path: Path):
    """With the real aiosqlite, no RuntimeWarning should be emitted."""
    pytest.importorskip("aiosqlite")
    import asyncio
    import warnings

    from dorm.db.backends.sqlite import SQLiteAsyncDatabaseWrapper

    db_file = tmp_path / "warn.sqlite3"
    wrapper = SQLiteAsyncDatabaseWrapper({"NAME": str(db_file)})

    async def go():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            conn = await wrapper._new_connection()
            await conn.close()
        return caught

    caught = asyncio.run(go())
    daemon_warnings = [
        w for w in caught
        if issubclass(w.category, RuntimeWarning) and "_thread" in str(w.message)
    ]
    assert daemon_warnings == [], \
        f"unexpected daemon warning: {[str(w.message) for w in daemon_warnings]}"


# ── Fix #1: custom-PK-column on PostgreSQL ────────────────────────────────────


def test_execute_insert_with_custom_pk_col():
    """Backend-level test: execute_insert(..., pk_col=NAME) must return the
    value from the named column. Guards against the prior bug where the PG
    backend hardcoded `RETURNING id`."""
    from dorm.db.connection import get_connection
    conn = get_connection()

    # Build a table with a non-default PK column name.
    if conn.vendor == "sqlite":
        ddl = (
            'CREATE TABLE "test_cli_pkcol" ('
            '"custom_pk" INTEGER PRIMARY KEY AUTOINCREMENT, '
            '"name" VARCHAR(50) NOT NULL)'
        )
    else:  # postgresql
        ddl = (
            'CREATE TABLE "test_cli_pkcol" ('
            '"custom_pk" SERIAL PRIMARY KEY, '
            '"name" VARCHAR(50) NOT NULL)'
        )
    try:
        conn.execute_script('DROP TABLE IF EXISTS "test_cli_pkcol"')
    except Exception:
        pass
    conn.execute_script(ddl)

    # The query builder emits %s for sqlite and $N for postgres. Match that.
    insert_ph = "%s" if conn.vendor == "sqlite" else "$1"
    select_ph = "%s" if conn.vendor == "sqlite" else "$1"

    try:
        new_pk = conn.execute_insert(
            f'INSERT INTO "test_cli_pkcol" ("name") VALUES ({insert_ph})',
            ["Alice"],
            pk_col="custom_pk",
        )
        assert new_pk is not None
        assert new_pk == 1

        rows = conn.execute(
            f'SELECT "name" FROM "test_cli_pkcol" WHERE "custom_pk" = {select_ph}',
            [new_pk],
        )
        assert len(rows) == 1
        # Row may be a sqlite3.Row or dict (psycopg dict_row); both indexable by "name".
        assert rows[0]["name"] == "Alice"
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "test_cli_pkcol"')


# ── Fix #2: get_running_loop instead of get_event_loop ────────────────────────


def test_no_get_event_loop_in_backends():
    """Source code should not call deprecated asyncio.get_event_loop()."""
    sqlite_src = (REPO_ROOT / "dorm/db/backends/sqlite.py").read_text()
    pg_src = (REPO_ROOT / "dorm/db/backends/postgresql.py").read_text()
    assert "get_event_loop()" not in sqlite_src, \
        "sqlite.py still uses deprecated asyncio.get_event_loop()"
    assert "get_event_loop()" not in pg_src, \
        "postgresql.py still uses deprecated asyncio.get_event_loop()"


def test_async_sqlite_no_deprecation_across_loops(tmp_path: Path):
    """Two consecutive asyncio.run() calls must not emit a DeprecationWarning
    for asyncio.get_event_loop()."""
    pytest.importorskip("aiosqlite")
    import asyncio
    import warnings

    from dorm.db.backends.sqlite import SQLiteAsyncDatabaseWrapper

    db_file = tmp_path / "loops.sqlite3"
    wrapper = SQLiteAsyncDatabaseWrapper({"NAME": str(db_file)})

    async def use_it():
        await wrapper.execute_script("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        await wrapper.execute("SELECT 1")

    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asyncio.run(use_it())
            asyncio.run(use_it())
    finally:
        # The wrapper isn't registered in dorm's connection cache, so
        # nothing else will close it for us. Without this, the aiosqlite
        # connection from the second asyncio.run leaks and emits
        # ``ResourceWarning: ... was deleted before being closed`` at
        # GC time.
        wrapper.force_close_sync()

    deps = [
        w for w in caught
        if issubclass(w.category, DeprecationWarning)
        and "get_event_loop" in str(w.message)
    ]
    assert deps == [], f"unexpected deprecation warnings: {[str(w.message) for w in deps]}"


# ── Fix #5: _to_pyformat skips placeholders inside string literals ────────────


def test_to_pyformat_basic():
    from dorm.db.backends.postgresql import _to_pyformat
    assert _to_pyformat("SELECT * FROM t WHERE a = $1 AND b = $2") == \
        "SELECT * FROM t WHERE a = %s AND b = %s"


def test_to_pyformat_skips_string_literals():
    """A literal '$1 USD' inside a string must NOT be converted."""
    from dorm.db.backends.postgresql import _to_pyformat
    assert _to_pyformat("SELECT '$1 USD' FROM t WHERE x = $1") == \
        "SELECT '$1 USD' FROM t WHERE x = %s"


def test_to_pyformat_skips_quoted_identifiers():
    """A quoted identifier "col$1" must NOT be converted."""
    from dorm.db.backends.postgresql import _to_pyformat
    assert _to_pyformat('SELECT "col$1" FROM t WHERE x = $2') == \
        'SELECT "col$1" FROM t WHERE x = %s'


def test_to_pyformat_handles_escaped_quotes():
    """Doubled-up quotes ('') inside a literal don't terminate the literal."""
    from dorm.db.backends.postgresql import _to_pyformat
    assert _to_pyformat("INSERT INTO t VALUES ('a''$1', $1)") == \
        "INSERT INTO t VALUES ('a''$1', %s)"


def test_to_pyformat_multidigit_placeholders():
    from dorm.db.backends.postgresql import _to_pyformat
    sql = "INSERT INTO t VALUES (" + ",".join(f"${i}" for i in range(1, 13)) + ")"
    out = _to_pyformat(sql)
    assert "%s" in out
    assert "$" not in out


# ── Fix #6: dorm init template includes pool tuning hints ─────────────────────


def test_init_template_includes_pool_settings(tmp_path: Path):
    """`dorm init` should emit pool-tuning comments for PostgreSQL."""
    result = _run([sys.executable, "-m", "dorm", "init"], cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    body = (tmp_path / "settings.py").read_text()
    assert "MIN_POOL_SIZE" in body
    assert "MAX_POOL_SIZE" in body
    assert "POOL_TIMEOUT" in body
    assert "application_name" in body or "sslmode" in body, \
        "OPTIONS examples for psycopg keys should appear in the template"


def test_init_with_app_creates_files(tmp_path: Path):
    """`dorm init --app NAME` should create NAME/__init__.py and models.py."""
    result = _run([sys.executable, "-m", "dorm", "init", "--app", "users"], cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "settings.py").exists()
    assert (tmp_path / "users" / "__init__.py").exists()
    models = (tmp_path / "users" / "models.py").read_text()
    assert "class User(dorm.Model)" in models


def test_init_does_not_overwrite_existing_settings(tmp_path: Path):
    """Existing settings.py must not be clobbered."""
    (tmp_path / "settings.py").write_text("# my custom settings\n")
    result = _run([sys.executable, "-m", "dorm", "init"], cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "settings.py").read_text() == "# my custom settings\n"
    assert "already exists" in result.stdout.lower()


# ── Fix #7: POOL_TIMEOUT setting is honored ───────────────────────────────────


def test_pool_timeout_is_passed_to_pool():
    """POOL_TIMEOUT from settings should reach ConnectionPool(timeout=...)."""
    from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper

    wrapper = PostgreSQLDatabaseWrapper({
        "NAME": "test",
        "USER": "test",
        "POOL_TIMEOUT": 7.5,
    })
    assert wrapper._pool_timeout == 7.5


def test_pool_timeout_default():
    from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper
    wrapper = PostgreSQLDatabaseWrapper({"NAME": "test", "USER": "test"})
    assert wrapper._pool_timeout == 30.0


# ── Fix #8: dorm help mentions `init` ─────────────────────────────────────────


def test_help_lists_init_and_help(tmp_path: Path):
    result = _run([sys.executable, "-m", "dorm", "help"], cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "init" in result.stdout
    assert "help" in result.stdout
    assert "IPython" in result.stdout, "shell help should mention IPython"
    assert "--app" in result.stdout or "starter app" in result.stdout.lower()
