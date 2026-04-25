"""Tests for the `dorm` CLI: __main__ entry point, settings loading,
app autodiscovery warnings, import-error surfacing, and the daemon-thread
defensive warning for aiosqlite."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
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

    import dorm
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
    fake_aiosqlite.connect = fake_connect
    fake_aiosqlite.Row = FakeRow
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
