"""In-process tests for the `dorm` CLI commands.

The existing test_cli.py spawns subprocesses, which means coverage doesn't
see the CLI module. This file calls each cmd_* function directly with an
argparse.Namespace, so pytest-cov records the lines they exercise.

Each test sets up a tmp project (settings.py + an app), points cwd at it,
and restores dorm's global configuration after the test."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from dorm import cli
from dorm.conf import settings as dorm_settings
from dorm.db.connection import reset_connections


# ── fixture: isolate each CLI test from the session-wide dorm config ──────────


@pytest.fixture
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Set up an empty project at *tmp_path* and chdir into it. Restore the
    previous dorm config + cwd at teardown so the autouse `clean_db` fixture
    still works for surrounding tests.

    Resets the connection cache both before and after each test:
    - **before**, because the autouse ``clean_db`` fixture has already
      opened a connection to the session-wide test DB; if we don't drop
      it, ``cmd_*``'s call to ``_load_settings`` reconfigures DATABASES
      but ``get_connection()`` keeps returning the stale handle pointing
      at the wrong path.
    - **after**, so the next test sees a clean cache.
    """
    saved_databases = dict(dorm_settings.DATABASES)
    saved_apps = list(dorm_settings.INSTALLED_APPS)
    saved_configured = dorm_settings._configured
    saved_modules = set(sys.modules)
    saved_path = list(sys.path)

    reset_connections()
    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    yield tmp_path

    # Drop any modules imported from tmp_path so the next test gets a fresh import
    for name in list(sys.modules):
        if name in saved_modules or name in {"dorm", "dorm.cli"}:
            continue
        mod = sys.modules.get(name)
        # Drop project-scoped modules (settings, app packages, migrations).
        # Their import path is anchored at tmp_path; once we leave, sys.modules
        # entries pointing there would cause the next test to mis-resolve.
        file = getattr(mod, "__file__", None) if mod is not None else None
        if file and str(tmp_path) in str(file):
            del sys.modules[name]
            continue
        # Re-imports of `settings` / `shop` / etc. as namespace packages with
        # no __file__ should also be dropped to avoid stale module objects
        # leaking across tests.
        if name in {"settings", "shop", "shop.models", "shop.migrations"}:
            del sys.modules[name]

    sys.path[:] = saved_path
    reset_connections()
    dorm_settings.DATABASES = saved_databases
    dorm_settings.INSTALLED_APPS = saved_apps
    dorm_settings._configured = saved_configured


def _make_settings(tmp: Path, *, db_path: str | None = None, apps: list[str] | None = None) -> None:
    """Write a minimal SQLite settings.py inside *tmp*."""
    db_path = db_path or str(tmp / "db.sqlite3")
    apps_repr = repr(apps) if apps is not None else "[]"
    (tmp / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": {db_path!r}}}}}\n'
        f'INSTALLED_APPS = {apps_repr}\n'
    )


def _make_app(tmp: Path, name: str, models_src: str) -> Path:
    app_dir = tmp / name
    app_dir.mkdir()
    (app_dir / "__init__.py").touch()
    (app_dir / "models.py").write_text(models_src)
    return app_dir


# ── help, init ────────────────────────────────────────────────────────────────


def test_cmd_help_prints_subcommands(capsys, cli_env: Path):
    """cmd_help should emit the usage block listing every subcommand."""
    parser = argparse.ArgumentParser(prog="dorm")
    sub = parser.add_subparsers()
    for name in ("makemigrations", "migrate", "showmigrations", "init"):
        sub.add_parser(name)
    cli.cmd_help(argparse.Namespace(parser=parser))
    out = capsys.readouterr().out
    assert "makemigrations" in out
    assert "migrate" in out
    assert "init" in out


def test_cmd_init_creates_settings(capsys, cli_env: Path):
    """`dorm init` must scaffold a settings.py with both DB blocks commented out."""
    cli.cmd_init(argparse.Namespace(app=None))
    out = capsys.readouterr().out
    assert "Created" in out
    assert (cli_env / "settings.py").exists()
    body = (cli_env / "settings.py").read_text()
    assert "ENGINE" in body and "sqlite" in body and "postgresql" in body


def test_cmd_init_with_app_scaffolds_models(capsys, cli_env: Path):
    """`dorm init --app NAME` should create NAME/__init__.py and NAME/models.py."""
    cli.cmd_init(argparse.Namespace(app="blog"))
    capsys.readouterr()
    assert (cli_env / "blog" / "__init__.py").exists()
    body = (cli_env / "blog" / "models.py").read_text()
    assert "class User" in body and "dorm.CharField" in body


def test_cmd_init_idempotent(capsys, cli_env: Path):
    """A second `dorm init` shouldn't overwrite an existing settings.py."""
    (cli_env / "settings.py").write_text("# user-edited\n")
    cli.cmd_init(argparse.Namespace(app=None))
    out = capsys.readouterr().out
    assert "already exists" in out
    assert (cli_env / "settings.py").read_text() == "# user-edited\n"


def test_cmd_init_with_app_existing_models(capsys, cli_env: Path):
    """If models.py already exists in the target app dir, `init --app` keeps it."""
    app = cli_env / "blog"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text("# preserved\n")
    cli.cmd_init(argparse.Namespace(app="blog"))
    out = capsys.readouterr().out
    assert "already exists" in out
    assert (app / "models.py").read_text() == "# preserved\n"


# ── makemigrations ────────────────────────────────────────────────────────────


def test_cmd_makemigrations_creates_initial(capsys, cli_env: Path):
    """First run on a model with no prior migrations must produce 0001_initial.py."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n"
        "    price = dorm.IntegerField()\n",
    )

    cli.cmd_makemigrations(argparse.Namespace(apps=[], empty=False, name=None, settings="settings"))
    out = capsys.readouterr().out
    assert "Created migration" in out
    assert (cli_env / "shop" / "migrations" / "0001_initial.py").exists()


def test_cmd_makemigrations_no_changes(capsys, cli_env: Path):
    """When the model already has a migration on disk, the second run reports no changes."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )

    args = argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings")
    cli.cmd_makemigrations(args)
    capsys.readouterr()
    cli.cmd_makemigrations(args)
    out = capsys.readouterr().out
    assert "No changes detected" in out


def test_cmd_makemigrations_empty_requires_app(capsys, cli_env: Path):
    """`--empty` without an app label is a usage error."""
    _make_settings(cli_env, apps=[])
    cli.cmd_makemigrations(
        argparse.Namespace(apps=[], empty=True, name=None, settings="settings")
    )
    out = capsys.readouterr().out
    assert "Error" in out and "--empty" in out


def test_cmd_makemigrations_empty_with_name(capsys, cli_env: Path):
    """`--empty --name foo` writes a stub migration named ..._foo.py."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(cli_env, "shop", "import dorm\n")

    cli.cmd_makemigrations(
        argparse.Namespace(apps=["shop"], empty=True, name="backfill", settings="settings")
    )
    out = capsys.readouterr().out
    assert "Created empty migration" in out
    files = list((cli_env / "shop" / "migrations").glob("*.py"))
    assert any("backfill" in f.name for f in files)


# ── migrate ───────────────────────────────────────────────────────────────────


def test_cmd_migrate_applies_initial(capsys, cli_env: Path):
    """`migrate` after `makemigrations` should create the table."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()
    cli.cmd_migrate(argparse.Namespace(
        app_label="shop", target=None, verbosity=1, dry_run=False, settings="settings",
    ))
    capsys.readouterr()  # we don't assert text — the side-effect is the table

    from dorm.db.connection import get_connection
    assert get_connection().table_exists("shop_product")


def test_cmd_migrate_dry_run_does_not_apply(capsys, cli_env: Path):
    """`--dry-run` prints SQL but doesn't update the recorder or create tables."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()

    cli.cmd_migrate(argparse.Namespace(
        app_label="shop", target=None, verbosity=1, dry_run=True, settings="settings",
    ))
    out = capsys.readouterr().out
    assert "SQL that would run" in out
    assert "CREATE TABLE" in out

    from dorm.db.connection import get_connection
    assert not get_connection().table_exists("shop_product")


def test_cmd_migrate_dry_run_with_target_errors(capsys, cli_env: Path):
    """Mixing --dry-run with a specific target is unsupported and exits 1."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()

    with pytest.raises(SystemExit) as exc:
        cli.cmd_migrate(argparse.Namespace(
            app_label="shop", target="0001", verbosity=1, dry_run=True, settings="settings",
        ))
    assert exc.value.code == 1
    assert "--dry-run is not supported with a target" in capsys.readouterr().out


def test_cmd_migrate_no_migrations_dir(capsys, cli_env: Path):
    """Migrating an app whose migrations dir doesn't exist should print a hint, not crash."""
    _make_settings(cli_env, apps=["empty_app"])
    app = cli_env / "empty_app"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text("# no models yet\n")

    cli.cmd_migrate(argparse.Namespace(
        app_label="empty_app", target=None, verbosity=1, dry_run=False, settings="settings",
    ))
    out = capsys.readouterr().out
    assert "No migrations directory" in out


# ── showmigrations ────────────────────────────────────────────────────────────


def test_cmd_showmigrations_marks_applied(capsys, cli_env: Path):
    """showmigrations renders [X] for applied and [ ] for pending."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()

    cli.cmd_showmigrations(argparse.Namespace(apps=["shop"], settings="settings"))
    pending = capsys.readouterr().out
    assert "0001_initial" in pending
    assert "[ ]" in pending and "[X]" not in pending

    cli.cmd_migrate(argparse.Namespace(
        app_label="shop", target=None, verbosity=0, dry_run=False, settings="settings",
    ))
    capsys.readouterr()

    cli.cmd_showmigrations(argparse.Namespace(apps=["shop"], settings="settings"))
    applied = capsys.readouterr().out
    assert "[X]" in applied


# ── sql ───────────────────────────────────────────────────────────────────────


def test_cmd_sql_prints_create_table(capsys, cli_env: Path):
    """`dorm sql ModelName` prints the CREATE TABLE DDL for that model."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n"
        "    sku = dorm.CharField(max_length=20, unique=True)\n",
    )
    cli.cmd_sql(argparse.Namespace(names=["Product"], all=False, settings="settings"))
    out = capsys.readouterr().out
    assert "CREATE TABLE" in out
    assert '"name"' in out
    assert '"sku"' in out


def test_cmd_sql_all(capsys, cli_env: Path):
    """`dorm sql --all` dumps every concrete model's DDL."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class A(dorm.Model):\n    x = dorm.IntegerField()\n"
        "class B(dorm.Model):\n    y = dorm.CharField(max_length=10)\n",
    )
    cli.cmd_sql(argparse.Namespace(names=[], all=True, settings="settings"))
    out = capsys.readouterr().out
    assert out.count("CREATE TABLE") >= 2


def test_cmd_sql_unknown_model_exits_error(capsys, cli_env: Path):
    """Asking for a model that isn't installed should exit non-zero with a clear message."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(cli_env, "shop", "import dorm\nclass A(dorm.Model):\n    x = dorm.IntegerField()\n")

    with pytest.raises(SystemExit) as exc:
        cli.cmd_sql(argparse.Namespace(names=["DoesNotExist"], all=False, settings="settings"))
    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().out


def test_cmd_sql_no_args(capsys, cli_env: Path):
    """No model names + no --all is a usage error."""
    _make_settings(cli_env, apps=[])

    with pytest.raises(SystemExit):
        cli.cmd_sql(argparse.Namespace(names=[], all=False, settings="settings"))
    assert "pass model names or --all" in capsys.readouterr().out


# ── dbcheck ───────────────────────────────────────────────────────────────────


def test_cmd_dbcheck_in_sync(capsys, cli_env: Path):
    """When the schema matches the model, dbcheck reports OK and exits 0."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()
    cli.cmd_migrate(argparse.Namespace(
        app_label="shop", target=None, verbosity=0, dry_run=False, settings="settings",
    ))
    capsys.readouterr()

    cli.cmd_dbcheck(argparse.Namespace(apps=["shop"], settings="settings"))
    out = capsys.readouterr().out
    assert "match the database schema" in out


def test_cmd_dbcheck_missing_table(capsys, cli_env: Path):
    """Models declared but never migrated → dbcheck reports drift and exits 1."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class NeverMigrated(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    with pytest.raises(SystemExit) as exc:
        cli.cmd_dbcheck(argparse.Namespace(apps=["shop"], settings="settings"))
    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "missing" in out


# ── squashmigrations ──────────────────────────────────────────────────────────


def test_cmd_squashmigrations_combines(capsys, cli_env: Path):
    """`squashmigrations` collapses a contiguous range into one file with replaces=[...]."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    # Migration 1: initial
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()
    # Edit the model — adds a column → migration 2
    (cli_env / "shop" / "models.py").write_text(
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n"
        "    sku = dorm.CharField(max_length=20, default='x')\n"
    )
    # Force re-import of shop.models so the new field is picked up
    for mod in list(sys.modules):
        if mod.startswith("shop"):
            del sys.modules[mod]
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()

    cli.cmd_squashmigrations(argparse.Namespace(
        app_label="shop", start_migration="1", end_migration="2",
        squashed_name="combined", settings="settings",
    ))
    out = capsys.readouterr().out
    assert "Created squashed migration" in out
    assert "Replaces" in out


def test_cmd_squashmigrations_no_dir(capsys, cli_env: Path):
    """Squashing an app with no migrations dir is a friendly error, not a stack trace."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(cli_env, "shop", "import dorm\n")

    cli.cmd_squashmigrations(argparse.Namespace(
        app_label="shop", start_migration="1", end_migration="9",
        squashed_name="x", settings="settings",
    ))
    out = capsys.readouterr().out
    assert "no migrations directory" in out.lower()


def test_cmd_squashmigrations_empty_range(capsys, cli_env: Path):
    """A range that doesn't cover any existing migration prints a clear error."""
    _make_settings(cli_env, apps=["shop"])
    _make_app(
        cli_env,
        "shop",
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n",
    )
    cli.cmd_makemigrations(argparse.Namespace(apps=["shop"], empty=False, name=None, settings="settings"))
    capsys.readouterr()

    cli.cmd_squashmigrations(argparse.Namespace(
        app_label="shop", start_migration="50", end_migration="99",
        squashed_name="x", settings="settings",
    ))
    out = capsys.readouterr().out
    assert "no migrations found" in out.lower()


# ── main() entry point ────────────────────────────────────────────────────────


def test_main_dispatches_help(capsys, cli_env: Path, monkeypatch):
    """main() should parse argv and dispatch to cmd_help without error."""
    monkeypatch.setattr(sys, "argv", ["dorm", "help"])
    cli.main()
    out = capsys.readouterr().out
    assert "makemigrations" in out


def test_main_dispatches_init(capsys, cli_env: Path, monkeypatch):
    """main() should run cmd_init when called with `init`."""
    monkeypatch.setattr(sys, "argv", ["dorm", "init"])
    cli.main()
    capsys.readouterr()
    assert (cli_env / "settings.py").exists()
