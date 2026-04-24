from __future__ import annotations

import argparse
import importlib
import os
import sys
from pathlib import Path


def _load_settings(settings_module: str):
    """Import a Python settings module and configure dorm."""
    module = importlib.import_module(settings_module)
    # Add the directory that contains the app packages to sys.path so that
    # app modules (e.g. "example.models") are importable regardless of the
    # working directory the user ran dorm from.
    if module.__file__:
        project_root = str(Path(module.__file__).resolve().parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
    from . import configure

    databases = getattr(module, "DATABASES", {})
    installed_apps = getattr(module, "INSTALLED_APPS", [])
    configure(DATABASES=databases, INSTALLED_APPS=installed_apps)
    return module


def _load_apps(installed_apps: list):
    """Import all app modules to register their models."""
    for app in installed_apps:
        try:
            # Try app.models first
            importlib.import_module(f"{app}.models")
        except ImportError:
            try:
                importlib.import_module(app)
            except ImportError:
                pass


def _find_migrations_dir(app_module: str) -> Path:
    try:
        mod = importlib.import_module(app_module)
        if mod.__file__ is None:
            raise TypeError
        base = Path(mod.__file__).parent
    except (ImportError, TypeError):
        base = Path.cwd() / app_module
    return base / "migrations"


def cmd_makemigrations(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    settings = _load_settings(settings_mod)
    installed_apps = getattr(settings, "INSTALLED_APPS", [])
    _load_apps(installed_apps)

    from .migrations.autodetector import MigrationAutodetector
    from .migrations.loader import MigrationLoader
    from .migrations.state import ProjectState
    from .migrations.writer import write_migration
    from .db.connection import get_connection

    apps = args.apps if args.apps else installed_apps

    for app in apps:
        print(f"Detecting changes for '{app}'...")
        conn = get_connection()
        loader = MigrationLoader(conn)
        mig_dir = _find_migrations_dir(app)
        loader.load(mig_dir, app)

        # from_state = state described by all migration files on disk
        from_state = loader.get_migration_state(app, all_migrations=True)

        # to_state = current model definitions
        to_state = ProjectState.from_apps(app_label=app)

        detector = MigrationAutodetector(from_state, to_state)
        changes = detector.changes(app_label=app)

        if app not in changes or not changes[app]:
            print(f"  No changes detected for '{app}'.")
            continue

        # Determine migration number
        existing = list(mig_dir.glob("*.py")) if mig_dir.exists() else []
        numbers = []
        for f in existing:
            try:
                numbers.append(int(f.stem.split("_")[0]))
            except ValueError:
                pass
        next_num = max(numbers, default=0) + 1

        ops = changes[app]
        path = write_migration(app, mig_dir, next_num, ops)
        print(f"  Created migration: {path}")


def cmd_migrate(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    settings = _load_settings(settings_mod)
    installed_apps = getattr(settings, "INSTALLED_APPS", [])
    _load_apps(installed_apps)

    from .migrations.executor import MigrationExecutor
    from .db.connection import get_connection

    conn = get_connection()
    executor = MigrationExecutor(conn, verbosity=args.verbosity)

    apps = args.apps if args.apps else installed_apps
    for app in apps:
        mig_dir = _find_migrations_dir(app)
        if not mig_dir.exists():
            print(f"  No migrations directory for '{app}'. Run makemigrations first.")
            continue
        executor.migrate(app, mig_dir)


def cmd_showmigrations(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    settings = _load_settings(settings_mod)
    installed_apps = getattr(settings, "INSTALLED_APPS", [])

    from .migrations.executor import MigrationExecutor
    from .db.connection import get_connection

    conn = get_connection()
    executor = MigrationExecutor(conn, verbosity=0)

    apps = args.apps if args.apps else installed_apps
    for app in apps:
        mig_dir = _find_migrations_dir(app)
        executor.show_migrations(app, mig_dir)


def cmd_shell(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    settings = _load_settings(settings_mod)
    installed_apps = getattr(settings, "INSTALLED_APPS", [])
    _load_apps(installed_apps)

    import code
    from .models import _model_registry
    import dorm

    models = {k: v for k, v in _model_registry.items() if "." not in k}
    local_vars = {"dorm": dorm, **models}
    banner = "djanorm interactive shell\nModels: " + ", ".join(sorted(models.keys()))

    try:
        import IPython

        IPython.embed(user_ns=local_vars, banner1=banner, using="asyncio")
        return
    except ImportError:
        pass

    try:
        import readline
        import rlcompleter

        readline.set_completer(rlcompleter.Completer(local_vars).complete)
        readline.parse_and_bind("tab: complete")
    except ImportError:
        pass

    code.interact(banner=banner, local=local_vars)


def main():
    parser = argparse.ArgumentParser(
        prog="dorm",
        description="djanorm management commands",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # makemigrations
    mm = sub.add_parser(
        "makemigrations", help="Detect model changes and create migrations"
    )
    mm.add_argument("apps", nargs="*", help="App labels to process")
    mm.add_argument("--settings", default=None)
    mm.set_defaults(func=cmd_makemigrations)

    # migrate
    mg = sub.add_parser("migrate", help="Apply pending migrations")
    mg.add_argument("apps", nargs="*", help="App labels to migrate")
    mg.add_argument("--verbosity", type=int, default=1)
    mg.add_argument("--settings", default=None)
    mg.set_defaults(func=cmd_migrate)

    # showmigrations
    sm = sub.add_parser("showmigrations", help="List all migrations and their status")
    sm.add_argument("apps", nargs="*")
    sm.add_argument("--settings", default=None)
    sm.set_defaults(func=cmd_showmigrations)

    # shell
    sh = sub.add_parser("shell", help="Start an interactive Python shell")
    sh.add_argument("--settings", default=None)
    sh.set_defaults(func=cmd_shell)

    parsed = parser.parse_args()
    parsed.func(parsed)


if __name__ == "__main__":
    main()
