from __future__ import annotations

import argparse
import importlib
import os
import sys
from pathlib import Path


def _load_settings(settings_module: str):
    """Import a Python settings module and configure dorm."""
    module = importlib.import_module(settings_module)
    # Add both the directory containing settings.py *and* its parent to
    # sys.path so apps are importable regardless of layout:
    #   - flat/nested:  apps live next to settings.py → need settings_dir
    #   - dotted pkg:   settings imported as "myproj.settings" → need parent
    settings_dir: Path | None = None
    if module.__file__:
        settings_dir = Path(module.__file__).resolve().parent
        for path in (str(settings_dir), str(settings_dir.parent)):
            if path not in sys.path:
                sys.path.insert(0, path)
    from . import configure
    from .conf import _discover_apps

    databases = getattr(module, "DATABASES", {})
    installed_apps = getattr(module, "INSTALLED_APPS", [])
    autodiscovered = False
    if not installed_apps and settings_dir is not None:
        installed_apps = _discover_apps(settings_dir)
        autodiscovered = True
    if not installed_apps and autodiscovered:
        print(
            "Warning: no apps detected. Make sure each app directory has "
            "__init__.py and models.py, or set INSTALLED_APPS in settings.py.",
            file=sys.stderr,
        )
    configure(DATABASES=databases, INSTALLED_APPS=installed_apps)
    return module


def _load_apps(installed_apps: list):
    """Import each app's models module, surfacing import errors instead of
    silently swallowing them. A missing models.py is allowed (we fall back
    to importing the app package); other failures are reported."""
    for app in installed_apps:
        try:
            importlib.import_module(f"{app}.models")
            continue
        except ModuleNotFoundError as exc:
            missing = exc.name or ""
            if missing not in (app, f"{app}.models"):
                # A real import error inside models.py — bubble up to user.
                print(
                    f"Warning: failed to import {app}.models: {exc}",
                    file=sys.stderr,
                )
                continue
        except ImportError as exc:
            print(
                f"Warning: failed to import {app}.models: {exc}",
                file=sys.stderr,
            )
            continue
        # No models.py — try the app package itself.
        try:
            importlib.import_module(app)
        except ImportError as exc:
            print(
                f"Warning: could not import app '{app}': {exc}",
                file=sys.stderr,
            )


def _find_migrations_dir(app_module: str) -> Path:
    try:
        mod = importlib.import_module(app_module)
        if mod.__file__ is None:
            raise TypeError
        base = Path(mod.__file__).parent
    except (ImportError, TypeError):
        base = Path.cwd() / app_module
    return base / "migrations"


def _next_migration_number(mig_dir: Path) -> int:
    existing = list(mig_dir.glob("*.py")) if mig_dir.exists() else []
    numbers = []
    for f in existing:
        try:
            numbers.append(int(f.stem.split("_")[0]))
        except ValueError:
            pass
    return max(numbers, default=0) + 1


def cmd_makemigrations(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    # ── Empty migration ───────────────────────────────────────────────────────
    if args.empty:
        if not args.apps:
            print("Error: specify at least one app when using --empty.")
            return
        from .migrations.writer import write_empty_migration

        for app in args.apps:
            mig_dir = _find_migrations_dir(app)
            next_num = _next_migration_number(mig_dir)
            name = args.name or "custom"
            path = write_empty_migration(app, mig_dir, next_num, name=name)
            print(f"  Created empty migration: {path}")
        return

    # ── Auto-detect changes ───────────────────────────────────────────────────
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

        next_num = _next_migration_number(mig_dir)
        ops = changes[app]
        path = write_migration(app, mig_dir, next_num, ops)
        print(f"  Created migration: {path}")


def cmd_squashmigrations(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    app_label = args.app_label
    start = int(args.start_migration)
    end = int(args.end_migration)
    squashed_name = args.squashed_name or "squashed"

    from .migrations.loader import MigrationLoader
    from .migrations.squasher import squash_operations
    from .migrations.writer import write_squashed_migration
    from .db.connection import get_connection

    mig_dir = _find_migrations_dir(app_label)
    if not mig_dir.exists():
        print(f"Error: no migrations directory found for '{app_label}'.")
        return

    conn = get_connection()
    loader = MigrationLoader(conn)
    loader.load(mig_dir, app_label)

    all_migs = sorted(loader.migrations.get(app_label, []), key=lambda x: x[0])
    in_range = [(num, name, mod) for num, name, mod in all_migs if start <= num <= end]

    if not in_range:
        print(f"Error: no migrations found for '{app_label}' in range [{start}, {end}].")
        return

    operations = []
    replaces = []
    for _num, name, mod in in_range:
        operations.extend(getattr(mod, "operations", []))
        replaces.append((app_label, name))

    optimized = squash_operations(operations)

    next_num = _next_migration_number(mig_dir)
    path = write_squashed_migration(
        app_label,
        mig_dir,
        next_num,
        optimized,
        replaces,
        name=squashed_name,
    )
    print(f"  Created squashed migration: {path}")
    print(f"  Replaces: {[name for _, name in replaces]}")
    print(f"  Operations before: {len(operations)}, after: {len(optimized)}")


def cmd_migrate(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    from .migrations.executor import MigrationExecutor
    from .db.connection import get_connection

    conn = get_connection()
    executor = MigrationExecutor(conn, verbosity=args.verbosity)

    app_label = getattr(args, "app_label", None)
    target = getattr(args, "target", None)
    dry_run = getattr(args, "dry_run", False)
    apps = [app_label] if app_label else installed_apps

    for app in apps:
        mig_dir = _find_migrations_dir(app)
        if not mig_dir.exists():
            print(f"  No migrations directory for '{app}'. Run makemigrations first.")
            continue
        if target:
            if dry_run:
                print("  Error: --dry-run is not supported with a target.")
                sys.exit(1)
            try:
                executor.migrate_to(app, mig_dir, target)
            except ValueError as exc:
                print(f"  Error: {exc}")
        else:
            captured = executor.migrate(app, mig_dir, dry_run=dry_run)
            if dry_run and captured:
                print(f"\n--- SQL that would run for '{app}' ---")
                for sql, params in captured:
                    sql_print = sql.strip()
                    print(f"\n{sql_print};")
                    if params:
                        print(f"  -- params: {params!r}")
                print()


def cmd_showmigrations(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS

    from .migrations.executor import MigrationExecutor
    from .db.connection import get_connection

    conn = get_connection()
    executor = MigrationExecutor(conn, verbosity=0)

    apps = args.apps if args.apps else installed_apps
    for app in apps:
        mig_dir = _find_migrations_dir(app)
        executor.show_migrations(app, mig_dir)


def cmd_sql(args):
    """Print the ``CREATE TABLE`` DDL for one or more models. Resolves
    each name as either a bare class name (``User``) or an
    app-qualified name (``users.User``). Useful for copying schema to
    ops, diffing against production, or generating fixture seeds."""
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    from .db.connection import get_connection
    from .migrations.operations import _field_to_column_sql
    from .models import _model_registry

    conn = get_connection()

    if args.all:
        targets = [
            (label, model)
            for label, model in _model_registry.items()
            if "." not in label and not model._meta.abstract
        ]
    else:
        if not args.names:
            print("Error: pass model names or --all.")
            sys.exit(1)
        targets = []
        for name in args.names:
            # Match by class name OR by app.ClassName
            matches = [
                (label, m) for label, m in _model_registry.items()
                if "." not in label and not m._meta.abstract
                and (m.__name__ == name or label == name)
            ]
            if not matches:
                print(f"Error: model {name!r} not found in INSTALLED_APPS.")
                sys.exit(1)
            targets.extend(matches)

    for label, model in targets:
        table = model._meta.db_table
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        cols = [c for c in cols if c]
        ddl = (
            f'-- {model.__name__} ({label})\n'
            f'CREATE TABLE "{table}" (\n  '
            + ",\n  ".join(cols)
            + "\n);"
        )
        print(ddl)
        print()


def cmd_dbcheck(args):
    """Compare each model's column set with what's currently in the
    database and print drift. Useful when the schema was edited by hand
    or when migrations have been mis-applied. Exit code 0 when in sync,
    1 when any drift is found."""
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    from .db.connection import get_connection
    from .models import _model_registry

    conn = get_connection()

    apps_to_check = args.apps if args.apps else installed_apps
    drift_found = False

    for app in apps_to_check:
        models = [
            m for label, m in _model_registry.items()
            if "." not in label and m._meta.app_label == app and not m._meta.abstract
        ]
        if not models:
            continue
        print(f"App '{app}':")
        for model in models:
            table = model._meta.db_table
            if not conn.table_exists(table):
                drift_found = True
                print(f"  ✗ {model.__name__}: table {table!r} missing")
                continue

            db_columns = {c["name"] for c in conn.get_table_columns(table)}
            model_columns = {f.column for f in model._meta.fields if f.column}

            missing = model_columns - db_columns      # in model, not in DB
            extra = db_columns - model_columns        # in DB, not in model

            if not missing and not extra:
                print(f"  ✓ {model.__name__} ({table})")
                continue

            drift_found = True
            print(f"  ✗ {model.__name__} ({table}):")
            for c in sorted(missing):
                print(f"      missing in DB: {c}")
            for c in sorted(extra):
                print(f"      missing in model: {c}")

    if drift_found:
        print("\nDrift detected. Run 'dorm makemigrations' / 'dorm migrate' to reconcile.")
        sys.exit(1)
    print("\nAll checked models match the database schema.")


def cmd_shell(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings
    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    import code
    from .models import _model_registry
    import dorm

    models = {k: v for k, v in _model_registry.items() if "." not in k}
    local_vars = {"dorm": dorm, **models}
    banner = "djanorm interactive shell\nModels: " + ", ".join(sorted(models.keys()))

    try:
        # IPython is an optional dev dependency — suppress the static import
        # check so ty doesn't fail on environments that don't have it. The
        # try/except still handles the runtime ImportError.
        import IPython  # ty: ignore[unresolved-import]

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


_SETTINGS_TEMPLATE = '''"""djanorm settings.

Uncomment the DATABASES block for the backend you want to use.
"""

# ── SQLite ────────────────────────────────────────────────────────────────────
# DATABASES = {
#     "default": {
#         "ENGINE": "sqlite",
#         "NAME": "db.sqlite3",
#     }
# }

# ── PostgreSQL ────────────────────────────────────────────────────────────────
# DATABASES = {
#     "default": {
#         "ENGINE": "postgresql",
#         "NAME": "mydb",
#         "USER": "postgres",
#         "PASSWORD": "postgres",
#         "HOST": "localhost",
#         "PORT": 5432,
#         # Connection pool tuning (defaults shown):
#         # "MIN_POOL_SIZE": 1,
#         # "MAX_POOL_SIZE": 10,
#         # "POOL_TIMEOUT": 30.0,   # seconds to wait for a free connection
#         # OPTIONS are passed straight to psycopg.connect — use psycopg keys,
#         # not Django-style names. Examples:
#         # "OPTIONS": {
#         #     "sslmode": "require",
#         #     "application_name": "myapp",
#         #     "connect_timeout": 10,
#         # },
#     }
# }

# Apps are autodiscovered from any directory next to settings.py that has
# both __init__.py and models.py. Set INSTALLED_APPS explicitly to override.
# INSTALLED_APPS = []
'''

_MODELS_TEMPLATE = '''import dorm


class User(dorm.Model):
    username = dorm.CharField(max_length=150, unique=True)
    email = dorm.EmailField(unique=True)
    is_active = dorm.BooleanField(default=True)
    created_at = dorm.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["username"]

    def __str__(self):
        return self.username
'''


def cmd_init(args):
    cwd = Path.cwd()
    settings_path = cwd / "settings.py"
    if settings_path.exists():
        print(f"settings.py already exists at {settings_path} — leaving it untouched.")
    else:
        settings_path.write_text(_SETTINGS_TEMPLATE)
        print(f"Created {settings_path}")

    app_name = args.app
    if app_name:
        app_dir = cwd / app_name
        app_dir.mkdir(exist_ok=True)
        init_file = app_dir / "__init__.py"
        if not init_file.exists():
            init_file.touch()
            print(f"Created {init_file}")
        models_file = app_dir / "models.py"
        if models_file.exists():
            print(f"{models_file} already exists — leaving it untouched.")
        else:
            models_file.write_text(_MODELS_TEMPLATE)
            print(f"Created {models_file}")

    print()
    print("Next steps:")
    print("  1. Edit settings.py and uncomment your DATABASES backend.")
    if app_name:
        print(f"  2. Run: dorm makemigrations {app_name}")
    else:
        print("  2. Run: dorm makemigrations")
    print("  3. Run: dorm migrate")


def cmd_help(args):
    args.parser.print_help()


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
    mm.add_argument(
        "--empty",
        action="store_true",
        default=False,
        help="Create a blank migration template for RunPython / RunSQL",
    )
    mm.add_argument(
        "--name",
        default=None,
        metavar="NAME",
        help="Custom name suffix for the empty migration file (default: 'custom')",
    )
    mm.add_argument("--settings", default=None)
    mm.set_defaults(func=cmd_makemigrations)

    # migrate
    mg = sub.add_parser(
        "migrate",
        help="Apply pending migrations (or rollback when a target is given)",
    )
    mg.add_argument(
        "app_label", nargs="?", default=None,
        help="App to migrate (default: all apps)",
    )
    mg.add_argument(
        "target", nargs="?", default=None,
        help="Target migration name / number prefix / 'zero' — "
             "applies forward or rolls back as needed",
    )
    mg.add_argument("--verbosity", type=int, default=1)
    mg.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print the SQL that would be executed without touching the "
             "database. The migration recorder is NOT updated, so the "
             "next run still sees the same set of pending migrations. "
             "Recommended as a pre-deploy review step.",
    )
    mg.add_argument("--settings", default=None)
    mg.set_defaults(func=cmd_migrate)

    # showmigrations
    sm = sub.add_parser("showmigrations", help="List all migrations and their status")
    sm.add_argument("apps", nargs="*")
    sm.add_argument("--settings", default=None)
    sm.set_defaults(func=cmd_showmigrations)

    # squashmigrations
    sq = sub.add_parser("squashmigrations", help="Squash a range of migrations into one")
    sq.add_argument("app_label", help="App label")
    sq.add_argument(
        "start_migration",
        nargs="?",
        default="1",
        help="Migration number to start from (default: 1)",
    )
    sq.add_argument("end_migration", help="Migration number to squash up to (inclusive)")
    sq.add_argument(
        "--squashed-name",
        default="squashed",
        metavar="NAME",
        help="Name suffix for the squashed migration file (default: squashed)",
    )
    sq.add_argument("--settings", default=None)
    sq.set_defaults(func=cmd_squashmigrations)

    # shell
    sh = sub.add_parser(
        "shell",
        help="Start an interactive Python shell (uses IPython if installed, otherwise the standard Python REPL)",
    )
    sh.add_argument("--settings", default=None)
    sh.set_defaults(func=cmd_shell)

    # sql — dump CREATE TABLE for given models
    sq2 = sub.add_parser(
        "sql",
        help="Print the CREATE TABLE DDL for one or more models. "
             "Useful for sharing schema with DBAs or seeding fixtures.",
    )
    sq2.add_argument(
        "names", nargs="*",
        help="Model names — bare (``User``) or app-qualified (``users.User``).",
    )
    sq2.add_argument(
        "--all", action="store_true", default=False,
        help="Dump every model in INSTALLED_APPS.",
    )
    sq2.add_argument("--settings", default=None)
    sq2.set_defaults(func=cmd_sql)

    # dbcheck
    dc = sub.add_parser(
        "dbcheck",
        help="Compare each model's columns against the live database schema "
             "and print drift (missing columns, hand-edited tables, etc.). "
             "Exits non-zero when drift is found — useful as a pre-deploy gate.",
    )
    dc.add_argument(
        "apps", nargs="*",
        help="App labels to check (default: all)",
    )
    dc.add_argument("--settings", default=None)
    dc.set_defaults(func=cmd_dbcheck)

    # init
    ini = sub.add_parser(
        "init",
        help=(
            "Scaffold settings.py in the current directory. "
            "Pass --app NAME to also create the app folder NAME/ with "
            "__init__.py and a models.py containing an example User model."
        ),
    )
    ini.add_argument(
        "--app",
        default=None,
        metavar="NAME",
        help=(
            "Name of an app to scaffold alongside settings.py. Creates "
            "NAME/ (if missing), NAME/__init__.py, and NAME/models.py "
            "with an example User model."
        ),
    )
    ini.set_defaults(func=cmd_init)

    # help
    hp = sub.add_parser("help", help="Show this help message and exit")
    hp.set_defaults(func=cmd_help, parser=parser)

    parsed = parser.parse_args()
    parsed.func(parsed)


if __name__ == "__main__":
    main()
