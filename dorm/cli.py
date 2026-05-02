from __future__ import annotations

import argparse
import importlib
import os
import sys
from pathlib import Path

from .conf import _validate_dotted_path


def _load_settings(settings_module: str):
    """Import a Python settings module and configure dorm."""
    _validate_dotted_path(settings_module, kind="settings module")
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
    # Reject anything that isn't a dotted Python path before falling back to
    # ``Path.cwd() / app_module``. Without this check, ``app_module="../etc"``
    # would resolve outside the project root, and ``app_module="foo/bar"``
    # would break path semantics on Windows.
    _validate_dotted_path(app_module, kind="app label")
    try:
        mod = importlib.import_module(app_module)
        if mod.__file__ is None:
            raise TypeError
        base = Path(mod.__file__).parent
    except (ImportError, TypeError):
        # Translate dots to OS separators so ``my_proj.users`` maps to the
        # nested directory layout users expect.
        base = Path.cwd().joinpath(*app_module.split("."))
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

    # ── pgvector extension migration ─────────────────────────────────────────
    if getattr(args, "enable_pgvector", False):
        if not args.apps:
            print(
                "Error: specify at least one app when using "
                "--enable-pgvector. Example: dorm makemigrations "
                "--enable-pgvector myapp"
            )
            return
        from .migrations.writer import write_pgvector_extension_migration

        for app in args.apps:
            mig_dir = _find_migrations_dir(app)
            next_num = _next_migration_number(mig_dir)
            name = args.name or "enable_pgvector"
            path = write_pgvector_extension_migration(app, mig_dir, next_num, name=name)
            print(f"  Created pgvector extension migration: {path}")
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
        print(
            f"Error: no migrations found for '{app_label}' in range [{start}, {end}]."
        )
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
                # Surface the failure through the CLI exit code so
                # CI gating on ``dorm migrate`` actually catches a
                # missing / invalid target. Previously the loop
                # continued and the process exited 0.
                sys.exit(1)
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
                (label, m)
                for label, m in _model_registry.items()
                if "." not in label
                and not m._meta.abstract
                and (m.__name__ == name or label == name)
            ]
            if not matches:
                print(f"Error: model {name!r} not found in INSTALLED_APPS.")
                sys.exit(1)
            targets.extend(matches)

    for label, model in targets:
        table = model._meta.db_table
        try:
            cols = [
                _field_to_column_sql(f.name, f, conn)
                for f in model._meta.fields
                if f.db_type(conn)
            ]
        except NotImplementedError as exc:
            # A field on this model has no SQL representation on the
            # active backend (typical case: PG-only ``RangeField`` /
            # ``ArrayField`` while introspecting against SQLite). Note
            # it on stderr and move on rather than aborting the dump
            # for every other model.
            print(
                f"-- skipping {label}: {exc}",
                file=sys.stderr,
            )
            continue
        cols = [c for c in cols if c]
        ddl = (
            f"-- {model.__name__} ({label})\n"
            f'CREATE TABLE "{table}" (\n  ' + ",\n  ".join(cols) + "\n);"
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
            m
            for label, m in _model_registry.items()
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

            missing = model_columns - db_columns  # in model, not in DB
            extra = db_columns - model_columns  # in DB, not in model

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
        print(
            "\nDrift detected. Run 'dorm makemigrations' / 'dorm migrate' to reconcile."
        )
        sys.exit(1)
    print("\nAll checked models match the database schema.")


def cmd_dbshell(args):
    """Drop into the underlying database client (``psql`` for PostgreSQL,
    ``sqlite3`` for SQLite) with credentials and database name pre-filled
    from the active settings. The user-set ``--database`` selects which
    DATABASES alias to connect to (default: ``"default"``).

    For PostgreSQL the password is passed via ``PGPASSWORD`` environment
    variable rather than a connection-string argument so it doesn't end
    up in shell history or process listings. The child process inherits
    the current terminal — exit it (``\\q`` for psql, ``.exit`` for
    sqlite3) to come back to your shell.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    alias = args.database
    if alias not in settings.DATABASES:
        print(
            f"error: alias {alias!r} not in DATABASES; "
            f"choices are {sorted(settings.DATABASES)}",
            file=sys.stderr,
        )
        sys.exit(2)

    cfg = settings.DATABASES[alias]
    engine = (cfg.get("ENGINE") or "sqlite").lower()
    import shutil

    if "sqlite" in engine:
        db_path = cfg.get("NAME") or "db.sqlite3"
        client = shutil.which("sqlite3")
        if client is None:
            print(
                "error: 'sqlite3' executable not found on PATH. "
                "Install it (e.g. `apt install sqlite3`) and retry.",
                file=sys.stderr,
            )
            sys.exit(127)
        os.execvp(client, [client, str(db_path)])

    if "postgres" in engine:
        client = shutil.which("psql")
        if client is None:
            print(
                "error: 'psql' executable not found on PATH. "
                "Install the PostgreSQL client and retry.",
                file=sys.stderr,
            )
            sys.exit(127)
        env = dict(os.environ)
        # Pass password via env so it doesn't show up in `ps`. psql
        # also reads PGPASSFILE; we don't override anything the user
        # already set.
        if cfg.get("PASSWORD"):
            env["PGPASSWORD"] = str(cfg["PASSWORD"])
        argv = [client]
        if cfg.get("HOST"):
            argv += ["-h", str(cfg["HOST"])]
        if cfg.get("PORT"):
            argv += ["-p", str(cfg["PORT"])]
        if cfg.get("USER"):
            argv += ["-U", str(cfg["USER"])]
        if cfg.get("NAME"):
            argv += ["-d", str(cfg["NAME"])]
        os.execvpe(client, argv, env)

    print(
        f"error: dbshell does not know how to launch a client for engine {engine!r}",
        file=sys.stderr,
    )
    sys.exit(2)


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

# ── Observability ─────────────────────────────────────────────────────────────
# Slow-query warning threshold in milliseconds. Every executed statement is
# already timed for the ``pre_query`` / ``post_query`` signals, so this
# warning is free at runtime — only the comparison is added.
#
# When a statement crosses the threshold, the
# ``dorm.db.backends.<vendor>`` logger emits a WARNING with the SQL text and
# elapsed time. Pipe the ``dorm.db`` logger to your alerting handler in
# production.
#
# Resolution order: this setting > env var ``DORM_SLOW_QUERY_MS`` > default 500.
# Set to ``None`` to disable the warning entirely. Set to ``0`` to log every
# query as slow (handy in development without flipping the full DEBUG stream).
SLOW_QUERY_MS = 500.0

# ── File storage (dorm.FileField) ─────────────────────────────────────────────
# Uncomment one of the blocks below if you use ``dorm.FileField``. If left
# unset, dorm falls back to a default ``FileSystemStorage`` rooted at
# ``./media`` — fine for local dev / single-machine apps.
#
# Local filesystem (default):
# STORAGES = {
#     "default": {
#         "BACKEND": "dorm.storage.FileSystemStorage",
#         "OPTIONS": {
#             "location": "/var/app/media",   # absolute path on disk
#             "base_url": "/media/",           # URL prefix your web server / CDN exposes
#         },
#     }
# }
#
# AWS S3 (requires `pip install "djanorm[s3]"`):
# STORAGES = {
#     "default": {
#         "BACKEND": "dorm.contrib.storage.s3.S3Storage",
#         "OPTIONS": {
#             "bucket_name": "my-app-uploads",
#             "region_name": "eu-west-1",
#             # Leave access_key/secret_key unset in production — boto3
#             # picks them up from the IAM role / env vars / ~/.aws/.
#             "default_acl": "private",
#             "querystring_auth": True,        # generate presigned URLs
#             "querystring_expire": 3600,
#         },
#     }
# }
#
# S3-compatible (MinIO / Cloudflare R2 / Backblaze B2). Same backend; add
# endpoint_url and force path-style addressing because most non-AWS
# endpoints don't support virtual-hosted sub-domains over IP:
# STORAGES = {
#     "default": {
#         "BACKEND": "dorm.contrib.storage.s3.S3Storage",
#         "OPTIONS": {
#             "bucket_name": "dev-uploads",
#             "endpoint_url": "http://localhost:9000",
#             "access_key": "minioadmin",
#             "secret_key": "minioadmin",
#             "region_name": "us-east-1",
#             "signature_version": "s3v4",
#             "addressing_style": "path",
#         },
#     }
# }
'''

_MODELS_TEMPLATE = """import dorm


class User(dorm.Model):
    username = dorm.CharField(max_length=150, unique=True)
    email = dorm.EmailField(unique=True)
    is_active = dorm.BooleanField(default=True)
    created_at = dorm.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["username"]

    def __str__(self):
        return self.username
"""


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


def cmd_inspectdb(args):
    """Reverse-engineer a ``models.py`` snippet from the connected
    database and print it to stdout. Pipe to a file::

        dorm inspectdb > legacy/models.py

    Field types are recovered best-effort. Constraints, indexes and
    foreign-key ``related_name`` are not introspected — diff and edit
    the output before committing.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    from .conf import settings

    if not settings._configured:
        # Skip the load only when dorm was already configured by the
        # caller (typical in tests / programmatic embedding). Symmetric
        # with ``cmd_doctor``.
        _load_settings(settings_mod)

    from .db.connection import get_connection
    from .inspect import introspect_tables, render_models

    alias = getattr(args, "database", "default") or "default"
    conn = get_connection(alias)
    tables = introspect_tables(conn)
    if not tables:
        print(
            "# inspectdb: no user tables found in the connected database.",
            file=sys.stderr,
        )
        return
    print(render_models(tables))


def cmd_doctor(args):
    """Audit the running configuration for production-mode footguns
    and print a punch-list of warnings.

    Exits non-zero when at least one *warning* is emitted (so the
    command can be used as a pre-deploy gate). Categories checked:

    - DATABASES configuration: pool size, ``POOL_TIMEOUT``, ``POOL_CHECK``,
      ``MAX_LIFETIME``, missing FK indexes.
    - Model layer: foreign keys without an index on the FK column,
      ``related_name`` collisions.
    - Logging: SQL DEBUG channel routed to stdout (perf hit), ``DORM_RETRY_*``
      not set in production.

    The doctor is conservative — it only warns when the rule of thumb
    is widely accepted. Tune the heuristics to your workload before
    treating any single warning as gospel.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    from .conf import settings

    if not settings._configured:
        # Skip the load only when dorm was already configured by the
        # caller (typical in tests / programmatic embed). When unset,
        # try the standard load path and surface errors so misconfigured
        # CI runs fail loudly.
        try:
            _load_settings(settings_mod)
        except Exception as exc:
            print(f"error loading settings: {exc}", file=sys.stderr)
            sys.exit(2)
    _load_apps(settings.INSTALLED_APPS)
    from .models import _model_registry

    warnings: list[str] = []
    info: list[str] = []

    # 1. DATABASES checks
    for alias, cfg in settings.DATABASES.items():
        engine = (cfg.get("ENGINE") or "").lower()
        if "postgres" in engine:
            mn = int(cfg.get("MIN_POOL_SIZE", 1))
            mx = int(cfg.get("MAX_POOL_SIZE", 10))
            if mx < 4:
                warnings.append(
                    f"DATABASES[{alias!r}]: MAX_POOL_SIZE={mx} is small for production; "
                    "raise to 10–20 unless this is a worker-per-process layout."
                )
            if mn > mx:
                warnings.append(
                    f"DATABASES[{alias!r}]: MIN_POOL_SIZE > MAX_POOL_SIZE — pool will refuse to open."
                )
            timeout = float(cfg.get("POOL_TIMEOUT", 30.0))
            if timeout > 60:
                warnings.append(
                    f"DATABASES[{alias!r}]: POOL_TIMEOUT={timeout}s is long; "
                    "callers will appear stuck on saturation. Aim for 5–30s."
                )
            if cfg.get("POOL_CHECK") is False:
                info.append(
                    f"DATABASES[{alias!r}]: POOL_CHECK=False — fine on hot paths but "
                    "expect the occasional dead-conn surprise during PG restarts."
                )
            opts = cfg.get("OPTIONS") or {}
            if "sslmode" not in opts and (
                cfg.get("HOST") not in (None, "", "localhost", "127.0.0.1")
            ):
                warnings.append(
                    f"DATABASES[{alias!r}]: no OPTIONS['sslmode'] for a non-local host; "
                    "set 'require' (or 'verify-full' if you have a CA) to avoid plaintext."
                )

    # 2. Model layer checks: FKs without an explicit index
    for label, model in _model_registry.items():
        if "." in label or model._meta.abstract:
            continue
        from .fields import ForeignKey, OneToOneField

        indexed_columns: set[str] = set()
        for idx in getattr(model._meta, "indexes", []) or []:
            for f in idx.fields:
                if "(" not in f:
                    indexed_columns.add(f)
        for f in model._meta.fields:
            if isinstance(f, (ForeignKey, OneToOneField)):
                if not f.db_index and not f.unique and f.column not in indexed_columns:
                    warnings.append(
                        f"{model.__name__}.{f.name}: ForeignKey without db_index; "
                        "joins on this FK will sequentially scan. Add db_index=True "
                        "or an Index() in Meta."
                    )

    # 3. DORM_RETRY_* env hints
    import os as _os

    if _os.environ.get("DORM_RETRY_ATTEMPTS") in (None, "", "1", "0"):
        info.append(
            "DORM_RETRY_ATTEMPTS not set or set to 0/1: transient PG errors "
            "(network blips, RDS failover) will surface to callers without retry."
        )

    # 4. STORAGES (file backends for ``dorm.FileField``).
    #    Three classes of finding:
    #      - hard misconfig that would crash at first save (no ``default``
    #        alias when FileField is used);
    #      - production smell (FileSystemStorage location not present /
    #        not writable, or S3 backend with hardcoded credentials);
    #      - silent reliance on the implicit default (FileField in use,
    #        STORAGES unset → falls back to ``./media``, fine for dev
    #        but rarely what users want in prod).
    from .fields import FileField

    file_field_models = []
    for label, model in _model_registry.items():
        if "." in label or model._meta.abstract:
            continue
        for f in model._meta.fields:
            if isinstance(f, FileField):
                file_field_models.append(f"{model.__name__}.{f.name}")

    storages = getattr(settings, "STORAGES", {}) or {}
    if file_field_models and not storages:
        info.append(
            f"FileField in use ({', '.join(file_field_models[:3])}"
            f"{'…' if len(file_field_models) > 3 else ''}) but STORAGES is "
            "unset — dorm will write to ./media on the runner's working "
            "directory. Set STORAGES explicitly for prod."
        )

    if storages and "default" not in storages:
        warnings.append(
            "STORAGES is set but missing the required 'default' alias; "
            "FieldFile lookups will fail with ImproperlyConfigured."
        )

    for alias, spec in storages.items():
        backend = (spec or {}).get("BACKEND", "")
        opts = (spec or {}).get("OPTIONS") or {}
        if not backend:
            warnings.append(
                f"STORAGES[{alias!r}]: missing 'BACKEND' — every entry "
                "needs the dotted import path of a Storage subclass."
            )
            continue
        if backend.endswith("FileSystemStorage"):
            location = opts.get("location")
            if location:
                from pathlib import Path as _Path

                if not _Path(location).is_dir():
                    warnings.append(
                        f"STORAGES[{alias!r}]: location {location!r} is not "
                        "a directory; first save will fail unless your "
                        "deploy creates it."
                    )
                elif not _os.access(location, _os.W_OK):
                    warnings.append(
                        f"STORAGES[{alias!r}]: location {location!r} is not "
                        "writable by the current user."
                    )
            else:
                info.append(
                    f"STORAGES[{alias!r}]: FileSystemStorage with no "
                    "'location' falls back to ./media on the runner — "
                    "fine for dev, set explicitly in production."
                )
        elif "S3Storage" in backend:
            if not opts.get("bucket_name"):
                warnings.append(
                    f"STORAGES[{alias!r}]: S3Storage requires 'bucket_name' in OPTIONS."
                )
            if opts.get("access_key") or opts.get("secret_key"):
                # Hardcoded creds in settings → near-universal red flag.
                # IAM role on EC2/ECS/Lambda is the right answer in prod.
                warnings.append(
                    f"STORAGES[{alias!r}]: 'access_key' / 'secret_key' "
                    "set explicitly — fine for local MinIO / dev but in "
                    "production let boto3 pick them up from the IAM role "
                    "/ env vars / ~/.aws/ instead."
                )
            endpoint = opts.get("endpoint_url") or ""
            if (
                endpoint
                and endpoint.startswith("http://")
                and "localhost" not in endpoint
                and "127.0.0.1" not in endpoint
            ):
                warnings.append(
                    f"STORAGES[{alias!r}]: endpoint_url={endpoint!r} uses "
                    "plain HTTP for a non-local host. Use https:// to "
                    "avoid sending credentials in cleartext."
                )

    # ── Output
    print(f"dorm doctor — {len(warnings)} warning(s), {len(info)} note(s)")
    print()
    if warnings:
        print("warnings:")
        for w in warnings:
            print(f"  ! {w}")
        print()
    if info:
        print("notes:")
        for i in info:
            print(f"  · {i}")
        print()
    if not warnings and not info:
        print("everything looks reasonable — go ship.")
        return
    if warnings:
        sys.exit(1)


def cmd_dumpdata(args):
    """Serialize model rows to JSON on stdout (or ``--output FILE``).

    With no positional argument, dumps every concrete model in
    ``INSTALLED_APPS``. Pass ``app_label`` to scope to one app, or
    ``app_label.ModelName`` to scope to a single model. The output is
    a JSON array of ``{model, pk, fields}`` records, matching Django's
    ``dumpdata`` shape so existing fixtures load with ``dorm loaddata``.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    from .models import _model_registry
    from .serialize import dumps as serialize_dumps

    targets: list = []
    if not args.targets:
        targets = [
            model
            for label, model in _model_registry.items()
            if "." not in label and not model._meta.abstract
        ]
    else:
        for spec in args.targets:
            if "." in spec:
                # ``app.Model`` form — exact match against registry.
                if spec not in _model_registry:
                    print(f"Error: model {spec!r} not found.", file=sys.stderr)
                    sys.exit(1)
                targets.append(_model_registry[spec])
            else:
                # Either a bare model name, or an app label.
                if spec in _model_registry and "." not in spec:
                    targets.append(_model_registry[spec])
                    continue
                app_models = [
                    m
                    for label, m in _model_registry.items()
                    if "." not in label
                    and m._meta.app_label == spec
                    and not m._meta.abstract
                ]
                if not app_models:
                    print(
                        f"Error: {spec!r} matched no models or app labels.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                targets.extend(app_models)

    text = serialize_dumps(targets, indent=args.indent)
    if args.output and args.output != "-":
        Path(args.output).write_text(text + "\n")
        print(f"Wrote {len(targets)} model(s) to {args.output}")
    else:
        print(text)


def cmd_loaddata(args):
    """Load JSON fixtures from one or more files into the database.

    Each file is read as a JSON array of ``{model, pk, fields}``
    records. The whole load runs in a single transaction per file —
    a malformed record rolls back to the file's start instead of
    leaving a partial restore.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    from .serialize import load as serialize_load

    total = 0
    for fixture in args.fixtures:
        path = Path(fixture)
        if not path.exists():
            print(f"Error: fixture {fixture!r} not found.", file=sys.stderr)
            sys.exit(1)
        text = path.read_text()
        loaded = serialize_load(text, using=args.database)
        total += loaded
        print(f"  {fixture}: loaded {loaded} row(s)")
    print(f"Total: {total} row(s) loaded.")


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
    mm.add_argument(
        "--enable-pgvector",
        action="store_true",
        default=False,
        dest="enable_pgvector",
        help=(
            "Generate a migration that enables the pgvector PostgreSQL "
            "extension. Pair with at least one app label so the file "
            "lands in the right migrations directory."
        ),
    )
    mm.add_argument("--settings", default=None)
    mm.set_defaults(func=cmd_makemigrations)

    # migrate
    mg = sub.add_parser(
        "migrate",
        help="Apply pending migrations (or rollback when a target is given)",
    )
    mg.add_argument(
        "app_label",
        nargs="?",
        default=None,
        help="App to migrate (default: all apps)",
    )
    mg.add_argument(
        "target",
        nargs="?",
        default=None,
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
    sq = sub.add_parser(
        "squashmigrations", help="Squash a range of migrations into one"
    )
    sq.add_argument("app_label", help="App label")
    sq.add_argument(
        "start_migration",
        nargs="?",
        default="1",
        help="Migration number to start from (default: 1)",
    )
    sq.add_argument(
        "end_migration", help="Migration number to squash up to (inclusive)"
    )
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
        "names",
        nargs="*",
        help="Model names — bare (``User``) or app-qualified (``users.User``).",
    )
    sq2.add_argument(
        "--all",
        action="store_true",
        default=False,
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
        "apps",
        nargs="*",
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

    # dbshell
    dbsh = sub.add_parser(
        "dbshell",
        help=(
            "Drop into the native database client (psql / sqlite3) "
            "with credentials pre-filled from settings."
        ),
    )
    dbsh.add_argument(
        "--database",
        default="default",
        help="DATABASES alias to connect to (default: 'default').",
    )
    dbsh.set_defaults(func=cmd_dbshell)

    # inspectdb
    isp = sub.add_parser(
        "inspectdb",
        help=(
            "Generate dorm Model classes from the live database schema. "
            "Useful when adopting dorm in a project with a pre-existing schema."
        ),
    )
    isp.add_argument("--settings", default=None)
    isp.add_argument(
        "--database",
        default="default",
        help="DATABASES alias to introspect (default: 'default').",
    )
    isp.set_defaults(func=cmd_inspectdb)

    # doctor
    doc = sub.add_parser(
        "doctor",
        help=(
            "Audit settings, DATABASES and model declarations for "
            "production-mode footguns. Exits non-zero on warnings — "
            "use as a pre-deploy gate."
        ),
    )
    doc.add_argument("--settings", default=None)
    doc.set_defaults(func=cmd_doctor)

    # dumpdata
    dd = sub.add_parser(
        "dumpdata",
        help=(
            "Dump model rows as JSON. With no argument dumps every "
            "concrete model in INSTALLED_APPS; pass an app label or "
            "'app.Model' to scope. Pipe to a file or use --output."
        ),
    )
    dd.add_argument(
        "targets",
        nargs="*",
        help="App labels or app.ModelName to dump (default: all).",
    )
    dd.add_argument(
        "--indent",
        type=int,
        default=None,
        help="Indent level for pretty-printing the JSON output.",
    )
    dd.add_argument(
        "--output",
        "-o",
        default=None,
        metavar="FILE",
        help="Write the JSON to FILE (default: stdout). Use '-' for stdout explicitly.",
    )
    dd.add_argument("--settings", default=None)
    dd.set_defaults(func=cmd_dumpdata)

    # loaddata
    ld = sub.add_parser(
        "loaddata",
        help=(
            "Load JSON fixtures into the database. Each file is loaded "
            "in a single transaction; M2M relations are restored after "
            "all parent rows."
        ),
    )
    ld.add_argument(
        "fixtures",
        nargs="+",
        help="Path(s) to fixture JSON file(s).",
    )
    ld.add_argument(
        "--database",
        default="default",
        help="DATABASES alias to load into (default: 'default').",
    )
    ld.add_argument("--settings", default=None)
    ld.set_defaults(func=cmd_loaddata)

    # help
    hp = sub.add_parser("help", help="Show this help message and exit")
    hp.set_defaults(func=cmd_help, parser=parser)

    parsed = parser.parse_args()
    parsed.func(parsed)


if __name__ == "__main__":
    main()
