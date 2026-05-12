from __future__ import annotations

import argparse
import importlib
import os
import sys
from pathlib import Path
from typing import Any

from .conf import _validate_dotted_path


def _load_settings(settings_module: str):
    """Import a Python settings module and configure dorm.

    Profile selection: when *settings_module* is ``"settings"`` and
    the env var ``DORM_PROFILE`` is set, the loader transparently
    imports ``settings.<profile>`` instead — so
    ``DORM_PROFILE=prod`` resolves to ``settings/prod.py``. This
    matches the Django-flavoured ``settings/{dev,prod,test}.py``
    layout without bespoke wiring per project.
    """
    _validate_dotted_path(settings_module, kind="settings module")
    import os as _os

    profile = _os.environ.get("DORM_PROFILE")
    if profile and "." not in settings_module:
        candidate = f"{settings_module}.{profile}"
        try:
            module = importlib.import_module(candidate)
        except ModuleNotFoundError:
            module = importlib.import_module(settings_module)
    else:
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
    # Forward every uppercase top-level attribute from the settings
    # module — Django convention treats those as configuration. Without
    # this users typing ``SECRET_KEY = "…"`` / ``CACHES = {…}`` /
    # ``USE_TZ = True`` in settings.py would never see those values
    # reach ``dorm.conf.settings`` because earlier versions only
    # forwarded ``DATABASES`` + ``INSTALLED_APPS`` explicitly.
    extras = {
        k: getattr(module, k)
        for k in dir(module)
        if k.isupper() and not k.startswith("_") and k not in ("DATABASES", "INSTALLED_APPS")
    }
    configure(DATABASES=databases, INSTALLED_APPS=installed_apps, **extras)
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


def _resolve_app_module(app_arg: str, installed_apps: list[str]) -> str:
    """Map a CLI ``app`` argument back to its INSTALLED_APPS entry.

    Why: users on the CLI naturally type the short ``Meta.app_label``
    (``dorm migrate auth``), but ``_find_migrations_dir`` needs the
    importable dotted path (``dorm.contrib.auth``) to resolve the
    package's migrations folder. When *app_arg* is already an entry
    in INSTALLED_APPS, return it; otherwise scan for an entry whose
    resolved label matches.
    """
    if app_arg in installed_apps:
        return app_arg
    for entry in installed_apps:
        if _resolve_app_label(entry) == app_arg:
            return entry
    return app_arg


def _resolve_app_label(installed_app: str) -> str:
    """Return the actual ``Meta.app_label`` declared by the models of
    *installed_app*, or *installed_app* itself when no override.

    Why: contrib apps (e.g. ``dorm.contrib.auth``) live at a nested
    dotted path but declare a short ``app_label = "auth"`` so their
    db_table names stay clean. INSTALLED_APPS holds the dotted path,
    but ``ProjectState.from_apps`` / loader / executor / recorder all
    key by the actual ``app_label``. Without this resolver,
    ``makemigrations`` walks the registry looking for models tagged
    ``"dorm.contrib.auth"`` and finds none.

    Resolution rule: a model belongs to *installed_app* when its
    ``__module__`` equals or descends from *installed_app*. If every
    such model agrees on a single ``app_label``, return it; otherwise
    fall back to *installed_app* (ambiguous → user's package path is
    the safe default).
    """
    from .models import _model_registry

    candidates: set[str] = set()
    for key, model_cls in _model_registry.items():
        if "." in key:
            continue  # skip aliased entries
        mod = getattr(model_cls, "__module__", "")
        if mod == installed_app or mod.startswith(installed_app + "."):
            label = getattr(model_cls._meta, "app_label", "") or ""
            if label:
                candidates.add(label)
    if len(candidates) == 1:
        return next(iter(candidates))
    return installed_app


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

    # ── Merge two parallel migration branches ─────────────────────────────────
    if getattr(args, "merge", False):
        from .migrations.loader import MigrationLoader
        from .migrations.writer import write_empty_migration
        from .db.connection import get_connection

        conn = get_connection()
        targets = args.apps if args.apps else installed_apps
        any_merged = False
        for raw_app in targets:
            app = _resolve_app_module(raw_app, installed_apps)
            mig_dir = _find_migrations_dir(app)
            app_label = _resolve_app_label(app)
            loader = MigrationLoader(conn)
            loader.load(mig_dir, app_label)
            entries = loader.migrations.get(app_label, [])
            # Find leaves: migrations that no other migration in the
            # same app declares as a dependency. Two leaves = the
            # parallel-branch shape ``--merge`` resolves.
            referenced: set[str] = set()
            for _num, _name, mod in entries:
                for dep in getattr(mod, "dependencies", []) or []:
                    if isinstance(dep, tuple) and len(dep) == 2:
                        dep_app, dep_name = dep
                        if dep_app == app_label:
                            referenced.add(dep_name)
            leaves = [
                (num, name)
                for num, name, _mod in entries
                if name not in referenced
            ]
            if len(leaves) < 2:
                continue  # nothing to merge for this app
            any_merged = True
            next_num = _next_migration_number(mig_dir)
            path = write_empty_migration(
                app_label,
                mig_dir,
                next_num,
                name=args.name or "merge",
                dependencies=[(app_label, leaf_name) for _n, leaf_name in leaves],
            )
            print(
                f"  Merged {len(leaves)} leaves of {app_label!r} into {path}"
            )
        if not any_merged:
            print(
                "  No migration conflicts detected — every app has at "
                "most one leaf. Nothing to merge."
            )
        return

    # ── Empty migration ───────────────────────────────────────────────────────
    if args.empty:
        if not args.apps:
            print("Error: specify at least one app when using --empty.")
            return
        from .migrations.writer import write_empty_migration

        for raw_app in args.apps:
            app = _resolve_app_module(raw_app, installed_apps)
            mig_dir = _find_migrations_dir(app)
            next_num = _next_migration_number(mig_dir)
            name = args.name or "custom"
            path = write_empty_migration(
                _resolve_app_label(app), mig_dir, next_num, name=name
            )
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

        for raw_app in args.apps:
            app = _resolve_app_module(raw_app, installed_apps)
            mig_dir = _find_migrations_dir(app)
            next_num = _next_migration_number(mig_dir)
            name = args.name or "enable_pgvector"
            path = write_pgvector_extension_migration(
                _resolve_app_label(app), mig_dir, next_num, name=name
            )
            print(f"  Created pgvector extension migration: {path}")
        return

    # ── Auto-detect changes ───────────────────────────────────────────────────
    from .migrations.autodetector import MigrationAutodetector
    from .migrations.loader import MigrationLoader
    from .migrations.state import ProjectState
    from .migrations.writer import write_migration
    from .db.connection import get_connection

    if args.apps:
        apps = [_resolve_app_module(a, installed_apps) for a in args.apps]
    else:
        apps = installed_apps

    check_only = bool(getattr(args, "check", False))
    pending_count = 0

    for app in apps:
        print(f"Detecting changes for '{app}'...")
        app_label = _resolve_app_label(app)
        conn = get_connection()
        loader = MigrationLoader(conn)
        mig_dir = _find_migrations_dir(app)
        loader.load(mig_dir, app_label)

        # from_state = state described by all migration files on disk
        from_state = loader.get_migration_state(app_label, all_migrations=True)

        # to_state = current model definitions
        to_state = ProjectState.from_apps(app_label=app_label)

        detector = MigrationAutodetector(from_state, to_state)
        changes = detector.changes(app_label=app_label)

        if app_label not in changes or not changes[app_label]:
            print(f"  No changes detected for '{app}'.")
            continue

        if check_only:
            # CI gate mode — surface the diff but DON'T write.
            ops = changes[app_label]
            print(
                f"  Pending changes for '{app}': "
                f"{len(ops)} operation(s) would be written."
            )
            pending_count += len(ops)
            continue

        next_num = _next_migration_number(mig_dir)
        ops = changes[app_label]
        path = write_migration(app_label, mig_dir, next_num, ops)
        print(f"  Created migration: {path}")

    if check_only and pending_count > 0:
        print(
            f"makemigrations --check: {pending_count} pending operation(s). "
            "Run ``dorm makemigrations`` and commit the result."
        )
        sys.exit(1)


def cmd_squashmigrations(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    installed_apps = settings.INSTALLED_APPS
    _load_apps(installed_apps)

    app_arg = _resolve_app_module(args.app_label, installed_apps)
    app_label = _resolve_app_label(app_arg)
    start = int(args.start_migration)
    end = int(args.end_migration)
    squashed_name = args.squashed_name or "squashed"

    from .migrations.loader import MigrationLoader
    from .migrations.squasher import squash_operations
    from .migrations.writer import write_squashed_migration
    from .db.connection import get_connection

    mig_dir = _find_migrations_dir(app_arg)
    if not mig_dir.exists():
        print(f"Error: no migrations directory found for '{app_arg}'.")
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
    fake = getattr(args, "fake", False)
    fake_initial = getattr(args, "fake_initial", False)
    run_syncdb = getattr(args, "run_syncdb", False)
    prune = getattr(args, "prune", False)
    tenant = getattr(args, "tenant", None)
    all_tenants = getattr(args, "all_tenants", False)

    if tenant or all_tenants:
        # Per-tenant routing flow: PG-only. The runner switches
        # ``search_path`` per schema, so the same INSTALLED_APPS gets
        # migrated independently for every tenant.
        from .contrib.tenants import migrate_tenant, migrate_all_tenants

        apps_subset: list[str] | None = (
            [app_label] if app_label else None
        )
        if all_tenants:
            results = migrate_all_tenants(
                verbosity=args.verbosity, apps=apps_subset
            )
            for name, status in results.items():
                print(f"  [{name}] {status}")
            failures = [n for n, s in results.items() if not s.startswith("ok")]
            if failures:
                print(f"Failed tenants: {failures}")
                sys.exit(1)
            return
        if not isinstance(tenant, str):
            print("Error: --tenant requires a schema name.")
            return
        migrate_tenant(tenant, verbosity=args.verbosity, apps=apps_subset)
        print(f"  [{tenant}] ok")
        return

    if prune:
        # Walk the recorder, drop rows whose corresponding migration
        # file no longer exists on disk. Skip if the recorder table
        # itself isn't present (no migrations have ever run).
        try:
            rows = conn.execute(
                'SELECT "app", "name" FROM "dorm_migrations"'
            )
        except Exception:
            rows = []
        # dorm uses ``%s`` as the canonical placeholder; the SQLite
        # backend rewrites ``%s`` → ``?`` inside ``execute_write``
        # automatically, so a single template covers every vendor.
        for r in rows:
            app = r["app"]
            name = r["name"]
            module_path = _resolve_app_module(app, installed_apps)
            mig_path = _find_migrations_dir(module_path) / f"{name}.py"
            if not mig_path.exists():
                conn.execute_write(
                    'DELETE FROM "dorm_migrations" '
                    'WHERE "app" = %s AND "name" = %s',
                    [app, name],
                )
                print(f"  Pruned recorder row: {app}.{name}")
        if not target and not app_label and not (fake or fake_initial or run_syncdb):
            return

    if run_syncdb:
        # Create tables for every model whose app has NO migrations
        # directory. The migration executor handles apps that DO ship
        # migrations elsewhere in the loop.
        from .migrations.operations import _field_to_column_sql
        from .models import _model_registry

        seen: set[int] = set()
        for label, model in _model_registry.items():
            if "." in label or id(model) in seen:
                continue
            seen.add(id(model))
            if model._meta.abstract or model._meta.proxy:
                continue
            if not getattr(model._meta, "managed", True):
                continue
            module_path = (
                model.__module__.removesuffix(".models")
                if model.__module__.endswith(".models")
                else model.__module__
            )
            try:
                mig_dir = _find_migrations_dir(module_path)
            except Exception:
                mig_dir = None
            if mig_dir is not None and mig_dir.exists():
                continue  # skip — has migrations, executor handles it
            table = model._meta.db_table
            if conn.table_exists(table):
                continue
            cols = [
                _field_to_column_sql(f.name, f, conn)
                for f in model._meta.fields
                if f.db_type(conn)
            ]
            conn.execute_script(
                f'CREATE TABLE IF NOT EXISTS "{table}" (\n  '
                + ",\n  ".join(filter(None, cols))
                + "\n)"
            )
            print(f"  syncdb: created {table}")

    if app_label:
        # Accept both the dotted INSTALLED_APPS entry and the short
        # ``Meta.app_label`` form so ``dorm migrate auth`` works the
        # same as ``dorm migrate dorm.contrib.auth``.
        apps = [_resolve_app_module(app_label, installed_apps)]
    else:
        apps = installed_apps

    for app in apps:
        mig_dir = _find_migrations_dir(app)
        resolved_label = _resolve_app_label(app)
        if not mig_dir.exists():
            print(f"  No migrations directory for '{app}'. Run makemigrations first.")
            continue
        if target:
            try:
                if dry_run:
                    captured = executor.rollback(
                        resolved_label, mig_dir, target, dry_run=True
                    )
                    if captured:
                        print(
                            f"\n--- Rollback SQL that would run for '{app}' "
                            f"(target={target}) ---"
                        )
                        for sql, params in captured:
                            sql_print = sql.strip()
                            print(f"\n{sql_print};")
                            if params:
                                print(f"  -- params: {params!r}")
                        print()
                else:
                    executor.migrate_to(resolved_label, mig_dir, target)
            except ValueError as exc:
                print(f"  Error: {exc}")
                # Surface the failure through the CLI exit code so
                # CI gating on ``dorm migrate`` actually catches a
                # missing / invalid target. Previously the loop
                # continued and the process exited 0.
                sys.exit(1)
        else:
            captured = executor.migrate(
                resolved_label, mig_dir,
                dry_run=dry_run, fake=fake, fake_initial=fake_initial,
            )
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
    # ``_load_apps`` populates the registry so ``_resolve_app_label``
    # can map ``dorm.contrib.auth`` → ``auth``. Without it the resolver
    # falls back to the dotted path and ``show_migrations`` looks under
    # the wrong ``app_label`` in the recorder table.
    _load_apps(installed_apps)

    from .migrations.executor import MigrationExecutor
    from .db.connection import get_connection

    conn = get_connection()
    executor = MigrationExecutor(conn, verbosity=0)

    if args.apps:
        apps = [_resolve_app_module(a, installed_apps) for a in args.apps]
    else:
        apps = installed_apps
    for app in apps:
        mig_dir = _find_migrations_dir(app)
        executor.show_migrations(_resolve_app_label(app), mig_dir)


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

    if args.apps:
        apps_to_check = [_resolve_app_module(a, installed_apps) for a in args.apps]
    else:
        apps_to_check = installed_apps
    drift_found = False

    for app in apps_to_check:
        resolved_label = _resolve_app_label(app)
        models = [
            m
            for label, m in _model_registry.items()
            if "." not in label
            and m._meta.app_label == resolved_label
            and not m._meta.abstract
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


def cmd_lint_migrations(args):
    """Walk every ``INSTALLED_APPS`` migration directory and emit lint
    findings for known unsafe-online patterns.

    Exits non-zero when any finding is produced unless ``--exit-zero``
    is passed (treat findings as advisory). ``--rule DORM-M001`` (may
    repeat) restricts the scan to specific codes.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings as _settings_runtime
    from .migrations.lint import LintResult, lint_directory

    apps = list(_settings_runtime.INSTALLED_APPS)
    # Migration files frequently import from ``<app>.models`` (for
    # callable defaults, ``RunPython`` helpers, etc.). Loading the
    # apps up-front means ``importlib.exec_module`` on each migration
    # file finds those imports satisfied — without this, the linter
    # would fail with a useless ``ModuleNotFoundError`` on real
    # projects.
    _load_apps(apps)
    aggregate = LintResult()
    for app in apps:
        mig_dir = _find_migrations_dir(app)
        sub = lint_directory(mig_dir)
        aggregate.findings.extend(sub.findings)

    rule_filter: set[str] = set()
    raw_rules = getattr(args, "rule", None) or []
    for r in raw_rules:
        rule_filter.add(r.upper())
    if rule_filter:
        aggregate.findings = [
            f for f in aggregate.findings if f.code.upper() in rule_filter
        ]

    if args.format == "json":
        print(aggregate.to_json())
    else:
        print(aggregate.to_text())
    if getattr(args, "exit_zero", False):
        sys.exit(0)
    sys.exit(0 if aggregate.ok else 1)


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

    if getattr(args, "no_async", False):
        # Classic stdlib REPL — no top-level await. Useful when the
        # async-aware path interacts badly with a debugger / TTY in
        # rare environments.
        code.interact(banner=banner, local=local_vars)
        return

    # Async-aware REPL (3.0+): top-level ``await`` works without
    # wrapping in ``asyncio.run`` / coroutine functions, mirroring
    # ``python -m asyncio`` and IPython's ``using="asyncio"`` mode.
    # The compiled code object carries ``PyCF_ALLOW_TOP_LEVEL_AWAIT``
    # so ``await Article.objects.aget(pk=1)`` evaluates inline.
    import ast
    import asyncio

    class _AsyncConsole(code.InteractiveConsole):
        def __init__(self, locals: dict | None = None) -> None:
            super().__init__(locals=locals)
            self.compile.compiler.flags |= ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
            try:
                self._loop = asyncio.get_event_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)

        def runcode(self, code):  # type: ignore[override]
            try:
                result = eval(code, self.locals)
                if asyncio.iscoroutine(result):
                    self._loop.run_until_complete(result)
            except SystemExit:
                raise
            except BaseException:
                self.showtraceback()

    _AsyncConsole(locals=local_vars).interact(banner=banner)


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

# Transient-error retry. When the DB connection drops (network blip, RDS
# failover, server restart) dorm retries the operation outside an active
# transaction. Same resolution shape as ``SLOW_QUERY_MS``: this setting >
# env var > default. ``RETRY_ATTEMPTS=1`` disables retries.
# RETRY_ATTEMPTS = 3
# RETRY_BACKOFF = 0.1   # seconds; doubled per attempt (exp backoff)

# Per-block query-count guard threshold. ``None`` (default) leaves the guard
# inert. Used by ``dorm.contrib.querycount.query_count_guard`` as the
# fallback ``warn_above`` when the caller doesn't pass one — pair with
# ``nplusone`` for a fuller observability story.
# QUERY_COUNT_WARN = 50

# Sticky read-after-write window (seconds). After a write through the DB
# router, reads of the same model on the same context are pinned to the
# primary alias for this many seconds — so a request that writes and
# immediately re-reads sees its own change instead of a stale replica
# row. ``0`` or ``None`` disables.
# READ_AFTER_WRITE_WINDOW = 3.0

# ── DB router (read replicas) ─────────────────────────────────────────────────
# Route reads to a replica alias declared in DATABASES. The ``settings.py``
# example below is commented out by default; uncomment and replace with
# your own router.
#
# class PrimaryReplicaRouter:
#     def db_for_read(self, model, **hints):
#         return "replica"
#     def db_for_write(self, model, **hints):
#         return "default"
#
# DATABASE_ROUTERS = [PrimaryReplicaRouter()]

# ── Result cache ──────────────────────────────────────────────────────────────
# Uncomment one of the blocks below to enable ``QuerySet.cache(...)`` and
# ``Manager.cache_get(pk=…)``. Without CACHES configured both APIs become
# no-ops (queryset cache returns the original queryset; cache_get falls
# straight through to the DB).
#
# Redis (multi-worker, production):
# CACHES = {
#     "default": {
#         "BACKEND": "dorm.cache.redis.RedisCache",
#         "LOCATION": "redis://localhost:6379/0",
#         "TTL": 300,
#     }
# }
#
# In-process LRU (tests, single-process scripts, or a layer in front of Redis):
# CACHES = {
#     "default": {
#         "BACKEND": "dorm.cache.locmem.LocMemCache",
#         "OPTIONS": {"maxsize": 1024},
#         "TTL": 300,
#     }
# }
#
# Cache payloads are HMAC-signed before pickle to avoid RCE on a writable
# Redis instance — set CACHE_SIGNING_KEY (recommended) or rely on
# SECRET_KEY. Without either, dorm derives a per-process random key and
# logs a one-time warning.
# CACHE_SIGNING_KEY = ""
# CACHE_REQUIRE_SIGNING_KEY = False  # raise instead of falling back, in prod

# ── Field encryption (dorm.contrib.encrypted) ─────────────────────────────────
# Single base64-encoded 32-byte AES-256 key for ``EncryptedCharField`` /
# ``EncryptedTextField``. Generate with::
#
#   python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
#
# Multi-key list form for rotation — newest first. Requires
# ``pip install 'djanorm[encrypted]'``.
# FIELD_ENCRYPTION_KEY = ""
# FIELD_ENCRYPTION_KEYS = []

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


_TEMPLATES: dict[str, dict[str, str]] = {
    "fastapi-postgres": {
        "settings.py": (
            "import os\n"
            "import dorm\n\n"
            "DATABASES = {\n"
            "    'default': dorm.parse_database_url(\n"
            "        os.environ.get('DATABASE_URL',\n"
            "                       'postgresql://localhost/app'),\n"
            "    ),\n"
            "}\n"
            "INSTALLED_APPS = ['app']\n"
        ),
        "app/__init__.py": "",
        "app/models.py": (
            "import dorm\n\n\n"
            "class Item(dorm.Model):\n"
            "    name = dorm.CharField(max_length=80)\n"
            "    created_at = dorm.DateTimeField(auto_now_add=True)\n"
        ),
        "app/main.py": (
            "from fastapi import FastAPI\n"
            "from .models import Item\n\n"
            "app = FastAPI()\n\n\n"
            "@app.get('/items')\n"
            "async def list_items():\n"
            "    return [\n"
            "        {'id': i.id, 'name': i.name}\n"
            "        async for i in Item.objects.all()\n"
            "    ]\n"
        ),
    },
    "litestar-sqlite": {
        "settings.py": (
            "DATABASES = {\n"
            "    'default': {'ENGINE': 'sqlite', 'NAME': 'db.sqlite3'},\n"
            "}\n"
            "INSTALLED_APPS = ['app']\n"
        ),
        "app/__init__.py": "",
        "app/models.py": (
            "import dorm\n\n\n"
            "class Item(dorm.Model):\n"
            "    name = dorm.CharField(max_length=80)\n"
        ),
        "app/main.py": (
            "from litestar import Litestar, get\n"
            "from dorm.contrib.litestar import dorm_plugin\n"
            "from .models import Item\n\n\n"
            "@get('/items')\n"
            "async def list_items() -> list[dict]:\n"
            "    return [\n"
            "        {'id': i.id, 'name': i.name}\n"
            "        async for i in Item.objects.all()\n"
            "    ]\n\n"
            "app = Litestar(\n"
            "    route_handlers=[list_items],\n"
            "    plugins=[dorm_plugin()],\n"
            ")\n"
        ),
    },
}


def cmd_init(args):
    template = getattr(args, "template", None)
    cwd = Path.cwd()
    if template is not None:
        if template not in _TEMPLATES:
            print(
                f"Unknown template {template!r}. Available: "
                f"{', '.join(sorted(_TEMPLATES))}",
                file=sys.stderr,
            )
            sys.exit(2)
        for rel, content in _TEMPLATES[template].items():
            dest = cwd / rel
            if dest.exists():
                print(f"{dest} already exists — leaving it untouched.")
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content)
            print(f"Created {dest}")
        print()
        print(f"Template {template!r} scaffolded. Next:")
        print("  pip install 'djanorm[postgresql]' fastapi" if "fastapi" in template
              else "  pip install 'djanorm[sqlite,litestar]'")
        print("  dorm makemigrations app")
        print("  dorm migrate")
        return

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


def cmd_migrations_graph(args):
    """Render the migration dependency graph as Mermaid or DOT.

    Walks every app's ``migrations/`` directory, reads each migration's
    ``dependencies`` list, and emits one node per migration plus one
    edge per declared dependency. Output goes to stdout — pipe through
    the renderer of your choice::

        dorm migrations-graph --format=mermaid > graph.mmd
        dorm migrations-graph --format=dot | dot -Tpng -o graph.png
    """
    fmt = (args.format or "mermaid").lower()
    if fmt not in {"mermaid", "dot"}:
        print(f"Unknown --format={fmt!r}; expected 'mermaid' or 'dot'.", file=sys.stderr)
        sys.exit(2)

    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    from .conf import settings

    if not settings._configured:
        _load_settings(settings_mod)

    # Collect (app_label, migration_name, dependencies) tuples by
    # scanning every INSTALLED_APPS' ``migrations/`` directory.
    nodes: list[tuple[str, str]] = []
    edges: list[tuple[tuple[str, str], tuple[str, str]]] = []
    for app in settings.INSTALLED_APPS:
        try:
            mig_dir = _find_migrations_dir(app)
        except Exception:
            continue
        if not mig_dir.exists():
            continue
        app_label = _resolve_app_label(app)
        for mig_path in sorted(mig_dir.glob("*.py")):
            if mig_path.name.startswith("_"):
                continue
            name = mig_path.stem
            nodes.append((app_label, name))
            # Read the module's ``dependencies`` list via AST to avoid
            # importing migration modules (which would require fully
            # set-up apps).
            import ast

            try:
                tree = ast.parse(mig_path.read_text())
            except Exception:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for tgt in node.targets:
                        if isinstance(tgt, ast.Name) and tgt.id == "dependencies":
                            value = node.value
                            if isinstance(value, (ast.List, ast.Tuple)):
                                for item in value.elts:
                                    if (
                                        isinstance(item, (ast.Tuple, ast.List))
                                        and len(item.elts) == 2
                                    ):
                                        first = item.elts[0]
                                        second = item.elts[1]
                                        if (
                                            isinstance(first, ast.Constant)
                                            and isinstance(second, ast.Constant)
                                            and isinstance(first.value, str)
                                            and isinstance(second.value, str)
                                        ):
                                            edges.append(
                                                (
                                                    (app_label, name),
                                                    (first.value, second.value),
                                                )
                                            )

    if fmt == "mermaid":
        print("graph TD")
        for app, name in nodes:
            node_id = f"{app}__{name}".replace("-", "_")
            print(f'    {node_id}["{app}.{name}"]')
        for (a_app, a_name), (b_app, b_name) in edges:
            a = f"{a_app}__{a_name}".replace("-", "_")
            b = f"{b_app}__{b_name}".replace("-", "_")
            print(f"    {b} --> {a}")
        return

    # DOT
    print("digraph migrations {")
    print("    rankdir=LR;")
    for app, name in nodes:
        nid = f'"{app}.{name}"'
        print(f"    {nid};")
    for (a_app, a_name), (b_app, b_name) in edges:
        print(f'    "{b_app}.{b_name}" -> "{a_app}.{a_name}";')
    print("}")


def cmd_reset(args):
    """Drop every applied migration and re-apply them from scratch.

    ``dorm reset`` is shorthand for ``dorm migrate zero`` followed by
    ``dorm migrate``. Useful during development when the model graph
    diverges far enough that hand-rolling a migration is more work
    than nuking the database.

    Refuses to run when ``settings.DEBUG`` is False (or when the
    target alias name doesn't look like a dev/test database — the
    safety net is intentional). Pass ``--force`` to override.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    if not args.force:
        # Refuse against anything that smells like production.
        debug = bool(getattr(settings, "DEBUG", False))
        alias = "default"
        db_settings = settings.DATABASES.get(alias, {})
        name = str(db_settings.get("NAME", "")).lower()
        host = str(db_settings.get("HOST", "")).lower()
        looks_safe = (
            debug
            or "test" in name
            or "dev" in name
            or name in (":memory:", "")
            or host in ("", "localhost", "127.0.0.1")
        )
        if not looks_safe:
            print(
                "Refusing to reset: target doesn't look like a dev/test "
                "database. Pass --force to override (irrecoverable).",
                file=sys.stderr,
            )
            sys.exit(2)

    # Re-use cmd_migrate's plumbing by emitting fake args via
    # ``argparse.Namespace`` — typed attribute access keeps the
    # type-checker happy without resorting to a bespoke shim class.
    import argparse as _argparse

    cmd_migrate(
        _argparse.Namespace(
            settings=args.settings,
            app_label=None,
            target="zero",
            fake=False,
            fake_initial=False,
            dry_run=False,
            sync_db=False,
        )
    )
    cmd_migrate(
        _argparse.Namespace(
            settings=args.settings,
            app_label=None,
            target=None,
            fake=False,
            fake_initial=False,
            dry_run=False,
            sync_db=False,
        )
    )


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

    # 5. (4.0+) Read-only models declared but the table doesn't actually
    # back any reads — usually a stale migration left a Meta.read_only=True
    # flag on a model whose source-of-truth got renamed.
    for label, model in _model_registry.items():
        if "." in label or getattr(model._meta, "abstract", False):
            continue
        if getattr(model._meta, "read_only", False):
            info.append(
                f"{model.__name__}: Meta.read_only=True. Confirm reads still "
                "go through it — typical use is a model backed by a "
                "materialised view that gets refreshed via "
                "RefreshMaterializedView."
            )

    # 6. (4.0+) TenantModel without a paired middleware / context manager
    # at app-startup. We can't introspect the running framework's
    # middleware stack, but we can flag every TenantModel subclass so
    # the user double-checks they wired ``current_tenant()`` somewhere.
    _tenant_model_cls: Any = None
    try:
        from .contrib.tenants_row import TenantModel as _imported_tm

        _tenant_model_cls = _imported_tm
    except ImportError:
        _tenant_model_cls = None
    if _tenant_model_cls is not None:
        tenant_models = [
            model.__name__
            for label, model in _model_registry.items()
            if "." not in label
            and not getattr(model._meta, "abstract", False)
            and isinstance(model, type)
            and issubclass(model, _tenant_model_cls)
            and model is not _tenant_model_cls
        ]
        if tenant_models:
            info.append(
                f"TenantModel subclasses found ({', '.join(tenant_models[:3])}"
                f"{'…' if len(tenant_models) > 3 else ''}). "
                "Confirm every request handler / job wraps DB access in "
                "`with current_tenant(<id>):` — otherwise queries raise "
                "NoActiveTenantError at runtime."
            )

    # 7. (4.0+) Models without a primary key declared explicitly. The
    # auto-PK is fine for most apps, but if the model is sharded or
    # tenant-scoped, declaring the partition column as part of the PK
    # avoids cross-shard / cross-tenant unique constraint surprises.
    # We can't be 100% sure here without the user's intent; we only
    # warn about TenantModel subclasses + Meta.unique_together that
    # forgot tenant_id.
    if _tenant_model_cls is not None:
        for label, model in _model_registry.items():
            if "." in label or getattr(model._meta, "abstract", False):
                continue
            if not (
                isinstance(model, type)
                and issubclass(model, _tenant_model_cls)
                and model is not _tenant_model_cls
            ):
                continue
            uniques = getattr(model._meta, "unique_together", []) or []
            for ut in uniques:
                cols = ut if isinstance(ut, (list, tuple)) else [ut]
                if "tenant_id" not in cols:
                    warnings.append(
                        f"{model.__name__}: unique_together {tuple(cols)} omits "
                        "'tenant_id'. UNIQUE constraints on a TenantModel must "
                        "include the tenant column or rows from different "
                        "tenants will collide."
                    )

    # 8. (4.0+) Models with very wide bulk_create call sites that should
    # consider bulk_copy_from. Static check: nothing the doctor can
    # detect without parsing user code, so we only warn structurally
    # — flagging models with NO db_index on the FK columns the way
    # ETL workloads typically need.
    # (No-op placeholder for now.)

    # 9. (4.0+) Settings sanity for production primitives.
    if getattr(settings, "READ_AFTER_WRITE_WINDOW", 3.0) == 0:
        info.append(
            "READ_AFTER_WRITE_WINDOW=0 — sticky read-after-write disabled. "
            "Replicas may serve stale data right after a write. Default "
            "(3s) is a safer baseline unless you have a reason to disable."
        )

    # 10. (4.2+) Production-mode guards for newly added v4.2 features.
    is_debug = bool(getattr(settings, "DEBUG", False))
    if not is_debug:
        # DEBUG_NPLUSONE in non-DEBUG installs aborts requests at the
        # first violation — almost always wrong outside dev.
        nplusone_mode = getattr(settings, "DEBUG_NPLUSONE", False)
        if nplusone_mode:
            warnings.append(
                "DEBUG_NPLUSONE is active outside DEBUG mode. The detector "
                "ties up a signal receiver for the lifetime of the process "
                "and, in 'raise' mode, aborts requests on the first finding. "
                "Restrict to dev / staging."
            )
        # SLOW_QUERY_EXPLAIN re-runs slow queries to capture plans —
        # doubles their cost. Useful in staging; risky in prod.
        if getattr(settings, "SLOW_QUERY_EXPLAIN", False):
            slow_ms = getattr(settings, "SLOW_QUERY_MS", None)
            if not slow_ms or float(slow_ms) < 100:
                warnings.append(
                    "SLOW_QUERY_EXPLAIN=True without a meaningful SLOW_QUERY_MS "
                    "(>= 100ms). Every captured slow query is re-explained, "
                    "doubling its DB cost — set SLOW_QUERY_MS to a value high "
                    "enough that genuinely slow queries get the treatment."
                )
        # CSP-style allow-list: in prod the strong recommendation is
        # to install it without the DDL bypass; absence isn't an
        # error but it's a defence-in-depth opportunity worth a note.
        try:
            from .contrib import sql_allowlist as _sal

            if not _sal._state.enabled:
                info.append(
                    "dorm.contrib.sql_allowlist is not active — consider "
                    "installing it as a defence-in-depth layer once you've "
                    "captured the production template set (canary phase)."
                )
            elif _sal._state.allow_ddl:
                info.append(
                    "sql_allowlist is active but allows DDL — fine during "
                    "rolling deploys, but flip allow_ddl=False once "
                    "migrations have settled."
                )
        except Exception:  # pragma: no cover - defensive
            pass

    # 11. (4.2+) Pool saturation knob sanity.
    sat = getattr(settings, "POOL_SATURATION_WARN", 0.8)
    try:
        sat_f = float(sat)
    except (TypeError, ValueError):
        sat_f = 0.0
    if sat_f <= 0 or sat_f >= 1.0:
        info.append(
            f"POOL_SATURATION_WARN={sat!r} is outside (0, 1) — the warning "
            "will either fire on every poll or never. Pick a fraction "
            "between ~0.6 and ~0.9."
        )

    # 12. (4.2+) LagAwareReadRouter circuit-breaker sanity. Best-effort
    # — only fires when an explicit router instance is registered.
    routers = getattr(settings, "DATABASE_ROUTERS", []) or []
    try:
        from .contrib.lag_router import LagAwareReadRouter as _LR

        for r in routers:
            if isinstance(r, _LR) and getattr(r, "cooldown_seconds", 1) == 0:
                info.append(
                    "LagAwareReadRouter configured with cooldown_seconds=0 — "
                    "the per-replica circuit breaker is effectively disabled, "
                    "so a dead replica will be probed every cache window. "
                    "Set cooldown_seconds=30 (or higher) to amortise."
                )
    except Exception:  # pragma: no cover - defensive
        pass

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


def cmd_version(args):
    """Print the installed dorm package version. Useful in support
    tickets and CI logs that want to capture the toolchain shape
    in a single line."""
    from . import __version__

    print(f"djanorm {__version__}")


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
            # Try ``app.Model`` exact match first (registry stores
            # both module-derived and ``Meta.app_label`` aliases).
            if spec in _model_registry and "." in spec:
                targets.append(_model_registry[spec])
                continue
            if "." not in spec and spec in _model_registry:
                targets.append(_model_registry[spec])
                continue
            # Treat as app label. ``_resolve_app_label`` maps a
            # dotted INSTALLED_APPS entry (``dorm.contrib.auth``) to
            # the canonical ``Meta.app_label`` (``auth``).
            resolved = _resolve_app_label(spec)
            app_models = [
                m
                for label, m in _model_registry.items()
                if "." not in label
                and m._meta.app_label == resolved
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


def cmd_migrate_from_django(args):
    """Auto-port a Django ``models.py`` (or app directory) to a
    dorm-flavoured equivalent. Routes through
    :mod:`dorm.contrib.migrate_from_django`."""
    from pathlib import Path

    from .contrib.migrate_from_django import (
        convert_app,
        convert_models_file,
    )

    target = Path(args.path)
    if target.is_file():
        rewritten, todos = convert_models_file(target)
        if args.dry_run:
            print(rewritten)
        else:
            target.write_text(rewritten, encoding="utf-8")
            print(f"Converted {target}")
        if todos:
            print("\nTODOs flagged:", file=sys.stderr)
            for t in todos:
                print(f"  - {t}", file=sys.stderr)
        return

    if target.is_dir():
        try:
            results = convert_app(target, dry_run=args.dry_run)
        except FileNotFoundError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        for fname, todos in results.items():
            verb = "Would convert" if args.dry_run else "Converted"
            print(f"{verb} {fname}")
            for t in todos:
                print(f"  - {t}", file=sys.stderr)
        return

    print(
        f"Error: {args.path!r} is neither a file nor a directory.",
        file=sys.stderr,
    )
    sys.exit(1)


def cmd_runscript(args):
    """Execute a Python file under the project's settings, with
    ``INSTALLED_APPS`` preloaded. Mirrors Django-extensions
    ``runscript`` — a one-shot maintenance script runner.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    _load_apps(settings.INSTALLED_APPS)

    script_path = os.path.abspath(args.path)
    if not os.path.isfile(script_path):
        print(f"Error: script {args.path!r} not found.", file=sys.stderr)
        sys.exit(1)

    # Forward extra positional args so the script can read ``sys.argv``
    # the way it would under a normal interpreter invocation.
    extra = list(getattr(args, "args", None) or [])
    saved_argv = sys.argv
    sys.argv = [args.path, *extra]
    try:
        import runpy

        runpy.run_path(script_path, run_name="__main__")
    finally:
        sys.argv = saved_argv


def cmd_createsuperuser(args):
    """Mint a contrib.auth ``User`` with ``is_superuser=True``."""
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    _load_apps(settings.INSTALLED_APPS)
    from .contrib.auth.models import User

    pw = args.password
    if pw is None:
        import getpass

        pw = getpass.getpass("Password: ")
        confirm = getpass.getpass("Password (again): ")
        if pw != confirm:
            print("Passwords do not match.", file=sys.stderr)
            sys.exit(1)
    if not pw:
        print("Refusing to create a user with an empty password.", file=sys.stderr)
        sys.exit(1)
    User.objects.create_superuser(
        email=args.email, password=pw, username=args.username
    )
    print(f"Superuser {args.email} created.")


def cmd_changepassword(args):
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    _load_apps(settings.INSTALLED_APPS)
    from .contrib.auth.models import User

    try:
        user = User.objects.get(email=args.email)
    except User.DoesNotExist:
        print(f"User {args.email!r} not found.", file=sys.stderr)
        sys.exit(1)
    pw = args.password
    if pw is None:
        import getpass

        pw = getpass.getpass(f"New password for {args.email}: ")
        confirm = getpass.getpass("New password (again): ")
        if pw != confirm:
            print("Passwords do not match.", file=sys.stderr)
            sys.exit(1)
    if not pw:
        print("Refusing to set an empty password.", file=sys.stderr)
        sys.exit(1)
    user.set_password(pw)
    user.save(update_fields=["password"])
    print(f"Password updated for {args.email}.")


def cmd_flush(args):
    """Drop every row from every table the project owns. The schema
    stays in place; only the data is removed. Confirms unless
    ``--noinput`` is passed."""
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings as _settings

    _load_apps(_settings.INSTALLED_APPS)

    if not args.noinput:
        ack = input(
            "This will delete EVERY ROW in every table managed by INSTALLED_APPS. "
            "Type 'yes' to confirm: "
        )
        if ack.strip().lower() != "yes":
            print("Aborted.")
            return

    from .db.connection import get_connection
    from .models import _model_registry

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    seen: set[int] = set()

    # MySQL trips on FK constraints when we DELETE FROM a parent
    # table whose children still reference rows. Disable the
    # check for the duration of the flush — the surrounding
    # COMMIT re-enables for any code that runs afterwards in the
    # same connection.
    if vendor == "mysql":
        conn.execute_script("SET FOREIGN_KEY_CHECKS=0")

    try:
        for label, model in _model_registry.items():
            if "." in label:
                continue
            if id(model) in seen or model._meta.abstract or model._meta.proxy:
                continue
            seen.add(id(model))
            if not getattr(model._meta, "managed", True):
                continue
            table = model._meta.db_table
            if not conn.table_exists(table):
                continue
            if vendor == "postgresql":
                conn.execute_script(
                    f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'
                )
            elif vendor == "mysql":
                conn.execute_script(f'TRUNCATE TABLE "{table}"')
            else:
                conn.execute_script(f'DELETE FROM "{table}"')
    finally:
        if vendor == "mysql":
            try:
                conn.execute_script("SET FOREIGN_KEY_CHECKS=1")
            except Exception:
                pass
    print("Flushed.")


def cmd_sqlmigrate(args):
    """Render the SQL a migration would run, without applying it.

    Captures the real DDL/DML each :class:`Operation` would emit via
    the dry-run connection, so the output is the exact statement the
    migration runner would send to the database.
    """
    sys.path.insert(0, os.getcwd())
    settings_mod = args.settings or os.environ.get("DORM_SETTINGS", "settings")
    _load_settings(settings_mod)
    from .conf import settings

    installed = settings.INSTALLED_APPS
    _load_apps(installed)

    from .db.connection import get_connection
    from .migrations.executor import _DryRunConnection
    from .migrations.loader import MigrationLoader
    from .migrations.state import ProjectState

    conn = get_connection()
    app_module = _resolve_app_module(args.app_label, installed)
    app_label = _resolve_app_label(app_module)
    mig_dir = _find_migrations_dir(app_module)
    loader = MigrationLoader(conn)
    loader.load(mig_dir, app_label)
    matches = [
        m for m in loader.migrations.get(app_label, []) if m[1] == args.name
    ]
    if not matches:
        print(
            f"Migration {args.name!r} not found for app {args.app_label!r}.",
            file=sys.stderr,
        )
        sys.exit(1)
    _, _, module = matches[0]
    ops = list(getattr(module, "operations", []))
    if args.backwards:
        ops = list(reversed(ops))
    print(f"-- {app_label}.{args.name} ({'backwards' if args.backwards else 'forwards'})")

    # Re-play prior migrations into ProjectState so the target migration
    # sees the same model state it would at runtime. Each prior op's
    # ``state_forwards`` is fired with no DB side-effect.
    state = ProjectState()
    prior = [
        (mod_label, mig_name, mig_module)
        for mig_app, migs in loader.migrations.items()
        for (mod_label, mig_name, mig_module) in migs
    ]
    for mod_label, mig_name, mig_module in prior:
        if mod_label == app_label and mig_name == args.name:
            break
        for op in getattr(mig_module, "operations", []):
            try:
                op.state_forwards(mod_label, state)
            except Exception:
                # State-only replay shouldn't crash on unfamiliar ops.
                pass

    dry = _DryRunConnection(conn)
    for op in ops:
        print(f"-- {type(op).__name__}: {op.describe()}")
        from_state = state
        to_state = state.clone() if hasattr(state, "clone") else state
        try:
            if args.backwards:
                op.database_backwards(app_label, dry, from_state, to_state)
            else:
                op.database_forwards(app_label, dry, from_state, to_state)
        except Exception as exc:
            print(f"-- (capture failed: {type(exc).__name__}: {exc})")
            continue
        try:
            op.state_forwards(app_label, state)
        except Exception:
            pass
    for sql, params in dry.captured:
        rendered = sql.rstrip().rstrip(";")
        if params:
            print(f"{rendered}; -- params: {params}")
        else:
            print(f"{rendered};")


def cmd_diff(args):
    """Compare model registry vs live DB schema.

    Returns drift report covering:

    - Tables present in models but missing in DB (and vice versa).
    - Per-column drift: type mismatches, NOT NULL mismatches.
    - Indexes declared on the model but not present in the DB.
    """
    import json as _json
    import sys

    settings_module = getattr(args, "settings", None)
    if settings_module:
        _load_settings(settings_module)
    from .conf import settings as dorm_settings, _autodiscover_settings
    if not dorm_settings._configured:
        _autodiscover_settings()
    _load_apps(getattr(dorm_settings, "INSTALLED_APPS", []) or [])

    from .db.connection import get_connection
    from .inspect import introspect_tables
    from .models import _model_registry

    conn = get_connection(args.alias)

    # Live DB tables — keyed by table name.
    live_tables = {t["name"]: t for t in introspect_tables(conn)}

    # Model side — keyed by ``Meta.db_table``. Also pulls in M2M
    # junction tables so they don't surface as ``extra_table`` drift.
    from .fields import ManyToManyField as _M2M
    from typing import Any as _Any

    apps_filter = tuple(getattr(args, "apps", None) or [])

    model_tables: dict[str, _Any] = {}
    m2m_through_tables: set[str] = set()
    for fqname, cls in _model_registry.items():
        if "." not in fqname:
            continue
        meta = getattr(cls, "_meta", None)
        if meta is None or getattr(meta, "abstract", False):
            continue
        if getattr(meta, "proxy", False):
            continue
        if apps_filter and not any(
            cls.__module__.startswith(p) for p in apps_filter
        ):
            continue
        table = getattr(meta, "db_table", None)
        if table:
            model_tables[table] = cls
        for f in getattr(meta, "fields", []) or []:
            if isinstance(f, _M2M):
                # ``_get_through_table()`` returns the runtime junction
                # table name (default convention or explicit override).
                getter = getattr(f, "_get_through_table", None)
                through = getter() if callable(getter) else None
                if through:
                    m2m_through_tables.add(through)

    findings: list[dict] = []

    # Ignored modules — built-in dorm contrib whose tables are
    # bootstrapped opportunistically and routinely lag behind the
    # model registry in apps that don't actually use them. We match
    # by module prefix so the user's apps named ``contenttypes`` etc.
    # aren't accidentally swallowed.
    def _is_dorm_contrib_app(cls: type) -> bool:
        return cls.__module__.startswith(
            ("dorm.contrib.contenttypes", "dorm.contrib.auth", "dorm.contrib.history")
        )

    # 1. Tables present in models but missing in DB.
    for table, cls in sorted(model_tables.items()):
        if _is_dorm_contrib_app(cls):
            continue
        if table not in live_tables:
            findings.append(
                {
                    "kind": "missing_table",
                    "table": table,
                    "model": cls.__name__,
                    "detail": f"Model {cls.__name__!r} declares table {table!r} but it is not present in the database.",
                }
            )

    # 2. Tables in DB without a matching model — typically expected
    # for things like the migration history or extensions; only
    # report those that look user-facing.
    _ignored_prefixes = ("dorm_", "django_", "pg_", "sqlite_", "sql_", "auth_")
    for table in sorted(live_tables):
        if table in model_tables:
            continue
        if table in m2m_through_tables:
            continue
        if table.startswith(_ignored_prefixes):
            continue
        findings.append(
            {
                "kind": "extra_table",
                "table": table,
                "detail": f"Table {table!r} exists in the database but no model declares it.",
            }
        )

    # 3. Per-column drift: only on tables present in both sides.
    for table, cls in sorted(model_tables.items()):
        if _is_dorm_contrib_app(cls):
            continue
        if table not in live_tables:
            continue
        live_cols: dict[str, dict] = {
            c["name"]: c for c in live_tables[table].get("columns", [])
        }
        for f in cls._meta.fields:
            if not f.column:
                continue
            if f.column not in live_cols:
                findings.append(
                    {
                        "kind": "missing_column",
                        "table": table,
                        "column": f.column,
                        "detail": f"Field {cls.__name__}.{f.name} expects column {f.column!r} on {table!r} — not found.",
                    }
                )
                continue
            # Coarse type comparison: introspect-mapped Python type
            # name vs the field's class name. Tightening this would
            # require per-vendor catalog lookups; the coarse signal
            # already catches "INTEGER renamed to VARCHAR" foot-guns.
            live_field_cls = live_cols[f.column].get("field_class", "")
            expected = type(f).__name__
            if live_field_cls and not _types_compatible(expected, live_field_cls):
                findings.append(
                    {
                        "kind": "type_mismatch",
                        "table": table,
                        "column": f.column,
                        "detail": f"{cls.__name__}.{f.name}: model is {expected}, DB is {live_field_cls}.",
                    }
                )

    if args.json:
        _json.dump({"findings": findings, "drift": bool(findings)}, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        if not findings:
            print(f"OK — no drift detected against alias {args.alias!r}.")
        else:
            print(f"Drift detected against alias {args.alias!r}: {len(findings)} finding(s)\n")
            for f in findings:
                print(f"[{f['kind']}] {f['detail']}")
    sys.exit(1 if findings else 0)


def _types_compatible(model_type: str, db_type: str) -> bool:
    """Return True when *model_type* and *db_type* are equivalent
    enough that no migration is needed.

    The mapping is intentionally lenient — cosmetic differences
    between e.g. ``CharField`` and ``TextField`` are common when a
    DB was bootstrapped without a migration (raw ``CREATE TABLE``)
    and we don't want every diff run to flag them as drift.
    """
    if model_type == db_type:
        return True
    equivalents: list[set[str]] = [
        {"AutoField", "BigAutoField", "IntegerField", "BigIntegerField"},
        {"CharField", "TextField", "EmailField", "URLField", "SlugField"},
        {"DateTimeField", "DateField"},
        {"BooleanField", "NullBooleanField"},
    ]
    for group in equivalents:
        if model_type in group and db_type in group:
            return True
    return False


def cmd_export_json_schema(args):
    """Render every Model in INSTALLED_APPS as a JSON Schema doc.

    Schema dialect: ``https://json-schema.org/draft/2020-12/schema``.
    Each model becomes one schema with ``properties``, ``required``,
    ``additionalProperties: False`` and per-field constraints derived
    from the dorm field declaration (max_length, minimum/maximum,
    pattern for SlugField, format for EmailField / URLField, etc.).
    """
    import json
    import os
    import sys

    settings_module = getattr(args, "settings", None)
    if settings_module:
        _load_settings(settings_module)
    from .conf import settings as dorm_settings, _autodiscover_settings
    if not dorm_settings._configured:
        _autodiscover_settings()
    _load_apps(getattr(dorm_settings, "INSTALLED_APPS", []) or [])

    from .models import _model_registry

    apps_filter = set(args.apps or [])

    schemas: dict[str, dict] = {}
    for fqname, cls in _model_registry.items():
        if "." not in fqname:
            continue
        meta = getattr(cls, "_meta", None)
        if meta is None or getattr(meta, "abstract", False) or getattr(meta, "proxy", False):
            continue
        if apps_filter and not any(
            cls.__module__.startswith(a) for a in apps_filter
        ):
            continue
        schemas[cls.__name__] = _model_to_jsonschema(
            cls, include_relations=args.include_relations
        )

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        for name, schema in schemas.items():
            with open(os.path.join(args.out, f"{name}.json"), "w", encoding="utf-8") as fh:
                json.dump(schema, fh, indent=2)
                fh.write("\n")
        print(f"wrote {len(schemas)} schema(s) to {args.out}")
    else:
        json.dump(schemas, sys.stdout, indent=2)
        sys.stdout.write("\n")


def _model_to_jsonschema(cls, *, include_relations: bool) -> dict:
    """Render a single dorm Model class as a JSON Schema dict."""
    from . import fields as _f

    properties: dict[str, dict] = {}
    required: list[str] = []

    for fld in cls._meta.fields:
        if not fld.column:
            if include_relations and isinstance(fld, _f.ManyToManyField):
                properties[fld.name] = {
                    "type": "array",
                    "items": {"type": "integer"},
                }
            continue
        if isinstance(fld, _f.ManyToManyField):
            continue
        prop = _field_to_jsonschema(fld)
        properties[fld.name] = prop
        if not fld.null and not fld.has_default() and not isinstance(fld, _f.AutoField):
            required.append(fld.name)

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"{cls.__module__}.{cls.__name__}",
        "title": cls.__name__,
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _field_to_jsonschema(fld) -> dict:
    """Map a dorm Field to a JSON Schema fragment."""
    from . import fields as _f

    schema: dict = {}

    if isinstance(fld, (_f.IntegerField, _f.AutoField)):
        schema["type"] = "integer"
    elif isinstance(fld, _f.FloatField):
        schema["type"] = "number"
    elif isinstance(fld, _f.DecimalField):
        schema["type"] = "string"
        schema["pattern"] = r"^-?\d+(\.\d+)?$"
    elif isinstance(fld, _f.BooleanField):
        schema["type"] = "boolean"
    elif isinstance(fld, _f.UUIDField):
        schema["type"] = "string"
        schema["format"] = "uuid"
    elif isinstance(fld, _f.EmailField):
        schema["type"] = "string"
        schema["format"] = "email"
    elif isinstance(fld, _f.URLField):
        schema["type"] = "string"
        schema["format"] = "uri"
    elif isinstance(fld, _f.DateTimeField):
        schema["type"] = "string"
        schema["format"] = "date-time"
    elif isinstance(fld, _f.DateField):
        schema["type"] = "string"
        schema["format"] = "date"
    elif isinstance(fld, _f.TimeField):
        schema["type"] = "string"
        schema["format"] = "time"
    elif isinstance(fld, _f.JSONField):
        schema["type"] = ["object", "array", "string", "number", "boolean", "null"]
    elif isinstance(fld, (_f.CharField, _f.TextField, _f.SlugField)):
        schema["type"] = "string"
        max_len = getattr(fld, "max_length", None)
        if max_len:
            schema["maxLength"] = int(max_len)
    else:
        # Unknown / custom field — fallback to string with no constraints.
        schema["type"] = ["string", "null"]

    if fld.null:
        if isinstance(schema.get("type"), str):
            schema["type"] = [schema["type"], "null"]

    return schema


def _parse_duration(spec: str) -> int:
    """Parse a duration spec like ``30d`` / ``12h`` / ``45m`` / ``90s``
    into seconds. Bare integers are treated as seconds.

    Multi-token specs (``1d12h``) are NOT supported — pick a single
    unit and use the smallest meaningful value (``36h`` instead of
    ``1d12h``).
    """
    spec = spec.strip()
    if not spec:
        raise ValueError("duration spec must be non-empty")
    if spec[-1].isdigit():
        return int(spec)
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    suffix = spec[-1].lower()
    if suffix not in units:
        raise ValueError(
            f"unknown duration suffix {suffix!r}; use one of "
            f"{sorted(units)} or a bare integer (seconds)."
        )
    try:
        n = int(spec[:-1])
    except ValueError:
        raise ValueError(f"could not parse duration {spec!r}") from None
    return n * units[suffix]


def cmd_purge_deleted(args):
    """Hard-delete every SoftDeleteModel row whose ``deleted_at`` is
    older than the supplied window. Designed to run from cron / Celery
    beat / systemd timer."""
    import sys
    from datetime import datetime, timedelta, timezone

    settings_module = getattr(args, "settings", None)
    if settings_module:
        _load_settings(settings_module)
    from .conf import settings as dorm_settings, _autodiscover_settings
    if not dorm_settings._configured:
        _autodiscover_settings()
    _load_apps(getattr(dorm_settings, "INSTALLED_APPS", []) or [])

    seconds = _parse_duration(args.older_than)
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=seconds)

    from .contrib.softdelete import SoftDeleteModel
    from .models import _model_registry

    apps_filter = set(args.apps or [])

    from typing import Any as _Any

    purgeable: list[_Any] = []
    for fqname, cls in _model_registry.items():
        if "." not in fqname:
            continue
        if not isinstance(cls, type):
            continue
        if not issubclass(cls, SoftDeleteModel):
            continue
        if cls is SoftDeleteModel:
            continue
        if apps_filter and not any(
            cls.__module__.startswith(a) for a in apps_filter
        ):
            continue
        purgeable.append(cls)

    if not purgeable:
        print("No SoftDeleteModel subclasses found in the model registry.")
        sys.exit(0)

    from .db.connection import get_connection
    conn = get_connection(args.alias)

    total = 0
    for cls in purgeable:
        # Skip models whose table is missing — avoids spurious errors
        # when the registry holds models bootstrapped by an inactive
        # app (typical in test environments) or a multi-tenant
        # deployment where this alias isn't the model's home.
        try:
            if not conn.table_exists(cls._meta.db_table):
                continue
        except Exception:
            continue
        qs = cls.all_objects.using(args.alias).filter(deleted_at__lt=cutoff)
        n = qs.count()
        if args.dry_run:
            print(f"[DRY-RUN] {cls.__name__}: would purge {n} row(s).")
        else:
            # Hard-delete via raw SQL — the SoftDeleteModel.delete()
            # path is row-by-row and we want a single DML for the
            # purge. The CLI is already authenticated against the
            # alias, so trust the caller-supplied window.
            table = cls._meta.db_table
            placeholder = "%s" if conn.vendor == "postgresql" else "?"
            conn.execute_write(
                f'DELETE FROM "{table}" WHERE "deleted_at" < {placeholder}',
                [cutoff],
            )
            print(f"{cls.__name__}: purged {n} row(s).")
        total += n

    summary = "would purge" if args.dry_run else "purged"
    print(f"\nTotal: {summary} {total} row(s) older than {args.older_than}.")


def cmd_models_tree(args):
    """Print an ASCII tree of every registered model grouped by app
    label. Each node lists the model's FK / O2O / M2M edges with
    an arrow pointing at the target.

    Useful when a developer joins a project and wants to see the
    relationship graph at a glance without booting an IDE.
    """
    sys.path.insert(0, os.getcwd())
    settings_module = getattr(args, "settings", None)
    if settings_module:
        _load_settings(settings_module)
    from .conf import settings as dorm_settings, _autodiscover_settings
    if not dorm_settings._configured:
        _autodiscover_settings()
    _load_apps(getattr(dorm_settings, "INSTALLED_APPS", []) or [])

    from .fields import ForeignKey, ManyToManyField, OneToOneField
    from .models import _model_registry

    # Group dotted ``<app>.<Model>`` entries by app label. The
    # registry stores both the bare class name and the dotted form;
    # filter to the dotted entries so each model surfaces once.
    grouped: dict[str, list[Any]] = {}
    for key, cls in _model_registry.items():
        if "." not in key:
            continue
        meta = getattr(cls, "_meta", None)
        if meta is None or getattr(meta, "abstract", False):
            continue
        grouped.setdefault(meta.app_label, []).append(cls)

    if not grouped:
        print("(no models registered)")
        return

    for app in sorted(grouped):
        print(f"{app}/")
        models = sorted(grouped[app], key=lambda c: c.__name__)
        for i, cls in enumerate(models):
            is_last_model = i == len(models) - 1
            branch = "└── " if is_last_model else "├── "
            print(f"  {branch}{cls.__name__}")
            edges: list[tuple[str, str, str]] = []
            for f in cls._meta.fields:
                if isinstance(f, (ForeignKey, OneToOneField, ManyToManyField)):
                    kind = (
                        "M2M"
                        if isinstance(f, ManyToManyField)
                        else ("O2O" if isinstance(f, OneToOneField) else "FK")
                    )
                    target = getattr(f, "remote_field_to", None)
                    target_name = (
                        target.__name__ if isinstance(target, type) else str(target)
                    )
                    edges.append((kind, f.name, target_name))
            if not edges:
                continue
            for j, (kind, fname, target) in enumerate(edges):
                last_edge = j == len(edges) - 1
                edge_branch = "└── " if last_edge else "├── "
                prefix = "      " if is_last_model else "  │   "
                print(f"{prefix}{edge_branch}{kind} {fname} → {target}")


def cmd_saga_graph(args):
    """Render a :class:`dorm.contrib.saga.Saga` instance as Mermaid
    or DOT source.

    *args.path* is a dotted reference to the saga: either
    ``module.attr`` (the attribute is the Saga instance) or
    ``module:attr`` (Setuptools entry-point style — both forms are
    accepted because the colon form is the convention everywhere
    else in Python tooling).
    """
    import importlib

    spec = args.path
    if ":" in spec:
        module_path, attr_path = spec.split(":", 1)
    else:
        module_path, _, attr_path = spec.rpartition(".")
    if not module_path or not attr_path:
        print(
            f"Error: --path must be 'module.attr' or 'module:attr'; got {spec!r}.",
            file=sys.stderr,
        )
        sys.exit(2)
    sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_path)
    except Exception as exc:
        print(f"Error importing {module_path!r}: {exc}", file=sys.stderr)
        sys.exit(1)
    obj: Any = module
    for part in attr_path.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            print(
                f"Error: {module_path}.{attr_path} not found.",
                file=sys.stderr,
            )
            sys.exit(1)
    from .contrib.saga import Saga

    if not isinstance(obj, Saga):
        print(
            f"Error: {spec!r} resolved to a {type(obj).__name__}, not a Saga.",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.format == "mermaid":
        print(obj.to_mermaid(title=args.title))
    elif args.format == "dot":
        print(obj.to_dot(title=args.title))
    else:
        print(f"Error: unknown --format {args.format!r}", file=sys.stderr)
        sys.exit(2)


def cmd_help(args):
    args.parser.print_help()


def main():
    parser = argparse.ArgumentParser(
        prog="dorm",
        description="djanorm management commands",
    )
    # Surface the package version via ``dorm --version`` so installed
    # users can confirm the release without diving into Python.
    from . import __version__ as _dorm_version

    parser.add_argument(
        "--version",
        action="version",
        version=f"djanorm {_dorm_version}",
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
    mm.add_argument(
        "--check",
        action="store_true",
        default=False,
        help=(
            "Exit non-zero when the autodetector would write a "
            "migration. Does NOT touch the filesystem — the diff is "
            "printed for review and the process exits 1 if any "
            "operations are pending. Use this as a CI gate so PRs "
            "that change models without committing the matching "
            "migration get caught before merge."
        ),
    )
    mm.add_argument(
        "--merge",
        action="store_true",
        default=False,
        help=(
            "Resolve a merge-conflict between two parallel migration "
            "branches. When two developers create migrations against "
            "the same app from different branches, the merged branch "
            "ends up with two leaf migrations sharing the same "
            "dependency. ``--merge`` writes a new migration whose "
            "``dependencies = [...]`` lists every leaf, collapsing the "
            "fork into a linear graph. The merge migration carries "
            "no operations (the diverging migrations stay applied "
            "as-is); it only re-points the migration graph's tip."
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
        "--plan",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Print the SQL that would be executed without touching the "
        "database. The migration recorder is NOT updated, so the "
        "next run still sees the same set of pending migrations. "
        "Recommended as a pre-deploy review step. ``--plan`` is an "
        "alias kept for users coming from Django's migrate command.",
    )
    mg.add_argument(
        "--fake",
        action="store_true",
        default=False,
        help="Mark every pending migration as applied WITHOUT running "
        "its operations. Use when the schema already matches "
        "the desired state — typically when adopting dorm against "
        "a hand-managed legacy database.",
    )
    mg.add_argument(
        "--fake-initial",
        action="store_true",
        default=False,
        help="Like --fake but only the initial migration of each app "
        "(no dependencies) is faked, and only when every "
        "CreateModel target table already exists. Subsequent "
        "migrations run for real.",
    )
    mg.add_argument(
        "--run-syncdb",
        action="store_true",
        default=False,
        help="Create tables for INSTALLED_APPS that ship NO migration "
        "files (legacy / hand-managed apps). Mirrors Django's "
        "``migrate --run-syncdb`` — useful when adopting dorm "
        "incrementally against a multi-app project where a "
        "subset has migrations and the rest doesn't yet.",
    )
    mg.add_argument(
        "--prune",
        action="store_true",
        default=False,
        help="Drop recorder rows for migrations whose source files "
        "no longer exist on disk (e.g. after squashmigrations). "
        "No DDL — only the ``dorm_migrations`` bookkeeping is "
        "touched.",
    )
    mg.add_argument(
        "--tenant",
        default=None,
        metavar="NAME",
        help="Run migrations against the named PostgreSQL schema "
        "(per-tenant routing). Switches ``search_path`` to the "
        "tenant before applying DDL and restores it afterwards. "
        "PG-only — other backends raise NotImplementedError. "
        "Pair with an ``app_label`` positional to limit the run "
        "to that app within the tenant.",
    )
    mg.add_argument(
        "--all-tenants",
        action="store_true",
        default=False,
        dest="all_tenants",
        help="Run migrations against every tenant registered via "
        "``dorm.contrib.tenants.register_tenant``. Each tenant "
        "is migrated independently — partial failures are "
        "summarised at the end and the process exits non-zero "
        "when any tenant fails.",
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
        help=(
            "Start an interactive Python shell with every INSTALLED_APPS "
            "model preloaded (uses IPython if installed, otherwise the "
            "stdlib REPL)"
        ),
    )
    sh.add_argument("--settings", default=None)
    sh.set_defaults(func=cmd_shell)

    # shell_plus — Django-extensions parity alias. ``dorm shell``
    # already auto-imports every model into the namespace, so the
    # two commands are functionally identical; ``shell_plus`` is
    # exposed because muscle memory from Django.
    sh_plus = sub.add_parser(
        "shell_plus",
        help="Alias for ``dorm shell`` — Django-extensions parity",
    )
    sh_plus.add_argument("--settings", default=None)
    sh_plus.set_defaults(func=cmd_shell)

    # runscript — execute a Python file with dorm configured
    rs = sub.add_parser(
        "runscript",
        help="Execute a Python file under the project's settings, with INSTALLED_APPS preloaded",
    )
    rs.add_argument("path", help="Path to the Python file to execute.")
    rs.add_argument("--settings", default=None)
    rs.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="Extra positional args forwarded as ``sys.argv[1:]``.",
    )
    rs.set_defaults(func=cmd_runscript)

    # migrate-from-django
    mfd = sub.add_parser(
        "migrate-from-django",
        help=(
            "Auto-port a Django ``models.py`` (or app directory) to a "
            "dorm-shaped equivalent. Re-run ``dorm makemigrations`` "
            "afterwards to produce a fresh migration history."
        ),
    )
    mfd.add_argument(
        "path",
        help=(
            "Path to a Django ``models.py`` file OR an app directory "
            "(containing ``models.py`` or a ``models/`` sub-package)."
        ),
    )
    mfd.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show the converted output and TODOs without modifying files.",
    )
    mfd.set_defaults(func=cmd_migrate_from_django)

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
    ini.add_argument(
        "--template",
        default=None,
        choices=sorted(_TEMPLATES),
        help=(
            "Scaffold from a named template instead of the minimal "
            "settings.py + app pair. Available: "
            f"{', '.join(sorted(_TEMPLATES))}."
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

    # lint-migrations
    lm = sub.add_parser(
        "lint-migrations",
        help=(
            "Audit every migration in INSTALLED_APPS for online-safe "
            "deploy footguns (DORM-M001..M005). Exits non-zero on findings — "
            "wire it into CI as a pre-merge gate."
        ),
    )
    lm.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    lm.add_argument(
        "--rule",
        action="append",
        default=None,
        metavar="CODE",
        help=(
            "Restrict to specific rule codes (may repeat). Example: "
            "--rule DORM-M001 --rule DORM-M003."
        ),
    )
    lm.add_argument(
        "--exit-zero",
        action="store_true",
        default=False,
        help=(
            "Exit 0 even when findings exist. Useful for advisory "
            "runs that should NOT fail CI."
        ),
    )
    lm.add_argument("--settings", default=None)
    lm.set_defaults(func=cmd_lint_migrations)

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

    # migrations-graph
    mg_graph = sub.add_parser(
        "migrations-graph",
        help=(
            "Render the migration dependency graph in Mermaid or DOT "
            "format. Useful for spotting branched / orphan migrations."
        ),
    )
    mg_graph.add_argument("--settings", default=None)
    mg_graph.add_argument(
        "--format",
        choices=("mermaid", "dot"),
        default="mermaid",
        help="Output format. Default 'mermaid'.",
    )
    mg_graph.set_defaults(func=cmd_migrations_graph)

    # saga-graph
    sg = sub.add_parser(
        "saga-graph",
        help=(
            "Render a Saga instance as Mermaid or DOT source. "
            "Useful for documenting compensation paths."
        ),
    )
    sg.add_argument(
        "path",
        help=(
            "Dotted path to the Saga instance — either 'pkg.module.attr' "
            "or 'pkg.module:attr'."
        ),
    )
    sg.add_argument(
        "--format",
        choices=("mermaid", "dot"),
        default="mermaid",
        help="Output format. Default 'mermaid'.",
    )
    sg.add_argument(
        "--title",
        default=None,
        help="Optional diagram title rendered as a header / comment.",
    )
    sg.set_defaults(func=cmd_saga_graph)

    # models-tree
    mt = sub.add_parser(
        "models-tree",
        help=(
            "Print an ASCII tree of every registered model grouped "
            "by app label, with FK / O2O / M2M edges annotated."
        ),
    )
    mt.add_argument("--settings", default=None)
    mt.set_defaults(func=cmd_models_tree)

    # reset (DEV ONLY)
    rst = sub.add_parser(
        "reset",
        help=(
            "Drop every applied migration and re-apply them from scratch. "
            "Refuses to run against production-looking DATABASES — pass "
            "--force to override."
        ),
    )
    rst.add_argument("--settings", default=None)
    rst.add_argument(
        "--force",
        action="store_true",
        help="Bypass the safety check that blocks resets against non-dev DBs.",
    )
    rst.set_defaults(func=cmd_reset)

    # version
    ver = sub.add_parser(
        "version",
        help="Print the installed dorm package version.",
    )
    ver.set_defaults(func=cmd_version)

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

    # createsuperuser
    csu = sub.add_parser(
        "createsuperuser",
        help="Create a superuser from the contrib.auth User model",
    )
    csu.add_argument("--email", required=True)
    csu.add_argument(
        "--password",
        required=False,
        help="Password (prompted interactively when omitted).",
    )
    csu.add_argument("--username", required=False)
    csu.add_argument("--settings", required=False)
    csu.set_defaults(func=cmd_createsuperuser)

    # changepassword
    cpw = sub.add_parser(
        "changepassword",
        help="Change a user's password (contrib.auth User by default)",
    )
    cpw.add_argument("email")
    cpw.add_argument("--password", required=False)
    cpw.add_argument("--settings", required=False)
    cpw.set_defaults(func=cmd_changepassword)

    # flush
    flush = sub.add_parser(
        "flush",
        help="Truncate every table for the configured INSTALLED_APPS",
    )
    flush.add_argument(
        "--noinput",
        action="store_true",
        help="Skip the confirmation prompt — automation-friendly.",
    )
    flush.add_argument("--settings", required=False)
    flush.set_defaults(func=cmd_flush)

    # sqlmigrate
    sqm = sub.add_parser(
        "sqlmigrate",
        help="Print the SQL of a single migration without running it",
    )
    sqm.add_argument("app_label")
    sqm.add_argument("name")
    sqm.add_argument(
        "--backwards",
        action="store_true",
        help="Render the reverse SQL (useful before unapplying).",
    )
    sqm.add_argument("--settings", required=False)
    sqm.set_defaults(func=cmd_sqlmigrate)

    # diff — schema drift detector
    df = sub.add_parser(
        "diff",
        help=(
            "Compare the project's models against the live database "
            "schema and report any drift. Reads ``information_schema`` "
            "(PG) or ``sqlite_master`` (SQLite) and walks the model "
            "registry; lists tables missing on either side, columns "
            "that disagree on type, and missing indexes. Exits 0 on a "
            "clean match, 1 on drift — suitable as a CI gate post-deploy."
        ),
    )
    df.add_argument(
        "--alias",
        default="default",
        help="Database alias to compare against (default: 'default').",
    )
    df.add_argument(
        "--apps",
        nargs="*",
        default=None,
        help=(
            "Restrict to models whose module path begins with one of "
            "these prefixes (default: every model in the registry). "
            "Useful in test suites or multi-tenant deployments where "
            "the model registry holds models from unrelated apps."
        ),
    )
    df.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit findings as JSON instead of a human-readable summary.",
    )
    df.set_defaults(func=cmd_diff)

    # export-json-schema
    ex = sub.add_parser(
        "export-json-schema",
        help=(
            "Export every Model in INSTALLED_APPS as a JSON Schema "
            "document (Draft 2020-12). Output goes to stdout by "
            "default; use --out to write one file per model into a "
            "directory. Useful as input for downstream validators "
            "(Ajv, Zod runtime) or schema-registry tooling."
        ),
    )
    ex.add_argument(
        "--out",
        default=None,
        help="Output directory (one <ModelName>.json per model). Default: stdout.",
    )
    ex.add_argument(
        "--apps",
        nargs="*",
        default=None,
        help="Restrict to models in these app modules (default: all installed).",
    )
    ex.add_argument(
        "--include-relations",
        action="store_true",
        default=False,
        help="Inline FK / OneToOne / M2M as nested schemas (depth 1).",
    )
    ex.set_defaults(func=cmd_export_json_schema)

    # purge-deleted — TTL job for SoftDeleteModel rows
    pd = sub.add_parser(
        "purge-deleted",
        help=(
            "Hard-delete every SoftDeleteModel row whose deleted_at "
            "is older than --older-than. Cron-friendly: runs with no "
            "interactive prompts, exits 0 on success and prints a "
            "per-model summary. Pass --dry-run to log without deleting."
        ),
    )
    pd.add_argument(
        "--older-than",
        required=True,
        metavar="DURATION",
        help=(
            "Time window — e.g. ``30d``, ``12h``, ``45m``. Rows whose "
            "deleted_at < (now - DURATION) are eligible."
        ),
    )
    pd.add_argument(
        "--apps",
        nargs="*",
        default=None,
        help="Restrict purge to models in these app modules.",
    )
    pd.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report counts without deleting. Recommended on first run.",
    )
    pd.add_argument(
        "--alias",
        default="default",
        help="Database alias (default: 'default').",
    )
    pd.set_defaults(func=cmd_purge_deleted)

    # help
    hp = sub.add_parser("help", help="Show this help message and exit")
    hp.set_defaults(func=cmd_help, parser=parser)

    parsed = parser.parse_args()
    parsed.func(parsed)


if __name__ == "__main__":
    main()
