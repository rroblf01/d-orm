from __future__ import annotations

import logging
import re
from pathlib import Path

from .exceptions import ImproperlyConfigured

# Auto-discovery is convenience-grade: it loads ``settings.py`` from the
# script directory or cwd via ``importlib.exec_module``, which means we
# execute arbitrary Python from the filesystem. Logging the path that was
# loaded (and any import failures) makes that decision auditable in
# production without changing behaviour.
_logger = logging.getLogger("dorm.conf")

# A valid Python dotted module path: ``foo``, ``foo.bar``, ``my_app.models``.
# Anything else (path separators, ``..``, special characters) is rejected
# before being handed to ``importlib`` — this is a defence-in-depth for the
# CLI's ``--settings`` flag and ``DORM_SETTINGS`` env var, both of which
# accept user-controllable strings.
_DOTTED_PATH_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def _validate_dotted_path(value: str, *, kind: str = "module path") -> str:
    """Return *value* unchanged if it's a valid Python dotted module path,
    otherwise raise :class:`ImproperlyConfigured`. Used for the settings
    module name and app labels coming from the CLI / env."""
    if not isinstance(value, str) or not value:
        raise ImproperlyConfigured(
            f"Invalid {kind}: expected a non-empty string, got {value!r}."
        )
    if not _DOTTED_PATH_RE.match(value):
        raise ImproperlyConfigured(
            f"Invalid {kind} {value!r}: must be a Python dotted path "
            "(letters, digits, underscores, separated by dots; cannot start "
            "with a digit). Filesystem paths are not accepted."
        )
    return value

# Identifiers that we splice into SQL without parameter binding (table names,
# column names, related-name aliases) must match this pattern. Since SQL
# identifiers are user-controllable (via Meta.db_table, db_column,
# related_name, etc.), we validate them at model-attach time so a mistake
# raises eagerly with a clear message rather than producing a SQL injection.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENTIFIER_LEN = 63  # PostgreSQL's NAMEDATALEN-1; SQLite has no real cap.


def _validate_identifier(value: str, *, kind: str = "identifier") -> str:
    """Reject anything that wouldn't survive being spliced into a quoted
    SQL identifier (``"foo"``). Allowed: ASCII letters, digits, underscore;
    must start with a letter or underscore; up to 63 characters."""
    if not isinstance(value, str) or not value:
        raise ImproperlyConfigured(
            f"Invalid {kind}: expected a non-empty string, got {value!r}."
        )
    if len(value) > _MAX_IDENTIFIER_LEN:
        raise ImproperlyConfigured(
            f"Invalid {kind} {value!r}: exceeds {_MAX_IDENTIFIER_LEN} characters."
        )
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ImproperlyConfigured(
            f"Invalid {kind} {value!r}: must match {_SAFE_IDENTIFIER_RE.pattern}. "
            "Identifiers are spliced into SQL without quoting and must be safe."
        )
    return value

_AUTODISCOVER_EXCLUDE = {
    "venv", ".venv", "env", ".env", "site-packages",
    "__pycache__", ".git", ".hg", ".tox",
    "dist", "build", "node_modules", "migrations",
    ".mypy_cache", ".ruff_cache", ".pytest_cache",
}


def _discover_apps(root: Path) -> list[str]:
    """Return dotted app labels for every package under *root* that has a models.py.

    A valid app directory must:
      - contain a models.py
      - itself be a Python package (__init__.py present)
      - have no ancestor directory (up to root) that is not a Python package
      - not sit inside any of the excluded directory names
    """
    apps: list[str] = []
    for models_file in sorted(root.rglob("models.py")):
        pkg_dir = models_file.parent
        try:
            parts = pkg_dir.relative_to(root).parts
        except ValueError:
            continue

        # ``models.py`` at the search root has zero ``parts`` and would
        # produce an empty app label. The settings loader rejects empty
        # dotted paths anyway, so skip it here to avoid noise.
        if not parts:
            continue

        # Skip excluded or hidden directories
        if any(p in _AUTODISCOVER_EXCLUDE or p.startswith(".") for p in parts):
            continue

        # Every directory in the chain must be a Python package
        current = pkg_dir
        valid = True
        while current != root:
            if not (current / "__init__.py").exists():
                valid = False
                break
            current = current.parent
        if not valid:
            continue

        apps.append(".".join(parts))

    return apps


class Settings:
    DATABASES: dict = {}
    INSTALLED_APPS: list = []
    DEFAULT_AUTO_FIELD: str = "dorm.fields.BigAutoField"
    # Routers: list of objects with optional ``db_for_read(model, **hints)``
    # / ``db_for_write(model, **hints)`` methods. The first router that
    # returns a non-None alias wins. Use to point reads at a replica:
    #
    #     class ReplicaRouter:
    #         def db_for_read(self, model, **hints): return "replica"
    #         def db_for_write(self, model, **hints): return "default"
    #
    #     dorm.configure(DATABASES={...}, DATABASE_ROUTERS=[ReplicaRouter()])
    DATABASE_ROUTERS: list = []
    # NOTE: TIME_ZONE and USE_TZ are reserved for future timezone-aware
    # datetime support. They are NOT yet wired into the field encoding
    # paths — datetime values are stored exactly as Python provides them.
    # If you need timezone safety today, store UTC datetimes explicitly
    # at the application layer.
    TIME_ZONE: str = "UTC"
    USE_TZ: bool = False

    _configured = False

    def configure(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self._configured = True

    def __getattr__(self, name):
        raise ImproperlyConfigured(
            f"Requested setting '{name}' but dorm is not configured. "
            "Call dorm.configure(...) first."
        )


settings = Settings()


def configure(**kwargs):
    settings.configure(**kwargs)


def _autodiscover_settings() -> bool:
    """
    Look for a settings.py next to the running script or in cwd and
    auto-configure dorm from it.  Returns True if configuration succeeded.
    """
    import importlib.util
    import os
    import sys

    if settings._configured:
        return True

    candidates: list[str] = []

    # 1. Directory of the script being executed (sys.argv[0])
    if sys.argv and sys.argv[0]:
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        candidates.append(script_dir)

    # 2. Current working directory
    cwd = os.getcwd()
    if cwd not in candidates:
        candidates.append(cwd)

    for directory in candidates:
        path = os.path.join(directory, "settings.py")
        if not os.path.isfile(path):
            continue

        spec = importlib.util.spec_from_file_location("_dorm_auto_settings", path)
        if spec is None:
            continue
        if spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        # Auto-discovery executes the first ``settings.py`` it finds. Logging
        # at INFO so deployment auditors can see which file shaped the config.
        _logger.info("dorm autodiscovered settings: %s", path)
        spec.loader.exec_module(module)

        # Make the settings directory importable so models can be found
        if directory not in sys.path:
            sys.path.insert(0, directory)

        databases = getattr(module, "DATABASES", {})
        installed_apps = getattr(module, "INSTALLED_APPS", [])
        if not installed_apps:
            installed_apps = _discover_apps(Path(directory))
        settings.configure(DATABASES=databases, INSTALLED_APPS=installed_apps)

        # Import app models so they are registered before any query runs.
        # We distinguish three cases:
        #   - ModuleNotFoundError where the missing name is the app itself
        #     or its ``.models`` submodule → expected (the app may legitimately
        #     have no models.py); fall back to importing the package.
        #   - any other ModuleNotFoundError → a real missing dependency inside
        #     models.py (e.g. ``import psycopg2`` in a project on psycopg3);
        #     surface it instead of swallowing.
        #   - SyntaxError / other ImportError subclasses → broken user code;
        #     also surface so the user sees the traceback.
        import importlib as _imp
        for app in installed_apps:
            _validate_dotted_path(app, kind="INSTALLED_APPS entry")
            try:
                _imp.import_module(f"{app}.models")
                continue
            except ModuleNotFoundError as exc:
                missing = exc.name or ""
                if missing not in (app, f"{app}.models"):
                    _logger.warning(
                        "Failed to import %s.models: %s", app, exc
                    )
                    raise
            except (ImportError, SyntaxError):
                _logger.exception(
                    "Failed to import %s.models — see traceback above", app
                )
                raise
            try:
                _imp.import_module(app)
            except ModuleNotFoundError as exc:
                missing = exc.name or ""
                if missing != app:
                    _logger.warning("Failed to import app %s: %s", app, exc)
                    raise

        return True

    return False
