from __future__ import annotations

from pathlib import Path

from .exceptions import ImproperlyConfigured

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
        spec.loader.exec_module(module)

        # Make the settings directory importable so models can be found
        if directory not in sys.path:
            sys.path.insert(0, directory)

        databases = getattr(module, "DATABASES", {})
        installed_apps = getattr(module, "INSTALLED_APPS", [])
        if not installed_apps:
            installed_apps = _discover_apps(Path(directory))
        settings.configure(DATABASES=databases, INSTALLED_APPS=installed_apps)

        # Import app models so they are registered before any query runs
        import importlib as _imp
        for app in installed_apps:
            try:
                _imp.import_module(f"{app}.models")
            except ImportError:
                try:
                    _imp.import_module(app)
                except ImportError:
                    pass

        return True

    return False
