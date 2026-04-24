from .exceptions import ImproperlyConfigured


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
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        # Make the settings directory importable so models can be found
        if directory not in sys.path:
            sys.path.insert(0, directory)

        databases = getattr(module, "DATABASES", {})
        installed_apps = getattr(module, "INSTALLED_APPS", [])
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
