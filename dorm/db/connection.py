from __future__ import annotations

import threading
from typing import Any

from ..exceptions import ImproperlyConfigured

_sync_connections: dict[str, Any] = {}
_async_connections: dict[str, Any] = {}
_sync_lock = threading.Lock()


def _get_settings(alias: str = "default") -> dict:
    from ..conf import settings, _autodiscover_settings
    if not settings._configured:
        _autodiscover_settings()
    if not settings.DATABASES:
        raise ImproperlyConfigured(
            "DATABASES is not configured. Call dorm.configure(DATABASES={...}) first, "
            "or place a settings.py next to your script."
        )
    if alias not in settings.DATABASES:
        raise ImproperlyConfigured(
            f"Database alias '{alias}' not found in DATABASES configuration."
        )
    return settings.DATABASES[alias]


def _create_sync_connection(alias: str, db_settings: dict):
    engine = db_settings.get("ENGINE", "sqlite").lower()

    if "sqlite" in engine:
        from .backends.sqlite import SQLiteDatabaseWrapper
        return SQLiteDatabaseWrapper(db_settings)
    if "postgresql" in engine or "postgres" in engine:
        from .backends.postgresql import PostgreSQLDatabaseWrapper
        return PostgreSQLDatabaseWrapper(db_settings)

    raise ImproperlyConfigured(
        f"Unsupported database engine: '{engine}'. "
        "Supported: 'sqlite', 'postgresql'."
    )


def _create_async_connection(alias: str, db_settings: dict):
    engine = db_settings.get("ENGINE", "sqlite").lower()

    if "sqlite" in engine:
        from .backends.sqlite import SQLiteAsyncDatabaseWrapper
        return SQLiteAsyncDatabaseWrapper(db_settings)
    if "postgresql" in engine or "postgres" in engine:
        from .backends.postgresql import PostgreSQLAsyncDatabaseWrapper
        return PostgreSQLAsyncDatabaseWrapper(db_settings)

    raise ImproperlyConfigured(
        f"Unsupported database engine: '{engine}'. "
        "Supported: 'sqlite', 'postgresql'."
    )


def get_connection(alias: str = "default"):
    if alias in _sync_connections:
        return _sync_connections[alias]
    with _sync_lock:
        if alias not in _sync_connections:
            db_settings = _get_settings(alias)
            _sync_connections[alias] = _create_sync_connection(alias, db_settings)
    return _sync_connections[alias]


def get_async_connection(alias: str = "default"):
    if alias in _async_connections:
        return _async_connections[alias]
    with _sync_lock:
        if alias not in _async_connections:
            db_settings = _get_settings(alias)
            _async_connections[alias] = _create_async_connection(alias, db_settings)
    return _async_connections[alias]


def close_all():
    for conn in _sync_connections.values():
        if hasattr(conn, "close"):
            conn.close()
    _sync_connections.clear()


async def close_all_async():
    for conn in _async_connections.values():
        if hasattr(conn, "close"):
            await conn.close()
    _async_connections.clear()


def reset_connections():
    """Force re-creation of connections (useful for testing)."""
    for conn in _sync_connections.values():
        if hasattr(conn, "close"):
            try:
                conn.close()
            except Exception:
                pass
    _sync_connections.clear()
    # Async pools can only be closed with await; they will be GC'd.
    _async_connections.clear()
