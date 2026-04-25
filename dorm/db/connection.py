from __future__ import annotations

import atexit
import threading
from typing import Any

from ..exceptions import ImproperlyConfigured

_sync_connections: dict[str, Any] = {}
_async_connections: dict[str, Any] = {}
_sync_lock = threading.Lock()


def router_db_for_read(model, *, default: str = "default", **hints) -> str:
    """Consult ``settings.DATABASE_ROUTERS`` for the alias to use when
    reading rows of *model*. First router that returns a truthy string
    wins; otherwise *default*."""
    from ..conf import settings

    for router in getattr(settings, "DATABASE_ROUTERS", []) or []:
        fn = getattr(router, "db_for_read", None)
        if fn is None:
            continue
        try:
            alias = fn(model, **hints)
        except Exception:
            continue
        if alias:
            return alias
    return default


def router_db_for_write(model, *, default: str = "default", **hints) -> str:
    """Mirror of :func:`router_db_for_read` for writes."""
    from ..conf import settings

    for router in getattr(settings, "DATABASE_ROUTERS", []) or []:
        fn = getattr(router, "db_for_write", None)
        if fn is None:
            continue
        try:
            alias = fn(model, **hints)
        except Exception:
            continue
        if alias:
            return alias
    return default


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


def health_check(alias: str = "default", timeout: float = 5.0) -> dict[str, Any]:
    """Run a trivial ``SELECT 1`` against the configured backend and return
    a status dict suitable for a Kubernetes / ECS / Render readiness probe.

    Returns a ``dict`` with ``status`` (``"ok"`` / ``"error"``), ``alias``,
    ``elapsed_ms``, and (on failure) ``error`` describing the underlying
    exception. Never raises — health checks must always respond, even when
    the database is down.
    """
    import time as _time

    start = _time.perf_counter()
    try:
        conn = get_connection(alias)
        conn.execute("SELECT 1")
    except Exception as exc:
        return {
            "status": "error",
            "alias": alias,
            "elapsed_ms": (_time.perf_counter() - start) * 1000.0,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "status": "ok",
        "alias": alias,
        "elapsed_ms": (_time.perf_counter() - start) * 1000.0,
    }


async def ahealth_check(alias: str = "default", timeout: float = 5.0) -> dict[str, Any]:
    """Async counterpart of :func:`health_check` for FastAPI / Starlette /
    Sanic routes."""
    import asyncio
    import time as _time

    start = _time.perf_counter()
    try:
        conn = get_async_connection(alias)
        await asyncio.wait_for(conn.execute("SELECT 1"), timeout=timeout)
    except Exception as exc:
        return {
            "status": "error",
            "alias": alias,
            "elapsed_ms": (_time.perf_counter() - start) * 1000.0,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "status": "ok",
        "alias": alias,
        "elapsed_ms": (_time.perf_counter() - start) * 1000.0,
    }


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


def _atexit_close() -> None:
    """Close sync connections at process exit. Async connections rely on
    their daemon worker thread being torn down by the interpreter."""
    _async_connections.clear()
    close_all()


atexit.register(_atexit_close)
