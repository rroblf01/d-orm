from __future__ import annotations

import atexit
import contextvars
import threading
import time as _stickytime
from typing import Any

from ..exceptions import ImproperlyConfigured

_sync_connections: dict[str, Any] = {}
_async_connections: dict[str, Any] = {}
_sync_lock = threading.Lock()


# Sticky read-after-write state. The ContextVar holds a 2-tuple
# ``(owner_id, dict)``; ``owner_id`` is the ``id()`` of the dict the
# task first wrote, so we know whether the dict is private to this
# task (mutate in place — the common path) or shared with a parent
# context (copy-on-write once, then mutate for the rest of the
# task's lifetime). The empty default sentinel ``(0, {})`` is shared
# across every task that never wrote, so the first write upgrades
# to a private dict.
_EMPTY_STICKY: tuple[int, dict[tuple[str, str], float]] = (0, {})
_sticky_until: contextvars.ContextVar[
    tuple[int, dict[tuple[str, str], float]]
] = contextvars.ContextVar("dorm_sticky_until", default=_EMPTY_STICKY)


def _model_key(model: Any) -> tuple[str, str]:
    """``(app_label, ModelName)`` for the sticky map. Falls back to a
    safe sentinel when called with a non-model object so the helper
    doesn't crash inside a router that exposes plain strings."""
    meta = getattr(model, "_meta", None)
    if meta is None:
        return ("", getattr(model, "__name__", str(model)))
    return (
        getattr(meta, "app_label", "") or "",
        getattr(meta, "model_name", "") or model.__class__.__name__,
    )


def _read_after_write_window() -> float:
    """Configured sticky window in seconds (``0`` disables)."""
    try:
        from ..conf import settings

        val = getattr(settings, "READ_AFTER_WRITE_WINDOW", None)
    except Exception:
        return 0.0
    if val is None:
        return 0.0
    try:
        return max(0.0, float(val))
    except (TypeError, ValueError):
        return 0.0


def _own_sticky_dict() -> dict[tuple[str, str], float]:
    """Return the per-task dict, upgrading from the shared empty
    sentinel on first write. After the upgrade every later mutation
    on this task is in place — no per-write dict copy."""
    owner, mapping = _sticky_until.get()
    if owner != id(mapping):
        # Either the empty sentinel or a dict inherited from a parent
        # context that another task may still observe. Take a private
        # copy and stamp ourselves as the owner.
        mapping = dict(mapping)
        _sticky_until.set((id(mapping), mapping))
    return mapping


def _mark_recent_write(model: Any) -> None:
    """Record a write through the router so subsequent reads of the
    same model in the same context return the primary alias for the
    configured window. Cheap no-op when the window is zero."""
    window = _read_after_write_window()
    if window <= 0:
        return
    mapping = _own_sticky_dict()
    mapping[_model_key(model)] = _stickytime.monotonic() + window


def _is_sticky(model: Any) -> bool:
    key = _model_key(model)
    _owner, mapping = _sticky_until.get()
    expires = mapping.get(key)
    if expires is None:
        return False
    if expires <= _stickytime.monotonic():
        # Expired — drop the entry so the dict doesn't grow unbounded.
        # Upgrade to a private dict before mutating so we don't touch
        # one another task is reading.
        _own_sticky_dict().pop(key, None)
        return False
    return True


def clear_read_after_write_window() -> None:
    """Drop every recorded sticky entry on the current context. Useful
    in tests or middleware that want a clean window per request."""
    _sticky_until.set(_EMPTY_STICKY)


def router_db_for_read(model, *, default: str = "default", **hints) -> str:
    """Consult ``settings.DATABASE_ROUTERS`` for the alias to use when
    reading rows of *model*. First router that returns a truthy string
    wins; otherwise *default*.

    When a write through :func:`router_db_for_write` happened on the
    current context within the last
    ``settings.READ_AFTER_WRITE_WINDOW`` seconds (default 3.0), the
    router is *bypassed* and *default* is returned — so the request
    that wrote the row sees its own change instead of a stale replica
    snapshot. Pass ``sticky=False`` in ``hints`` to opt out of this
    behaviour for a specific call (analytics queries that explicitly
    want the replica even right after a write)."""
    from ..conf import settings

    if hints.get("sticky", True) and _is_sticky(model):
        return default

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
    """Mirror of :func:`router_db_for_read` for writes. Records the
    write so subsequent reads stay sticky to the primary for the
    configured window."""
    from ..conf import settings

    _mark_recent_write(model)

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

    # libsql must be checked before the bare ``sqlite`` substring
    # match — otherwise ``ENGINE="libsql"`` would route through the
    # plain SQLite wrapper and skip the remote / embedded-replica
    # connection setup.
    if "libsql" in engine:
        from .backends.libsql import LibSQLDatabaseWrapper
        return LibSQLDatabaseWrapper(db_settings)
    if "sqlite" in engine:
        from .backends.sqlite import SQLiteDatabaseWrapper
        return SQLiteDatabaseWrapper(db_settings)
    if "postgresql" in engine or "postgres" in engine:
        from .backends.postgresql import PostgreSQLDatabaseWrapper
        return PostgreSQLDatabaseWrapper(db_settings)
    if "mysql" in engine or "mariadb" in engine:
        # Scaffold backend (3.0+); raises ``ImproperlyConfigured``
        # at construction time pointing at the v3.1 milestone.
        from .backends.mysql import MySQLDatabaseWrapper
        return MySQLDatabaseWrapper(db_settings, alias)

    raise ImproperlyConfigured(
        f"Unsupported database engine: '{engine}'. "
        "Supported: 'sqlite', 'postgresql', 'libsql' (mysql scaffold ships in 3.0+)."
    )


def _create_async_connection(alias: str, db_settings: dict):
    engine = db_settings.get("ENGINE", "sqlite").lower()

    if "libsql" in engine:
        from .backends.libsql import LibSQLAsyncDatabaseWrapper
        return LibSQLAsyncDatabaseWrapper(db_settings)
    if "sqlite" in engine:
        from .backends.sqlite import SQLiteAsyncDatabaseWrapper
        return SQLiteAsyncDatabaseWrapper(db_settings)
    if "postgresql" in engine or "postgres" in engine:
        from .backends.postgresql import PostgreSQLAsyncDatabaseWrapper
        return PostgreSQLAsyncDatabaseWrapper(db_settings)
    if "mysql" in engine or "mariadb" in engine:
        from .backends.mysql import MySQLAsyncDatabaseWrapper
        return MySQLAsyncDatabaseWrapper(db_settings, alias)

    raise ImproperlyConfigured(
        f"Unsupported database engine: '{engine}'. "
        "Supported: 'sqlite', 'postgresql', 'libsql' (mysql scaffold ships in 3.0+)."
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


def _health_check_basic(alias: str = "default", timeout: float = 5.0) -> dict[str, Any]:
    """Run a trivial ``SELECT 1`` against the configured backend and return
    a status dict suitable for a Kubernetes / ECS / Render readiness probe.

    Returns a ``dict`` with ``status`` (``"ok"`` / ``"error"``), ``alias``,
    ``elapsed_ms``, and (on failure) ``error`` describing the underlying
    exception. Never raises — health checks must always respond, even when
    the database is down.

    Public callers should use :func:`health_check` instead, which adds
    optional ``deep=True`` pool stats. The split exists so the basic
    probe stays fast and the deep variant can compose it.
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


async def _ahealth_check_basic(alias: str = "default", timeout: float = 5.0) -> dict[str, Any]:
    """Async counterpart of :func:`_health_check_basic` for FastAPI /
    Starlette / Sanic routes."""
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


def pool_stats(alias: str = "default") -> dict[str, Any]:
    """Return live pool statistics for *alias*.

    Returned keys, when available:

    - ``alias`` — the database alias.
    - ``vendor`` — ``"postgresql"`` or ``"sqlite"``.
    - ``has_pool`` — whether this backend has a real connection pool.
    - For PostgreSQL with an open pool: ``pool_min``, ``pool_max``,
      ``pool_size`` (currently open connections), ``pool_available``
      (idle / not in use), ``requests_waiting``, ``requests_num``
      (total checkouts), ``usage_ms``, ``connections_ms`` and the rest
      of psycopg's `get_stats() <https://www.psycopg.org/psycopg3/docs/advanced/pool.html>`_
      output, all under the same key names.
    - For SQLite (no pool): a minimal dict with the in-flight
      atomic-block depth so dashboards have *something* to graph.

    Use this in a Prometheus / OpenTelemetry exporter or a debug
    endpoint. Never raises — returns ``{"status": "uninitialised"}`` if
    the alias has no live connection yet (calling this in a healthz
    handler before the first query is fine).
    """
    if alias not in _sync_connections and alias not in _async_connections:
        return {"alias": alias, "status": "uninitialised"}

    sync_conn = _sync_connections.get(alias)
    async_conn = _async_connections.get(alias)
    conn = sync_conn or async_conn
    if conn is None:
        return {"alias": alias, "status": "uninitialised"}

    vendor = getattr(conn, "vendor", "sqlite")
    out: dict[str, Any] = {"alias": alias, "vendor": vendor, "has_pool": False}

    if vendor == "postgresql":
        # The pool lives on the async wrapper; sync uses a per-call
        # pool too but we only surface the async one because that's what
        # production traffic typically hits. If both wrappers exist,
        # prefer whichever has an open pool.
        for c in (async_conn, sync_conn):
            pool = getattr(c, "_pool", None)
            if pool is None:
                continue
            try:
                stats = pool.get_stats()
            except Exception:  # pragma: no cover — defensive
                continue
            out["has_pool"] = True
            out.update(stats)
            out["pool_min"] = getattr(pool, "min_size", None)
            out["pool_max"] = getattr(pool, "max_size", None)
            break
    elif vendor == "sqlite":
        # SQLite has no pool, but reporting atomic depth is cheap and
        # actually useful for "stuck transaction" detection.
        out["atomic_depth"] = getattr(conn, "_atomic_depth", 0)

    return out


def health_check(
    alias: str = "default",
    timeout: float = 5.0,
    deep: bool = False,
) -> dict[str, Any]:
    """Override-friendly version of the basic :func:`health_check`.

    See :func:`_health_check_basic` for the always-on probe. Pass
    ``deep=True`` to additionally include :func:`pool_stats` so the
    same endpoint can serve both readiness and observability.
    """
    result = _health_check_basic(alias, timeout=timeout)
    if deep:
        result["pool"] = pool_stats(alias)
    return result


async def ahealth_check(
    alias: str = "default",
    timeout: float = 5.0,
    deep: bool = False,
) -> dict[str, Any]:
    """Async counterpart of :func:`health_check`."""
    result = await _ahealth_check_basic(alias, timeout=timeout)
    if deep:
        result["pool"] = pool_stats(alias)
    return result


def reset_connections():
    """Force re-creation of connections (useful for testing)."""
    # Drop any process-level caches that key off the previous
    # connection's data — the most-bitten offender being the
    # ContentType ``(app_label, model)`` → instance cache, which
    # otherwise survives a test's ``DROP TABLE`` and hands later
    # callers a row whose pk no longer exists. Wrapped in a broad
    # try because the import path is optional (some installs may
    # not pull contrib.contenttypes).
    try:
        from ..contrib.contenttypes.models import ContentType as _CT

        _CT.objects.clear_cache()
    except Exception:
        pass

    for conn in _sync_connections.values():
        if hasattr(conn, "close"):
            try:
                conn.close()
            except Exception:
                pass
    _sync_connections.clear()
    # Async wrappers can't be awaited from here, but each backend exposes
    # a ``force_close_sync`` that releases its underlying handles
    # deterministically — without it the GC finalises the SQLite
    # connection later (``ResourceWarning: unclosed database``) and
    # leaves the aiosqlite worker thread parked on its queue, which under
    # ``pytest -n 4`` can keep the interpreter from exiting.
    for conn in _async_connections.values():
        force = getattr(conn, "force_close_sync", None)
        if force is not None:
            try:
                force()
            except Exception:
                pass
    _async_connections.clear()


def _atexit_close() -> None:
    """Close all connections at process exit, sync and async.

    Async wrappers can't be awaited from an atexit hook, but each one
    exposes :meth:`force_close_sync` to release its underlying handles
    deterministically (closing the sqlite3 connection or scheduling the
    pool close on its original loop)."""
    for conn in _async_connections.values():
        force = getattr(conn, "force_close_sync", None)
        if force is not None:
            try:
                force()
            except Exception:
                pass
    _async_connections.clear()
    close_all()


atexit.register(_atexit_close)
