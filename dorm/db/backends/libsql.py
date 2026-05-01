"""libsql backend powered by ``pyturso``.

Three modes share a single configuration shape:

- **Local file** — drop-in SQLite replacement, no server needed.
  Uses ``turso.connect(path)``.
- **Embedded replica** — local file kept in sync with a remote
  ``sqld`` instance you run yourself (typical: a VPS exposing
  ``/v1/...`` on HTTPS). Reads land on the local replica;
  writes flush to the remote master, then replicate back. Uses
  ``turso.sync.connect(path, remote_url, auth_token=…)``.
- **Turso Cloud** — same wire protocol as a self-hosted ``sqld``,
  so the same ``SYNC_URL`` + ``AUTH_TOKEN`` knobs work against
  ``https://<db>-<org>.turso.io``.

Async support uses ``turso.aio`` for the local-file path
(native async I/O). When ``SYNC_URL`` is set the wrapper has
to fall back to the sync client on a dedicated worker thread
because pyturso's async API is local-only today.

Vector support is enabled via the ``experimental_features``
flag — pyturso ships ``vector`` (and a handful of other
experimental features) under that gate. The wrapper passes
``experimental_features="vector"`` so ``F32_BLOB(N)`` columns
and the ``vector_distance_*`` SQL functions are available out
of the box.

The dependency is optional: ``pip install djanorm[libsql]``.
Without it the module imports cleanly; only when a libsql
connection is opened does :class:`ImproperlyConfigured` surface
the install command.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from typing import Any

from ...exceptions import ImproperlyConfigured
from .sqlite import SQLiteAsyncDatabaseWrapper, SQLiteDatabaseWrapper


def _import_turso():
    """Resolve the ``turso`` package lazily.

    Imports are deferred so ``djanorm`` itself loads cleanly even
    when the optional ``[libsql]`` extra isn't installed; the
    helpful error surfaces only at connection-open time.
    """
    import importlib

    try:
        return importlib.import_module("turso")
    except ImportError:
        raise ImproperlyConfigured(
            "libsql backend requires pyturso. Install via:\n"
            "    pip install 'djanorm[libsql]'\n"
            "    pip install pyturso"
        )


def _import_turso_sync():
    """Resolve ``turso.sync`` (embedded-replica + remote)."""
    _import_turso()
    import importlib

    return importlib.import_module("turso.sync")


def _import_turso_aio():
    """Resolve ``turso.aio`` (native async, local-only)."""
    _import_turso()
    import importlib

    return importlib.import_module("turso.aio")


# Experimental feature flags pyturso must enable for native
# vector functions (``F32_BLOB`` / ``vector_distance_*``).
# ``vector`` is the documented Turso feature name; if the
# pyturso build doesn't ship it, the connection still opens —
# the vector-using queries themselves will fail at SQL-parse
# time with a clear ``OperationalError``.
_VECTOR_FEATURES = "vector"


class LibSQLDatabaseWrapper(SQLiteDatabaseWrapper):
    """Synchronous libsql wrapper.

    Configuration shape mirrors the SQLite backend with three
    optional keys:

    ``NAME``
        Local file path. Defaults to ``:memory:``. With
        ``SYNC_URL`` set this becomes the embedded-replica file.
    ``SYNC_URL``
        Remote endpoint URL — typically ``https://your-vps.example``
        for a self-hosted ``sqld`` or ``https://<db>-<org>.turso.io``
        for Turso Cloud. Setting this turns the connection into
        an embedded replica.
    ``AUTH_TOKEN``
        Bearer token sent as ``Authorization: Bearer <token>`` on
        every sync round-trip. Optional for self-hosted ``sqld``
        running in a trusted network; required by Turso Cloud and
        recommended for any internet-exposed ``sqld``.
    """

    vendor = "libsql"

    def __init__(self, settings: dict):
        super().__init__(settings)
        self.sync_url: str | None = settings.get("SYNC_URL")
        self.auth_token: str | None = settings.get("AUTH_TOKEN")

    def _new_connection(self) -> sqlite3.Connection:
        # Pick the right pyturso entrypoint:
        #   * remote / embedded replica → ``turso.sync.connect``
        #   * local-only                → ``turso.connect``
        # Both return a ``sqlite3.Connection``-shaped object so
        # the SQLite wrapper's row hydration works unchanged.
        if self.sync_url:
            turso_sync = _import_turso_sync()
            kwargs: dict[str, Any] = {
                "remote_url": self.sync_url,
                "experimental_features": _VECTOR_FEATURES,
            }
            if self.auth_token:
                kwargs["auth_token"] = self.auth_token
            try:
                conn = turso_sync.connect(self.database, **kwargs)
            except TypeError:
                # Forward-compat: a future pyturso may rename or
                # drop a kwarg. Drop the experimental flag and
                # retry — the connection is still useful, just
                # without native vector support.
                kwargs.pop("experimental_features", None)
                conn = turso_sync.connect(self.database, **kwargs)
        else:
            turso = _import_turso()
            try:
                conn = turso.connect(
                    self.database,
                    experimental_features=_VECTOR_FEATURES,
                )
            except TypeError:
                conn = turso.connect(self.database)

        try:
            # pyturso ships its own ``turso.Row`` class — supports
            # both index and name-based access like ``sqlite3.Row``
            # but with a different internal layout, so we can't
            # simply reuse ``sqlite3.Row`` as the factory.
            try:
                import turso as _turso

                conn.row_factory = _turso.Row
            except Exception:
                pass
            try:
                conn.execute("PRAGMA foreign_keys = ON")
            except Exception:
                # Some sqld deployments treat PRAGMA as a no-op;
                # don't fail the connection on a non-fatal
                # PRAGMA reject.
                pass
            if self._autocommit:
                try:
                    conn.isolation_level = None
                except Exception:
                    pass
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise
        return conn

    def sync_replica(self) -> None:
        """Force a pull from the remote master into the embedded
        replica. No-op for local-only mode (nothing to sync).

        Useful between an external write (e.g. a write made by
        another writer node) and a local read when you need
        strong consistency.
        """
        if not self.sync_url:
            return
        conn = self.get_connection()
        sync = getattr(conn, "sync", None)
        if sync is None:
            return
        try:
            sync()
        except Exception:
            # Network blip / auth error / replica-not-ready —
            # never propagate into the caller's hot path.
            pass

    # ── Param coercion ──────────────────────────────────────────
    #
    # pyturso (like the older libsql_experimental client) is
    # stricter than stdlib ``sqlite3`` — bind params must be a
    # tuple or a Mapping. The shared SQLite wrapper code calls
    # ``conn.execute(sql, params or [])`` with a list, so we
    # coerce in the four execute-shaped entry points.

    @staticmethod
    def _coerce_params(params: Any) -> Any:
        if params is None:
            return ()
        if isinstance(params, (list, tuple)):
            return tuple(params)
        return params  # already a Mapping

    def execute(self, sql: str, params: Any = None) -> list:
        return super().execute(sql, self._coerce_params(params))

    def execute_write(self, sql: str, params: Any = None) -> int:
        return super().execute_write(sql, self._coerce_params(params))

    def execute_insert(
        self, sql: str, params: Any = None, pk_col: str = "id"
    ) -> Any:
        return super().execute_insert(
            sql, self._coerce_params(params), pk_col=pk_col
        )

    def execute_bulk_insert(
        self,
        sql: str,
        params: Any = None,
        pk_col: str = "id",
        count: int = 1,
    ) -> list[int]:
        return super().execute_bulk_insert(
            sql, self._coerce_params(params), pk_col=pk_col, count=count
        )


class LibSQLAsyncDatabaseWrapper(SQLiteAsyncDatabaseWrapper):
    """Asynchronous libsql wrapper.

    Two paths:

    - **Local-only** (no ``SYNC_URL``): uses ``turso.aio.connect``
      for native async I/O — the wrapper awaits cursor calls
      directly without bouncing onto a worker thread.
    - **Embedded replica / remote** (``SYNC_URL`` set): pyturso's
      async API is local-only, so the wrapper falls back to the
      sync client and runs every call on a dedicated single-thread
      ``ThreadPoolExecutor``. Single thread matters: pyturso
      connections are NOT thread-safe — sharing one across the
      default ``asyncio.to_thread`` pool produces native-code
      crashes.
    """

    vendor = "libsql"

    def __init__(self, settings: dict):
        # Bypass the SQLite parent's aiosqlite-specific init —
        # we don't want to pre-open a file. Re-create the
        # plumbing the wrapper needs.
        from concurrent.futures import ThreadPoolExecutor

        self.settings = settings
        self.database = settings.get("NAME", ":memory:")
        self.sync_url: str | None = settings.get("SYNC_URL")
        self.auth_token: str | None = settings.get("AUTH_TOKEN")
        self._autocommit: bool = False
        self._vec_extension_enabled: bool = False
        self._lock = asyncio.Lock()
        self._conn_lock = threading.Lock()
        self._async_conn: Any = None
        self._sync_conn: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # Lazy executor — only allocated when a remote-mode
        # connection is opened so the local-async path stays
        # zero-thread.
        self._executor: ThreadPoolExecutor | None = None

    @staticmethod
    def _coerce_params(params: Any) -> Any:
        if params is None:
            return ()
        if isinstance(params, (list, tuple)):
            return tuple(params)
        return params

    def _get_executor(self):
        from concurrent.futures import ThreadPoolExecutor

        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="dorm-libsql-async"
            )
        return self._executor

    async def _run_remote(self, fn) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._get_executor(), fn)

    async def _get_conn(self) -> Any:
        """Return the active connection.

        Local-only path returns a ``turso.aio.Connection`` (every
        call is awaitable). Remote path returns the sync client
        wrapped in ``self._async_conn=None`` and a separate
        ``self._sync_conn``; callers branch on ``self.sync_url``.
        """
        if self.sync_url:
            return await self._get_sync_conn()
        return await self._get_async_conn()

    async def _get_async_conn(self) -> Any:
        if self._async_conn is not None:
            return self._async_conn
        async with self._lock:
            if self._async_conn is not None:
                return self._async_conn
            turso_aio = _import_turso_aio()
            try:
                conn = await turso_aio.connect(
                    self.database,
                    experimental_features=_VECTOR_FEATURES,
                )
            except TypeError:
                conn = await turso_aio.connect(self.database)
            try:
                await conn.execute("PRAGMA foreign_keys = ON")
            except Exception:
                pass
            self._async_conn = conn
            self._loop = asyncio.get_running_loop()
            return self._async_conn

    async def _get_sync_conn(self) -> Any:
        if self._sync_conn is not None:
            return self._sync_conn
        async with self._lock:
            if self._sync_conn is not None:
                return self._sync_conn

            def _open() -> Any:
                turso_sync = _import_turso_sync()
                kwargs: dict[str, Any] = {
                    "remote_url": self.sync_url,
                    "experimental_features": _VECTOR_FEATURES,
                }
                if self.auth_token:
                    kwargs["auth_token"] = self.auth_token
                try:
                    c = turso_sync.connect(self.database, **kwargs)
                except TypeError:
                    kwargs.pop("experimental_features", None)
                    c = turso_sync.connect(self.database, **kwargs)
                try:
                    import turso as _turso

                    c.row_factory = _turso.Row
                except Exception:
                    pass
                try:
                    c.execute("PRAGMA foreign_keys = ON")
                except Exception:
                    pass
                return c

            self._sync_conn = await self._run_remote(_open)
            self._loop = asyncio.get_running_loop()
            return self._sync_conn

    async def execute(self, sql: str, params=None) -> list:
        bound = self._coerce_params(params)
        adapted = sql.replace("%s", "?")
        if self.sync_url:
            conn = await self._get_sync_conn()

            def _do() -> list:
                cursor = conn.execute(adapted, bound)
                return cursor.fetchall()

            return await self._run_remote(_do)
        conn = await self._get_async_conn()
        cursor = await conn.execute(adapted, bound)
        return await cursor.fetchall()

    async def execute_write(self, sql: str, params=None) -> int:
        bound = self._coerce_params(params)
        adapted = sql.replace("%s", "?")
        if self.sync_url:
            conn = await self._get_sync_conn()

            def _do() -> int:
                cursor = conn.execute(adapted, bound)
                try:
                    conn.commit()
                except Exception:
                    pass
                return cursor.rowcount

            return await self._run_remote(_do)
        conn = await self._get_async_conn()
        cursor = await conn.execute(adapted, bound)
        try:
            await conn.commit()
        except Exception:
            pass
        return getattr(cursor, "rowcount", 0)

    async def execute_insert(
        self, sql: str, params=None, pk_col: str = "id"
    ) -> Any:
        del pk_col
        bound = self._coerce_params(params)
        adapted = sql.replace("%s", "?")
        if self.sync_url:
            conn = await self._get_sync_conn()

            def _do() -> Any:
                cursor = conn.execute(adapted, bound)
                try:
                    conn.commit()
                except Exception:
                    pass
                return cursor.lastrowid

            return await self._run_remote(_do)
        conn = await self._get_async_conn()
        cursor = await conn.execute(adapted, bound)
        try:
            await conn.commit()
        except Exception:
            pass
        return getattr(cursor, "lastrowid", None)

    async def execute_script(self, sql: str) -> None:
        if self.sync_url:
            conn = await self._get_sync_conn()

            def _do() -> None:
                try:
                    conn.executescript(sql)
                except AttributeError:
                    for stmt in (s.strip() for s in sql.split(";")):
                        if stmt:
                            conn.execute(stmt)

            await self._run_remote(_do)
            return
        conn = await self._get_async_conn()
        execscript = getattr(conn, "executescript", None)
        if execscript is None:
            for stmt in (s.strip() for s in sql.split(";")):
                if stmt:
                    await conn.execute(stmt)
            return
        await execscript(sql)

    async def close(self) -> None:
        # Close the async connection (if any) on the event loop.
        async_conn = self._async_conn
        self._async_conn = None
        if async_conn is not None:
            try:
                await async_conn.close()
            except Exception:
                pass
        # Close the sync connection (if any) on the worker thread
        # so libsql sees the close from the same thread that
        # opened the connection.
        sync_conn = self._sync_conn
        self._sync_conn = None
        if sync_conn is not None:

            def _close() -> None:
                try:
                    sync_conn.close()
                except Exception:
                    pass

            try:
                await self._run_remote(_close)
            except Exception:
                pass
        # Drain the executor so pending tasks finish before the
        # caller proceeds (matters under pytest where temp-dir
        # teardown can race the close).
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=True)
            except Exception:
                pass
            self._executor = None

    def force_close_sync(self) -> None:
        """Sync teardown — used by ``reset_connections`` / atexit
        hooks where awaiting isn't possible."""
        sync_conn = self._sync_conn
        self._sync_conn = None
        if sync_conn is not None and self._executor is not None:
            try:
                self._executor.submit(sync_conn.close).result(timeout=5)
            except Exception:
                pass
        self._async_conn = None
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False)
            except Exception:
                pass
            self._executor = None
