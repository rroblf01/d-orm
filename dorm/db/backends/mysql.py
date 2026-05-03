"""MySQL / MariaDB backend.

Backed by ``pymysql`` (sync) and ``aiomysql`` (async) — both
pure-Python drivers available on every platform without a C
toolchain. The wrappers expose the same surface as the SQLite /
PostgreSQL backends so the rest of dorm doesn't branch on vendor.

Dialect notes:
- ``ANSI_QUOTES`` mode is forced on every connection so double-
  quoted identifiers (the form dorm emits everywhere) parse the
  same as PostgreSQL / SQLite. Without this MySQL would treat
  ``"name"`` as a string literal.
- DDL is **not** transactional. ``ALTER TABLE`` / ``CREATE TABLE``
  auto-commit; wrapping them in :func:`atomic` won't roll them
  back if the function body raises. The migration executor still
  works (each migration is its own transaction for the recorder
  row), but the schema change itself can't be undone after the
  fact.
- ``RETURNING`` works on MariaDB ≥ 10.5 but not on MySQL. The
  insert path falls back to ``cursor.lastrowid`` for autoincrement
  PKs; composite keys round-trip via ``execute_write`` + a follow-
  up ``SELECT``.
- Upsert uses ``INSERT ... ON DUPLICATE KEY UPDATE`` (the syntax
  the bulk_create code path emits when the user passes
  ``update_conflicts=True``).

The async wrapper is structurally similar but uses ``aiomysql``'s
connection-per-task pattern. No connection pool is shipped here
yet — production users running heavy async traffic on MySQL
should run ``aiomysql.create_pool`` themselves and inject via
the ``OPTIONS`` dict (a future iteration may wrap this).
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from ...exceptions import (
    ImproperlyConfigured,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)

_logger = logging.getLogger("dorm.db.backends.mysql")


def _import_pymysql():
    try:
        import pymysql
    except ImportError as exc:
        raise ImproperlyConfigured(
            "MySQL backend requires the ``pymysql`` package. "
            "Install it via ``pip install 'djanorm[mysql]'``."
        ) from exc
    return pymysql


def _import_aiomysql():
    try:
        import aiomysql
    except ImportError as exc:
        raise ImproperlyConfigured(
            "Async MySQL backend requires the ``aiomysql`` package. "
            "Install it via ``pip install 'djanorm[mysql]'``."
        ) from exc
    return aiomysql


def _normalize_settings(settings: dict[str, Any]) -> dict[str, Any]:
    """Translate dorm's ``DATABASES`` shape to PyMySQL/aiomysql kwargs.

    Both libraries accept the same kwarg names except for ``database``
    vs ``db`` (PyMySQL prefers ``database`` since 1.0; we use the
    portable ``db`` form so the same dict feeds both). ``OPTIONS``
    is a free-form dict that we forward verbatim — caller can pin
    ``charset``, ``ssl``, ``connect_timeout``, etc.
    """
    out: dict[str, Any] = {}
    if "HOST" in settings:
        out["host"] = settings["HOST"]
    if "PORT" in settings and settings["PORT"]:
        out["port"] = int(settings["PORT"])
    if "USER" in settings:
        out["user"] = settings["USER"]
    if "PASSWORD" in settings:
        out["password"] = settings["PASSWORD"]
    if "NAME" in settings:
        out["db"] = settings["NAME"]
    out.setdefault("charset", "utf8mb4")
    extra = settings.get("OPTIONS") or {}
    out.update(extra)
    return out


def _normalize_exception(exc: Exception) -> Exception:
    """Map PyMySQL / aiomysql errors to dorm.exceptions equivalents."""
    pymysql = _import_pymysql()
    if isinstance(exc, pymysql.err.IntegrityError):
        return IntegrityError(str(exc))
    if isinstance(exc, pymysql.err.OperationalError):
        return OperationalError(str(exc))
    if isinstance(exc, pymysql.err.ProgrammingError):
        return ProgrammingError(str(exc))
    if isinstance(exc, pymysql.err.MySQLError):
        return OperationalError(str(exc))
    return exc


class MySQLDatabaseWrapper:
    """Sync MySQL / MariaDB wrapper.

    Single connection per process per alias — same shape SQLite
    uses. Heavy production deployments should configure a pool
    via ``OPTIONS["pool"]`` (forwarded to a future-compatible
    pool manager) once the async wrapper grows real pooling.
    """

    vendor = "mysql"

    def __init__(self, settings: dict[str, Any], alias: str = "default") -> None:
        self.settings = settings
        self.alias = alias
        self._conn = None
        self._lock = threading.Lock()
        self._atomic_depth = 0
        self._kwargs = _normalize_settings(settings)
        self._dsn = settings  # for diagnostics

    def get_connection(self):
        if self._conn is not None and self._conn.open:
            return self._conn
        pymysql = _import_pymysql()
        try:
            self._conn = pymysql.connect(**self._kwargs)
        except Exception as exc:
            raise _normalize_exception(exc) from exc
        # Force ANSI_QUOTES so dorm's double-quoted identifiers parse.
        with self._conn.cursor() as cur:
            cur.execute("SET SESSION sql_mode='ANSI_QUOTES,STRICT_ALL_TABLES'")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Transaction helpers ────────────────────────────────────────────────

    def begin(self) -> None:
        self._atomic_depth += 1
        if self._atomic_depth == 1:
            self.get_connection().begin()

    def commit(self) -> None:
        if self._atomic_depth <= 0:
            return
        self._atomic_depth -= 1
        if self._atomic_depth == 0:
            self.get_connection().commit()

    def rollback(self) -> None:
        if self._atomic_depth <= 0:
            return
        depth = self._atomic_depth
        self._atomic_depth = 0
        if depth > 0:
            try:
                self.get_connection().rollback()
            except Exception:
                pass

    # ── execute paths ──────────────────────────────────────────────────────

    def execute(self, sql: str, params: list | None = None) -> list:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params or ()))
                if cur.description is None:
                    return []
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                return [dict(zip(cols, row)) for row in rows]
        except Exception as exc:
            raise _normalize_exception(exc) from exc

    def execute_write(self, sql: str, params: list | None = None) -> int:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params or ()))
                rc = cur.rowcount
            if self._atomic_depth == 0:
                conn.commit()
            return rc
        except Exception as exc:
            if self._atomic_depth == 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise _normalize_exception(exc) from exc

    def execute_insert(
        self, sql: str, params: list | None = None, pk_col: str | None = None
    ) -> Any:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params or ()))
                pk = cur.lastrowid
            if self._atomic_depth == 0:
                conn.commit()
            return pk
        except Exception as exc:
            if self._atomic_depth == 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise _normalize_exception(exc) from exc

    def execute_script(self, sql: str) -> None:
        """Run one or more statements separated by ``;``. MySQL doesn't
        ship a multi-statement helper; iterate manually so each
        statement gets its own cursor cycle."""
        conn = self.get_connection()
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        try:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            if self._atomic_depth == 0:
                conn.commit()
        except Exception as exc:
            if self._atomic_depth == 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise _normalize_exception(exc) from exc

    # ── Introspection ──────────────────────────────────────────────────────

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s",
            [table_name],
        )
        return bool(rows)

    def get_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        rows = self.execute(
            "SELECT column_name AS name, data_type AS type, "
            "is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "ORDER BY ordinal_position",
            [table_name],
        )
        return rows


class MySQLAsyncDatabaseWrapper:
    """Async MySQL / MariaDB wrapper backed by ``aiomysql``.

    A new connection is opened per async invocation context to keep
    the surface predictable. Production users that need pooling
    should pass an ``aiomysql.Pool`` object through
    ``OPTIONS["pool"]`` — the wrapper picks it up if present and
    routes acquire / release through it instead of opening fresh
    connections.
    """

    vendor = "mysql"

    def __init__(self, settings: dict[str, Any], alias: str = "default") -> None:
        self.settings = settings
        self.alias = alias
        self._kwargs = _normalize_settings(settings)
        self._kwargs.setdefault("autocommit", False)
        self._pool = settings.get("OPTIONS", {}).get("pool")

    async def _acquire(self):
        aiomysql = _import_aiomysql()
        if self._pool is not None:
            return await self._pool.acquire()
        try:
            return await aiomysql.connect(**self._kwargs)
        except Exception as exc:
            raise _normalize_exception(exc) from exc

    async def _release(self, conn) -> None:
        if self._pool is not None:
            self._pool.release(conn)
            return
        try:
            await conn.ensure_closed()
        except Exception:
            pass

    async def execute(self, sql: str, params: list | None = None) -> list:
        conn = await self._acquire()
        try:
            async with conn.cursor() as cur:
                await cur.execute("SET SESSION sql_mode='ANSI_QUOTES'")
                await cur.execute(sql, tuple(params or ()))
                if cur.description is None:
                    return []
                cols = [d[0] for d in cur.description]
                rows = await cur.fetchall()
                return [dict(zip(cols, row)) for row in rows]
        except Exception as exc:
            raise _normalize_exception(exc) from exc
        finally:
            await self._release(conn)

    async def execute_write(self, sql: str, params: list | None = None) -> int:
        conn = await self._acquire()
        try:
            async with conn.cursor() as cur:
                await cur.execute("SET SESSION sql_mode='ANSI_QUOTES'")
                await cur.execute(sql, tuple(params or ()))
                await conn.commit()
                return cur.rowcount
        except Exception as exc:
            try:
                await conn.rollback()
            except Exception:
                pass
            raise _normalize_exception(exc) from exc
        finally:
            await self._release(conn)

    async def execute_insert(
        self, sql: str, params: list | None = None, pk_col: str | None = None
    ) -> Any:
        conn = await self._acquire()
        try:
            async with conn.cursor() as cur:
                await cur.execute("SET SESSION sql_mode='ANSI_QUOTES'")
                await cur.execute(sql, tuple(params or ()))
                pk = cur.lastrowid
                await conn.commit()
                return pk
        except Exception as exc:
            try:
                await conn.rollback()
            except Exception:
                pass
            raise _normalize_exception(exc) from exc
        finally:
            await self._release(conn)

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()

    def force_close_sync(self) -> None:
        if self._pool is not None:
            try:
                self._pool.terminate()
            except Exception:
                pass


__all__ = ["MySQLDatabaseWrapper", "MySQLAsyncDatabaseWrapper"]
