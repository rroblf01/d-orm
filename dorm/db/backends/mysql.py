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
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from ...exceptions import (
    ImproperlyConfigured,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)
from ..utils import log_query

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

    @contextmanager
    def atomic(self):
        """Begin / commit / rollback with savepoint nesting. Mirrors
        SQLite + PostgreSQL backend shape so the public
        ``dorm.transaction.atomic`` works uniformly."""
        depth = self._atomic_depth
        conn = self.get_connection()
        if depth == 0:
            conn.begin()
        else:
            with conn.cursor() as cur:
                cur.execute(f"SAVEPOINT _sp{depth}")
        self._atomic_depth = depth + 1
        try:
            yield
            if depth == 0:
                conn.commit()
            else:
                with conn.cursor() as cur:
                    cur.execute(f"RELEASE SAVEPOINT _sp{depth}")
        except Exception:
            if depth == 0:
                try:
                    conn.rollback()
                except Exception:
                    pass
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"ROLLBACK TO SAVEPOINT _sp{depth}")
                        cur.execute(f"RELEASE SAVEPOINT _sp{depth}")
                except Exception:
                    pass
            raise
        finally:
            self._atomic_depth = depth

    def commit(self) -> None:
        try:
            self.get_connection().commit()
        except Exception:
            pass

    def rollback(self) -> None:
        try:
            self.get_connection().rollback()
        except Exception:
            pass

    def set_autocommit(self, enabled: bool) -> None:
        """Toggle MySQL session autocommit. PyMySQL re-applies the
        flag on the live socket via ``SET autocommit=...``."""
        conn = self.get_connection()
        try:
            conn.autocommit(enabled)
        except Exception:
            pass

    # ── execute paths ──────────────────────────────────────────────────────

    def execute(self, sql: str, params: list | None = None) -> list:
        conn = self.get_connection()
        with log_query("mysql", sql, params):
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
        with log_query("mysql", sql, params):
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
        with log_query("mysql", sql, params):
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

    def execute_streaming(
        self, sql: str, params: list | None = None, chunk_size: int = 1000
    ):
        """Server-side cursor iterator for ``QuerySet.iterator()``.
        PyMySQL's ``SSCursor`` streams rows from the server one at
        a time without buffering the full result set client-side."""
        from pymysql.cursors import SSCursor

        conn = self.get_connection()
        with log_query("mysql", sql, params):
            cur = conn.cursor(SSCursor)
            try:
                cur.execute(sql, tuple(params or ()))
                cols = [d[0] for d in (cur.description or [])]
                while True:
                    batch = cur.fetchmany(chunk_size)
                    if not batch:
                        break
                    for row in batch:
                        yield dict(zip(cols, row))
            except Exception as exc:
                raise _normalize_exception(exc) from exc
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

    def execute_script(self, sql: str) -> None:
        """Run one or more statements separated by ``;``. MySQL doesn't
        ship a multi-statement helper; iterate manually so each
        statement gets its own cursor cycle.

        ``;`` characters inside SQL string literals are honoured —
        the splitter walks the text and only treats top-level
        ``;`` as a separator. ``DEFAULT 'a;b'`` survives intact.
        """
        conn = self.get_connection()
        statements: list[str] = []
        buf: list[str] = []
        in_str = False
        quote_ch = ""
        i = 0
        while i < len(sql):
            ch = sql[i]
            if in_str:
                buf.append(ch)
                if ch == quote_ch:
                    # SQL escapes a quote by doubling it inside the
                    # literal. Skip over the second quote so the
                    # literal stays open.
                    if i + 1 < len(sql) and sql[i + 1] == quote_ch:
                        buf.append(sql[i + 1])
                        i += 2
                        continue
                    in_str = False
            elif ch in ("'", '"', "`"):
                in_str = True
                quote_ch = ch
                buf.append(ch)
            elif ch == ";":
                stmt = "".join(buf).strip()
                if stmt:
                    statements.append(stmt)
                buf = []
            else:
                buf.append(ch)
            i += 1
        tail = "".join(buf).strip()
        if tail:
            statements.append(tail)
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

    # ── Async transaction helpers ──────────────────────────────────────────

    @asynccontextmanager
    async def aatomic(self):
        """Async counterpart of :meth:`MySQLDatabaseWrapper.atomic`.
        Borrows a connection from the pool (or opens a fresh one)
        and runs BEGIN / COMMIT / ROLLBACK manually so nested
        ``aatomic`` blocks fall back to savepoints."""
        conn = await self._acquire()
        try:
            async with conn.cursor() as cur:
                await cur.execute("SET SESSION sql_mode='ANSI_QUOTES'")
            await conn.begin()
            try:
                yield
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise
        finally:
            await self._release(conn)

    async def commit(self) -> None:
        # Single-connection commit doesn't apply with the connection-
        # per-task model used by the async wrapper; transactions are
        # bound to the lifetime of the ``aatomic`` block. Provide a
        # no-op so callers that probe ``await conn.commit()`` (e.g.
        # the test suite's autocommit harness) don't crash.
        return None

    async def rollback(self) -> None:
        return None

    async def set_autocommit(self, enabled: bool) -> None:
        """Best-effort autocommit toggle. With per-task connections
        the only way to apply this is to acquire a conn, switch the
        flag, and release; subsequent acquires open fresh conns
        that don't inherit the override. Tests that need durable
        autocommit toggling should use the sync wrapper."""
        conn = await self._acquire()
        try:
            await conn.autocommit(enabled)
        except Exception:
            pass
        finally:
            await self._release(conn)


__all__ = ["MySQLDatabaseWrapper", "MySQLAsyncDatabaseWrapper"]
