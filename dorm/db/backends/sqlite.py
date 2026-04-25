from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import asynccontextmanager, contextmanager

from ..utils import ASYNC_ATOMIC_STATE, normalize_db_exception


class SQLiteDatabaseWrapper:
    vendor = "sqlite"

    def __init__(self, settings: dict):
        self.settings = settings
        self.database = settings.get("NAME", ":memory:")
        self._local = threading.local()
        self._autocommit: bool = False

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        journal_mode = self.settings.get("OPTIONS", {}).get("journal_mode")
        if journal_mode:
            conn.execute(f"PRAGMA journal_mode = {journal_mode}")
        if self._autocommit:
            conn.isolation_level = None
        return conn

    def get_connection(self) -> sqlite3.Connection:
        # Auto-reconnect: if the cached connection no longer responds to a
        # trivial probe (process forked, file deleted, etc.), discard and
        # reopen. Errors during the close attempt are intentionally ignored —
        # the connection is being thrown away regardless.
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.execute("SELECT 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
        return conn

    @property
    def _atomic_depth(self) -> int:
        return getattr(self._local, "atomic_depth", 0)

    @_atomic_depth.setter
    def _atomic_depth(self, value: int) -> None:
        self._local.atomic_depth = value

    @contextmanager
    def atomic(self):
        depth = self._atomic_depth
        conn = self.get_connection()
        if depth > 0:
            conn.execute(f"SAVEPOINT _sp{depth}")
        self._atomic_depth = depth + 1
        try:
            yield
            if depth == 0:
                conn.commit()
            else:
                conn.execute(f"RELEASE SAVEPOINT _sp{depth}")
        except Exception:
            if depth == 0:
                conn.rollback()
            else:
                try:
                    conn.execute(f"ROLLBACK TO SAVEPOINT _sp{depth}")
                    conn.execute(f"RELEASE SAVEPOINT _sp{depth}")
                except Exception:
                    pass
            raise
        finally:
            self._atomic_depth = depth

    @staticmethod
    def _adapt(sql: str) -> str:
        return sql.replace("%s", "?")

    def execute(self, sql: str, params=None) -> list:
        conn = self.get_connection()
        try:
            cursor = conn.execute(self._adapt(sql), params or [])
        except Exception as exc:
            normalize_db_exception(exc)
            raise
        return cursor.fetchall()

    def execute_write(self, sql: str, params=None) -> int:
        conn = self.get_connection()
        try:
            cursor = conn.execute(self._adapt(sql), params or [])
        except Exception as exc:
            normalize_db_exception(exc)
            raise
        if self._atomic_depth == 0 and not self._autocommit:
            conn.commit()
        return cursor.rowcount

    def execute_insert(self, sql: str, params=None, pk_col: str = "id"):
        # pk_col is accepted for parity with the PostgreSQL backend; SQLite
        # always returns cursor.lastrowid (which equals the PK for INTEGER
        # PRIMARY KEY columns regardless of name).
        del pk_col
        conn = self.get_connection()
        try:
            cursor = conn.execute(self._adapt(sql), params or [])
        except Exception as exc:
            normalize_db_exception(exc)
            raise
        if self._atomic_depth == 0 and not self._autocommit:
            conn.commit()
        return cursor.lastrowid

    def execute_bulk_insert(self, sql: str, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        conn = self.get_connection()
        try:
            cursor = conn.execute(self._adapt(sql), params or [])
        except Exception as exc:
            normalize_db_exception(exc)
            raise
        if self._atomic_depth == 0 and not self._autocommit:
            conn.commit()
        last = cursor.lastrowid
        if not last:
            return []
        return list(range(last - count + 1, last + 1))

    def execute_script(self, sql: str):
        conn = self.get_connection()
        conn.executescript(sql)
        conn.commit()

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            [table_name],
        )
        return bool(rows)

    def get_table_columns(self, table_name: str) -> list[dict]:
        rows = self.execute(f'PRAGMA table_info("{table_name}")')
        return [dict(r) for r in rows]

    def set_autocommit(self, enabled: bool) -> None:
        self._autocommit = enabled
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.isolation_level = None if enabled else ""

    def commit(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.commit()

    def rollback(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.rollback()

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None


class SQLiteAsyncDatabaseWrapper:
    vendor = "sqlite"

    def __init__(self, settings: dict):
        self.settings = settings
        self.database = settings.get("NAME", ":memory:")
        self._conn = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._autocommit: bool = False
        # Safe to create outside a running loop (Python 3.10+).
        self._lock = asyncio.Lock()

    @staticmethod
    def _adapt(sql: str) -> str:
        return sql.replace("%s", "?")

    async def _check_loop(self) -> None:
        """Reset connection and lock if the running event loop has changed."""
        current_loop = asyncio.get_running_loop()
        if self._loop is not current_loop:
            # Don't await close() on a connection from a dead loop — its
            # worker thread is daemonized and will be reaped at exit. Just
            # drop the reference so the next op opens a fresh connection.
            self._conn = None
            self._loop = current_loop
            self._lock = asyncio.Lock()

    async def _new_connection(self):
        import aiosqlite

        isolation = None if self._autocommit else ""
        pending = aiosqlite.connect(self.database, isolation_level=isolation)
        # aiosqlite's worker is a non-daemon Thread; in Python 3.13+ the
        # interpreter joins non-daemon threads before atexit fires, so a
        # forgotten close() hangs the process. Mark the thread as daemon
        # before it starts (set via __await__) so the process can exit.
        worker = getattr(pending, "_thread", None)
        if worker is not None:
            worker.daemon = True
        else:
            import warnings
            warnings.warn(
                "aiosqlite Connection has no '_thread' attribute; the "
                "worker thread cannot be daemonized. Forgetting to await "
                "connection close may hang the process at exit. Pin "
                "aiosqlite to a known-good version (>=0.22,<0.23).",
                RuntimeWarning,
                stacklevel=2,
            )
        conn = await pending
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        journal_mode = self.settings.get("OPTIONS", {}).get("journal_mode")
        if journal_mode:
            await conn.execute(f"PRAGMA journal_mode = {journal_mode}")
        return conn

    async def _get_conn(self):
        if self._conn is not None:
            try:
                await self._conn.execute("SELECT 1")
            except Exception:
                try:
                    await self._conn.close()
                except Exception:
                    pass
                self._conn = None
        if self._conn is None:
            self._conn = await self._new_connection()
        return self._conn

    def _in_atomic(self) -> bool:
        state = ASYNC_ATOMIC_STATE.get()
        return state is not None and state[0] is self

    @asynccontextmanager
    async def _operation_conn(self):
        """Yield the connection, acquiring lock only when not inside aatomic()."""
        if self._in_atomic():
            yield ASYNC_ATOMIC_STATE.get()[1]  # type: ignore[index]
        else:
            await self._check_loop()
            async with self._lock:
                yield await self._get_conn()

    @asynccontextmanager
    async def aatomic(self):
        state = ASYNC_ATOMIC_STATE.get()

        if state is None or state[0] is not self:
            # Reset before acquiring the lock so we don't replace it mid-hold.
            await self._check_loop()
            # Top-level: acquire lock for the entire block so other coroutines wait.
            await self._lock.acquire()
            conn = await self._get_conn()
            token = ASYNC_ATOMIC_STATE.set((self, conn, 1))
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
                ASYNC_ATOMIC_STATE.reset(token)
                self._lock.release()
        else:
            # Nested: use savepoint on the already-held connection.
            _, conn, depth = state
            sp = f"_sp{depth}"
            await conn.execute(f"SAVEPOINT {sp}")
            token = ASYNC_ATOMIC_STATE.set((self, conn, depth + 1))
            try:
                yield
                await conn.execute(f"RELEASE SAVEPOINT {sp}")
            except Exception:
                try:
                    await conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    await conn.execute(f"RELEASE SAVEPOINT {sp}")
                except Exception:
                    pass
                raise
            finally:
                ASYNC_ATOMIC_STATE.reset(token)

    async def execute(self, sql: str, params=None) -> list:
        async with self._operation_conn() as conn:
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                rows = await cursor.fetchall()
            except Exception as exc:
                normalize_db_exception(exc)
                raise
            return list(rows)

    async def execute_write(self, sql: str, params=None) -> int:
        async with self._operation_conn() as conn:
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                if not self._in_atomic() and not self._autocommit:
                    await conn.commit()
                return cursor.rowcount
            except Exception as exc:
                normalize_db_exception(exc)
                raise

    async def execute_insert(self, sql: str, params=None, pk_col: str = "id"):
        # pk_col accepted for parity with PostgreSQL; aiosqlite uses lastrowid.
        del pk_col
        async with self._operation_conn() as conn:
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                if not self._in_atomic() and not self._autocommit:
                    await conn.commit()
                return cursor.lastrowid
            except Exception as exc:
                normalize_db_exception(exc)
                raise

    async def execute_bulk_insert(self, sql: str, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        async with self._operation_conn() as conn:
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                if not self._in_atomic() and not self._autocommit:
                    await conn.commit()
                last = cursor.lastrowid
                if not last:
                    return []
                return list(range(last - count + 1, last + 1))
            except Exception as exc:
                normalize_db_exception(exc)
                raise

    async def execute_script(self, sql: str):
        async with self._lock:
            conn = await self._get_conn()
            await conn.executescript(sql)
            await conn.commit()

    async def table_exists(self, table_name: str) -> bool:
        rows = await self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            [table_name],
        )
        return bool(rows)

    async def get_table_columns(self, table_name: str) -> list[dict]:
        rows = await self.execute(f'PRAGMA table_info("{table_name}")')
        return [dict(r) for r in rows]

    async def set_autocommit(self, enabled: bool) -> None:
        self._autocommit = enabled
        # Close the current connection so the next operation opens a fresh one
        # with the correct isolation_level passed to aiosqlite.connect().
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None

    async def commit(self) -> None:
        if self._conn is not None:
            await self._conn.commit()

    async def rollback(self) -> None:
        if self._conn is not None:
            await self._conn.rollback()

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
