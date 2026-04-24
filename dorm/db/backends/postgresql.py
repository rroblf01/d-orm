from __future__ import annotations

import asyncio
import re
import threading
from contextlib import asynccontextmanager, contextmanager

from ..utils import ASYNC_ATOMIC_STATE, normalize_db_exception

_POSITIONAL_PLACEHOLDER = re.compile(r"\$\d+")


def _to_pyformat(sql: str) -> str:
    """Convert $1, $2, ... placeholders to %s (psycopg3 style)."""
    return _POSITIONAL_PLACEHOLDER.sub("%s", sql)


def _build_dsn(settings: dict) -> dict:
    dsn = {
        "host": settings.get("HOST", "localhost"),
        "port": int(settings.get("PORT", 5432)),
        "dbname": settings.get("NAME", ""),
        "user": settings.get("USER", ""),
        "password": settings.get("PASSWORD", ""),
    }
    dsn.update(settings.get("OPTIONS", {}))
    return dsn


def _dsn_to_conninfo(dsn: dict) -> str:
    try:
        from psycopg.conninfo import make_conninfo
    except ImportError as e:
        raise ImportError(
            "psycopg is required for PostgreSQL support. "
            "Install it with: pip install 'djanorm[postgresql]'"
        ) from e
    return make_conninfo(**{k: v for k, v in dsn.items() if v is not None and v != ""})


class PostgreSQLDatabaseWrapper:
    vendor = "postgresql"

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)
        self._min_size = int(settings.get("MIN_POOL_SIZE", 1))
        self._max_size = int(settings.get("MAX_POOL_SIZE", 10))
        self._pool = None
        self._pool_lock = threading.Lock()
        self._local = threading.local()  # per-instance atomic state per thread
        self._autocommit: bool = False

    @property
    def _atomic_conn(self):
        return getattr(self._local, "atomic_conn", None)

    @property
    def _atomic_depth(self) -> int:
        return getattr(self._local, "atomic_depth", 0)

    def _get_pool(self):
        if self._pool is not None:
            return self._pool
        with self._pool_lock:
            if self._pool is None:
                try:
                    from psycopg_pool import ConnectionPool
                    from psycopg.rows import dict_row
                except ImportError as e:
                    raise ImportError(
                        "psycopg[pool] is required for PostgreSQL support. "
                        "Install it with: pip install 'djanorm[postgresql]'"
                    ) from e
                self._pool = ConnectionPool(
                    _dsn_to_conninfo(self._dsn),
                    min_size=self._min_size,
                    max_size=self._max_size,
                    open=True,
                    kwargs={"row_factory": dict_row},
                    check=ConnectionPool.check_connection,
                )
        return self._pool

    @contextmanager
    def atomic(self):
        conn = self._atomic_conn
        depth = self._atomic_depth

        if conn is None:
            # Top-level: check out a pool connection and hold it for the block.
            with self._get_pool().connection() as c:
                self._local.atomic_conn = c
                self._local.atomic_depth = 1
                try:
                    yield
                finally:
                    self._local.atomic_conn = None
                    self._local.atomic_depth = 0
        else:
            # Nested: use a savepoint on the already-held connection.
            sp = f"_sp{depth}"
            conn.execute(f"SAVEPOINT {sp}")
            self._local.atomic_depth = depth + 1
            try:
                yield
                conn.execute(f"RELEASE SAVEPOINT {sp}")
            except Exception:
                try:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    conn.execute(f"RELEASE SAVEPOINT {sp}")
                except Exception:
                    pass
                raise
            finally:
                self._local.atomic_depth = depth

    def _exec(self, conn, sql: str, params, *, write: bool = False, insert: bool = False):
        try:
            with conn.cursor() as cur:
                _sql = _to_pyformat(sql) + (" RETURNING id" if insert else "")
                cur.execute(_sql, params or [])
                if insert:
                    row = cur.fetchone()
                    return row["id"] if row else None
                if write:
                    return cur.rowcount
                try:
                    return cur.fetchall()
                except Exception:
                    return []
        except Exception as exc:
            normalize_db_exception(exc)
            raise

    def _get_persistent_conn(self):
        """Return a thread-local persistent connection used in autocommit mode."""
        conn = getattr(self._local, "autocommit_conn", None)
        if conn is None or conn.closed:
            import psycopg
            from psycopg.rows import dict_row
            conn = psycopg.connect(_dsn_to_conninfo(self._dsn), row_factory=dict_row, autocommit=True)  # type: ignore
            self._local.autocommit_conn = conn
        return conn

    def _choose_conn(self):
        """Return atomic conn, or autocommit persistent conn, or None (use pool)."""
        atomic = self._atomic_conn
        if atomic is not None:
            return atomic
        if self._autocommit:
            return self._get_persistent_conn()
        return None

    def execute(self, sql: str, params=None) -> list:
        conn = self._choose_conn()
        if conn is not None:
            return self._exec(conn, sql, params)
        with self._get_pool().connection() as c:
            return self._exec(c, sql, params)

    def execute_write(self, sql: str, params=None) -> int:
        conn = self._choose_conn()
        if conn is not None:
            return self._exec(conn, sql, params, write=True)
        with self._get_pool().connection() as c:
            return self._exec(c, sql, params, write=True)

    def execute_insert(self, sql: str, params=None):
        conn = self._choose_conn()
        if conn is not None:
            return self._exec(conn, sql, params, insert=True)
        with self._get_pool().connection() as c:
            return self._exec(c, sql, params, insert=True)

    def _exec_bulk(self, conn, sql: str, params, pk_col: str) -> list[int]:
        try:
            with conn.cursor() as cur:
                _sql = _to_pyformat(sql) + f' RETURNING "{pk_col}"'
                cur.execute(_sql, params or [])
                rows = cur.fetchall()
                return [r[pk_col] for r in rows]
        except Exception as exc:
            normalize_db_exception(exc)
            raise

    def execute_bulk_insert(self, sql: str, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        conn = self._choose_conn()
        if conn is not None:
            return self._exec_bulk(conn, sql, params, pk_col)
        with self._get_pool().connection() as c:
            return self._exec_bulk(c, sql, params, pk_col)

    def execute_script(self, sql: str):
        with self._get_pool().connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute(
            "SELECT tablename FROM pg_tables WHERE tablename = %s",
            [table_name],
        )
        return bool(rows)

    def get_table_columns(self, table_name: str) -> list[dict]:
        rows = self.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        return [{"name": r["column_name"], **{k: v for k, v in dict(r).items() if k != "column_name"}} for r in rows]

    def set_autocommit(self, enabled: bool) -> None:
        self._autocommit = enabled
        if not enabled:
            conn = getattr(self._local, "autocommit_conn", None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                self._local.autocommit_conn = None

    def commit(self) -> None:
        conn = getattr(self._local, "autocommit_conn", None)
        if conn is not None and not conn.autocommit:
            conn.commit()

    def rollback(self) -> None:
        conn = getattr(self._local, "autocommit_conn", None)
        if conn is not None and not conn.autocommit:
            conn.rollback()

    def close(self):
        conn = getattr(self._local, "autocommit_conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.autocommit_conn = None
        if self._pool is not None:
            self._pool.close()
            self._pool = None


class PostgreSQLAsyncDatabaseWrapper:
    vendor = "postgresql"

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)
        self._min_size = int(settings.get("MIN_POOL_SIZE", 1))
        self._max_size = int(settings.get("MAX_POOL_SIZE", 10))
        self._pool = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pool_lock = asyncio.Lock()
        self._autocommit: bool = False
        self._autocommit_conn = None

    async def _get_pool(self):
        current_loop = asyncio.get_event_loop()

        if self._loop is not current_loop and self._pool is not None:
            old_pool = self._pool
            self._pool = None
            self._loop = None
            self._pool_lock = asyncio.Lock()
            try:
                await old_pool.close()
            except Exception:
                pass

        if self._pool is not None:
            return self._pool

        async with self._pool_lock:
            if self._pool is None:
                try:
                    from psycopg_pool import AsyncConnectionPool
                    from psycopg.rows import dict_row
                except ImportError as e:
                    raise ImportError(
                        "psycopg[pool] is required for async PostgreSQL support. "
                        "Install it with: pip install 'djanorm[postgresql]'"
                    ) from e
                pool = AsyncConnectionPool(
                    _dsn_to_conninfo(self._dsn),
                    min_size=self._min_size,
                    max_size=self._max_size,
                    open=False,
                    kwargs={"row_factory": dict_row},
                    check=AsyncConnectionPool.check_connection,
                )
                await pool.open()
                self._pool = pool
                self._loop = current_loop
        return self._pool

    @asynccontextmanager
    async def aatomic(self):
        state = ASYNC_ATOMIC_STATE.get()

        if state is None or state[0] is not self:
            # Top-level: check out a pool connection and hold it for the block.
            pool = await self._get_pool()
            async with pool.connection() as c:
                token = ASYNC_ATOMIC_STATE.set((self, c, 1))
                try:
                    yield
                finally:
                    ASYNC_ATOMIC_STATE.reset(token)
        else:
            # Nested: use a savepoint on the already-held connection.
            _, c, depth = state
            sp = f"_sp{depth}"
            await c.execute(f"SAVEPOINT {sp}")
            token = ASYNC_ATOMIC_STATE.set((self, c, depth + 1))
            try:
                yield
                await c.execute(f"RELEASE SAVEPOINT {sp}")
            except Exception:
                try:
                    await c.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    await c.execute(f"RELEASE SAVEPOINT {sp}")
                except Exception:
                    pass
                raise
            finally:
                ASYNC_ATOMIC_STATE.reset(token)

    async def _aexec(self, conn, sql: str, params, *, write: bool = False, insert: bool = False):
        try:
            async with conn.cursor() as cur:
                _sql = _to_pyformat(sql) + (" RETURNING id" if insert else "")
                await cur.execute(_sql, params or [])
                if insert:
                    row = await cur.fetchone()
                    return row["id"] if row else None
                if write:
                    return cur.rowcount
                try:
                    return await cur.fetchall()
                except Exception:
                    return []
        except Exception as exc:
            normalize_db_exception(exc)
            raise

    async def _get_autocommit_conn(self):
        if self._autocommit_conn is None or self._autocommit_conn.closed:
            import psycopg
            from psycopg.rows import dict_row
            self._autocommit_conn = await psycopg.AsyncConnection.connect(
                _dsn_to_conninfo(self._dsn), row_factory=dict_row, autocommit=True  # type: ignore
            )
        return self._autocommit_conn

    async def _choose_conn(self):
        """Return atomic conn or autocommit persistent conn, or None (use pool)."""
        state = ASYNC_ATOMIC_STATE.get()
        if state is not None and state[0] is self:
            return state[1]
        if self._autocommit:
            return await self._get_autocommit_conn()
        return None

    async def execute(self, sql: str, params=None) -> list:
        conn = await self._choose_conn()
        if conn is not None:
            return await self._aexec(conn, sql, params)
        async with (await self._get_pool()).connection() as c:
            return await self._aexec(c, sql, params)

    async def execute_write(self, sql: str, params=None) -> int:
        conn = await self._choose_conn()
        if conn is not None:
            return await self._aexec(conn, sql, params, write=True)
        async with (await self._get_pool()).connection() as c:
            return await self._aexec(c, sql, params, write=True)

    async def execute_insert(self, sql: str, params=None):
        conn = await self._choose_conn()
        if conn is not None:
            return await self._aexec(conn, sql, params, insert=True)
        async with (await self._get_pool()).connection() as c:
            return await self._aexec(c, sql, params, insert=True)

    async def _aexec_bulk(self, conn, sql: str, params, pk_col: str) -> list[int]:
        try:
            async with conn.cursor() as cur:
                _sql = _to_pyformat(sql) + f' RETURNING "{pk_col}"'
                await cur.execute(_sql, params or [])
                rows = await cur.fetchall()
                return [r[pk_col] for r in rows]
        except Exception as exc:
            normalize_db_exception(exc)
            raise

    async def execute_bulk_insert(self, sql: str, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        conn = await self._choose_conn()
        if conn is not None:
            return await self._aexec_bulk(conn, sql, params, pk_col)
        async with (await self._get_pool()).connection() as c:
            return await self._aexec_bulk(c, sql, params, pk_col)

    async def execute_script(self, sql: str):
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

    async def table_exists(self, table_name: str) -> bool:
        rows = await self.execute(
            "SELECT tablename FROM pg_tables WHERE tablename = %s",
            [table_name],
        )
        return bool(rows)

    async def get_table_columns(self, table_name: str) -> list[dict]:
        rows = await self.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        return [{"name": r["column_name"], **{k: v for k, v in dict(r).items() if k != "column_name"}} for r in rows]

    async def set_autocommit(self, enabled: bool) -> None:
        self._autocommit = enabled
        if not enabled and self._autocommit_conn is not None:
            try:
                await self._autocommit_conn.close()
            except Exception:
                pass
            self._autocommit_conn = None

    async def commit(self) -> None:
        if self._autocommit_conn is not None and not self._autocommit_conn.autocommit:
            await self._autocommit_conn.commit()

    async def rollback(self) -> None:
        if self._autocommit_conn is not None and not self._autocommit_conn.autocommit:
            await self._autocommit_conn.rollback()

    async def close(self):
        if self._autocommit_conn is not None:
            try:
                await self._autocommit_conn.close()
            except Exception:
                pass
            self._autocommit_conn = None
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
