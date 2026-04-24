from __future__ import annotations

import asyncio
import re
import threading

_POSITIONAL_PLACEHOLDER = re.compile(r"\$\d+")


def _to_pyformat(sql: str) -> str:
    """Convert $1, $2, ... placeholders to %s (psycopg3 style)."""
    return _POSITIONAL_PLACEHOLDER.sub("%s", sql)


def _raise_migration_hint(exc: Exception) -> None:
    from dorm.exceptions import OperationalError

    msg = str(exc)
    match = re.search(r'relation "([^"]+)" does not exist', msg, re.IGNORECASE)
    if match:
        table = match.group(1)
        raise OperationalError(
            f'Table "{table}" does not exist.\n\n'
            "It looks like you forgot to create or apply your migrations.\n\n"
            "  Run the following commands:\n"
            "    dorm makemigrations\n"
            "    dorm migrate\n\n"
            "  Or, if you use a custom settings module:\n"
            "    dorm makemigrations --settings=<your_settings_module>\n"
            "    dorm migrate        --settings=<your_settings_module>\n"
        ) from exc


def _build_dsn(settings: dict) -> dict:
    return {
        "host": settings.get("HOST", "localhost"),
        "port": int(settings.get("PORT", 5432)),
        "dbname": settings.get("NAME", ""),
        "user": settings.get("USER", ""),
        "password": settings.get("PASSWORD", ""),
    }


def _dsn_to_conninfo(dsn: dict) -> str:
    """Build a conninfo string, omitting empty values."""
    try:
        from psycopg.conninfo import make_conninfo
    except ImportError as e:
        raise ImportError(
            "psycopg is required for PostgreSQL support. "
            "Install it with: pip install 'djanorm[postgresql]'"
        ) from e
    return make_conninfo(**{k: v for k, v in dsn.items() if v})


class PostgreSQLDatabaseWrapper:
    vendor = "postgresql"

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)
        self._min_size = int(settings.get("MIN_POOL_SIZE", 1))
        self._max_size = int(settings.get("MAX_POOL_SIZE", 10))
        self._pool = None
        self._pool_lock = threading.Lock()

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
                    kwargs={"row_factory": dict_row},
                )
        return self._pool

    def execute(self, sql: str, params=None) -> list:
        try:
            with self._get_pool().connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(_to_pyformat(sql), params or [])
                    try:
                        return cur.fetchall()
                    except Exception:
                        return []
        except Exception as exc:
            _raise_migration_hint(exc)
            raise

    def execute_write(self, sql: str, params=None) -> int:
        try:
            with self._get_pool().connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(_to_pyformat(sql), params or [])
                    return cur.rowcount
        except Exception as exc:
            _raise_migration_hint(exc)
            raise

    def execute_insert(self, sql: str, params=None):
        try:
            with self._get_pool().connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(_to_pyformat(sql) + " RETURNING id", params or [])
                    row = cur.fetchone()
                return row["id"] if row else None
        except Exception as exc:
            _raise_migration_hint(exc)
            raise

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
        return [dict(r) for r in rows]

    def close(self):
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
        # Lock guards pool initialisation. Safe to create outside a running loop (Python 3.10+).
        self._pool_lock = asyncio.Lock()

    async def _get_pool(self):
        current_loop = asyncio.get_event_loop()

        if self._loop is not current_loop and self._pool is not None:
            # Event loop changed — the old pool's connections belong to a closed loop.
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
                )
                await pool.open()
                self._pool = pool
                self._loop = current_loop
        return self._pool

    async def execute(self, sql: str, params=None) -> list:
        try:
            async with (await self._get_pool()).connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(_to_pyformat(sql), params or [])
                    try:
                        return await cur.fetchall()
                    except Exception:
                        return []
        except Exception as exc:
            _raise_migration_hint(exc)
            raise

    async def execute_write(self, sql: str, params=None) -> int:
        try:
            async with (await self._get_pool()).connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(_to_pyformat(sql), params or [])
                    return cur.rowcount
        except Exception as exc:
            _raise_migration_hint(exc)
            raise

    async def execute_insert(self, sql: str, params=None):
        try:
            async with (await self._get_pool()).connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(_to_pyformat(sql) + " RETURNING id", params or [])
                    row = await cur.fetchone()
                return row["id"] if row else None
        except Exception as exc:
            _raise_migration_hint(exc)
            raise

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
        return [dict(r) for r in rows]

    async def close(self):
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
