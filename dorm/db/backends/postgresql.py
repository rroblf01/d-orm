from __future__ import annotations

import re
import threading


def _raise_migration_hint(exc: Exception) -> None:
    """Re-raise a missing-table error with a friendly migration hint."""
    from dorm.exceptions import OperationalError

    msg = str(exc)
    # PostgreSQL: 'relation "<name>" does not exist'
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
        "database": settings.get("NAME", ""),
        "user": settings.get("USER", ""),
        "password": settings.get("PASSWORD", ""),
    }


class PostgreSQLDatabaseWrapper:
    vendor = "postgresql"
    _local = threading.local()

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)

    def get_connection(self):
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError as e:
            raise ImportError(
                "psycopg2-binary is required for PostgreSQL support. "
                "Install it with: pip install d-orm[postgresql]"
            ) from e

        if not hasattr(self._local, "conn") or self._local.conn is None or self._local.conn.closed:
            conn = psycopg2.connect(**self._dsn)
            conn.autocommit = False
            self._local.conn = conn
        return self._local.conn

    def _cursor(self):
        import psycopg2.extras
        conn = self.get_connection()
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def execute(self, sql: str, params=None) -> list:
        cursor = self._cursor()
        try:
            cursor.execute(sql, params or [])
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        try:
            return cursor.fetchall()
        except Exception:
            return []

    def execute_write(self, sql: str, params=None) -> int:
        conn = self.get_connection()
        cursor = self._cursor()
        try:
            cursor.execute(sql, params or [])
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        conn.commit()
        return cursor.rowcount

    def execute_insert(self, sql: str, params=None):
        conn = self.get_connection()
        cursor = self._cursor()
        try:
            cursor.execute(sql + " RETURNING id", params or [])
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        conn.commit()
        row = cursor.fetchone()
        return row["id"] if row else None

    def execute_script(self, sql: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

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
        if hasattr(self._local, "conn") and self._local.conn and not self._local.conn.closed:
            self._local.conn.close()
            self._local.conn = None


class PostgreSQLAsyncDatabaseWrapper:
    vendor = "postgresql"

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)
        self._pool = None

    async def _get_pool(self):
        try:
            import asyncpg
        except ImportError as e:
            raise ImportError(
                "asyncpg is required for async PostgreSQL support. "
                "Install it with: pip install d-orm[postgresql]"
            ) from e

        if self._pool is None:
            self._pool = await asyncpg.create_pool(**self._dsn)
        return self._pool

    async def execute(self, sql: str, params=None) -> list:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *(params or []))
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        return [dict(r) for r in rows]

    async def execute_write(self, sql: str, params=None) -> int:
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(sql, *(params or []))
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0

    async def execute_insert(self, sql: str, params=None):
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(sql + " RETURNING id", *(params or []))
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        return row["id"] if row else None

    async def execute_script(self, sql: str):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(sql)

    async def table_exists(self, table_name: str) -> bool:
        rows = await self.execute(
            "SELECT tablename FROM pg_tables WHERE tablename = $1",
            [table_name],
        )
        return bool(rows)

    async def get_table_columns(self, table_name: str) -> list[dict]:
        rows = await self.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        return [dict(r) for r in rows]

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
