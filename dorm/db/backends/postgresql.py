from __future__ import annotations

import re
import threading

_POSITIONAL_PLACEHOLDER = re.compile(r"\$\d+")


def _to_pyformat(sql: str) -> str:
    """Convert $1, $2, ... placeholders to %s (psycopg3 style)."""
    return _POSITIONAL_PLACEHOLDER.sub("%s", sql)


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
    """DSN for psycopg3 (uses 'dbname')."""
    return {
        "host": settings.get("HOST", "localhost"),
        "port": int(settings.get("PORT", 5432)),
        "dbname": settings.get("NAME", ""),
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
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as e:
            raise ImportError(
                "psycopg is required for PostgreSQL support. "
                "Install it with: pip install 'djanorm[postgresql]'"
            ) from e

        if (
            not hasattr(self._local, "conn")
            or self._local.conn is None
            or self._local.conn.closed
        ):
            import psycopg
            from psycopg.rows import dict_row

            self._local.conn = psycopg.connect(**self._dsn, row_factory=dict_row)  # type: ignore
        return self._local.conn

    def execute(self, sql: str, params=None) -> list:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(_to_pyformat(sql), params or [])
                try:
                    return cur.fetchall()
                except Exception:
                    return []
        except Exception as exc:
            conn.rollback()
            _raise_migration_hint(exc)
            raise

    def execute_write(self, sql: str, params=None) -> int:
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(_to_pyformat(sql), params or [])
                rowcount = cur.rowcount
            conn.commit()
            return rowcount
        except Exception as exc:
            conn.rollback()
            _raise_migration_hint(exc)
            raise

    def execute_insert(self, sql: str, params=None):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(_to_pyformat(sql) + " RETURNING id", params or [])
                row = cur.fetchone()
            conn.commit()
            return row["id"] if row else None
        except Exception as exc:
            conn.rollback()
            _raise_migration_hint(exc)
            raise

    def execute_script(self, sql: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise exc

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
        if (
            hasattr(self._local, "conn")
            and self._local.conn
            and not self._local.conn.closed
        ):
            self._local.conn.close()
            self._local.conn = None


class PostgreSQLAsyncDatabaseWrapper:
    vendor = "postgresql"

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)
        self._conn = None

    async def _get_conn(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as e:
            raise ImportError(
                "psycopg is required for async PostgreSQL support. "
                "Install it with: pip install 'djanorm[postgresql]'"
            ) from e

        if self._conn is None or self._conn.closed:
            import psycopg
            from psycopg.rows import dict_row

            self._conn = await psycopg.AsyncConnection.connect(
                **self._dsn, row_factory=dict_row  # type: ignore
            )
        return self._conn

    async def execute(self, sql: str, params=None) -> list:
        conn = await self._get_conn()
        try:
            async with conn.cursor() as cur:
                await cur.execute(_to_pyformat(sql), params or [])
                try:
                    return await cur.fetchall()
                except Exception:
                    return []
        except Exception as exc:
            await conn.rollback()
            _raise_migration_hint(exc)
            raise

    async def execute_write(self, sql: str, params=None) -> int:
        conn = await self._get_conn()
        try:
            async with conn.cursor() as cur:
                await cur.execute(_to_pyformat(sql), params or [])
                rowcount = cur.rowcount
            await conn.commit()
            return rowcount
        except Exception as exc:
            await conn.rollback()
            _raise_migration_hint(exc)
            raise

    async def execute_insert(self, sql: str, params=None):
        conn = await self._get_conn()
        try:
            async with conn.cursor() as cur:
                await cur.execute(_to_pyformat(sql) + " RETURNING id", params or [])
                row = await cur.fetchone()
            await conn.commit()
            return row["id"] if row else None
        except Exception as exc:
            await conn.rollback()
            _raise_migration_hint(exc)
            raise

    async def execute_script(self, sql: str):
        conn = await self._get_conn()
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql)
            await conn.commit()
        except Exception as exc:
            await conn.rollback()
            raise exc

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
        if self._conn and not self._conn.closed:
            await self._conn.close()
            self._conn = None
