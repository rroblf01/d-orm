from __future__ import annotations

import asyncio
import re
import sqlite3
import threading


def _raise_migration_hint(exc: Exception, table: str | None = None) -> None:
    from dorm.exceptions import OperationalError

    msg = str(exc)
    match = re.search(r"no such table: (\S+)", msg, re.IGNORECASE)
    if match:
        table = table or match.group(1)
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


class SQLiteDatabaseWrapper:
    vendor = "sqlite"
    _local = threading.local()

    def __init__(self, settings: dict):
        self.settings = settings
        self.database = settings.get("NAME", ":memory:")
        self._conn: sqlite3.Connection | None = None

    def get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.database, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            self._local.conn = conn
        return self._local.conn

    @staticmethod
    def _adapt(sql: str) -> str:
        return sql.replace("%s", "?")

    def execute(self, sql: str, params=None) -> list:
        conn = self.get_connection()
        params = params or []
        try:
            cursor = conn.execute(self._adapt(sql), params)
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        return cursor.fetchall()

    def execute_write(self, sql: str, params=None) -> int:
        conn = self.get_connection()
        params = params or []
        try:
            cursor = conn.execute(self._adapt(sql), params)
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        conn.commit()
        return cursor.rowcount

    def execute_insert(self, sql: str, params=None):
        conn = self.get_connection()
        params = params or []
        try:
            cursor = conn.execute(self._adapt(sql), params)
        except Exception as exc:
            _raise_migration_hint(exc)
            raise
        conn.commit()
        return cursor.lastrowid

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
        # Safe to create outside a running loop (Python 3.10+).
        self._lock = asyncio.Lock()

    @staticmethod
    def _adapt(sql: str) -> str:
        return sql.replace("%s", "?")

    async def _get_conn(self):
        import aiosqlite

        current_loop = asyncio.get_event_loop()
        if self._loop is not current_loop:
            if self._conn is not None:
                try:
                    await self._conn.close()
                except Exception:
                    pass
            self._conn = None
            self._loop = current_loop
            self._lock = asyncio.Lock()

        if self._conn is None:
            self._conn = await aiosqlite.connect(self.database)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA foreign_keys = ON")
            await self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn

    async def execute(self, sql: str, params=None) -> list:
        async with self._lock:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                rows = await cursor.fetchall()
            except Exception as exc:
                _raise_migration_hint(exc)
                raise
            return list(rows)

    async def execute_write(self, sql: str, params=None) -> int:
        async with self._lock:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                await conn.commit()
                return cursor.rowcount
            except Exception as exc:
                _raise_migration_hint(exc)
                raise

    async def execute_insert(self, sql: str, params=None):
        async with self._lock:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute(self._adapt(sql), params or [])
                await conn.commit()
                return cursor.lastrowid
            except Exception as exc:
                _raise_migration_hint(exc)
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

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
