"""DuckDB backend wrapper.

DuckDB is an embedded analytical (OLAP) database. The wrapper mirrors
the SQLite backend's shape because DuckDB ships an SQLite-compatible
``DB-API`` adapter and shares the in-process / single-file footprint.
The big differences:

- columnar storage tuned for aggregate scans;
- vectorised execution engine, single-digit-ms scans on M-row tables;
- Postgres-flavoured SQL (``::CAST``, ``GENERATED`` columns,
  ``ARRAY`` / ``STRUCT`` literals) on top of the SQLite ergonomics.

Use it for analytical workloads inside the same Python process —
dashboards, ETL staging, ML feature stores. For OLTP keep using
PostgreSQL or libsql.

The async variant runs DuckDB in a worker thread (DuckDB's own API
is sync). For genuinely async workloads, prefer libsql or PG.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any

from ..utils import (
    log_query,
    normalize_db_exception,
    with_transient_retry,
)


class DuckDBDatabaseWrapper:
    vendor = "duckdb"

    def __init__(self, settings: dict) -> None:
        self.settings = settings
        # Default to in-memory; treat ``":memory:"`` and ``""`` the
        # same so the SQLite-equivalent settings DSN works.
        path = settings.get("NAME", ":memory:") or ":memory:"
        self.database = path
        self._local = threading.local()
        self._conns: dict[int, Any] = {}
        self._conns_lock = threading.Lock()
        self._autocommit = False

    def _new_connection(self):
        try:
            import duckdb
        except ImportError as exc:
            raise ImportError(
                "DuckDB backend requires the duckdb package. "
                "Install with: pip install 'djanorm[duckdb]'"
            ) from exc
        conn = duckdb.connect(self.database)
        # DuckDB defaults to column-oriented row factory; switch to
        # dict so the rest of the dorm runtime can read by column name.
        # Achieved post-fetch by wrapping cursor results — see
        # ``_to_dicts`` below.
        return conn

    def get_connection(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
            with self._conns_lock:
                self._conns[threading.get_ident()] = conn
        return conn

    def close(self) -> None:
        with self._conns_lock:
            for conn in list(self._conns.values()):
                try:
                    conn.close()
                except Exception:
                    pass
            self._conns.clear()
        if hasattr(self._local, "conn"):
            del self._local.conn

    def _adapt(self, sql: str) -> str:
        """DuckDB accepts both ``?`` and ``$N`` style placeholders.
        We normalise ``$N`` to ``?`` so dorm's PG-style query
        compilation survives."""
        import re

        return re.sub(r"\$\d+", "?", sql)

    def _to_dicts(self, cursor: Any) -> list[dict]:
        cols = [d[0] for d in (cursor.description or [])]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    @property
    def _atomic_depth(self) -> int:
        return getattr(self._local, "atomic_depth", 0)

    @_atomic_depth.setter
    def _atomic_depth(self, value: int) -> None:
        self._local.atomic_depth = value

    @contextmanager
    def atomic(self):
        conn = self.get_connection()
        depth = self._atomic_depth
        if depth == 0:
            conn.execute("BEGIN")
            self._atomic_depth = 1
            try:
                yield
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            finally:
                self._atomic_depth = 0
        else:
            # DuckDB has no SAVEPOINT support — nested ``atomic()``
            # blocks degrade to a no-op marker. The outer transaction
            # still owns the commit/rollback boundary, so any
            # exception inside an inner block correctly aborts the
            # whole block (the outer wrapper sees the raise on the
            # way out and rolls back). Behaviour matches "savepoint
            # not available" semantics: less granular than
            # SQLite/PG but consistent with DuckDB's transactional
            # model.
            self._atomic_depth = depth + 1
            try:
                yield
            finally:
                self._atomic_depth = depth

    def execute(self, sql: str, params=None) -> list:
        in_tx = self._atomic_depth > 0

        def _do() -> list:
            with log_query("duckdb", sql, params):
                conn = self.get_connection()
                try:
                    cur = conn.execute(self._adapt(sql), params or [])
                    return self._to_dicts(cur)
                except Exception as exc:
                    normalize_db_exception(exc)
                    raise

        return with_transient_retry(_do, in_transaction=in_tx)

    def execute_write(self, sql: str, params=None) -> int:
        in_tx = self._atomic_depth > 0

        def _do() -> int:
            with log_query("duckdb", sql, params):
                conn = self.get_connection()
                try:
                    cur = conn.execute(self._adapt(sql), params or [])
                    # DuckDB's cursor exposes a ``rowcount`` attribute
                    # but it returns -1 for many DML statements; fall
                    # back to a follow-up query when unset.
                    rc = getattr(cur, "rowcount", -1)
                    if rc is None or rc < 0:
                        return 0
                    return rc
                except Exception as exc:
                    normalize_db_exception(exc)
                    raise

        return with_transient_retry(_do, in_transaction=in_tx)

    def execute_insert(self, sql: str, params=None, pk_col: str = "id"):
        # DuckDB's auto-incrementing PKs are produced via sequences
        # explicitly; emulate ``RETURNING`` by issuing it inline.
        appended_returning = sql + f' RETURNING "{pk_col}"'
        rows = self.execute(appended_returning, params)
        if not rows:
            return None
        return rows[0].get(pk_col)

    def execute_bulk_insert(self, sql: str, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        appended = sql + f' RETURNING "{pk_col}"'
        rows = self.execute(appended, params)
        return [r.get(pk_col) for r in rows]

    def execute_bulk_insert_returning(self, sql: str, params=None) -> list[dict]:
        return self.execute(sql, params)

    def execute_script(self, sql: str) -> None:
        conn = self.get_connection()
        with log_query("duckdb", sql, None):
            conn.execute(sql)

    def execute_streaming(self, sql: str, params=None, chunk_size: int = 1000):
        conn = self.get_connection()
        with log_query("duckdb", sql, params):
            cur = conn.execute(self._adapt(sql), params or [])
            cols = [d[0] for d in (cur.description or [])]
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(cols, row))

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            [table_name],
        )
        return bool(rows)

    def get_table_columns(self, table_name: str) -> list[dict]:
        # DuckDB exposes schema via ``information_schema.columns`` —
        # PG-flavoured names so the ``cmd_diff`` introspection
        # path picks it up uniformly.
        rows = self.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_name = ? "
            "ORDER BY ordinal_position",
            [table_name],
        )
        return [
            {"name": r["column_name"], **{k: v for k, v in r.items() if k != "column_name"}}
            for r in rows
        ]

    def set_autocommit(self, enabled: bool) -> None:
        self._autocommit = enabled

    def commit(self) -> None:
        # DuckDB transactions auto-commit on next BEGIN; explicit
        # commit is supported but only meaningful when autocommit=False.
        try:
            self.get_connection().commit()
        except Exception:
            pass

    def rollback(self) -> None:
        try:
            self.get_connection().rollback()
        except Exception:
            pass

    def pool_stats(self) -> dict[str, Any]:
        return {"vendor": "duckdb", "has_pool": False}


class DuckDBAsyncDatabaseWrapper:
    """Async wrapper for DuckDB.

    Independent class (does NOT inherit ``DuckDBDatabaseWrapper``)
    so the type checker doesn't try to reconcile sync vs async
    method signatures. Composes a ``DuckDBDatabaseWrapper`` and
    delegates every coroutine to ``asyncio.to_thread``: DuckDB's
    Python API is synchronous, so the async surface is thin.

    For genuine async workloads use libsql or PostgreSQL.
    """

    vendor = "duckdb"

    def __init__(self, settings: dict) -> None:
        self._inner = DuckDBDatabaseWrapper(settings)

    @property
    def settings(self) -> dict:
        return self._inner.settings

    async def execute(self, sql: str, params=None) -> list:
        import asyncio

        return await asyncio.to_thread(self._inner.execute, sql, params)

    async def execute_write(self, sql: str, params=None) -> int:
        import asyncio

        return await asyncio.to_thread(self._inner.execute_write, sql, params)

    async def execute_insert(self, sql: str, params=None, pk_col: str = "id"):
        import asyncio

        return await asyncio.to_thread(
            self._inner.execute_insert, sql, params, pk_col
        )

    async def execute_bulk_insert(self, sql: str, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        import asyncio

        return await asyncio.to_thread(
            self._inner.execute_bulk_insert, sql, params, pk_col, count
        )

    async def execute_script(self, sql: str) -> None:
        import asyncio

        await asyncio.to_thread(self._inner.execute_script, sql)

    async def table_exists(self, table_name: str) -> bool:
        import asyncio

        return await asyncio.to_thread(self._inner.table_exists, table_name)

    def close(self) -> None:
        self._inner.close()

    def pool_stats(self) -> dict[str, Any]:
        return self._inner.pool_stats()

    @property
    def force_close_sync(self):
        return self.close


__all__ = ["DuckDBDatabaseWrapper", "DuckDBAsyncDatabaseWrapper"]
