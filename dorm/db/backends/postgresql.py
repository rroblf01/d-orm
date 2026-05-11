from __future__ import annotations

import asyncio
import functools
import logging
import threading
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from ..utils import (
    ASYNC_ATOMIC_STATE,
    awith_transient_retry,
    log_query,
    normalize_db_exception,
    with_transient_retry,
)

# Lifecycle (pool open/close, autocommit toggle, etc.). Connection metadata
# (db name, host) is emitted at DEBUG so an INFO-level log routed to a
# shared sink doesn't leak per-tenant database names. Open/close events
# themselves stay at INFO so ops can still spot boot/shutdown.
_lifecycle = logging.getLogger("dorm.db.lifecycle.postgresql")

# Module-level graveyard for drained psycopg pools and autocommit
# connections. ``force_close_sync`` parks drained objects here so
# their refcount never reaches zero, preventing ``__del__`` from
# firing during a ``gc.collect()`` pass that lands mid-test under
# ``pytest -n N`` on Python 3.14. The destructor would otherwise
# reach for the loop the pool was bound to — by then in tear-down
# — and SIGSEGV the worker. Per-process lifetime: this list grows
# by at most one pool per ``reset_connections()`` call and is
# never cleared. The leak (one pointer-sized object per drained
# pool) is intentional.
_drained_pool_graveyard: list = []


@functools.lru_cache(maxsize=4096)
def _to_pyformat(sql: str) -> str:
    """Convert $1, $2, ... placeholders to %s (psycopg3 style), skipping
    occurrences inside single-quoted string literals and double-quoted
    identifiers so user-supplied data containing $N is never mangled.

    Cached because most apps reuse the same SQL strings across requests
    (a typical app issues a few dozen distinct SELECT/INSERT shapes that
    are then repeated millions of times). The state machine below is
    O(len(sql)) but compiling it is pure overhead on the hot path. With
    a 4096-entry LRU we cover any realistic application's distinct
    queries while bounding memory."""
    out: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            # Single-quoted literal — handle SQL '' escape sequence.
            out.append(c)
            i += 1
            while i < n:
                if sql[i] == "'" and i + 1 < n and sql[i + 1] == "'":
                    out.append("''")
                    i += 2
                    continue
                out.append(sql[i])
                if sql[i] == "'":
                    i += 1
                    break
                i += 1
            continue
        if c == '"':
            # Double-quoted identifier — handle "" escape.
            out.append(c)
            i += 1
            while i < n:
                if sql[i] == '"' and i + 1 < n and sql[i + 1] == '"':
                    out.append('""')
                    i += 2
                    continue
                out.append(sql[i])
                if sql[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == "$" and i + 1 < n and sql[i + 1].isdigit():
            j = i + 1
            while j < n and sql[j].isdigit():
                j += 1
            out.append("%s")
            i = j
            continue
        out.append(c)
        i += 1
    return "".join(out)


_PGBOUNCER_MODES = frozenset({"transaction", "statement"})


def _coerce_pgbouncer_mode(value: Any) -> str | None:
    """Normalise the ``PGBOUNCER_MODE`` setting.

    Accepted values:

    - ``False`` / ``None`` / empty string → ``None`` (off).
    - ``True`` → ``"transaction"`` (the most common pgbouncer mode).
    - ``"transaction"`` / ``"statement"`` → kept as-is. ``"session"``
      mode is functionally identical to no pgbouncer at all for our
      purposes (the connection isn't multiplexed) so we treat it as
      off.
    """
    if not value:
        return None
    if value is True:
        return "transaction"
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _PGBOUNCER_MODES:
            return lowered
        if lowered == "session":
            return None
    raise ValueError(
        f"PGBOUNCER_MODE must be False/True/'transaction'/'statement'/'session'; "
        f"got {value!r}"
    )


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
        self._pool_timeout = float(settings.get("POOL_TIMEOUT", 30.0))
        # MAX_IDLE: idle connections older than this are recycled.
        # MAX_LIFETIME: every connection is recycled after living this long
        # regardless of activity (defends against server-side memory growth
        # on long-running PG sessions).
        self._max_idle = float(settings.get("MAX_IDLE", 600.0))
        self._max_lifetime = float(settings.get("MAX_LIFETIME", 3600.0))
        # POOL_CHECK runs `SELECT 1` on each checkout to detect stale
        # connections. Default-on for safety; turn off for high-throughput
        # apps where the ~ms overhead matters more than transparent reconnect.
        self._pool_check = bool(settings.get("POOL_CHECK", True))
        # PGBOUNCER mode: when True (or when settings.PGBOUNCER_MODE is
        # ``"transaction"`` / ``"statement"``), force ``prepare_threshold=None``
        # so psycopg never issues ``PREPARE`` / ``DEALLOCATE``. PgBouncer in
        # transaction-pooling mode reuses the same backend connection across
        # client transactions, so server-side prepared statements survive
        # past the client that allocated them and fail on the next reuse
        # ("prepared statement S_1 already exists" / "does not exist"). The
        # shim is opt-in because turning it on disables a real PG
        # performance feature.
        self._pgbouncer_mode = _coerce_pgbouncer_mode(
            settings.get("PGBOUNCER_MODE", False)
        )
        # PREPARE_THRESHOLD controls server-side prepared-statement caching.
        # ``None`` defers to psycopg's default (5 — i.e. cache after the 5th
        # execution of the same SQL shape). Set to ``0`` for "always prepare"
        # on workloads dominated by repeated SELECT/UPDATE shapes, or to a
        # higher value when most queries are unique.
        prep = settings.get("PREPARE_THRESHOLD")
        if self._pgbouncer_mode:
            # Hard override — even if the caller explicitly passed
            # PREPARE_THRESHOLD, transaction-pool mode forbids prepared
            # statements. Log loudly so a misconfigured deployment is
            # observable, but don't crash: returning to plain PG by flipping
            # PGBOUNCER_MODE back off must not require a restart.
            if prep is not None:
                _lifecycle.warning(
                    "PGBOUNCER_MODE=%r overrides PREPARE_THRESHOLD=%r; "
                    "prepared statements are disabled.",
                    self._pgbouncer_mode,
                    prep,
                )
            self._prepare_threshold: int | None = None
        else:
            self._prepare_threshold = int(prep) if prep is not None else None
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
        # Double-checked locking: the unsynchronised fast-path read is safe
        # under CPython because attribute writes are atomic with respect to
        # the GIL — a thread either sees ``None`` or the fully-constructed
        # pool, never a half-built object. The lock-protected slow path
        # ensures only one thread builds the pool. Free-threaded / no-GIL
        # builds (PEP 703) would need a memory barrier here; revisit when
        # we drop GIL-only support.
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
                conn_kwargs: dict[str, Any] = {"row_factory": dict_row}
                if self._prepare_threshold is not None:
                    conn_kwargs["prepare_threshold"] = self._prepare_threshold
                elif self._pgbouncer_mode:
                    # PgBouncer transaction-pool mode: forbid prepared
                    # statements outright. psycopg treats ``None`` as
                    # "disabled", but we must pass it explicitly because
                    # the library's own default (5) re-enables prep.
                    conn_kwargs["prepare_threshold"] = None
                pool_kwargs: dict[str, Any] = dict(
                    min_size=self._min_size,
                    max_size=self._max_size,
                    timeout=self._pool_timeout,
                    max_idle=self._max_idle,
                    max_lifetime=self._max_lifetime,
                    open=True,
                    kwargs=conn_kwargs,
                )
                if self._pool_check:
                    pool_kwargs["check"] = ConnectionPool.check_connection
                self._pool = ConnectionPool(
                    _dsn_to_conninfo(self._dsn),
                    **pool_kwargs,
                )
                _lifecycle.info(
                    "sync pool opened: min=%d max=%d timeout=%.1fs check=%s",
                    self._min_size,
                    self._max_size,
                    self._pool_timeout,
                    self._pool_check,
                )
                # Per-tenant identifying metadata only at DEBUG so a default
                # INFO-level deployment doesn't leak it.
                _lifecycle.debug(
                    "sync pool target: db=%s host=%s",
                    self._dsn.get("dbname"),
                    self._dsn.get("host"),
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

    def _exec(
        self,
        conn,
        sql: str,
        params,
        *,
        write: bool = False,
        insert: bool = False,
        pk_col: str = "id",
    ):
        with log_query("postgresql", sql, params):
            try:
                with conn.cursor() as cur:
                    _sql = _to_pyformat(sql) + (f' RETURNING "{pk_col}"' if insert else "")
                    cur.execute(_sql, params or [])
                    if insert:
                        row = cur.fetchone()
                        return row[pk_col] if row else None
                    if write:
                        return cur.rowcount
                    try:
                        return cur.fetchall()
                    # cur.fetchall() raises ProgrammingError on statements that
                    # produce no result set (DDL, etc.); treat that as "no rows".
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
        in_tx = self._atomic_conn is not None

        def _do() -> list:
            conn = self._choose_conn()
            if conn is not None:
                return self._exec(conn, sql, params)
            with self._get_pool().connection() as c:
                return self._exec(c, sql, params)

        return with_transient_retry(_do, in_transaction=in_tx)

    def execute_write(self, sql: str, params=None) -> int:
        in_tx = self._atomic_conn is not None

        def _do() -> int:
            conn = self._choose_conn()
            if conn is not None:
                return self._exec(conn, sql, params, write=True)
            with self._get_pool().connection() as c:
                return self._exec(c, sql, params, write=True)

        return with_transient_retry(_do, in_transaction=in_tx)

    def execute_insert(self, sql: str, params=None, pk_col: str = "id"):
        in_tx = self._atomic_conn is not None

        def _do():
            conn = self._choose_conn()
            if conn is not None:
                return self._exec(conn, sql, params, insert=True, pk_col=pk_col)
            with self._get_pool().connection() as c:
                return self._exec(c, sql, params, insert=True, pk_col=pk_col)

        return with_transient_retry(_do, in_transaction=in_tx)

    def _exec_bulk(self, conn, sql: str, params, pk_col: str) -> list[int]:
        with log_query("postgresql", sql, params):
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
        in_tx = self._atomic_conn is not None

        def _do() -> list[int]:
            conn = self._choose_conn()
            if conn is not None:
                return self._exec_bulk(conn, sql, params, pk_col)
            with self._get_pool().connection() as c:
                return self._exec_bulk(c, sql, params, pk_col)

        return with_transient_retry(_do, in_transaction=in_tx)

    def _exec_bulk_returning(self, conn, sql: str, params) -> list[dict]:
        # SQL already carries its own ``RETURNING …`` tail (built by
        # ``SQLQuery.as_bulk_insert(returning_cols=…)``). Don't re-append.
        with log_query("postgresql", sql, params):
            try:
                with conn.cursor() as cur:
                    cur.execute(_to_pyformat(sql), params or [])
                    return list(cur.fetchall())
            except Exception as exc:
                normalize_db_exception(exc)
                raise

    def execute_bulk_insert_returning(self, sql: str, params=None) -> list[dict]:
        """Execute a bulk INSERT whose SQL already contains a multi-column
        ``RETURNING …`` tail and return the rows as dicts. Used by
        :meth:`QuerySet.bulk_create(returning=…)` to back-fill DB-side
        defaults / generated columns onto the inserted objects."""
        in_tx = self._atomic_conn is not None

        def _do() -> list[dict]:
            conn = self._choose_conn()
            if conn is not None:
                return self._exec_bulk_returning(conn, sql, params)
            with self._get_pool().connection() as c:
                return self._exec_bulk_returning(c, sql, params)

        return with_transient_retry(_do, in_transaction=in_tx)

    def execute_script(self, sql: str):
        # Honour any active atomic() / autocommit() context so DDL run by
        # migrations participates in the surrounding transaction. Before
        # this fix, ``execute_script`` always checked out its own pool
        # connection, so a CREATE TABLE issued from a migration op
        # committed independently and survived an atomic() rollback.
        conn = self._choose_conn()
        if conn is not None:
            with conn.cursor() as cur:
                cur.execute(sql)
            return
        with self._get_pool().connection() as c:
            with c.cursor() as cur:
                cur.execute(sql)

    def execute_streaming(self, sql: str, params=None, chunk_size: int = 1000):
        """Yield rows from a server-side named cursor — for huge result
        sets that would blow up memory if fetched all at once. PG holds
        the result set on the server and streams in batches of
        ``chunk_size``.

        Refuses to run inside an ``atomic()`` block: named cursors need
        their own transaction, and silently falling back to a non-
        streaming fetch would materialise the whole result set in
        memory — the exact failure mode the caller used streaming to
        avoid. Better to fail loudly so the caller can restructure.
        """
        if self._atomic_conn is not None:
            raise RuntimeError(
                "execute_streaming() cannot be used inside an atomic() block: "
                "PostgreSQL named cursors require their own transaction. "
                "Move the streaming read outside the atomic() block, or use "
                "the non-streaming iterator() if the result set fits in memory."
            )

        sql_adapted = _to_pyformat(sql)
        with log_query("postgresql", sql, params):
            with self._get_pool().connection() as conn:
                # Named cursor → server-side. ``itersize`` is psycopg's
                # batch fetch size.
                with conn.cursor(name="dorm_stream") as cur:
                    cur.itersize = chunk_size
                    try:
                        cur.execute(sql_adapted, params or [])
                    except Exception as exc:
                        normalize_db_exception(exc)
                        raise
                    for row in cur:
                        yield row

    def copy_from(
        self,
        table: str,
        columns: list[str],
        rows,
        *,
        binary: bool = False,
    ) -> int:
        """Bulk-load *rows* into *table* via PostgreSQL ``COPY ... FROM STDIN``.

        Each item in *rows* is a sequence of values aligned with *columns*.
        Returns the number of rows written. With ``binary=True`` the COPY
        runs in binary mode — faster but every value must already be the
        right Python type for the column (psycopg's adapter handles the
        encoding).

        Honours the active atomic() block / autocommit connection.
        """
        in_tx = self._atomic_conn is not None

        col_list = ", ".join(f'"{c}"' for c in columns)
        suffix = " (FORMAT BINARY)" if binary else ""
        sql = f'COPY "{table}" ({col_list}) FROM STDIN{suffix}'

        def _do() -> int:
            conn = self._choose_conn()
            n = 0
            with log_query("postgresql", sql, None):
                try:
                    if conn is not None:
                        with conn.cursor() as cur, cur.copy(sql) as cp:
                            for row in rows:
                                cp.write_row(row)
                                n += 1
                    else:
                        with self._get_pool().connection() as c, c.cursor() as cur, cur.copy(sql) as cp:
                            for row in rows:
                                cp.write_row(row)
                                n += 1
                except Exception as exc:
                    normalize_db_exception(exc)
                    raise
            return n

        return with_transient_retry(_do, in_transaction=in_tx)

    def copy_to(
        self,
        sql: str,
        params=None,
        *,
        binary: bool = False,
    ):
        """Yield rows from a ``COPY (<query>) TO STDOUT`` stream.

        *sql* is a plain ``SELECT``; the wrapper builds the surrounding
        ``COPY (...) TO STDOUT`` envelope. Each yielded element is a tuple
        of values when ``binary=True`` (typed via psycopg adapters), or
        a tuple of strings in text mode.

        Refuses to run inside an ``atomic()`` block: COPY TO holds a server
        cursor open and silently materialising it would defeat the purpose.
        Move the export outside the atomic() block.
        """
        if self._atomic_conn is not None:
            raise RuntimeError(
                "copy_to() cannot be used inside an atomic() block: "
                "COPY TO holds a server-side stream open. Move the call "
                "outside the atomic() block."
            )
        sql_adapted = _to_pyformat(sql)
        suffix = " WITH (FORMAT BINARY)" if binary else ""
        copy_sql = f"COPY ({sql_adapted}) TO STDOUT{suffix}"
        with log_query("postgresql", copy_sql, params):
            with self._get_pool().connection() as conn:
                with conn.cursor() as cur:
                    try:
                        with cur.copy(copy_sql, params or []) as cp:
                            for row in cp.rows():
                                yield row
                    except Exception as exc:
                        normalize_db_exception(exc)
                        raise

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

    def pool_stats(self) -> dict[str, Any]:
        """Return current pool statistics — useful for capacity planning,
        admin dashboards, and Prometheus exporters. Keys depend on
        psycopg-pool's internal stats; ``open`` is always present."""
        if self._pool is None:
            return {"open": False, "vendor": "postgresql"}
        try:
            stats = self._pool.get_stats()
        except Exception:
            stats = {}
        return {
            "open": True,
            "vendor": "postgresql",
            "min_size": self._min_size,
            "max_size": self._max_size,
            **stats,
        }

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
            _lifecycle.info("sync pool closed")
            _lifecycle.debug("sync pool closed: db=%s", self._dsn.get("dbname"))


class PostgreSQLAsyncDatabaseWrapper:
    vendor = "postgresql"

    def __init__(self, settings: dict):
        self.settings = settings
        self._dsn = _build_dsn(settings)
        self._min_size = int(settings.get("MIN_POOL_SIZE", 1))
        self._max_size = int(settings.get("MAX_POOL_SIZE", 10))
        self._pool_timeout = float(settings.get("POOL_TIMEOUT", 30.0))
        self._max_idle = float(settings.get("MAX_IDLE", 600.0))
        self._max_lifetime = float(settings.get("MAX_LIFETIME", 3600.0))
        self._pool_check = bool(settings.get("POOL_CHECK", True))
        self._pgbouncer_mode = _coerce_pgbouncer_mode(
            settings.get("PGBOUNCER_MODE", False)
        )
        prep = settings.get("PREPARE_THRESHOLD")
        if self._pgbouncer_mode:
            if prep is not None:
                _lifecycle.warning(
                    "PGBOUNCER_MODE=%r overrides PREPARE_THRESHOLD=%r "
                    "(async); prepared statements are disabled.",
                    self._pgbouncer_mode,
                    prep,
                )
            self._prepare_threshold: int | None = None
        else:
            self._prepare_threshold = int(prep) if prep is not None else None
        self._pool = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pool_lock = asyncio.Lock()
        self._autocommit: bool = False
        self._autocommit_conn = None

    async def _get_pool(self):
        current_loop = asyncio.get_running_loop()

        if self._loop is not current_loop and self._pool is not None:
            # The old pool's tasks/connections belong to a dead loop —
            # awaiting close() on the new loop is unreliable. Try the
            # graceful close() on the old loop first (covers the case
            # where the old loop is still alive in another thread);
            # if the old loop is closed, fall back to ``pgconn.finish()``
            # on every idle connection in the pool. Without that
            # fallback, libpq sockets leaked until process exit and
            # the GC's eventual ``__del__`` could SIGSEGV under
            # ``pytest -n 4`` (same path :meth:`force_close_sync`
            # already handles for the in-process teardown case).
            old_pool = self._pool
            old_loop = self._loop
            self._pool = None
            self._loop = None
            self._pool_lock = asyncio.Lock()
            graceful_scheduled = False
            if old_loop is not None and not old_loop.is_closed():
                try:
                    asyncio.run_coroutine_threadsafe(
                        old_pool.close(), old_loop
                    )
                    graceful_scheduled = True
                except RuntimeError:
                    pass
            if not graceful_scheduled:
                # Loop is dead — close the libpq sockets ourselves so
                # the pool's ``__del__`` doesn't trip on them later.
                for conn in list(getattr(old_pool, "_pool", None) or ()):
                    try:
                        pgconn = getattr(conn, "pgconn", None)
                        if pgconn is not None:
                            pgconn.finish()
                    except Exception:
                        pass
                try:
                    old_pool._pool.clear()  # type: ignore[attr-defined]
                except Exception:
                    pass
                try:
                    old_pool._closed = True  # type: ignore[attr-defined]
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
                conn_kwargs: dict[str, Any] = {"row_factory": dict_row}
                if self._prepare_threshold is not None:
                    conn_kwargs["prepare_threshold"] = self._prepare_threshold
                elif self._pgbouncer_mode:
                    # PgBouncer transaction-pool mode: forbid prepared
                    # statements outright. psycopg treats ``None`` as
                    # "disabled", but we must pass it explicitly because
                    # the library's own default (5) re-enables prep.
                    conn_kwargs["prepare_threshold"] = None
                pool_kwargs: dict[str, Any] = dict(
                    min_size=self._min_size,
                    max_size=self._max_size,
                    timeout=self._pool_timeout,
                    max_idle=self._max_idle,
                    max_lifetime=self._max_lifetime,
                    open=False,
                    kwargs=conn_kwargs,
                )
                if self._pool_check:
                    pool_kwargs["check"] = AsyncConnectionPool.check_connection
                pool = AsyncConnectionPool(
                    _dsn_to_conninfo(self._dsn),
                    **pool_kwargs,
                )
                await pool.open()
                self._pool = pool
                self._loop = current_loop
                _lifecycle.info(
                    "async pool opened: min=%d max=%d timeout=%.1fs check=%s",
                    self._min_size,
                    self._max_size,
                    self._pool_timeout,
                    self._pool_check,
                )
                _lifecycle.debug(
                    "async pool target: db=%s host=%s",
                    self._dsn.get("dbname"),
                    self._dsn.get("host"),
                )
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

    async def _aexec(
        self,
        conn,
        sql: str,
        params,
        *,
        write: bool = False,
        insert: bool = False,
        pk_col: str = "id",
    ):
        with log_query("postgresql", sql, params):
            try:
                async with conn.cursor() as cur:
                    _sql = _to_pyformat(sql) + (f' RETURNING "{pk_col}"' if insert else "")
                    await cur.execute(_sql, params or [])
                    if insert:
                        row = await cur.fetchone()
                        return row[pk_col] if row else None
                    if write:
                        return cur.rowcount
                    try:
                        return await cur.fetchall()
                    # See sync _exec: DDL statements raise on fetchall().
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
        """Return atomic conn, pinned conn, autocommit persistent conn,
        or None (use pool). Resolution order:

        1. Active ``aatomic()`` block — transactions always win.
        2. ``dorm.contrib.task_pool.pinned_connection()`` — task-local
           pinning honoured next, so request-scoped helpers reuse one
           checkout across all queries in the task.
        3. Autocommit persistent connection.
        4. ``None`` → checkout per call from the pool.
        """
        state = ASYNC_ATOMIC_STATE.get()
        if state is not None and state[0] is self:
            return state[1]
        # Task-local pin is opt-in (only set by users of
        # ``dorm.contrib.task_pool.pinned_connection``); the import
        # is local so the contrib module isn't loaded for every
        # ORM call.
        from dorm.contrib.task_pool import get_pinned_connection
        pinned = get_pinned_connection()
        if pinned is not None:
            return pinned
        if self._autocommit:
            return await self._get_autocommit_conn()
        return None

    def _in_async_atomic(self) -> bool:
        state = ASYNC_ATOMIC_STATE.get()
        return state is not None and state[0] is self

    async def execute(self, sql: str, params=None) -> list:
        async def _do():
            conn = await self._choose_conn()
            if conn is not None:
                return await self._aexec(conn, sql, params)
            async with (await self._get_pool()).connection() as c:
                return await self._aexec(c, sql, params)

        return await awith_transient_retry(_do, in_transaction=self._in_async_atomic())

    async def execute_write(self, sql: str, params=None) -> int:
        async def _do():
            conn = await self._choose_conn()
            if conn is not None:
                return await self._aexec(conn, sql, params, write=True)
            async with (await self._get_pool()).connection() as c:
                return await self._aexec(c, sql, params, write=True)

        return await awith_transient_retry(_do, in_transaction=self._in_async_atomic())

    async def execute_insert(self, sql: str, params=None, pk_col: str = "id"):
        async def _do():
            conn = await self._choose_conn()
            if conn is not None:
                return await self._aexec(conn, sql, params, insert=True, pk_col=pk_col)
            async with (await self._get_pool()).connection() as c:
                return await self._aexec(c, sql, params, insert=True, pk_col=pk_col)

        return await awith_transient_retry(_do, in_transaction=self._in_async_atomic())

    async def _aexec_bulk(self, conn, sql: str, params, pk_col: str) -> list[int]:
        with log_query("postgresql", sql, params):
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
        async def _do():
            conn = await self._choose_conn()
            if conn is not None:
                return await self._aexec_bulk(conn, sql, params, pk_col)
            async with (await self._get_pool()).connection() as c:
                return await self._aexec_bulk(c, sql, params, pk_col)

        return await awith_transient_retry(_do, in_transaction=self._in_async_atomic())

    async def _aexec_bulk_returning(self, conn, sql: str, params) -> list[dict]:
        with log_query("postgresql", sql, params):
            try:
                async with conn.cursor() as cur:
                    await cur.execute(_to_pyformat(sql), params or [])
                    rows = await cur.fetchall()
                    return list(rows)
            except Exception as exc:
                normalize_db_exception(exc)
                raise

    async def execute_bulk_insert_returning(self, sql: str, params=None) -> list[dict]:
        async def _do() -> list[dict]:
            conn = await self._choose_conn()
            if conn is not None:
                return await self._aexec_bulk_returning(conn, sql, params)
            async with (await self._get_pool()).connection() as c:
                return await self._aexec_bulk_returning(c, sql, params)

        return await awith_transient_retry(_do, in_transaction=self._in_async_atomic())

    async def execute_script(self, sql: str):
        # Same atomic-respecting logic as the sync wrapper: if there's
        # an active aatomic() block, use that block's pinned connection
        # so DDL participates in the outer transaction.
        state = ASYNC_ATOMIC_STATE.get()
        if state is not None and state[0] is self:
            async with state[1].cursor() as cur:
                await cur.execute(sql)
            return
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

    async def execute_streaming(self, sql: str, params=None, chunk_size: int = 1000):
        """Async streaming via a server-side named cursor. See
        :meth:`PostgreSQLDatabaseWrapper.execute_streaming` for the sync
        version's notes about transaction interactions."""
        state = ASYNC_ATOMIC_STATE.get()
        if state is not None and state[0] is self:
            raise RuntimeError(
                "execute_streaming() cannot be used inside an aatomic() block: "
                "PostgreSQL named cursors require their own transaction. "
                "Move the streaming read outside the aatomic() block, or use "
                "the non-streaming aiterator() if the result set fits in memory."
            )

        sql_adapted = _to_pyformat(sql)
        with log_query("postgresql", sql, params):
            async with (await self._get_pool()).connection() as conn:
                async with conn.cursor(name="dorm_stream") as cur:
                    cur.itersize = chunk_size
                    try:
                        await cur.execute(sql_adapted, params or [])
                    except Exception as exc:
                        normalize_db_exception(exc)
                        raise
                    async for row in cur:
                        yield row

    async def copy_from(
        self,
        table: str,
        columns: list[str],
        rows,
        *,
        binary: bool = False,
    ) -> int:
        """Async ``COPY ... FROM STDIN``. *rows* may be a sync iterable
        or an ``AsyncIterable`` — both are accepted, the loop adapts
        automatically. See the sync wrapper for the rest of the contract."""
        col_list = ", ".join(f'"{c}"' for c in columns)
        suffix = " (FORMAT BINARY)" if binary else ""
        sql = f'COPY "{table}" ({col_list}) FROM STDIN{suffix}'
        in_tx = self._in_async_atomic()

        async def _do() -> int:
            conn = await self._choose_conn()
            n = 0
            with log_query("postgresql", sql, None):
                try:
                    async def _drain(cp):
                        nonlocal n
                        if hasattr(rows, "__aiter__"):
                            async for row in rows:
                                await cp.write_row(row)
                                n += 1
                        else:
                            for row in rows:
                                await cp.write_row(row)
                                n += 1

                    if conn is not None:
                        async with conn.cursor() as cur, cur.copy(sql) as cp:
                            await _drain(cp)
                    else:
                        async with (await self._get_pool()).connection() as c:
                            async with c.cursor() as cur, cur.copy(sql) as cp:
                                await _drain(cp)
                except Exception as exc:
                    normalize_db_exception(exc)
                    raise
            return n

        return await awith_transient_retry(_do, in_transaction=in_tx)

    async def copy_to(
        self,
        sql: str,
        params=None,
        *,
        binary: bool = False,
    ):
        """Async generator over ``COPY (<query>) TO STDOUT``. See the
        sync wrapper for semantics. Refuses to run inside ``aatomic()``."""
        if self._in_async_atomic():
            raise RuntimeError(
                "copy_to() cannot be used inside an aatomic() block: "
                "COPY TO holds a server-side stream open. Move the call "
                "outside the aatomic() block."
            )
        sql_adapted = _to_pyformat(sql)
        suffix = " WITH (FORMAT BINARY)" if binary else ""
        copy_sql = f"COPY ({sql_adapted}) TO STDOUT{suffix}"
        with log_query("postgresql", copy_sql, params):
            async with (await self._get_pool()).connection() as conn:
                async with conn.cursor() as cur:
                    try:
                        async with cur.copy(copy_sql, params or []) as cp:
                            async for row in cp.rows():
                                yield row
                    except Exception as exc:
                        normalize_db_exception(exc)
                        raise

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

    def pool_stats(self) -> dict[str, Any]:
        """Async pool stats — same shape as the sync wrapper's
        :meth:`pool_stats`. Safe to call without awaiting."""
        if self._pool is None:
            return {"open": False, "vendor": "postgresql"}
        try:
            stats = self._pool.get_stats()
        except Exception:
            stats = {}
        return {
            "open": True,
            "vendor": "postgresql",
            "min_size": self._min_size,
            "max_size": self._max_size,
            **stats,
        }

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
            _lifecycle.info("async pool closed")
            _lifecycle.debug("async pool closed: db=%s", self._dsn.get("dbname"))

    def force_close_sync(self) -> None:
        """Release the held pool from a non-async context.

        Called from :func:`dorm.db.connection.reset_connections` and the
        atexit hook. The graceful path schedules ``pool.close()`` on
        the loop the pool was opened on (if that loop is still alive);
        regardless of that, we then synchronously **finish every
        pgconn** in the pool's idle deque. That last step matters: the
        pool's ``__del__`` running on a dead loop has been observed to
        take a CPython worker down with a SIGSEGV / abort under
        ``pytest -n 4`` on Python 3.14 + FastAPI ``TestClient`` (each
        request creates its own portal loop, so the pool quickly
        accumulates references tied to closed loops). Closing the
        underlying libpq sockets up front guarantees the pool is inert
        by the time the GC reaches it.

        Never blocks — sync teardown can't await.
        """
        pool = self._pool
        autocommit_conn = self._autocommit_conn
        self._pool = None
        # ``self._loop`` is dropped without binding to a local — the
        # graveyard branch below doesn't need it (we no longer
        # schedule ``pool.close()`` on the originating loop), and
        # keeping the attribute populated would just leak a stale
        # loop reference past ``reset_connections()``.
        self._loop = None
        self._autocommit_conn = None
        # Park drained pools / autocommit conns in a module-level
        # graveyard so refcount stays > 0 and ``__del__`` never fires.
        # On Python 3.14 the GC was finalising psycopg-pool's
        # ``AsyncConnectionPool`` mid-test (different test, same
        # worker), and the Task / coroutine that ``__del__`` reaches
        # for landed on a loop in tear-down — SIGSEGV in libpq.
        # Holding the pool alive forever leaks a pointer-sized
        # object per worker; that's the trade we accept to keep
        # ``pytest -n N`` workers from crashing.
        if pool is not None:
            _drained_pool_graveyard.append(pool)
        if autocommit_conn is not None:
            _drained_pool_graveyard.append(autocommit_conn)
        if pool is not None:
            # Mark the pool closed FIRST so the dispatcher can't hand
            # out an idle connection while we're tearing one down.
            try:
                pool._closed = True  # type: ignore[attr-defined]
            except Exception:
                pass
            # Defensive teardown of the libpq layer. ``pgconn.finish``
            # is the sync C-level close — safe from any thread because
            # libpq's connection objects aren't bound to an event loop.
            # Drain the deque BEFORE attempting any async close so the
            # two paths can't race over the same pgconn (concurrent
            # ``pool.close()`` running on the session loop while the
            # GC reaches a closed pgconn here was the SIGSEGV under
            # ``pytest -n N`` on Python 3.14).
            for conn in list(getattr(pool, "_pool", None) or ()):
                try:
                    pgconn = getattr(conn, "pgconn", None)
                    if pgconn is not None:
                        pgconn.finish()
                except Exception:
                    pass
            try:
                pool._pool.clear()  # type: ignore[attr-defined]
            except Exception:
                pass
            # Skip ``pool.close()`` entirely — we already drained the
            # libpq sockets above and zero-ed out ``_pool._pool`` /
            # ``_pool._closed``. Scheduling the coroutine on the
            # original loop ran into two SIGSEGV paths under
            # ``pytest -n N`` on Python 3.14:
            #
            #   1. ``run_coroutine_threadsafe`` returns a Future whose
            #      backing Task holds a strong ref to the pool object.
            #      When ``fut.result(timeout=0.5)`` times out, the Task
            #      stays pending on the (possibly closing) loop and the
            #      GC finalises the pool against an inconsistent loop
            #      state.
            #
            #   2. Even with ``fut.cancel()`` after timeout, the cancel
            #      is queued; the Task only actually cancels when the
            #      loop processes its next tick. If the worker exits
            #      first, the Task is destroyed mid-coroutine and
            #      psycopg-pool's ``__aexit__`` paths reach for handles
            #      we already finished.
            #
            # The synchronous drain above is sufficient — every libpq
            # socket is closed, ``_pool._pool`` is empty, and
            # ``_closed`` is True. ``__del__`` on the bare pool object
            # is now safe.
        if autocommit_conn is not None:
            try:
                pgconn = getattr(autocommit_conn, "pgconn", None)
                if pgconn is not None:
                    pgconn.finish()
            except Exception:
                pass
            # Same reasoning as the pool path above — pgconn.finish()
            # is the authoritative shutdown; an extra ``conn.close()``
            # scheduled on the loop only created teardown races.

    async def notify(self, channel: str, payload: str = "") -> None:
        """Send a ``NOTIFY`` to *channel* with optional *payload*.

        ``channel`` is validated as a SQL identifier; ``payload`` is
        passed as a bound parameter so it can carry any UTF-8 text
        (PostgreSQL's NOTIFY payload limit is 8000 bytes by default).
        Emit from anywhere — inside or outside a transaction. When
        called inside an :func:`aatomic` block the NOTIFY isn't
        delivered until the surrounding transaction commits, which is
        usually exactly what you want.
        """
        from ...query import _validate_identifier

        _validate_identifier(channel, kind="NOTIFY channel")
        # NOTIFY's payload arg can be parameterised via pg_notify().
        await self.execute("SELECT pg_notify(%s, %s)", [channel, payload])

    async def listen(self, channel: str):
        """Async iterator yielding notification objects for *channel*.

        Opens its **own** dedicated connection (LISTEN holds the
        connection for the lifetime of the subscription, so we don't
        want to tie up a pool slot). Each yielded value is a
        ``psycopg.Notify`` with ``channel``, ``payload`` and ``pid``
        attributes.

        ``channel`` is validated as a SQL identifier so callers passing
        user input can't smuggle arbitrary SQL.

        Use it as ``async for msg in conn.listen("orders"): …`` — the
        loop never returns by itself; break out when you want to
        unsubscribe (or cancel the consuming task). On exit, the
        underlying connection is closed cleanly.
        """
        import psycopg
        from psycopg.rows import dict_row
        from ...query import _validate_identifier

        _validate_identifier(channel, kind="LISTEN channel")
        # psycopg requires LISTEN to run on a dedicated, autocommit
        # connection — the generator owns this connection for its
        # lifetime. We can't re-use a pool connection because LISTEN
        # registers an asynchronous notification handler on it.
        conn = await psycopg.AsyncConnection.connect(
            _dsn_to_conninfo(self._dsn),
            autocommit=True,
            row_factory=dict_row,  # type: ignore
        )
        try:
            async with conn.cursor() as cur:
                # Identifier is already validated; safe to splice.
                # ``cur.execute`` wants a LiteralString; the f-string here
                # is built only from validated identifiers, so it's safe.
                await cur.execute(f'LISTEN "{channel}"')  # type: ignore
            async for notify in conn.notifies():
                yield notify
        finally:
            try:
                await conn.close()
            except Exception:
                pass
