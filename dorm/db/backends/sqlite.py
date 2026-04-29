from __future__ import annotations

import asyncio
import datetime
import sqlite3
import threading
from contextlib import asynccontextmanager, contextmanager

from ...exceptions import ImproperlyConfigured
from ..utils import ASYNC_ATOMIC_STATE, log_query, normalize_db_exception


# SQLite has no native interval type, so :class:`dorm.DurationField`
# stores the value as a number of microseconds in a ``BIGINT`` column.
# Registering a process-wide adapter lets ``DurationField.get_db_prep_value``
# return the raw :class:`datetime.timedelta` (which PostgreSQL binds
# natively as INTERVAL); the SQLite cursor converts it on the way out.
# This is a global side effect — but it only attaches a converter for a
# type the standard library doesn't know about, so it can't override an
# existing user-registered adapter, and the value semantics (microseconds
# round-tripped) match what :meth:`DurationField.from_db_value` expects.
def _adapt_timedelta_to_microseconds(td: datetime.timedelta) -> int:
    return td.days * 86_400 * 10 ** 6 + td.seconds * 10 ** 6 + td.microseconds


sqlite3.register_adapter(datetime.timedelta, _adapt_timedelta_to_microseconds)


# SQLite's PRAGMA syntax doesn't accept bound parameters, so the value has
# to be spliced into SQL. We guard against injection from a misconfigured
# settings.py (or one populated from env vars) by mapping each documented
# journal_mode to a hard-coded SQL literal: ``_validate_journal_mode`` only
# ever returns a key from this table, so the f-string at the call site can
# never contain attacker-controlled bytes — even if a future change to the
# regex below loosens validation. See
# https://sqlite.org/pragma.html#pragma_journal_mode
_JOURNAL_MODE_SQL: dict[str, str] = {
    "DELETE": "PRAGMA journal_mode = DELETE",
    "TRUNCATE": "PRAGMA journal_mode = TRUNCATE",
    "PERSIST": "PRAGMA journal_mode = PERSIST",
    "MEMORY": "PRAGMA journal_mode = MEMORY",
    "WAL": "PRAGMA journal_mode = WAL",
    "OFF": "PRAGMA journal_mode = OFF",
}
_VALID_JOURNAL_MODES = frozenset(_JOURNAL_MODE_SQL)


def _is_single_statement(sql: str) -> bool:
    """Return True if ``sql`` contains exactly one SQL statement.

    Used by :meth:`SQLiteDatabaseWrapper.execute_script` to choose
    between transaction-respecting ``execute()`` and the auto-committing
    ``executescript()``. The check ignores ``;`` characters that appear
    inside single- or double-quoted literals — naive ``;`` counting
    would misclassify e.g. ``INSERT INTO t VALUES ('a;b')``.

    Comments aren't stripped because the migration writer never emits
    them. If a future caller passes commented SQL, the worst case is a
    false negative (use executescript, which still works) — never a
    false positive that would split a single statement.
    """
    stripped = sql.strip()
    if not stripped:
        return True  # nothing to run; treated as trivially single
    in_single = False
    in_double = False
    semis = 0
    last_non_ws = -1
    for i, ch in enumerate(stripped):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == ";" and not in_single and not in_double:
            semis += 1
            last_non_ws = i
            continue
        if not ch.isspace():
            last_non_ws = i
    if semis == 0:
        return True
    if semis == 1 and last_non_ws == stripped.rfind(";"):
        return True  # single statement with trailing semicolon
    return False


def _validate_journal_mode(value: str) -> str:
    """Return *value* uppercased if it's a recognised SQLite journal mode,
    otherwise raise ``ImproperlyConfigured``. The returned string is
    guaranteed to be a key in :data:`_JOURNAL_MODE_SQL`, so callers should
    look up the SQL there rather than building it themselves."""
    if not isinstance(value, str):
        raise ImproperlyConfigured(
            f"DATABASES['default']['OPTIONS']['journal_mode'] must be a string, "
            f"got {type(value).__name__}."
        )
    upper = value.strip().upper()
    if upper not in _VALID_JOURNAL_MODES:
        raise ImproperlyConfigured(
            f"Invalid SQLite journal_mode {value!r}. "
            f"Allowed: {sorted(_VALID_JOURNAL_MODES)}."
        )
    return upper


class SQLiteDatabaseWrapper:
    vendor = "sqlite"

    def __init__(self, settings: dict):
        self.settings = settings
        self.database = settings.get("NAME", ":memory:")
        self._local = threading.local()
        # ``threading.local`` only gives the calling thread access to its
        # own connection, so :meth:`close` running in one thread couldn't
        # release connections opened by sibling threads — they leaked as
        # ``ResourceWarning: unclosed database`` on GC. We mirror every
        # opened connection in a thread-id-keyed dict (guarded by
        # ``_conns_lock``) so :meth:`close` and :meth:`close_all_threads`
        # can release every live one.
        self._conns: dict[int, sqlite3.Connection] = {}
        self._conns_lock = threading.Lock()
        self._autocommit: bool = False

    def _new_connection(self) -> sqlite3.Connection:
        # Validate before we open: an ImproperlyConfigured raised after
        # ``sqlite3.connect`` would leak the just-opened handle to the GC
        # (``ResourceWarning: unclosed database``).
        journal_mode = self.settings.get("OPTIONS", {}).get("journal_mode")
        mode = _validate_journal_mode(journal_mode) if journal_mode else None
        conn = sqlite3.connect(self.database, check_same_thread=False)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            if mode is not None:
                # SQL is selected from a hard-coded mapping, not concatenated
                # from ``mode``, so even a future change that weakens the
                # validator can't reach this execute() with attacker bytes.
                conn.execute(_JOURNAL_MODE_SQL[mode])
            if self._autocommit:
                conn.isolation_level = None
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise
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
                with self._conns_lock:
                    self._conns.pop(threading.get_ident(), None)
                conn = None
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
            with self._conns_lock:
                self._conns[threading.get_ident()] = conn
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
        if depth == 0:
            # Python's sqlite3 module in legacy-transaction-control mode
            # only auto-BEGINs before DML (INSERT/UPDATE/DELETE), NOT
            # before DDL or SELECT. That made atomic() useless for
            # migrations: a ``CREATE TABLE`` ran outside any transaction
            # and survived a subsequent rollback. Emit BEGIN explicitly
            # so DDL participates too.
            #
            # ``conn.in_transaction`` tells us whether sqlite3 has
            # already auto-begun a transaction — if it has, BEGIN would
            # error ("cannot start a transaction within a transaction").
            if not conn.in_transaction:
                conn.execute("BEGIN")
        else:
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
        with log_query("sqlite", sql, params):
            try:
                cursor = conn.execute(self._adapt(sql), params or [])
            except Exception as exc:
                normalize_db_exception(exc)
                raise
        return cursor.fetchall()

    def execute_write(self, sql: str, params=None) -> int:
        conn = self.get_connection()
        with log_query("sqlite", sql, params):
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
        with log_query("sqlite", sql, params):
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
        with log_query("sqlite", sql, params):
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
        """Run a multi-statement SQL script.

        **SQLite gotcha:** ``sqlite3.Connection.executescript()`` issues
        an implicit ``COMMIT`` before the script and another after — so
        calling it inside an ``atomic()`` block silently ends the
        surrounding transaction and breaks the rollback guarantee.

        To keep migrations transactional, we route single-statement SQL
        (the common case for ``CREATE TABLE`` / ``DROP TABLE`` / ``ALTER
        TABLE`` produced by the migration ops) through ``conn.execute()``,
        which DOES participate in the active transaction. Multi-statement
        scripts still go through ``executescript()`` and remain non-
        transactional — there's no SQLite primitive that runs multiple
        statements atomically in one call.
        """
        conn = self.get_connection()
        if _is_single_statement(sql):
            conn.execute(sql)
            if self._atomic_depth == 0 and not self._autocommit:
                conn.commit()
            return
        conn.executescript(sql)
        # executescript() already commits; an explicit commit() here would
        # be redundant. Skip it so we don't generate spurious wal-frames.

    def execute_streaming(self, sql: str, params=None, chunk_size: int = 1000):
        """Yield rows lazily without buffering the whole result set.

        SQLite's default cursor already streams from disk in arraysize
        chunks, so we just expose its iterator. ``chunk_size`` tunes
        ``cursor.arraysize``.

        The cursor is closed in a ``finally`` block so a caller that breaks
        out of the loop early (or hits an exception while iterating) doesn't
        leave a half-consumed result set holding read locks.
        """
        conn = self.get_connection()
        with log_query("sqlite", sql, params):
            try:
                cursor = conn.execute(self._adapt(sql), params or [])
            except Exception as exc:
                normalize_db_exception(exc)
                raise
            cursor.arraysize = chunk_size
        try:
            for row in cursor:
                yield row
        finally:
            try:
                cursor.close()
            except Exception:
                pass

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

    def pool_stats(self) -> dict:
        """SQLite has no pool — returns a minimal dict for API parity with
        the PG wrappers (``open`` reflects whether a thread-local conn
        exists for the calling thread)."""
        conn = getattr(self._local, "conn", None)
        return {"open": conn is not None, "vendor": "sqlite"}

    def close(self):
        # Close every connection opened against this wrapper, regardless of
        # which thread opened it. ``check_same_thread=False`` is set on
        # creation so cross-thread close is safe. The thread-local on the
        # calling thread is also reset; sibling threads that still hold a
        # reference will fall through ``get_connection``'s liveness probe
        # on next use and reopen.
        with self._conns_lock:
            conns = list(self._conns.values())
            self._conns.clear()
        for c in conns:
            try:
                c.close()
            except Exception:
                pass
        if getattr(self._local, "conn", None) is not None:
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

    @staticmethod
    def _force_close_sync(conn) -> None:
        """Tear down an :mod:`aiosqlite` Connection without an event loop.

        Used when the loop a connection was opened on has been closed —
        ``await conn.close()`` would have to be scheduled on a dead loop.
        ``aiosqlite.Connection.stop()`` queues a close-and-stop callable
        on the worker thread (which can run without any event loop and
        does the actual ``sqlite3.Connection.close``) and survives the
        no-running-loop case by setting its internal future to ``None``.
        We join the worker afterwards so the underlying handle is closed
        before the wrapper's reference is dropped — otherwise the GC
        finalises the sqlite3 handle later as
        ``ResourceWarning: unclosed database`` and a few of these were
        enough under ``pytest -n 4`` to keep the interpreter from
        exiting cleanly.

        Best-effort: every step is wrapped because the only thing worse
        than leaking a connection is raising while trying to release one.
        """
        try:
            conn.stop()
        except Exception:
            pass
        worker = getattr(conn, "_thread", None)
        if worker is not None and worker.is_alive():
            try:
                # Five seconds is generous for closing an in-flight sqlite
                # handle; we don't want a stuck worker to block teardown
                # indefinitely.
                worker.join(timeout=5.0)
            except Exception:
                pass

    async def _check_loop(self) -> None:
        """Reset connection and lock if the running event loop has changed."""
        current_loop = asyncio.get_running_loop()
        if self._loop is not current_loop:
            old_conn = self._conn
            old_loop = self._loop
            self._conn = None
            self._loop = current_loop
            self._lock = asyncio.Lock()
            # Best-effort cleanup of the connection from the previous loop.
            # If that loop is still alive, schedule the proper async close on
            # it; otherwise force-close the underlying sqlite3 handle so we
            # don't leak it to the GC as an unraisable ResourceWarning.
            if old_conn is not None:
                if old_loop is not None and not old_loop.is_closed():
                    try:
                        asyncio.run_coroutine_threadsafe(old_conn.close(), old_loop)
                    except RuntimeError:
                        self._force_close_sync(old_conn)
                else:
                    self._force_close_sync(old_conn)

    async def _new_connection(self):
        import aiosqlite

        # Validate up front so an ImproperlyConfigured can't fire after the
        # aiosqlite worker thread has started; the dangling Connection
        # would otherwise leak as ``ResourceWarning: unclosed database``.
        journal_mode = self.settings.get("OPTIONS", {}).get("journal_mode")
        mode = _validate_journal_mode(journal_mode) if journal_mode else None

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
        try:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON")
            if mode is not None:
                await conn.execute(_JOURNAL_MODE_SQL[mode])
        except Exception:
            try:
                await conn.close()
            except Exception:
                pass
            raise
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
            with log_query("sqlite", sql, params):
                try:
                    cursor = await conn.execute(self._adapt(sql), params or [])
                    rows = await cursor.fetchall()
                except Exception as exc:
                    normalize_db_exception(exc)
                    raise
            return list(rows)

    async def execute_write(self, sql: str, params=None) -> int:
        async with self._operation_conn() as conn:
            with log_query("sqlite", sql, params):
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
            with log_query("sqlite", sql, params):
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
            with log_query("sqlite", sql, params):
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
        """Async counterpart of :meth:`SQLiteDatabaseWrapper.execute_script`.

        Same SQLite limitation applies: ``executescript()`` commits before
        and after, so the surrounding ``aatomic()`` block can no longer roll
        back statements that ran earlier in it.

        Uses :meth:`_operation_conn` so that calling this inside an
        ``aatomic()`` block reuses the already-held connection instead of
        deadlocking on ``self._lock`` (the lock is held for the entire
        duration of the atomic block).
        """
        async with self._operation_conn() as conn:
            await conn.executescript(sql)
            # executescript() already commits internally; an extra commit
            # here would just be a no-op round-trip.

    async def execute_streaming(self, sql: str, params=None, chunk_size: int = 1000):
        """Async equivalent of :meth:`SQLiteDatabaseWrapper.execute_streaming`.

        See the sync version's note about closing the cursor on early exit;
        the same guarantee applies here via the ``finally`` block."""
        async with self._operation_conn() as conn:
            with log_query("sqlite", sql, params):
                try:
                    cursor = await conn.execute(self._adapt(sql), params or [])
                except Exception as exc:
                    normalize_db_exception(exc)
                    raise
            cursor.arraysize = chunk_size
            try:
                async for row in cursor:
                    yield row
            finally:
                try:
                    await cursor.close()
                except Exception:
                    pass

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

    def pool_stats(self) -> dict:
        """API-parity shim for the async wrapper. SQLite uses a single
        connection per loop, no pool."""
        return {"open": self._conn is not None, "vendor": "sqlite"}

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def force_close_sync(self) -> None:
        """Release the held aiosqlite connection from a non-async context.

        Called by the global :func:`dorm.db.connection.reset_connections`
        and the atexit hook. Always tears down deterministically rather
        than scheduling an async close on the original loop —
        ``run_coroutine_threadsafe`` accepts the call when the loop is
        not closed, but if the loop isn't actively running the close
        never executes and the underlying sqlite3 handle still leaks at
        GC time as ``ResourceWarning: unclosed database``.
        """
        conn = self._conn
        if conn is None:
            return
        self._conn = None
        self._force_close_sync(conn)
