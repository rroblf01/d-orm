from __future__ import annotations

import contextvars
import logging
import os
import re
import time
from contextlib import contextmanager

_HINT = (
    "It looks like you forgot to create or apply your migrations.\n\n"
    "  Run the following commands:\n"
    "    dorm makemigrations\n"
    "    dorm migrate\n\n"
    "  Or, if you use a custom settings module:\n"
    "    dorm makemigrations --settings=<your_settings_module>\n"
    "    dorm migrate        --settings=<your_settings_module>\n"
)

# ContextVar shared by all async backends.
# Value: (wrapper_instance, connection, nesting_depth) or None.
# Each backend checks `state[0] is self` so multiple databases don't interfere.
ASYNC_ATOMIC_STATE: contextvars.ContextVar = contextvars.ContextVar(
    "dorm_async_atomic_state", default=None
)

# ── Query logging ─────────────────────────────────────────────────────────────
# Enable with `logging.getLogger("dorm.db").setLevel(logging.DEBUG)`.
# Slow-query threshold (ms) controlled by env var DORM_SLOW_QUERY_MS (default 500).

_slow_log = logging.getLogger("dorm.db")


def _slow_query_ms() -> float:
    """Read the slow-query threshold dynamically so tests / runtime tweaks work."""
    try:
        return float(os.environ.get("DORM_SLOW_QUERY_MS", "500"))
    except (TypeError, ValueError):
        return 500.0


@contextmanager
def log_query(vendor: str, sql: str, params=None):
    """Time a SQL statement, emitting DEBUG for every query and WARNING when
    the elapsed time exceeds DORM_SLOW_QUERY_MS. The logger name is
    ``dorm.db.backends.<vendor>`` so users can filter per backend."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        log = logging.getLogger(f"dorm.db.backends.{vendor}")
        if log.isEnabledFor(logging.DEBUG):
            log.debug("(%.2fms) %s; params=%r", elapsed_ms, sql, params)
        threshold = _slow_query_ms()
        if elapsed_ms >= threshold:
            log.warning(
                "slow query (%.2fms ≥ %.0fms): %s", elapsed_ms, threshold, sql
            )


def raise_migration_hint(exc: Exception) -> None:
    """Re-raise a missing-table error with a friendly hint."""
    from dorm.exceptions import OperationalError

    msg = str(exc)
    match = re.search(r"no such table: (\S+)", msg, re.IGNORECASE) or re.search(
        r'relation "([^"]+)" does not exist', msg, re.IGNORECASE
    )
    if match:
        raise OperationalError(
            f'Table "{match.group(1)}" does not exist.\n\n{_HINT}'
        ) from exc


def normalize_db_exception(exc: Exception) -> None:
    """Convert backend exceptions to dorm exceptions, then check migration hint."""
    import sqlite3
    from dorm.exceptions import IntegrityError, OperationalError, ProgrammingError

    # ── SQLite ────────────────────────────────────────────────────────────────
    if isinstance(exc, sqlite3.IntegrityError):
        raise IntegrityError(str(exc)) from exc
    if isinstance(exc, sqlite3.OperationalError):
        raise_migration_hint(exc)
        raise OperationalError(str(exc)) from exc
    if isinstance(exc, sqlite3.ProgrammingError):
        raise ProgrammingError(str(exc)) from exc
    if isinstance(exc, sqlite3.DatabaseError):
        raise_migration_hint(exc)
        raise ProgrammingError(str(exc)) from exc

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    try:
        import psycopg.errors as pg_errors
        import psycopg as psycopg_mod

        if isinstance(exc, pg_errors.IntegrityError):
            raise IntegrityError(str(exc)) from exc
        if isinstance(exc, (pg_errors.SyntaxError, pg_errors.ProgrammingError)):
            raise ProgrammingError(str(exc)) from exc
        if isinstance(exc, psycopg_mod.OperationalError):
            raise_migration_hint(exc)
            raise OperationalError(str(exc)) from exc
        if isinstance(exc, psycopg_mod.DatabaseError):
            raise_migration_hint(exc)
            raise ProgrammingError(str(exc)) from exc
    except ImportError:
        pass

    raise_migration_hint(exc)
