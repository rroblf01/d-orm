"""PostgreSQL advisory-lock helpers.

PostgreSQL advisory locks are application-level locks that the database
manages but doesn't interpret. They are widely used for:

- Single-leader patterns (only one worker fires the nightly job).
- Singleton initialisation (only one process bootstraps a cache).
- Cross-process mutual exclusion that survives connection pooling.

Two scopes are available on PostgreSQL:

- **Session locks** — held until ``pg_advisory_unlock`` or session end.
  Implemented by :func:`advisory_lock` (blocking) and
  :func:`try_advisory_lock` (non-blocking).
- **Transaction locks** — released automatically when the surrounding
  transaction commits or rolls back. Implemented by
  :func:`advisory_xact_lock` and :func:`try_advisory_xact_lock`.

Non-PG backends raise :class:`NotImplementedError` — keep portability
in mind when adding the helper to shared code paths.

Example::

    from dorm.contrib.advisory import advisory_lock, try_advisory_lock

    # Block until we own the lock; releases on context exit.
    with advisory_lock("nightly-report-runner"):
        run_nightly_report()

    # Skip when contended, run the work elsewhere.
    with try_advisory_lock("nightly-report-runner") as acquired:
        if acquired:
            run_nightly_report()
        else:
            log.info("another worker already running the nightly report")
"""
from __future__ import annotations

import hashlib
import logging
from contextlib import asynccontextmanager, contextmanager
from typing import Any

_log = logging.getLogger("dorm.contrib.advisory")


def _key_to_bigint(key: int | str | tuple[int, int]) -> tuple[Any, ...]:
    """Render *key* as the argument tuple PG's lock functions accept.

    PG advisory locks accept a single ``bigint`` (one-arg form) or a
    pair of ``int4`` values (two-arg form). We support three caller
    shapes:

    - ``int`` — passed straight through as ``bigint``.
    - ``str`` — hashed via blake2b-8 → reduced to a signed 64-bit
      integer. Same string always maps to the same lock, deterministic
      across processes (unlike Python's ``hash()``).
    - ``(int, int)`` tuple — emits the two-arg variant.

    Returns the positional arguments to pass to PG.
    """
    if isinstance(key, str):
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
        signed = int.from_bytes(digest, "big", signed=True)
        return (signed,)
    if isinstance(key, tuple):
        if len(key) != 2:
            raise ValueError(
                "advisory_lock: tuple key must be exactly (int, int); "
                f"got length {len(key)}"
            )
        a, b = key
        if not isinstance(a, int) or not isinstance(b, int):
            raise TypeError("advisory_lock: tuple key elements must be int")
        return (a, b)
    if not isinstance(key, int):
        raise TypeError(
            f"advisory_lock: key must be int / str / (int, int) tuple; "
            f"got {type(key).__name__}"
        )
    return (key,)


def _require_postgres(conn: Any, *, fn: str) -> None:
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            f"{fn}() is PostgreSQL-only — advisory locks have no portable "
            "equivalent on other backends."
        )


def _placeholders(n: int, conn: Any) -> str:
    """Render ``n`` placeholders matching the connection's paramstyle.
    psycopg uses ``%s``; SQLite/libsql use ``?``; the dorm pg backend
    accepts both."""
    return ", ".join(["%s"] * n)


@contextmanager
def advisory_lock(
    key: int | str | tuple[int, int],
    *,
    using: str = "default",
):
    """Block until *key* is acquired, then release on context exit.

    Maps to ``pg_advisory_lock`` /  ``pg_advisory_unlock``. The lock
    is session-scoped — released on context exit or session close.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_postgres(conn, fn="advisory_lock")
    args = _key_to_bigint(key)
    placeholders = _placeholders(len(args), conn)
    conn.execute_write(f"SELECT pg_advisory_lock({placeholders})", list(args))
    try:
        yield
    finally:
        try:
            conn.execute_write(
                f"SELECT pg_advisory_unlock({placeholders})", list(args)
            )
        except Exception:  # pragma: no cover - best effort
            _log.warning("advisory_lock release failed for key=%r", key, exc_info=True)


@contextmanager
def try_advisory_lock(
    key: int | str | tuple[int, int],
    *,
    using: str = "default",
):
    """Non-blocking variant: ``pg_try_advisory_lock``.

    Yields ``True`` when the lock was acquired, ``False`` when it was
    already held by another session. Always releases on exit when
    acquired."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_postgres(conn, fn="try_advisory_lock")
    args = _key_to_bigint(key)
    placeholders = _placeholders(len(args), conn)
    rows = conn.execute(
        f"SELECT pg_try_advisory_lock({placeholders}) AS acquired", list(args)
    )
    acquired = bool(rows and list(rows[0].values())[0])
    try:
        yield acquired
    finally:
        if acquired:
            try:
                conn.execute_write(
                    f"SELECT pg_advisory_unlock({placeholders})", list(args)
                )
            except Exception:  # pragma: no cover
                _log.warning(
                    "try_advisory_lock release failed for key=%r", key, exc_info=True
                )


@contextmanager
def advisory_xact_lock(
    key: int | str | tuple[int, int],
    *,
    using: str = "default",
):
    """Transaction-scoped advisory lock — ``pg_advisory_xact_lock``.

    Must be called inside an :func:`dorm.transaction.atomic` block;
    the lock is released automatically on commit / rollback. Blocking.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_postgres(conn, fn="advisory_xact_lock")
    if getattr(conn, "_atomic_conn", None) is None:
        raise RuntimeError(
            "advisory_xact_lock() must be called inside a "
            "dorm.transaction.atomic() block — the lock is "
            "transaction-scoped."
        )
    args = _key_to_bigint(key)
    placeholders = _placeholders(len(args), conn)
    conn.execute_write(f"SELECT pg_advisory_xact_lock({placeholders})", list(args))
    yield
    # No explicit unlock — PG releases on commit / rollback.


@contextmanager
def try_advisory_xact_lock(
    key: int | str | tuple[int, int],
    *,
    using: str = "default",
):
    """Non-blocking transaction-scoped variant —
    ``pg_try_advisory_xact_lock``. Yields ``True`` / ``False``."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    _require_postgres(conn, fn="try_advisory_xact_lock")
    if getattr(conn, "_atomic_conn", None) is None:
        raise RuntimeError(
            "try_advisory_xact_lock() must be called inside a "
            "dorm.transaction.atomic() block."
        )
    args = _key_to_bigint(key)
    placeholders = _placeholders(len(args), conn)
    rows = conn.execute(
        f"SELECT pg_try_advisory_xact_lock({placeholders}) AS acquired",
        list(args),
    )
    acquired = bool(rows and list(rows[0].values())[0])
    yield acquired


# ── Async variants ──────────────────────────────────────────────────────────


@asynccontextmanager
async def aadvisory_lock(
    key: int | str | tuple[int, int],
    *,
    using: str = "default",
):
    """Async counterpart of :func:`advisory_lock`."""
    from ..db.connection import get_async_connection

    conn = get_async_connection(using)
    _require_postgres(conn, fn="aadvisory_lock")
    args = _key_to_bigint(key)
    placeholders = _placeholders(len(args), conn)
    await conn.execute_write(
        f"SELECT pg_advisory_lock({placeholders})", list(args)
    )
    try:
        yield
    finally:
        try:
            await conn.execute_write(
                f"SELECT pg_advisory_unlock({placeholders})", list(args)
            )
        except Exception:  # pragma: no cover
            _log.warning(
                "aadvisory_lock release failed for key=%r", key, exc_info=True
            )


@asynccontextmanager
async def atry_advisory_lock(
    key: int | str | tuple[int, int],
    *,
    using: str = "default",
):
    """Async counterpart of :func:`try_advisory_lock`."""
    from ..db.connection import get_async_connection

    conn = get_async_connection(using)
    _require_postgres(conn, fn="atry_advisory_lock")
    args = _key_to_bigint(key)
    placeholders = _placeholders(len(args), conn)
    rows = await conn.execute(
        f"SELECT pg_try_advisory_lock({placeholders}) AS acquired",
        list(args),
    )
    acquired = bool(rows and list(rows[0].values())[0])
    try:
        yield acquired
    finally:
        if acquired:
            try:
                await conn.execute_write(
                    f"SELECT pg_advisory_unlock({placeholders})", list(args)
                )
            except Exception:  # pragma: no cover
                _log.warning(
                    "atry_advisory_lock release failed for key=%r", key, exc_info=True
                )


__all__ = [
    "advisory_lock",
    "try_advisory_lock",
    "advisory_xact_lock",
    "try_advisory_xact_lock",
    "aadvisory_lock",
    "atry_advisory_lock",
]
