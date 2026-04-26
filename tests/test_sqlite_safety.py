"""Regressions for the SQLite backend's safety surface:
- PRAGMA journal_mode injection / whitelist
- ``execute_script`` redundant commit (sync)
- ``execute_script`` deadlock inside ``aatomic`` (async)
- ``aiosqlite.Connection._thread`` attribute presence (daemon-thread fix)

These all live behind specific configuration paths or specific call
sequences, so the existing happy-path suite doesn't catch them."""

from __future__ import annotations

import pytest

from dorm.db.backends.sqlite import (
    SQLiteAsyncDatabaseWrapper,
    SQLiteDatabaseWrapper,
    _validate_journal_mode,
)
from dorm.exceptions import ImproperlyConfigured


# ── PRAGMA journal_mode whitelist ─────────────────────────────────────────────


def test_validate_journal_mode_accepts_documented_modes():
    for mode in ["wal", "WAL", "Delete", "TRUNCATE", "PERSIST", "MEMORY", "OFF"]:
        # All return upper-cased; no exception.
        assert _validate_journal_mode(mode) == mode.upper()


def test_validate_journal_mode_rejects_unknown_value():
    """Anything outside the SQLite-documented set must be rejected — this
    is what stops PRAGMA from being a SQL-injection sink."""
    with pytest.raises(ImproperlyConfigured):
        _validate_journal_mode("WAL; DROP TABLE dorm_migrations; --")
    with pytest.raises(ImproperlyConfigured):
        _validate_journal_mode("BOGUS")
    with pytest.raises(ImproperlyConfigured):
        _validate_journal_mode("")


def test_validate_journal_mode_rejects_non_string():
    with pytest.raises(ImproperlyConfigured):
        _validate_journal_mode(123)  # ty: ignore[invalid-argument-type]


def test_sync_wrapper_rejects_invalid_journal_mode_at_connect_time(tmp_path):
    """The whitelist must trip when the wrapper opens its first connection,
    not at some random later query — fail fast, fail loud."""
    wrapper = SQLiteDatabaseWrapper(
        {"NAME": str(tmp_path / "x.db"), "OPTIONS": {"journal_mode": "EVIL; --"}}
    )
    with pytest.raises(ImproperlyConfigured):
        wrapper.get_connection()


def test_sync_wrapper_accepts_valid_journal_mode(tmp_path):
    """Validation should be transparent for the documented values."""
    wrapper = SQLiteDatabaseWrapper(
        {"NAME": str(tmp_path / "y.db"), "OPTIONS": {"journal_mode": "wal"}}
    )
    conn = wrapper.get_connection()
    row = conn.execute("PRAGMA journal_mode").fetchone()
    assert row[0].upper() == "WAL"


@pytest.mark.asyncio
async def test_async_wrapper_rejects_invalid_journal_mode(tmp_path):
    wrapper = SQLiteAsyncDatabaseWrapper(
        {"NAME": str(tmp_path / "a.db"), "OPTIONS": {"journal_mode": "DROP TABLE x"}}
    )
    with pytest.raises(ImproperlyConfigured):
        await wrapper._new_connection()


@pytest.mark.asyncio
async def test_async_wrapper_accepts_valid_journal_mode(tmp_path):
    wrapper = SQLiteAsyncDatabaseWrapper(
        {"NAME": str(tmp_path / "b.db"), "OPTIONS": {"journal_mode": "WAL"}}
    )
    conn = await wrapper._new_connection()
    cur = await conn.execute("PRAGMA journal_mode")
    row = await cur.fetchone()
    assert row[0].upper() == "WAL"
    await conn.close()


# ── execute_script redundancy + atomic interaction ───────────────────────────


def _is_sqlite() -> bool:
    """The fixes target SQLite specifically; PG isn't affected."""
    from dorm.db.connection import get_connection
    return getattr(get_connection(), "vendor", "sqlite") == "sqlite"


def test_sync_execute_script_creates_table():
    """Smoke: execute_script still works for the migration code path."""
    if not _is_sqlite():
        pytest.skip("SQLite-only behaviour")
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "scratch_es"')
    conn.execute_script(
        'CREATE TABLE "scratch_es" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
    )
    try:
        assert conn.table_exists("scratch_es")
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "scratch_es"')


def test_sync_execute_script_does_not_double_commit():
    """The redundant ``conn.commit()`` after ``executescript()`` is gone.

    We can't observe that directly, but we can verify the semantics didn't
    change: a script run outside any atomic block must still persist."""
    if not _is_sqlite():
        pytest.skip("SQLite-only behaviour")
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "scratch_dbl"')
    conn.execute_script(
        'CREATE TABLE "scratch_dbl" (id INTEGER PRIMARY KEY); '
        'INSERT INTO "scratch_dbl" VALUES (1); '
        'INSERT INTO "scratch_dbl" VALUES (2);'
    )
    try:
        rows = conn.execute('SELECT id FROM "scratch_dbl" ORDER BY id')
        assert [r[0] for r in rows] == [1, 2]
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "scratch_dbl"')


@pytest.mark.asyncio
async def test_async_execute_script_does_not_deadlock_inside_aatomic():
    """Pre-fix bug: aatomic acquired ``self._lock`` for the whole block,
    and ``execute_script`` tried to acquire it again with ``async with
    self._lock:`` — second acquire blocks forever because asyncio.Lock
    isn't re-entrant.

    With the fix, ``execute_script`` goes through ``_operation_conn``,
    which detects we're inside aatomic and reuses the already-held
    connection without re-locking."""
    if not _is_sqlite():
        pytest.skip("SQLite-only deadlock; PG holds a per-call connection")

    from dorm.db.connection import get_async_connection

    conn = get_async_connection()

    # Without an explicit timeout, a regression here would hang the test
    # session forever. wait_for forces it to fail fast.
    import asyncio

    async def _do() -> None:
        async with conn.aatomic():
            await conn.execute_script('DROP TABLE IF EXISTS "deadlock_canary"')
            await conn.execute_script(
                'CREATE TABLE "deadlock_canary" (id INTEGER PRIMARY KEY)'
            )

    await asyncio.wait_for(_do(), timeout=5.0)

    # Cleanup outside aatomic
    await conn.execute_script('DROP TABLE IF EXISTS "deadlock_canary"')


# ── aiosqlite._thread attribute (daemon-thread regression) ───────────────────


@pytest.mark.asyncio
async def test_aiosqlite_connection_exposes_thread_attribute():
    """The async wrapper relies on ``aiosqlite.Connection._thread`` to
    daemonize the worker thread before connect, so the interpreter can
    exit even if the user forgets to ``await close()``. If a future
    aiosqlite version renames or removes this attribute, this test
    catches it loudly instead of silently leaking non-daemon threads."""
    import aiosqlite

    # Build the connection coroutine but don't await it — the wrapper
    # accesses ``_thread`` on the pending task before awaiting.
    pending = aiosqlite.connect(":memory:")
    try:
        assert hasattr(pending, "_thread"), (
            "aiosqlite.Connection no longer exposes '_thread'. "
            "The daemon-thread fix in dorm.db.backends.sqlite is now "
            "a no-op: bump or unpin aiosqlite only after replacing it."
        )
        # Sanity: it's a Thread (not None) before connect kicks it.
        import threading
        assert isinstance(pending._thread, threading.Thread)
    finally:
        conn = await pending
        await conn.close()


# ── docs sanity: ImproperlyConfigured chain ───────────────────────────────────


def test_journal_mode_error_mentions_setting_path():
    """When the error trips, the message should help the user find the
    setting that was wrong — not just say 'invalid value'."""
    wrapper = SQLiteDatabaseWrapper(
        {"NAME": ":memory:", "OPTIONS": {"journal_mode": 123}}
    )
    with pytest.raises(ImproperlyConfigured) as exc:
        wrapper.get_connection()
    msg = str(exc.value)
    # The validator mentions either the setting key or the journal_mode name
    # so a beginner can grep their config.
    assert "journal_mode" in msg or "string" in msg
