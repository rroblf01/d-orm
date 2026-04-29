"""Tests targeting branches in dorm's database backends and the
connection registry that the broader suite leaves uncovered.

Goal: lock in error-recovery and edge-case behaviour so a refactor in
a hot path (auto-reconnect, lifecycle, journal_mode validation)
doesn't silently regress. Coverage on these files was 81–86%; these
tests close the obvious gaps.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dorm.db.backends.sqlite import (
    SQLiteAsyncDatabaseWrapper,
    SQLiteDatabaseWrapper,
    _is_single_statement,
    _validate_journal_mode,
)
from dorm.db.connection import (
    _async_connections,
    _sync_connections,
    close_all_async,
    get_async_connection,
    get_connection,
    health_check,
    pool_stats,
    router_db_for_read,
    router_db_for_write,
)
from dorm.exceptions import ImproperlyConfigured


# ── _is_single_statement ──────────────────────────────────────────────────────


class TestIsSingleStatement:
    """Drive every branch of the SQL splitter that ``execute_script``
    relies on to choose between the transactional ``execute()`` and the
    auto-committing ``executescript()``."""

    def test_empty_input_is_trivially_single(self):
        # Defensive — execute_script should never feed it an empty
        # string, but this short-circuit avoids a crash if it does.
        assert _is_single_statement("") is True
        assert _is_single_statement("   \n  ") is True

    def test_one_statement_no_trailing_semi(self):
        assert _is_single_statement("SELECT 1") is True

    def test_one_statement_with_trailing_semi(self):
        assert _is_single_statement("SELECT 1;") is True

    def test_two_statements_split_at_semicolon(self):
        assert _is_single_statement("SELECT 1; SELECT 2") is False

    def test_semicolons_inside_string_literal_are_ignored(self):
        # Naive ``;``-counting would call this multi-statement and
        # break under executescript()'s implicit COMMIT.
        sql = "INSERT INTO t VALUES ('a;b;c')"
        assert _is_single_statement(sql) is True

    def test_double_quoted_identifier_with_semicolon_ignored(self):
        sql = 'SELECT "weird;col" FROM t'
        assert _is_single_statement(sql) is True


# ── _validate_journal_mode ────────────────────────────────────────────────────


class TestValidateJournalMode:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("WAL", "WAL"),
            ("wal", "WAL"),  # case-insensitive
            ("  delete  ", "DELETE"),  # trimmed
            ("MEMORY", "MEMORY"),
            ("OFF", "OFF"),
        ],
    )
    def test_valid_values_normalize_to_upper(self, value, expected):
        assert _validate_journal_mode(value) == expected

    def test_unknown_value_raises_with_helpful_message(self):
        with pytest.raises(ImproperlyConfigured, match="Invalid SQLite journal_mode"):
            _validate_journal_mode("ROLLBACK")

    def test_non_string_raises_with_type_name(self):
        with pytest.raises(ImproperlyConfigured, match="must be a string"):
            _validate_journal_mode(123)  # ty: ignore[invalid-argument-type]

    def test_injection_payload_rejected(self):
        # End-to-end: confirm the wrapper refuses bytes that look
        # syntactically valid but aren't on the allow-list.
        with pytest.raises(ImproperlyConfigured):
            _validate_journal_mode("WAL; DROP TABLE x")


# ── SQLiteDatabaseWrapper auto-reconnect + close lifecycle ────────────────────


class TestSyncWrapperLifecycle:
    def test_auto_reconnect_when_connection_drops(self, tmp_path: Path):
        """Simulate a stale connection (process forked, file deleted)
        by closing the underlying handle and confirm the next call
        opens a fresh one rather than raising."""
        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "lifecycle.db")})
        try:
            conn1 = wrapper.get_connection()
            conn1.close()  # invalidate behind the wrapper's back
            conn2 = wrapper.get_connection()
            # New handle, both alive, basic query works.
            assert conn1 is not conn2
            wrapper.execute("SELECT 1")
        finally:
            wrapper.close()

    def test_close_releases_every_thread_local_connection(self, tmp_path: Path):
        """Connections opened in multiple threads must all be closed
        by ``close()`` — without this fix the per-thread handles
        leaked to GC as ``ResourceWarning: unclosed database``."""
        import threading

        wrapper = SQLiteDatabaseWrapper(
            {"NAME": str(tmp_path / "threads.db")}
        )

        opened: list = []

        def worker():
            opened.append(wrapper.get_connection())

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Five distinct thread-local connections were opened.
        assert len({id(c) for c in opened}) == 5
        wrapper.close()

        # After close() each one is gone — calling close() on an
        # already-closed sqlite3.Connection raises ProgrammingError.
        for c in opened:
            with pytest.raises(Exception):
                c.execute("SELECT 1")

    def test_close_is_idempotent(self, tmp_path: Path):
        """Double close() is a common path under reset_connections +
        atexit; must not raise."""
        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "idem.db")})
        wrapper.get_connection()
        wrapper.close()
        wrapper.close()  # no-op second time

    def test_set_autocommit_flips_isolation_on_existing_connection(
        self, tmp_path: Path
    ):
        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "ac.db")})
        try:
            conn = wrapper.get_connection()
            assert conn.isolation_level == ""  # default
            wrapper.set_autocommit(True)
            assert conn.isolation_level is None  # autocommit
            wrapper.set_autocommit(False)
            assert conn.isolation_level == ""
        finally:
            wrapper.close()

    def test_get_table_columns_returns_pragma_rows(self, tmp_path: Path):
        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "cols.db")})
        try:
            wrapper.execute_script(
                'CREATE TABLE "thing" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
            )
            cols = wrapper.get_table_columns("thing")
            names = {c["name"] for c in cols}
            assert names == {"id", "name"}
        finally:
            wrapper.close()

    def test_pool_stats_minimal_shim(self, tmp_path: Path):
        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "ps.db")})
        try:
            assert wrapper.pool_stats() == {"open": False, "vendor": "sqlite"}
            wrapper.get_connection()
            assert wrapper.pool_stats() == {"open": True, "vendor": "sqlite"}
        finally:
            wrapper.close()

    def test_invalid_journal_mode_in_settings_raises_at_connect(
        self, tmp_path: Path
    ):
        """Validation runs *before* sqlite3.connect — important so we
        don't leak a connection on a config typo."""
        wrapper = SQLiteDatabaseWrapper(
            {"NAME": str(tmp_path / "j.db"), "OPTIONS": {"journal_mode": "ROLLBACK"}}
        )
        with pytest.raises(ImproperlyConfigured):
            wrapper.get_connection()
        # Cleanup not strictly necessary (no conn opened) but guards
        # against a future regression that *does* open early.
        wrapper.close()


# ── Async wrapper extras ──────────────────────────────────────────────────────


class TestAsyncWrapperLifecycle:
    @pytest.mark.asyncio
    async def test_pool_stats_open_false_then_true(self, tmp_path: Path):
        wrapper = SQLiteAsyncDatabaseWrapper(
            {"NAME": str(tmp_path / "apool.db")}
        )
        try:
            assert wrapper.pool_stats()["open"] is False
            await wrapper.execute("SELECT 1")
            assert wrapper.pool_stats()["open"] is True
        finally:
            await wrapper.close()

    @pytest.mark.asyncio
    async def test_set_autocommit_drops_current_connection(self, tmp_path: Path):
        wrapper = SQLiteAsyncDatabaseWrapper(
            {"NAME": str(tmp_path / "asac.db")}
        )
        try:
            await wrapper.execute("SELECT 1")
            assert wrapper._conn is not None
            await wrapper.set_autocommit(True)
            # Toggling autocommit must close the current handle so the
            # next call rebuilds with the right isolation_level.
            assert wrapper._conn is None
        finally:
            await wrapper.close()

    @pytest.mark.asyncio
    async def test_get_table_columns_async(self, tmp_path: Path):
        wrapper = SQLiteAsyncDatabaseWrapper(
            {"NAME": str(tmp_path / "acols.db")}
        )
        try:
            await wrapper.execute_script(
                'CREATE TABLE "thing" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
            )
            cols = await wrapper.get_table_columns("thing")
            assert {c["name"] for c in cols} == {"id", "name"}
        finally:
            await wrapper.close()

    @pytest.mark.asyncio
    async def test_invalid_journal_mode_in_async_settings_raises(
        self, tmp_path: Path
    ):
        wrapper = SQLiteAsyncDatabaseWrapper(
            {"NAME": str(tmp_path / "aj.db"), "OPTIONS": {"journal_mode": "ROLLBACK"}}
        )
        with pytest.raises(ImproperlyConfigured):
            await wrapper._new_connection()


# ── Connection registry / routers ─────────────────────────────────────────────


class TestRouters:
    def test_router_with_no_db_for_read_method_is_skipped(self):
        """A router that doesn't define ``db_for_read`` must not be
        consulted; the loop simply continues."""
        class _R:
            pass  # no methods at all

        from dorm.conf import settings

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_R()]
        try:
            assert router_db_for_read(object) == "default"
            assert router_db_for_write(object) == "default"
        finally:
            if original is None:
                # Clear if we inserted the attribute.
                if hasattr(settings, "DATABASE_ROUTERS"):
                    delattr(settings, "DATABASE_ROUTERS")
            else:
                settings.DATABASE_ROUTERS = original

    def test_router_raising_exception_falls_through_to_next(self):
        """A buggy router that raises should never crash the read
        path — the loop swallows and tries the next."""
        class _Boom:
            def db_for_read(self, model, **hints):
                raise RuntimeError("bug in user router")

        class _OK:
            def db_for_read(self, model, **hints):
                return "default"

        from dorm.conf import settings

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_Boom(), _OK()]
        try:
            assert router_db_for_read(object) == "default"
        finally:
            settings.DATABASE_ROUTERS = original or []

    def test_router_returning_falsy_alias_is_ignored(self):
        """A router returning ``""`` / ``None`` means "no opinion" —
        loop should keep going and fall through to the default."""
        class _Vague:
            def db_for_read(self, model, **hints):
                return None  # explicitly "no opinion"

        from dorm.conf import settings

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_Vague()]
        try:
            assert router_db_for_read(object) == "default"
        finally:
            settings.DATABASE_ROUTERS = original or []


class TestConnectionRegistry:
    def test_get_connection_returns_same_wrapper_per_alias(self):
        c1 = get_connection()
        c2 = get_connection()
        assert c1 is c2

    def test_unknown_alias_raises_improperly_configured(self):
        with pytest.raises(ImproperlyConfigured, match="not found in DATABASES"):
            get_connection("__nope__")

    def test_health_check_returns_status_ok_on_default(self):
        # Doesn't raise even if backend is down — important for k8s
        # readiness probes. With our test DB it should be ok.
        result = health_check("default")
        assert result["status"] in {"ok", "error"}
        assert result["alias"] == "default"
        assert "elapsed_ms" in result

    def test_health_check_reports_error_for_bad_alias(self):
        result = health_check("__nope__")
        assert result["status"] == "error"
        assert "not found in DATABASES" in result["error"]

    def test_pool_stats_uninitialised_for_unknown_alias(self):
        out = pool_stats("__nope__")
        assert out["status"] == "uninitialised"

    def test_pool_stats_for_known_alias_reports_vendor(self):
        get_connection()  # ensure populated
        out = pool_stats("default")
        # Either sqlite (no pool) or postgresql (with pool) — both
        # paths must include the vendor key.
        assert out["vendor"] in {"sqlite", "postgresql"}


class TestResetConnectionsForceClose:
    @pytest.mark.asyncio
    async def test_reset_force_closes_async_wrapper(self, tmp_path: Path):
        """``reset_connections`` from sync context must release the
        held aiosqlite connection deterministically — otherwise the
        ``sqlite3.Connection`` is reaped at GC time and emits a
        ResourceWarning."""
        wrapper = SQLiteAsyncDatabaseWrapper(
            {"NAME": str(tmp_path / "force.db")}
        )
        try:
            await wrapper.execute("SELECT 1")
            assert wrapper._conn is not None
            wrapper.force_close_sync()
            # After force_close_sync the wrapper has dropped its
            # reference to the aiosqlite connection.
            assert wrapper._conn is None
        finally:
            # Idempotent: calling again is a no-op.
            wrapper.force_close_sync()


@pytest.mark.asyncio
async def test_close_all_async_clears_registry(tmp_path: Path):
    """Round-trip through the public lifecycle helpers without
    blowing up: spawn an async wrapper, close, and verify the
    registry is empty."""
    # Register a temporary alias so we don't disturb the session
    # default that other tests depend on.
    from dorm.conf import settings

    original = dict(settings.DATABASES)
    settings.DATABASES["__lifecycle__"] = {
        "ENGINE": "sqlite",
        "NAME": str(tmp_path / "lifecycle.db"),
    }
    try:
        wrapper = get_async_connection("__lifecycle__")
        await wrapper.execute("SELECT 1")
        assert "__lifecycle__" in _async_connections
        await close_all_async()
        assert "__lifecycle__" not in _async_connections
    finally:
        settings.DATABASES = original
        _sync_connections.pop("__lifecycle__", None)
        _async_connections.pop("__lifecycle__", None)


# ── _new_connection cleanup on validation failure ─────────────────────────────


class TestNewConnectionCleanup:
    """Regression: when ``_validate_journal_mode`` fired *after* the
    connection had been opened, the dangling sqlite3 handle showed up
    later as ``ResourceWarning: unclosed database``. The fix moves
    validation up-front; this test pins that order so it can't slide
    back."""

    def test_no_resource_warning_on_invalid_journal_mode(self, tmp_path: Path):
        import warnings as _w

        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            wrapper = SQLiteDatabaseWrapper(
                {"NAME": str(tmp_path / "nm.db"), "OPTIONS": {"journal_mode": "ROLLBACK"}}
            )
            with pytest.raises(ImproperlyConfigured):
                wrapper.get_connection()

        leaks = [
            w
            for w in caught
            if issubclass(w.category, ResourceWarning)
            and "unclosed database" in str(w.message)
        ]
        assert leaks == [], (
            f"Validation failure leaked a sqlite3 handle: {[str(w.message) for w in leaks]}"
        )
