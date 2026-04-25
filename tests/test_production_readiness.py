"""Tests for production-readiness features:
- SQL logging (DEBUG + slow-query WARNING)
- Migration advisory lock
- Identifier validation at model attach time
"""

from __future__ import annotations

import logging

import pytest


# ── 1. SQL logging ────────────────────────────────────────────────────────────


def test_query_emits_debug_log(caplog):
    """Each query should emit a DEBUG record on its vendor's logger."""
    from dorm.db.connection import get_connection
    from tests.models import Author

    vendor = get_connection().vendor
    logger_name = f"dorm.db.backends.{vendor}"

    with caplog.at_level(logging.DEBUG, logger=logger_name):
        Author.objects.filter(name="__nope__").count()
    msgs = [r.message for r in caplog.records if r.name == logger_name]
    sql_msgs = [m for m in msgs if "SELECT" in m and "ms" in m]
    assert sql_msgs, f"expected at least one SELECT debug record; got: {msgs!r}"


def test_slow_query_warning_threshold(caplog, monkeypatch):
    """A query slower than DORM_SLOW_QUERY_MS should emit WARNING."""
    from tests.models import Author

    # Force every query to be "slow" by setting threshold to 0ms.
    monkeypatch.setenv("DORM_SLOW_QUERY_MS", "0")
    with caplog.at_level(logging.WARNING, logger="dorm.db"):
        Author.objects.filter(name="__nope__").count()

    warnings = [r for r in caplog.records if "slow query" in r.message]
    assert warnings, "expected slow-query warning at 0ms threshold"


def test_slow_query_silent_when_under_threshold(caplog, monkeypatch):
    from tests.models import Author

    monkeypatch.setenv("DORM_SLOW_QUERY_MS", "60000")  # 1 minute
    with caplog.at_level(logging.WARNING, logger="dorm.db"):
        Author.objects.filter(name="__nope__").count()

    warnings = [r for r in caplog.records if "slow query" in r.message]
    assert not warnings, f"unexpected slow-query warnings: {warnings!r}"


def test_logger_name_is_per_vendor():
    """Loggers are namespaced so users can filter by backend."""
    sqlite_log = logging.getLogger("dorm.db.backends.sqlite")
    pg_log = logging.getLogger("dorm.db.backends.postgresql")
    assert sqlite_log.name != pg_log.name
    # Both inherit from the dorm.db root logger so users can configure once.
    assert sqlite_log.name.startswith("dorm.db.")
    assert pg_log.name.startswith("dorm.db.")


# ── 2. Migration advisory lock ────────────────────────────────────────────────


class _FakePGConn:
    """Mock PG connection. The advisory-lock helper now wraps everything in
    ``connection.atomic()`` so that lock and unlock land on the same pool
    connection — the mock therefore needs a no-op atomic context manager."""

    vendor = "postgresql"

    def __init__(self):
        self.calls: list = []

    def execute(self, sql, params=None):
        self.calls.append(sql)
        return []

    from contextlib import contextmanager

    @contextmanager
    def atomic(self):  # type: ignore[misc]
        yield


def test_migration_lock_is_acquired_on_postgres():
    """On PG, _migration_lock issues pg_advisory_lock + pg_advisory_unlock."""
    from dorm.migrations.executor import _migration_lock, _DORM_MIGRATION_LOCK_ID

    fake = _FakePGConn()
    with _migration_lock(fake):
        pass

    assert any(f"pg_advisory_lock({_DORM_MIGRATION_LOCK_ID})" in s for s in fake.calls), \
        f"expected pg_advisory_lock call; got: {fake.calls}"
    assert any(f"pg_advisory_unlock({_DORM_MIGRATION_LOCK_ID})" in s for s in fake.calls), \
        f"expected pg_advisory_unlock call; got: {fake.calls}"


def test_migration_lock_releases_on_exception():
    """The unlock must run even if the migration body raises."""
    from dorm.migrations.executor import _migration_lock

    fake = _FakePGConn()
    with pytest.raises(RuntimeError):
        with _migration_lock(fake):
            raise RuntimeError("kaboom")

    unlock_calls = [s for s in fake.calls if "pg_advisory_unlock" in s]
    assert len(unlock_calls) == 1


def test_migration_lock_is_noop_on_sqlite():
    """SQLite serializes writers at the file-lock level; we don't issue
    explicit advisory-lock SQL."""
    from dorm.migrations.executor import _migration_lock

    calls: list = []

    class _FakeConn:
        vendor = "sqlite"

        def execute(self, sql, params=None):
            calls.append(sql)
            return []

    fake = _FakeConn()
    with _migration_lock(fake):
        pass

    assert calls == [], f"expected no SQL on SQLite, got: {calls}"


def test_migrate_uses_lock(monkeypatch):
    """High-level migrate() goes through _migration_lock."""
    import dorm.migrations.executor as exe_mod

    enter_count = 0

    class _CountingLock:
        def __init__(self, conn): pass
        def __enter__(self_):
            nonlocal enter_count
            enter_count += 1
        def __exit__(self_, *a): pass

    monkeypatch.setattr(exe_mod, "_migration_lock", _CountingLock)

    from dorm.db.connection import get_connection
    conn = get_connection()
    executor = exe_mod.MigrationExecutor(conn, verbosity=0)

    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        mig_dir = Path(d) / "migs"
        mig_dir.mkdir()
        (mig_dir / "__init__.py").touch()
        executor.migrate("nonexistent_app_for_lock_test", mig_dir)

    assert enter_count == 1


# ── 3. Identifier validation ──────────────────────────────────────────────────


def test_validate_identifier_accepts_safe_names():
    from dorm.conf import _validate_identifier

    for ok in ["users", "users_table", "_private", "T1", "abc_123"]:
        assert _validate_identifier(ok) == ok


def test_validate_identifier_rejects_unsafe():
    from dorm.conf import _validate_identifier
    from dorm.exceptions import ImproperlyConfigured

    bad_names = [
        "users; DROP TABLE x",      # SQL injection attempt
        'users"',                    # closing quote
        "users--",                   # comment
        "1users",                    # leading digit
        "user table",                # space
        "",                          # empty
        "x" * 64,                    # too long
        "user-name",                 # hyphen
    ]
    for bad in bad_names:
        with pytest.raises(ImproperlyConfigured):
            _validate_identifier(bad)


def test_validate_identifier_rejects_non_string():
    from dorm.conf import _validate_identifier
    from dorm.exceptions import ImproperlyConfigured

    for bad in [None, 123, [], object()]:
        with pytest.raises(ImproperlyConfigured):
            _validate_identifier(bad)  # type: ignore


def test_model_with_unsafe_db_table_raises():
    """Defining a Model with Meta.db_table containing SQL-special chars
    must fail at class-creation time, not at first query."""
    import dorm
    from dorm.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="db_table"):
        class _Bad(dorm.Model):  # noqa: PLR0902
            name = dorm.CharField(max_length=10)

            class Meta:
                db_table = 'bad"; DROP TABLE x; --'
                app_label = "tests"


def test_model_with_unsafe_db_column_raises():
    import dorm
    from dorm.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="db_column"):
        class _Bad(dorm.Model):
            name = dorm.CharField(max_length=10, db_column="bad name")

            class Meta:
                db_table = "bad_col_test"
                app_label = "tests"


def test_fk_with_unsafe_related_name_raises():
    import dorm
    from dorm.exceptions import ImproperlyConfigured
    from tests.models import Author

    with pytest.raises(ImproperlyConfigured, match="related_name"):
        class _BadBook(dorm.Model):
            title = dorm.CharField(max_length=10)
            author = dorm.ForeignKey(
                Author, on_delete=dorm.CASCADE, related_name="bad-name"
            )

            class Meta:
                db_table = "bad_rel_test"
                app_label = "tests"


def test_m2m_with_unsafe_through_raises():
    import dorm
    from dorm.exceptions import ImproperlyConfigured
    from tests.models import Tag

    with pytest.raises(ImproperlyConfigured, match="through"):
        class _BadArticle(dorm.Model):
            title = dorm.CharField(max_length=10)
            tags = dorm.ManyToManyField(Tag, through='bad"; DROP')

            class Meta:
                db_table = "bad_m2m_test"
                app_label = "tests"
