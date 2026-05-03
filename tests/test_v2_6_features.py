"""Comprehensive tests for v2.6.0 additions.

Covers:

- Items 1: ``settings.RETRY_ATTEMPTS`` / ``settings.RETRY_BACKOFF``.
- Item 2: ``dorm.contrib.querycount.query_count_guard``.
- Item 4: ``dorm migrate --plan`` (alias for ``--dry-run``).
- Item 5: ``dorm.test.assertNumQueries``.
- Item 6: Sticky read-after-write window in the DB router.
- Item 7: ``dorm.migrations.lint`` rules + ``dorm lint-migrations`` CLI.
- Item 8: ``dorm.contrib.querylog`` collector.
- Item 9: ``dorm.cache.locmem.LocMemCache``.
- Item 10: ``Manager.cache_get`` / ``acache_get``.
"""

from __future__ import annotations

import logging

import pytest

import dorm
from dorm.db.utils import (
    _invalidate_retry_cache,
    _resolve_retry_attempts,
    _resolve_retry_backoff,
    _retry_attempts,
    _retry_backoff,
)


# ──────────────────────────────────────────────────────────────────────────────
# Item 1: RETRY_* settings
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _restore_retry_settings():
    from dorm.conf import settings

    explicit_a = "RETRY_ATTEMPTS" in settings._explicit_settings
    explicit_b = "RETRY_BACKOFF" in settings._explicit_settings
    prev_a = getattr(settings, "RETRY_ATTEMPTS", 3)
    prev_b = getattr(settings, "RETRY_BACKOFF", 0.1)
    _invalidate_retry_cache()
    yield
    if not explicit_a:
        settings._explicit_settings.discard("RETRY_ATTEMPTS")
    if not explicit_b:
        settings._explicit_settings.discard("RETRY_BACKOFF")
    settings.RETRY_ATTEMPTS = prev_a
    settings.RETRY_BACKOFF = prev_b
    _invalidate_retry_cache()


def test_retry_default(_restore_retry_settings):
    assert _retry_attempts() == 3
    assert _retry_backoff() == 0.1


def test_retry_env_var(monkeypatch, _restore_retry_settings):
    monkeypatch.setenv("DORM_RETRY_ATTEMPTS", "5")
    monkeypatch.setenv("DORM_RETRY_BACKOFF", "0.25")
    _invalidate_retry_cache()
    assert _retry_attempts() == 5
    assert _retry_backoff() == 0.25


def test_retry_settings_override_env(monkeypatch, _restore_retry_settings):
    monkeypatch.setenv("DORM_RETRY_ATTEMPTS", "5")
    dorm.configure(RETRY_ATTEMPTS=7, RETRY_BACKOFF=0.5)
    assert _retry_attempts() == 7
    assert _retry_backoff() == 0.5


def test_retry_resolve_returns_cacheable_flag(_restore_retry_settings):
    dorm.configure(RETRY_ATTEMPTS=4)
    val, cacheable = _resolve_retry_attempts()
    assert val == 4
    assert cacheable is True


def test_retry_env_branch_not_cacheable(monkeypatch, _restore_retry_settings):
    monkeypatch.setenv("DORM_RETRY_BACKOFF", "0.7")
    _invalidate_retry_cache()
    val, cacheable = _resolve_retry_backoff()
    assert val == 0.7
    assert cacheable is False


# ──────────────────────────────────────────────────────────────────────────────
# Item 2: query_count_guard
# ──────────────────────────────────────────────────────────────────────────────


def test_query_count_guard_counts_queries():
    from dorm.contrib.querycount import query_count_guard
    from tests.models import Author

    with query_count_guard() as ctx:
        Author.objects.filter(name="x").count()
        Author.objects.filter(name="y").count()
    assert ctx.count >= 2


def test_query_count_guard_warns_above_threshold(caplog):
    from dorm.contrib.querycount import query_count_guard
    from tests.models import Author

    with caplog.at_level(logging.WARNING, logger="dorm.querycount"):
        with query_count_guard(warn_above=0, label="test"):
            Author.objects.filter(name="x").count()
    warns = [r for r in caplog.records if "query count exceeded" in r.message]
    assert warns, f"expected a warning, got: {[r.message for r in caplog.records]}"
    assert "[test]" in warns[0].message


def test_query_count_guard_silent_below_threshold(caplog):
    from dorm.contrib.querycount import query_count_guard
    from tests.models import Author

    with caplog.at_level(logging.WARNING, logger="dorm.querycount"):
        with query_count_guard(warn_above=1000):
            Author.objects.filter(name="x").count()
    warns = [r for r in caplog.records if "query count exceeded" in r.message]
    assert not warns


# ──────────────────────────────────────────────────────────────────────────────
# Item 5: assertNumQueries
# ──────────────────────────────────────────────────────────────────────────────


def test_assert_num_queries_pass():
    from dorm.test import assertNumQueries
    from tests.models import Author

    with assertNumQueries(1) as ctx:
        Author.objects.filter(name="x").count()
    assert ctx.count == 1


def test_assert_num_queries_fail_loud():
    from dorm.test import assertNumQueries
    from tests.models import Author

    with pytest.raises(AssertionError, match="expected 99"):
        with assertNumQueries(99):
            Author.objects.filter(name="x").count()


def test_assert_num_queries_decorator_factory():
    from dorm.test import assertNumQueriesFactory
    from tests.models import Author

    @assertNumQueriesFactory(1)
    def count_authors():
        return Author.objects.filter(name="x").count()

    assert count_authors() == 0


# ──────────────────────────────────────────────────────────────────────────────
# Item 6: sticky read-after-write window
# ──────────────────────────────────────────────────────────────────────────────


def test_sticky_window_pins_reads_to_primary():
    from dorm.db.connection import (
        clear_read_after_write_window,
        router_db_for_read,
        router_db_for_write,
    )
    from tests.models import Author

    class ReplicaRouter:
        def db_for_read(self, model, **hints):
            return "replica"
        def db_for_write(self, model, **hints):
            return "default"

    clear_read_after_write_window()
    dorm.configure(DATABASE_ROUTERS=[ReplicaRouter()], READ_AFTER_WRITE_WINDOW=3.0)
    try:
        # Pre-write: replica wins.
        assert router_db_for_read(Author, default="default") == "replica"
        # Write marks the model as recently-written.
        router_db_for_write(Author, default="default")
        # Post-write: sticky window pins to primary.
        assert router_db_for_read(Author, default="default") == "default"
    finally:
        clear_read_after_write_window()
        dorm.configure(DATABASE_ROUTERS=[], READ_AFTER_WRITE_WINDOW=3.0)


def test_sticky_window_disabled_when_zero():
    from dorm.db.connection import (
        clear_read_after_write_window,
        router_db_for_read,
        router_db_for_write,
    )
    from tests.models import Author

    class ReplicaRouter:
        def db_for_read(self, model, **hints):
            return "replica"
        def db_for_write(self, model, **hints):
            return "default"

    clear_read_after_write_window()
    dorm.configure(DATABASE_ROUTERS=[ReplicaRouter()], READ_AFTER_WRITE_WINDOW=0)
    try:
        router_db_for_write(Author, default="default")
        # Window is 0 → reads still go to replica even right after a write.
        assert router_db_for_read(Author, default="default") == "replica"
    finally:
        clear_read_after_write_window()
        dorm.configure(DATABASE_ROUTERS=[], READ_AFTER_WRITE_WINDOW=3.0)


def test_sticky_window_explicit_opt_out():
    from dorm.db.connection import (
        clear_read_after_write_window,
        router_db_for_read,
        router_db_for_write,
    )
    from tests.models import Author

    class ReplicaRouter:
        def db_for_read(self, model, **hints):
            return "replica"

    clear_read_after_write_window()
    dorm.configure(DATABASE_ROUTERS=[ReplicaRouter()], READ_AFTER_WRITE_WINDOW=3.0)
    try:
        router_db_for_write(Author, default="default")
        # ``sticky=False`` lets analytics queries opt out of the
        # post-write pin and read from the replica deliberately.
        assert (
            router_db_for_read(Author, default="default", sticky=False)
            == "replica"
        )
    finally:
        clear_read_after_write_window()
        dorm.configure(DATABASE_ROUTERS=[], READ_AFTER_WRITE_WINDOW=3.0)


# ──────────────────────────────────────────────────────────────────────────────
# Item 7: migration linter
# ──────────────────────────────────────────────────────────────────────────────


def test_lint_add_field_not_null_default():
    from dorm.fields import IntegerField
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    op = ops.AddField("MyModel", "score", IntegerField(default=0, null=False))
    result = lint_operations([op], file="0001_test.py")
    codes = {f.code for f in result.findings}
    assert "DORM-M001" in codes


def test_lint_run_python_no_reverse():
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    def forward(apps, schema_editor):
        pass

    op = ops.RunPython(forward)
    result = lint_operations([op], file="0001_test.py")
    codes = {f.code for f in result.findings}
    assert "DORM-M004" in codes


def test_lint_add_index_warns_without_concurrently():
    from dorm.indexes import Index
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    op = ops.AddIndex("MyModel", Index(fields=["email"], name="idx_email"))
    result = lint_operations([op], file="0001_test.py")
    codes = {f.code for f in result.findings}
    assert "DORM-M003" in codes


def test_lint_clean_returns_no_findings():
    from dorm.fields import IntegerField
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    # null=True, no default → safe.
    op = ops.AddField("MyModel", "score", IntegerField(null=True))
    result = lint_operations([op], file="0001_test.py")
    assert result.ok
    assert result.findings == []


def test_lint_result_to_json_round_trip():
    import json
    from dorm.fields import IntegerField
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    op = ops.AddField("MyModel", "score", IntegerField(default=0, null=False))
    result = lint_operations([op], file="0001_test.py")
    data = json.loads(result.to_json())
    assert any(d["code"] == "DORM-M001" for d in data)


# ──────────────────────────────────────────────────────────────────────────────
# Item 8: querylog
# ──────────────────────────────────────────────────────────────────────────────


def test_querylog_captures_records():
    from dorm.contrib.querylog import QueryLog
    from tests.models import Author

    with QueryLog() as log:
        Author.objects.filter(name="x").count()
        Author.objects.filter(name="y").count()
    assert log.count >= 2
    assert all("SELECT" in r.sql.upper() or "FROM" in r.sql.upper() for r in log.records)
    assert log.total_ms >= 0


def test_querylog_summary_groups_by_template():
    from dorm.contrib.querylog import QueryLog
    from tests.models import Author

    with QueryLog() as log:
        for name in ["a", "b", "c"]:
            Author.objects.filter(name=name).count()
    summary = log.summary()
    assert summary, "summary should not be empty"
    # All three calls share one SQL template, so one entry should
    # account for at least three records.
    counts = [s.count for s in summary]
    assert max(counts) >= 3


def test_querylog_inert_outside_block():
    from dorm.contrib.querylog import QueryLog
    from tests.models import Author

    with QueryLog() as log:
        pass
    # After exit, queries don't leak into the log.
    Author.objects.filter(name="z").count()
    assert log.count == 0


# ──────────────────────────────────────────────────────────────────────────────
# Item 9: LocMemCache
# ──────────────────────────────────────────────────────────────────────────────


def test_locmem_cache_get_set_delete():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"OPTIONS": {"maxsize": 4}, "TTL": 60})
    c.set("k1", b"v1")
    assert c.get("k1") == b"v1"
    c.delete("k1")
    assert c.get("k1") is None


def test_locmem_cache_lru_eviction():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"OPTIONS": {"maxsize": 2}})
    c.set("a", b"1")
    c.set("b", b"2")
    c.set("c", b"3")  # evicts "a"
    assert c.get("a") is None
    assert c.get("b") == b"2"
    assert c.get("c") == b"3"


def test_locmem_cache_ttl_expiry():
    import time
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"TTL": 60})
    c.set("k", b"v", timeout=0.05)  # ty:ignore[invalid-argument-type]
    assert c.get("k") == b"v"
    time.sleep(0.1)
    assert c.get("k") is None


def test_locmem_cache_delete_pattern():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("dormqs:app.User:1", b"a")
    c.set("dormqs:app.User:2", b"b")
    c.set("dormqs:app.Book:1", b"c")
    n = c.delete_pattern("dormqs:app.User:*")
    assert n == 2
    assert c.get("dormqs:app.User:1") is None
    assert c.get("dormqs:app.Book:1") == b"c"


@pytest.mark.asyncio
async def test_locmem_cache_async_parity():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    await c.aset("k", b"v")
    assert await c.aget("k") == b"v"
    await c.adelete("k")
    assert await c.aget("k") is None


# ──────────────────────────────────────────────────────────────────────────────
# Item 10: Manager.cache_get / acache_get
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _locmem_caches_alias():
    """Switch the ``default`` CACHES alias to LocMemCache for the test
    and reset it after."""
    from dorm.cache import reset_caches

    original = dict(getattr(dorm.conf.settings, "CACHES", {}) or {})
    dorm.configure(
        CACHES={
            "default": {
                "BACKEND": "dorm.cache.locmem.LocMemCache",
                "OPTIONS": {"maxsize": 64},
                "TTL": 60,
            }
        },
        # Predictable signing key keeps payloads round-trippable.
        CACHE_SIGNING_KEY="test-key-v26",
    )
    try:
        yield
    finally:
        dorm.configure(CACHES=original)
        reset_caches()


def test_cache_get_round_trip(_locmem_caches_alias):
    from tests.models import Author

    a = Author.objects.create(name="Alice", age=30)
    # First call: miss, populates cache.
    fetched = Author.objects.cache_get(pk=a.pk)
    assert fetched.pk == a.pk
    # Second call: hit. Behaviour identical, no exception.
    again = Author.objects.cache_get(pk=a.pk)
    assert again.pk == a.pk
    assert again.name == "Alice"


def test_cache_get_falls_through_when_no_cache_configured():
    """Without CACHES configured, ``cache_get`` must transparently fall
    through to the regular ``Manager.get`` path."""
    from tests.models import Author

    # Ensure no CACHES alias is configured for this test.
    original = dict(getattr(dorm.conf.settings, "CACHES", {}) or {})
    dorm.configure(CACHES={})
    try:
        a = Author.objects.create(name="Bob", age=25)
        fetched = Author.objects.cache_get(pk=a.pk)
        assert fetched.pk == a.pk
        assert fetched.name == "Bob"
    finally:
        dorm.configure(CACHES=original)


def test_cache_get_invalidated_by_save(_locmem_caches_alias):
    from dorm.cache import bump_model_cache_version
    from tests.models import Author

    a = Author.objects.create(name="Carol", age=40)
    Author.objects.cache_get(pk=a.pk)  # populate cache

    # Simulate a write — bumping the model version is what
    # post_save would do via the auto-invalidation hook.
    bump_model_cache_version(Author)
    a.name = "Carol 2"
    a.save()

    # cache_get after the bump must see the new state.
    refreshed = Author.objects.cache_get(pk=a.pk)
    assert refreshed.name == "Carol 2"
