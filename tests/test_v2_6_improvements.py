"""Tests for the v2.6 self-improvement pass.

Covers the post-feature refactor:

- ``MemoizedSetting`` resolver + central registry / invalidator.
- ``ScopedCollector`` per-task signal collector.
- Sticky read-after-write lazy-copy upgrade (one dict per task, not
  per write).
- ``LocMemCache`` prefix index for ``delete_pattern``.
- ``assertMaxQueries`` + ``assertMaxQueriesFactory``.
- ``Manager.cache_get_many`` / ``acache_get_many``.
- ``Manager.acache_get`` async path.
- ``QueryLogASGIMiddleware``.
- ``query_count_guard`` per-task isolation under ``asyncio.gather``.
- ``dorm lint-migrations --rule`` / ``--exit-zero`` flags.
"""

from __future__ import annotations

import asyncio

import pytest

import dorm


# ──────────────────────────────────────────────────────────────────────────────
# MemoizedSetting registry
# ──────────────────────────────────────────────────────────────────────────────


def test_memoized_setting_registers_and_invalidates_via_configure():
    """``configure(NAME=…)`` must invalidate the matching memoised
    setting through the central registry — adding a new knob must NOT
    require touching ``conf.configure`` itself."""
    from dorm._memoized_setting import (
        MemoizedSetting,
        get_registered,
        invalidate_all_for,
        _REGISTRY,
    )
    from dorm.conf import settings

    ms = MemoizedSetting(
        "_TEST_REG_KNOB",
        env_var=None,
        default=10,
        parser=int,
    )
    try:
        assert get_registered("_TEST_REG_KNOB") is ms
        assert ms.get() == 10

        # Configure: explicit + value. Default branch wasn't cached,
        # so next get() picks the new value up immediately.
        settings._explicit_settings.add("_TEST_REG_KNOB")
        settings._TEST_REG_KNOB = 99  # type: ignore[attr-defined]
        assert ms.get() == 99
        # Explicit branch IS cached → bypassing the registry leaves
        # the memoised value stale.
        settings._TEST_REG_KNOB = 7  # type: ignore[attr-defined]
        assert ms.get() == 99, "explicit-branch value should be memoised"
        invalidate_all_for({"_TEST_REG_KNOB": 7})
        assert ms.get() == 7
    finally:
        # Cleanup: drop the test-only registration and any explicit
        # marker on the global Settings singleton, so subsequent tests
        # in the same xdist worker don't observe a leaked knob.
        _REGISTRY.pop("_TEST_REG_KNOB", None)
        settings._explicit_settings.discard("_TEST_REG_KNOB")
        if hasattr(settings, "_TEST_REG_KNOB"):
            try:
                delattr(settings, "_TEST_REG_KNOB")
            except AttributeError:
                pass


def test_memoized_setting_recovers_from_corrupt_cache():
    """Same behaviour as the per-knob shims — corruption is dropped
    and the value is re-resolved."""
    from dorm._memoized_setting import MemoizedSetting, _REGISTRY

    ms = MemoizedSetting(
        "_TEST_CORRUPT", env_var=None, default=42, parser=int
    )
    try:
        ms._cache = "not-an-int"  # type: ignore[assignment]
        assert ms.get() == 42
    finally:
        _REGISTRY.pop("_TEST_CORRUPT", None)


# ──────────────────────────────────────────────────────────────────────────────
# ScopedCollector
# ──────────────────────────────────────────────────────────────────────────────


def test_scoped_collector_basic_open_close():
    from dorm._scoped import ScopedCollector
    from dorm.signals import Signal

    sig = Signal()
    collector: ScopedCollector[list[int]] = ScopedCollector(
        sig, "test_scoped_basic", lambda state, _kw: state.append(1)
    )

    state: list[int] = []
    token = collector.open(state)
    sig.send(sender="x")
    sig.send(sender="x")
    collector.close(token)
    assert state == [1, 1]
    # After close, further events don't append.
    sig.send(sender="x")
    assert state == [1, 1]


def test_scoped_collector_inert_outside_scope():
    """Without an open scope on the current task, the receiver
    short-circuits — no allocation, no list mutation."""
    from dorm._scoped import ScopedCollector
    from dorm.signals import Signal

    sig = Signal()
    sentinel = []

    def _on_event(state, _kw):
        sentinel.append("ran")

    ScopedCollector(sig, "test_scoped_inert", _on_event)
    sig.send(sender="y")
    sig.send(sender="y")
    assert sentinel == [], "receiver must not run when no scope is open"


# ──────────────────────────────────────────────────────────────────────────────
# Sticky window lazy-copy
# ──────────────────────────────────────────────────────────────────────────────


def test_sticky_lazy_copy_reuses_per_task_dict():
    """After the first write upgrades from the shared empty sentinel,
    subsequent writes mutate the SAME dict in place — no per-write
    copy."""
    from dorm.db.connection import (
        _own_sticky_dict,
        _sticky_until,
        clear_read_after_write_window,
    )

    clear_read_after_write_window()
    d1 = _own_sticky_dict()
    d2 = _own_sticky_dict()
    assert d1 is d2, "two reads on same task must share the dict"
    # And the ContextVar's tuple records us as the owner.
    owner, mapping = _sticky_until.get()
    assert owner == id(mapping)
    assert mapping is d1


def test_sticky_lazy_copy_preserves_isolation_across_tasks():
    from dorm.db.connection import (
        _mark_recent_write,
        _own_sticky_dict,
        clear_read_after_write_window,
    )
    from tests.models import Author

    async def _run() -> tuple[dict, dict]:
        clear_read_after_write_window()
        dorm.configure(READ_AFTER_WRITE_WINDOW=3.0)
        results: list[dict] = []

        async def task_a():
            _mark_recent_write(Author)
            await asyncio.sleep(0)
            results.append(dict(_own_sticky_dict()))

        async def task_b():
            await asyncio.sleep(0)
            results.append(dict(_own_sticky_dict()))

        await asyncio.gather(task_a(), task_b())
        return results[0], results[1]

    a, b = asyncio.get_event_loop().run_until_complete(_run()) if False else asyncio.run(_run())
    assert a, "task A wrote, must have an entry"
    assert b == {}, "task B never wrote — must see an empty dict"


# ──────────────────────────────────────────────────────────────────────────────
# LocMemCache prefix index
# ──────────────────────────────────────────────────────────────────────────────


def test_locmem_prefix_index_short_circuit():
    """``delete_pattern("prefix:*")`` should bypass the full ``fnmatch``
    scan and use the secondary prefix index."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"OPTIONS": {"maxsize": 100}})
    c.set("dormqs:app.User:1", b"a")
    c.set("dormqs:app.User:2", b"b")
    c.set("dormqs:app.Book:1", b"c")
    # Sanity: index has the User bucket populated.
    assert "dormqs:app.User" in c._by_prefix
    n = c.delete_pattern("dormqs:app.User:*")
    assert n == 2
    # User bucket dropped from the index entirely.
    assert "dormqs:app.User" not in c._by_prefix
    assert c.get("dormqs:app.User:1") is None
    assert c.get("dormqs:app.Book:1") == b"c"


def test_locmem_prefix_index_falls_back_for_complex_patterns():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"OPTIONS": {"maxsize": 100}})
    c.set("a:1:x", b"1")
    c.set("a:2:x", b"2")
    c.set("a:1:y", b"3")
    # Mid-glob pattern can't use the prefix bucket — must fall back
    # to fnmatch scan and still drop the right keys.
    n = c.delete_pattern("a:*:x")
    assert n == 2
    assert c.get("a:1:y") == b"3"


def test_locmem_index_cleanup_on_delete():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("ns:1", b"a")
    c.delete("ns:1")
    assert "ns" not in c._by_prefix


def test_locmem_index_cleanup_on_lru_eviction():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"OPTIONS": {"maxsize": 2}})
    c.set("ns:a", b"1")
    c.set("ns:b", b"2")
    c.set("ns:c", b"3")  # evicts ns:a
    # ns:a should be gone from BOTH the LRU dict and the prefix
    # bucket — otherwise delete_pattern would later try to drop a
    # missing key.
    assert "ns:a" not in c._store
    assert "ns:a" not in c._by_prefix.get("ns", set())


# ──────────────────────────────────────────────────────────────────────────────
# assertMaxQueries
# ──────────────────────────────────────────────────────────────────────────────


def test_assert_max_queries_passes_when_under():
    from dorm.test import assertMaxQueries
    from tests.models import Author

    with assertMaxQueries(5) as ctx:
        Author.objects.filter(name="x").count()
    assert ctx.count == 1


def test_assert_max_queries_fails_when_over():
    from dorm.test import assertMaxQueries
    from tests.models import Author

    with pytest.raises(AssertionError, match="at most 0"):
        with assertMaxQueries(0):
            Author.objects.filter(name="x").count()


def test_assert_max_queries_factory_async():
    from dorm.test import assertMaxQueriesFactory
    from tests.models import Author

    @assertMaxQueriesFactory(2)
    async def acount():
        return await Author.objects.filter(name="x").acount()

    n = asyncio.run(acount())
    assert n == 0


# ──────────────────────────────────────────────────────────────────────────────
# Manager.cache_get_many / acache_get + acache_get_many
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _locmem_caches_alias():
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
        CACHE_SIGNING_KEY="test-key-improvements",
    )
    try:
        yield
    finally:
        dorm.configure(CACHES=original)
        reset_caches()


def test_cache_get_many_round_trip(_locmem_caches_alias):
    from tests.models import Author

    a = Author.objects.create(name="A1", age=20)
    b = Author.objects.create(name="A2", age=21)
    out = Author.objects.cache_get_many(pks=[a.pk, b.pk])
    assert set(out.keys()) == {a.pk, b.pk}
    assert out[a.pk].name == "A1"
    # Second call: hit path.
    out2 = Author.objects.cache_get_many(pks=[a.pk, b.pk])
    assert out2[a.pk].name == "A1"


def test_cache_get_many_empty_pks(_locmem_caches_alias):
    from tests.models import Author

    assert Author.objects.cache_get_many(pks=[]) == {}


def test_cache_get_many_falls_through_when_no_cache_configured():
    from tests.models import Author

    original = dict(getattr(dorm.conf.settings, "CACHES", {}) or {})
    dorm.configure(CACHES={})
    try:
        a = Author.objects.create(name="X", age=33)
        out = Author.objects.cache_get_many(pks=[a.pk])
        assert out[a.pk].name == "X"
    finally:
        dorm.configure(CACHES=original)


@pytest.mark.asyncio
async def test_acache_get_round_trip(_locmem_caches_alias):
    from tests.models import Author

    a = await Author.objects.acreate(name="Alice", age=30)
    fetched = await Author.objects.acache_get(pk=a.pk)
    assert fetched.pk == a.pk
    again = await Author.objects.acache_get(pk=a.pk)
    assert again.name == "Alice"


@pytest.mark.asyncio
async def test_acache_get_many_round_trip(_locmem_caches_alias):
    from tests.models import Author

    a = await Author.objects.acreate(name="A1", age=20)
    b = await Author.objects.acreate(name="A2", age=21)
    out = await Author.objects.acache_get_many(pks=[a.pk, b.pk])
    assert set(out.keys()) == {a.pk, b.pk}
    out2 = await Author.objects.acache_get_many(pks=[a.pk, b.pk])
    assert out2[a.pk].name == "A1"


# ──────────────────────────────────────────────────────────────────────────────
# QueryLogASGIMiddleware
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_querylog_asgi_middleware_attaches_log_to_scope():
    from dorm.contrib.querylog import QueryLog, QueryLogASGIMiddleware
    from tests.models import Author

    captured: dict = {}

    async def app(scope, receive, send):
        log = scope.get("dorm_querylog")
        assert isinstance(log, QueryLog)
        # Issue a query inside the request.
        list(Author.objects.filter(name="z").values_list("id", flat=True))
        captured["log"] = log

    middleware = QueryLogASGIMiddleware(app)

    async def receive():
        return {"type": "http.request"}

    async def send(_msg):
        pass

    scope = {"type": "http", "method": "GET", "path": "/"}
    await middleware(scope, receive, send)
    assert "log" in captured
    assert captured["log"].count >= 1


@pytest.mark.asyncio
async def test_querylog_asgi_middleware_passes_through_lifespan():
    """Non-http / non-websocket scopes (lifespan) must NOT be wrapped
    — opening a QueryLog around the whole lifespan span would leak
    queries from the application's startup phase into a log nobody
    reads."""
    from dorm.contrib.querylog import QueryLogASGIMiddleware

    saw_log = []

    async def app(scope, receive, send):
        saw_log.append(scope.get("dorm_querylog"))

    middleware = QueryLogASGIMiddleware(app)
    await middleware({"type": "lifespan"}, lambda: None, lambda _m: None)
    assert saw_log == [None]


# ──────────────────────────────────────────────────────────────────────────────
# query_count_guard per-task isolation
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_query_count_guard_isolates_concurrent_tasks():
    """Two ``asyncio.gather``-launched tasks must each see their own
    counter — no cross-contamination through the shared ``pre_query``
    signal."""
    from dorm.contrib.querycount import query_count_guard
    from tests.models import Author

    counts: dict[str, int] = {}

    async def task(label: str, n_queries: int):
        with query_count_guard() as ctx:
            for _ in range(n_queries):
                await Author.objects.filter(name=label).acount()
        counts[label] = ctx.count

    await asyncio.gather(task("A", 2), task("B", 5))

    assert counts["A"] == 2, f"task A leaked counter, got {counts['A']}"
    assert counts["B"] == 5, f"task B leaked counter, got {counts['B']}"


# ──────────────────────────────────────────────────────────────────────────────
# lint-migrations: --rule and --exit-zero flags
# ──────────────────────────────────────────────────────────────────────────────


def test_lint_finding_file_serialises_path_to_str():
    """``Finding.file`` may be a Path; ``to_dict`` always emits a
    string so JSON output stays consumable."""
    from pathlib import Path
    from dorm.migrations.lint import Finding

    f = Finding(
        code="DORM-M001",
        file=Path("/tmp/0001_test.py"),
        operation="AddField(...)",
        message="...",
    )
    d = f.to_dict()
    assert isinstance(d["file"], str)
    assert d["file"] == "/tmp/0001_test.py"


def test_lint_cli_rule_filter(tmp_path, capsys):
    """The CLI ``--rule DORM-M001`` flag drops findings with other
    codes from the aggregated result."""
    from dorm.migrations.lint import LintResult, Finding
    from dorm.cli import cmd_lint_migrations
    import argparse

    import dorm.cli as cli_mod
    import dorm.migrations.lint as lint_mod

    real_load_settings = cli_mod._load_settings
    real_load_apps = cli_mod._load_apps
    real_lint_dir = lint_mod.lint_directory
    from dorm.conf import settings as conf_settings
    prev_apps = conf_settings.INSTALLED_APPS

    try:
        cli_mod._load_settings = lambda *_a, **_k: None
        cli_mod._load_apps = lambda *_a, **_k: None
        conf_settings.INSTALLED_APPS = ["fake_app"]
        fake_findings = [
            Finding("DORM-M001", "f.py", "op", "m1"),
            Finding("DORM-M003", "f.py", "op", "m3"),
        ]
        lint_mod.lint_directory = lambda _p: LintResult(findings=list(fake_findings))

        args = argparse.Namespace(
            settings=None,
            format="text",
            rule=["DORM-M001"],
            exit_zero=False,
        )
        with pytest.raises(SystemExit) as ei:
            cmd_lint_migrations(args)
        assert ei.value.code != 0
        out = capsys.readouterr().out
        assert "DORM-M001" in out
        assert "DORM-M003" not in out
    finally:
        cli_mod._load_settings = real_load_settings
        cli_mod._load_apps = real_load_apps
        lint_mod.lint_directory = real_lint_dir
        conf_settings.INSTALLED_APPS = prev_apps


def test_lint_cli_exit_zero_flag(tmp_path, capsys):
    from dorm.migrations.lint import LintResult, Finding
    from dorm.cli import cmd_lint_migrations
    import argparse
    import dorm.cli as cli_mod
    import dorm.migrations.lint as lint_mod

    real_load_settings = cli_mod._load_settings
    real_load_apps = cli_mod._load_apps
    real_lint_dir = lint_mod.lint_directory
    from dorm.conf import settings as conf_settings
    prev_apps = conf_settings.INSTALLED_APPS

    try:
        cli_mod._load_settings = lambda *_a, **_k: None
        cli_mod._load_apps = lambda *_a, **_k: None
        conf_settings.INSTALLED_APPS = ["fake_app"]
        lint_mod.lint_directory = lambda _p: LintResult(
            findings=[Finding("DORM-M001", "f.py", "op", "m")]
        )

        args = argparse.Namespace(
            settings=None, format="text", rule=None, exit_zero=True
        )
        with pytest.raises(SystemExit) as ei:
            cmd_lint_migrations(args)
        assert ei.value.code == 0, "--exit-zero must override the non-zero exit"
    finally:
        cli_mod._load_settings = real_load_settings
        cli_mod._load_apps = real_load_apps
        lint_mod.lint_directory = real_lint_dir
        conf_settings.INSTALLED_APPS = prev_apps


# ──────────────────────────────────────────────────────────────────────────────
# soft pytest dep (doesn't crash, exposes assertNumQueries)
# ──────────────────────────────────────────────────────────────────────────────


def test_dorm_test_module_exports_assertions_without_pytest_runtime():
    """Importing ``dorm.test`` should expose ``assertNumQueries`` and
    ``assertMaxQueries`` independently of whether pytest fixtures are
    usable. The fixtures themselves require pytest, but the
    assertions are pure context managers."""
    import dorm.test as t

    assert hasattr(t, "assertNumQueries")
    assert hasattr(t, "assertMaxQueries")
    assert hasattr(t, "assertNumQueriesFactory")
    assert hasattr(t, "assertMaxQueriesFactory")


# ──────────────────────────────────────────────────────────────────────────────
# QueryLog dataclass shape
# ──────────────────────────────────────────────────────────────────────────────


def test_query_record_has_slots_and_to_dict():
    from dorm.contrib.querylog import QueryRecord

    r = QueryRecord(sql="SELECT 1", params=None, alias="default", elapsed_ms=1.5, error=None)
    # ``slots=True`` means setting a new attribute raises.
    with pytest.raises(AttributeError):
        r.foo = "bar"  # type: ignore[attr-defined]
    d = r.to_dict()
    assert d["sql"] == "SELECT 1"
    assert d["alias"] == "default"
    assert d["error"] is None


def test_template_stats_has_to_dict():
    from dorm.contrib.querylog import TemplateStats

    s = TemplateStats(template="SELECT ?", count=3, total_ms=1.0, p50_ms=0.3, p95_ms=0.5)
    d = s.to_dict()
    assert d["template"] == "SELECT ?"
    assert d["count"] == 3
