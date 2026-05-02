"""Coverage gaps audit — pin behaviour for edge cases that the
v3.0 modules exposed but didn't have explicit tests for.

Each block targets a path that *could* harbour a bug under
adversarial / unusual input. Failing one of these means a real
regression, not a refactor inconvenience.
"""

from __future__ import annotations

import logging

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# encrypted: malformed ciphertext
# ──────────────────────────────────────────────────────────────────────────────


def _have_cryptography() -> bool:
    try:
        import cryptography  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _have_cryptography(), reason="needs djanorm[encrypted]")
def test_decrypt_invalid_base64_raises_value_error():
    """Garbage after the ``v1:`` prefix used to leak as
    ``binascii.Error`` — caller couldn't catch with a plain
    ``except ValueError``. Now wrapped consistently."""
    import dorm
    import base64

    raw = base64.b64encode(b"\x09" * 32).decode("ascii")
    dorm.configure(FIELD_ENCRYPTION_KEY=raw)
    try:
        from dorm.contrib.encrypted import _decrypt

        with pytest.raises(ValueError, match="could not decode"):
            _decrypt("v1:not-valid-base64!@#$")
    finally:
        dorm.configure(FIELD_ENCRYPTION_KEY="")


@pytest.mark.skipif(not _have_cryptography(), reason="needs djanorm[encrypted]")
def test_decrypt_truncated_payload_raises_value_error():
    """A blob shorter than ``nonce + AES-GCM tag`` (12 + 16 bytes)
    is malformed. AES-GCM would say ``InvalidTag`` here; we surface
    a clearer error pointing at the actual problem."""
    import dorm
    import base64

    raw = base64.b64encode(b"\x09" * 32).decode("ascii")
    dorm.configure(FIELD_ENCRYPTION_KEY=raw)
    try:
        from dorm.contrib.encrypted import _decrypt

        too_short = "v1:" + base64.b64encode(b"only-a-few-bytes").decode("ascii")
        with pytest.raises(ValueError, match="too short"):
            _decrypt(too_short)
    finally:
        dorm.configure(FIELD_ENCRYPTION_KEY="")


@pytest.mark.skipif(not _have_cryptography(), reason="needs djanorm[encrypted]")
def test_decrypt_empty_payload_after_prefix_raises_value_error():
    """``v1:`` + nothing → 0-byte blob. Same too-short branch."""
    import dorm
    import base64

    raw = base64.b64encode(b"\x09" * 32).decode("ascii")
    dorm.configure(FIELD_ENCRYPTION_KEY=raw)
    try:
        from dorm.contrib.encrypted import _decrypt

        with pytest.raises(ValueError, match="too short"):
            _decrypt("v1:")
    finally:
        dorm.configure(FIELD_ENCRYPTION_KEY="")


# ──────────────────────────────────────────────────────────────────────────────
# password: defensive type guards
# ──────────────────────────────────────────────────────────────────────────────


def test_check_password_rejects_non_string_password_input():
    """Untrusted JSON / form input may land here as ``None`` /
    ``int``. The function must return ``False`` rather than blow
    up — this is a security boundary."""
    from dorm.contrib.auth.password import check_password, make_password

    h = make_password("real")
    assert check_password(None, h) is False  # type: ignore[arg-type]
    assert check_password(123, h) is False  # type: ignore[arg-type]
    assert check_password(b"bytes", h) is False  # type: ignore[arg-type]


def test_check_password_rejects_non_string_encoded_input():
    from dorm.contrib.auth.password import check_password

    assert check_password("anything", None) is False  # type: ignore[arg-type]
    assert check_password("anything", 12345) is False  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# LocMemCache: edge-case TTL + delete patterns
# ──────────────────────────────────────────────────────────────────────────────


def test_locmem_set_with_timeout_zero_means_no_expire():
    """Match Redis semantics: ``timeout=0`` → store with no expiry,
    not "expire immediately"."""
    import time
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("k", b"v", timeout=0)
    # Pause briefly to make sure no early expiry fires.
    time.sleep(0.05)
    assert c.get("k") == b"v"


def test_locmem_set_with_negative_timeout_evicts_immediately():
    """Negative timeout is the documented way to evict on next
    read — same as a TTL that lapsed in the past."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("k", b"v", timeout=-1)
    assert c.get("k") is None


def test_locmem_delete_pattern_matches_everything():
    """Pattern ``"*"`` should evict every key, regardless of whether
    it's in the prefix index or not."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("plain", b"x")
    c.set("ns:1", b"y")
    c.set("ns:2", b"z")
    n = c.delete_pattern("*")
    assert n == 3
    assert c.get("plain") is None
    assert c.get("ns:1") is None
    # Index also empty.
    assert c._by_prefix == {}


def test_locmem_set_replacing_existing_key_does_not_double_index():
    """Re-setting an existing key must not append a duplicate to the
    prefix bucket — the bucket is a ``set`` so duplicates fold, but
    test pins the contract."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("ns:1", b"v1")
    c.set("ns:1", b"v2")
    assert c.get("ns:1") == b"v2"
    # Prefix bucket carries exactly one entry.
    assert c._by_prefix["ns"] == {"ns:1"}


def test_locmem_clear_resets_both_store_and_index():
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("ns:1", b"a")
    c.set("ns:2", b"b")
    c.clear()
    assert c.get("ns:1") is None
    assert c._store == {}
    assert c._by_prefix == {}


# ──────────────────────────────────────────────────────────────────────────────
# QueryLog / query_count_guard: nesting + exception-in-block
# ──────────────────────────────────────────────────────────────────────────────


def test_query_count_guard_releases_state_on_exception():
    """If the body raises, the ContextVar token must still be reset;
    otherwise a leaked ``state[0]`` pollutes the next guard."""
    from dorm.contrib.querycount import _collector, query_count_guard

    with pytest.raises(RuntimeError, match="boom"):
        with query_count_guard():
            raise RuntimeError("boom")
    # No active scope after exit, even on exception.
    assert _collector.current() is None


def test_query_count_guard_nested_scopes_isolated():
    """Inner guard should count only its own queries; outer keeps
    counting across the whole block including the inner one."""
    from dorm.contrib.querycount import query_count_guard
    from tests.models import Author

    with query_count_guard() as outer:
        Author.objects.filter(name="x").count()  # outer +1
        with query_count_guard() as inner:
            Author.objects.filter(name="y").count()  # inner +1
        Author.objects.filter(name="z").count()  # outer +1 (inner already closed)
    assert inner.count == 1
    assert outer.count == 3


def test_query_log_releases_state_on_exception():
    from dorm.contrib.querylog import QueryLog, _collector

    with pytest.raises(RuntimeError, match="boom"):
        with QueryLog():
            raise RuntimeError("boom")
    assert _collector.current() is None


def test_query_log_nested_scopes_isolated():
    """Inner log captures inner queries; outer captures everything."""
    from dorm.contrib.querylog import QueryLog
    from tests.models import Author

    with QueryLog() as outer:
        Author.objects.filter(name="a").count()
        with QueryLog() as inner:
            Author.objects.filter(name="b").count()
        Author.objects.filter(name="c").count()
    assert inner.count == 1
    assert outer.count == 3


def test_assert_num_queries_releases_state_on_exception():
    """If the wrapped block raises, the assertion at exit is
    *skipped* (the exception is the more interesting signal) but
    the ContextVar still resets."""
    from dorm.test import _collector, assertNumQueries

    with pytest.raises(RuntimeError, match="boom"):
        with assertNumQueries(99):
            raise RuntimeError("boom")
    assert _collector.current() is None


# ──────────────────────────────────────────────────────────────────────────────
# MemoizedSetting: registry overwrite + parser failure
# ──────────────────────────────────────────────────────────────────────────────


def test_memoized_setting_reregister_overwrites():
    """Registering the same name twice replaces the previous entry —
    tests that exercise a temporary knob can re-register without
    leaking the prior instance."""
    from dorm._memoized_setting import MemoizedSetting, _REGISTRY

    try:
        first = MemoizedSetting("_TEST_REREG", env_var=None, default=1, parser=int)
        second = MemoizedSetting("_TEST_REREG", env_var=None, default=2, parser=int)
        assert _REGISTRY["_TEST_REREG"] is second
        # First instance is no longer reachable from the registry; it
        # keeps working in isolation but ``invalidate_all_for`` won't
        # touch it.
        assert first.get() == 1
        assert second.get() == 2
    finally:
        _REGISTRY.pop("_TEST_REREG", None)


def test_memoized_setting_parser_value_error_falls_back_to_default():
    """A user who passes a value the parser can't coerce should fall
    through to the env / default branch instead of crashing."""
    import dorm
    from dorm._memoized_setting import MemoizedSetting, _REGISTRY

    ms = MemoizedSetting(
        "_TEST_PARSER", env_var=None, default=99, parser=int
    )
    try:
        dorm.configure(_TEST_PARSER="not-an-int")
        # Parser raises ValueError → resolver falls through to
        # default. ``get`` returns the default, no exception leaks.
        assert ms.get() == 99
    finally:
        _REGISTRY.pop("_TEST_PARSER", None)
        from dorm.conf import settings
        settings._explicit_settings.discard("_TEST_PARSER")
        if hasattr(settings, "_TEST_PARSER"):
            try:
                delattr(settings, "_TEST_PARSER")
            except AttributeError:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# Sticky window expiration
# ──────────────────────────────────────────────────────────────────────────────


def test_sticky_window_expires_after_configured_seconds():
    """Set window to a tiny value, write, sleep past it, read should
    NOT be sticky any more — the entry is dropped on the next
    ``_is_sticky`` call."""
    import time
    import dorm
    from dorm.db.connection import (
        _is_sticky,
        _mark_recent_write,
        _sticky_until,
        clear_read_after_write_window,
    )
    from tests.models import Author

    clear_read_after_write_window()
    dorm.configure(READ_AFTER_WRITE_WINDOW=0.05)
    try:
        _mark_recent_write(Author)
        assert _is_sticky(Author) is True
        time.sleep(0.1)
        # Window expired — entry dropped on next check.
        assert _is_sticky(Author) is False
        # Dict actually shrank.
        _, mapping = _sticky_until.get()
        assert ("tests", "author") not in mapping
    finally:
        clear_read_after_write_window()
        dorm.configure(READ_AFTER_WRITE_WINDOW=3.0)


# ──────────────────────────────────────────────────────────────────────────────
# Lint: empty / missing directory + RunSQL passthrough
# ──────────────────────────────────────────────────────────────────────────────


def test_lint_directory_missing_returns_empty_result(tmp_path):
    from dorm.migrations.lint import lint_directory

    result = lint_directory(tmp_path / "nonexistent")
    assert result.ok
    assert result.findings == []


def test_lint_directory_only_init_file_skipped(tmp_path):
    """Files named ``__init__.py`` are NOT migrations and must be
    skipped silently."""
    from dorm.migrations.lint import lint_directory

    (tmp_path / "__init__.py").write_text("")
    result = lint_directory(tmp_path)
    assert result.ok


def test_lint_run_sql_no_finding():
    """``RunSQL`` is in scope for migration safety in principle, but
    we don't lint it today (too easy to false-positive on safe
    statements). Make sure it doesn't accidentally trip another
    rule."""
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    op = ops.RunSQL("SELECT 1;", reverse_sql="SELECT 1;")
    result = lint_operations([op], file="0001_test.py")
    assert result.findings == []


# ──────────────────────────────────────────────────────────────────────────────
# Prometheus: empty exposition + double install
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=False)
def _reset_prom():
    from dorm.contrib.prometheus import uninstall
    uninstall()
    yield
    uninstall()


def test_prometheus_empty_exposition_is_valid_text(_reset_prom):
    """Calling ``metrics_response()`` before any query / cache event
    must still produce a string ending in ``\\n`` (Prometheus text
    format requires the trailing newline)."""
    from dorm.contrib.prometheus import install, metrics_response

    install()
    out = metrics_response()
    assert isinstance(out, str)
    assert out.endswith("\n")


def test_prometheus_double_install_does_not_attach_listener_twice(_reset_prom):
    """Two ``install()`` calls must result in exactly one increment
    per query; otherwise counters drift double the truth."""
    from dorm.contrib.prometheus import (
        _query_counter,
        install,
        metrics_response,
    )
    from tests.models import Author

    install()
    install()  # idempotent
    Author.objects.filter(name="z").count()
    # Sum the counter across all (vendor, outcome) pairs — should
    # equal 1 (one query), not 2.
    total = sum(_query_counter.values())
    assert total == 1, f"counter was incremented {total}× for one query"
    out = metrics_response()
    assert "dorm_queries_total" in out


# ──────────────────────────────────────────────────────────────────────────────
# AsyncGuard: disable when never enabled
# ──────────────────────────────────────────────────────────────────────────────


def test_disable_async_guard_when_never_enabled_is_noop():
    """A defensive teardown that calls ``disable_async_guard()``
    without a matching enable should not crash."""
    from dorm.contrib.asyncguard import disable_async_guard

    # Two back-to-back disables — second one runs against an
    # already-clean state. Must not raise.
    disable_async_guard()
    disable_async_guard()


def test_async_guard_warn_dedup_resets_after_disable_enable():
    """Disabling clears the dedup set, so re-enabling produces a
    fresh warning for the same call site instead of staying mute."""
    import asyncio
    import logging
    from dorm.contrib.asyncguard import disable_async_guard, enable_async_guard
    from tests.models import Author

    async def _fire():
        Author.objects.filter(name="z").count()

    enable_async_guard(mode="warn")
    asyncio.run(_fire())
    disable_async_guard()
    enable_async_guard(mode="warn")
    # First call after re-enable should warn again — would silently
    # stay quiet if the dedup set survived the disable cycle.
    caplog_records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = caplog_records.append  # type: ignore[assignment]
    logger = logging.getLogger("dorm.asyncguard")
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        asyncio.run(_fire())
    finally:
        logger.removeHandler(handler)
        disable_async_guard()
    assert any("async event loop" in r.getMessage() for r in caplog_records)
