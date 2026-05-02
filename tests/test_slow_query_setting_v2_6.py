"""Tests for the v2.6 ``settings.SLOW_QUERY_MS`` knob.

The slow-query warning machinery was already present (timing happens
unconditionally to feed ``pre_query`` / ``post_query`` signals); v2.6
exposes the threshold through ``settings.SLOW_QUERY_MS`` instead of
the env-only ``DORM_SLOW_QUERY_MS``. Resolution order: explicit
setting → env var → 500 ms default. ``None`` disables the warning
entirely.
"""

from __future__ import annotations

import logging

import pytest

import dorm
from dorm.db.utils import (
    _invalidate_slow_query_cache,
    _resolve_slow_query_ms,
    _slow_query_ms,
)


@pytest.fixture(autouse=True)
def _restore_slow_query_setting():
    """Reset both the memoised cache and any explicit-setting marker so
    the suite's other tests don't observe leaked SLOW_QUERY_MS state."""
    from dorm.conf import settings

    had_explicit = "SLOW_QUERY_MS" in settings._explicit_settings
    prev_value = getattr(settings, "SLOW_QUERY_MS", None)

    _invalidate_slow_query_cache()
    yield
    if had_explicit:
        settings.SLOW_QUERY_MS = prev_value  # type: ignore[assignment]
    else:
        settings._explicit_settings.discard("SLOW_QUERY_MS")
        settings.SLOW_QUERY_MS = 500.0  # restore class default
    _invalidate_slow_query_cache()


def test_default_threshold_is_500ms():
    """No env var, no explicit setting → 500 ms class default."""
    assert _slow_query_ms() == 500.0


def test_env_var_used_when_no_explicit_setting(monkeypatch):
    monkeypatch.setenv("DORM_SLOW_QUERY_MS", "123")
    _invalidate_slow_query_cache()
    assert _slow_query_ms() == 123.0


def test_explicit_setting_overrides_env_var(monkeypatch):
    """``configure(SLOW_QUERY_MS=…)`` always wins over the env var."""
    monkeypatch.setenv("DORM_SLOW_QUERY_MS", "999")
    dorm.configure(SLOW_QUERY_MS=42.0)
    assert _slow_query_ms() == 42.0


def test_explicit_none_disables_warning():
    dorm.configure(SLOW_QUERY_MS=None)
    assert _slow_query_ms() is None


def test_configure_invalidates_cache():
    """A second ``configure`` call must replace the memoised value."""
    dorm.configure(SLOW_QUERY_MS=10.0)
    assert _slow_query_ms() == 10.0
    dorm.configure(SLOW_QUERY_MS=20.0)
    assert _slow_query_ms() == 20.0


def test_resolve_returns_cacheable_flag():
    """Settings-derived values are cacheable; env / default are not."""
    dorm.configure(SLOW_QUERY_MS=7.5)
    val, cacheable = _resolve_slow_query_ms()
    assert val == 7.5
    assert cacheable is True


def test_resolve_env_branch_is_not_cacheable(monkeypatch):
    monkeypatch.setenv("DORM_SLOW_QUERY_MS", "250")
    _invalidate_slow_query_cache()
    val, cacheable = _resolve_slow_query_ms()
    assert val == 250.0
    assert cacheable is False


def test_invalid_env_var_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DORM_SLOW_QUERY_MS", "not-a-number")
    _invalidate_slow_query_cache()
    assert _slow_query_ms() == 500.0


def test_warning_emitted_when_threshold_zero(caplog):
    """Threshold 0 → every query crosses it → WARNING per query."""
    from tests.models import Author

    dorm.configure(SLOW_QUERY_MS=0)
    with caplog.at_level(logging.WARNING, logger="dorm.db"):
        Author.objects.filter(name="__nope__").count()
    warnings = [r for r in caplog.records if "slow query" in r.message]
    assert warnings, "expected slow-query warning at 0 ms threshold"


def test_no_warning_when_threshold_is_none(caplog):
    """``SLOW_QUERY_MS=None`` skips the comparison entirely."""
    from tests.models import Author

    dorm.configure(SLOW_QUERY_MS=None)
    with caplog.at_level(logging.WARNING, logger="dorm.db"):
        Author.objects.filter(name="__nope__").count()
    warnings = [r for r in caplog.records if "slow query" in r.message]
    assert not warnings, f"unexpected slow-query warnings: {warnings!r}"


def test_no_warning_when_threshold_is_high(caplog):
    """A high threshold suppresses the warning even when env says
    otherwise — explicit setting wins."""
    from tests.models import Author

    dorm.configure(SLOW_QUERY_MS=60_000.0)  # 1 minute
    with caplog.at_level(logging.WARNING, logger="dorm.db"):
        Author.objects.filter(name="__nope__").count()
    warnings = [r for r in caplog.records if "slow query" in r.message]
    assert not warnings, f"unexpected slow-query warnings: {warnings!r}"
