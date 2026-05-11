"""Tier-4 DX features added in v4.2."""
from __future__ import annotations

import pytest


# ── Manager.cached() sugar ──────────────────────────────────────────────────


class TestManagerCached:
    def test_cached_returns_cached_queryset(self):
        import dorm

        class _C(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        qs = _C.objects.cached(timeout=10)
        # The queryset carries the cache hint.
        assert qs._cache_alias == "default"
        assert qs._cache_timeout == 10

    def test_cached_clones_default_queryset(self):
        import dorm

        class _D(dorm.Model):
            name = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        qs1 = _D.objects.cached(timeout=60)
        qs2 = _D.objects.cached(timeout=60)
        # Different instances — chaining doesn't mutate the manager.
        assert qs1 is not qs2


# ── DEBUG_NPLUSONE auto-install ──────────────────────────────────────────────


@pytest.fixture
def reset_global_detector():
    import dorm.contrib.nplusone as np

    yield
    if np._GLOBAL_DEBUG_DETECTOR is not None:
        # Best-effort detach so subsequent tests aren't polluted.
        try:
            np._GLOBAL_DEBUG_DETECTOR.__exit__(None, None, None)
        except Exception:
            pass
        np._GLOBAL_DEBUG_DETECTOR = None


class TestDebugNPlusOne:
    def test_unset_returns_none(self, monkeypatch, reset_global_detector):
        import dorm.contrib.nplusone as np
        from dorm.conf import settings

        monkeypatch.delattr(settings, "DEBUG_NPLUSONE", raising=False)
        assert np.install_debug_global() is None

    def test_log_mode_installs_detector(self, reset_global_detector):
        import dorm
        import dorm.contrib.nplusone as np

        dorm.configure(DEBUG_NPLUSONE=True, DEBUG_NPLUSONE_THRESHOLD=3)
        detector = np.install_debug_global()
        assert detector is not None
        assert detector.threshold == 3
        assert detector.raise_on_detect is False

    def test_raise_mode_installs_strict_detector(self, reset_global_detector):
        import dorm
        import dorm.contrib.nplusone as np

        dorm.configure(DEBUG_NPLUSONE="raise", DEBUG_NPLUSONE_THRESHOLD=2)
        detector = np.install_debug_global()
        assert detector is not None
        assert detector.raise_on_detect is True

    def test_idempotent(self, reset_global_detector):
        import dorm
        import dorm.contrib.nplusone as np

        dorm.configure(DEBUG_NPLUSONE=True)
        first = np.install_debug_global()
        second = np.install_debug_global()
        assert first is second


# ── migrations-graph CLI ─────────────────────────────────────────────────────


class TestMigrationsGraphCLI:
    def test_handler_is_callable(self):
        from dorm.cli import cmd_migrations_graph

        assert callable(cmd_migrations_graph)

    def test_invalid_format_exits(self, monkeypatch, capsys):
        from dorm.cli import cmd_migrations_graph

        class _Args:
            settings = None
            format = "svg"

        with pytest.raises(SystemExit):
            cmd_migrations_graph(_Args())


# ── reset CLI safety ─────────────────────────────────────────────────────────


class TestResetSafety:
    def test_handler_is_callable(self):
        from dorm.cli import cmd_reset

        assert callable(cmd_reset)
