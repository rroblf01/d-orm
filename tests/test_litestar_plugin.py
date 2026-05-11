"""Smoke tests for the Litestar plugin — skipped automatically when
Litestar isn't installed (the package is an optional integration)."""
from __future__ import annotations

import pytest

litestar = pytest.importorskip("litestar")


def test_plugin_installs_middlewares_and_hooks():
    from litestar.config.app import AppConfig  # type: ignore[import-not-found]  # ty:ignore[unresolved-import]

    from dorm.contrib.litestar import DormPlugin

    plugin = DormPlugin(
        budget_timeout_ms=500,
        budget_max_rows=100,
        nplusone_threshold=5,
        otel=False,
    )
    config = AppConfig()
    new_config = plugin.on_app_init(config)
    # Two middlewares wired (budget + N+1; OTel disabled in this test).
    assert len(new_config.middleware) == 2
    assert len(new_config.on_startup) == 1
    assert len(new_config.on_shutdown) == 1


def test_otel_flag_adds_middleware():
    from litestar.config.app import AppConfig  # type: ignore[import-not-found]  # ty:ignore[unresolved-import]

    from dorm.contrib.litestar import DormPlugin

    plugin = DormPlugin(otel=True, nplusone_threshold=None, budget_timeout_ms=None)
    config = AppConfig()
    new_config = plugin.on_app_init(config)
    # Only OTel wired.
    assert len(new_config.middleware) == 1


def test_dorm_plugin_shortcut():
    from dorm.contrib.litestar import DormPlugin, dorm_plugin

    assert isinstance(dorm_plugin(otel=False), DormPlugin)
