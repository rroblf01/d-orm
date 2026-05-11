"""Litestar plugin — first-class integration helpers.

The ASGI middleware in :mod:`dorm.contrib.asgi` works untouched under
Litestar, but the plugin path below wires everything up with one call
and adds lifespan-aware connection management:

- ``dorm_plugin(...)`` returns a :class:`litestar.plugins.InitPluginProtocol`-
  compatible plugin that installs the middlewares with Litestar's own
  ``DefineMiddleware`` wrapper (so OpenAPI / route-scoped overrides
  still work) and registers async startup / shutdown hooks that warm
  the connection pool and drain it cleanly on shutdown.
- ``DormPlugin`` is the class form for users who prefer
  ``app = Litestar(plugins=[DormPlugin()])``.

Both forms tolerate Litestar being absent — importing this module
without ``pip install litestar`` raises ``ImportError`` with a hint.
The other paths in dorm don't import this module, so a SQLite-only
install isn't forced to pull Litestar.
"""
from __future__ import annotations

from typing import Any

try:
    from litestar.config.app import AppConfig  # type: ignore[import-not-found]
    from litestar.di import Provide  # type: ignore[import-not-found]  # noqa: F401
    from litestar.middleware.base import DefineMiddleware  # type: ignore[import-not-found]
    from litestar.plugins import InitPluginProtocol  # type: ignore[import-not-found]
except ImportError as _e:  # pragma: no cover - exercised by skipif gate
    raise ImportError(
        "dorm.contrib.litestar requires litestar. Install with "
        "`pip install litestar` (or `djanorm[litestar]` when the extra "
        "ships)."
    ) from _e

from .asgi import (
    NPlusOneMiddleware,
    OTelDormMiddleware,
    QueryBudgetMiddleware,
)


class DormPlugin(InitPluginProtocol):
    """Litestar plugin that wires up query-budget, N+1 detection,
    OTel parent spans, and lifespan-aware pool management.

    Args:
        budget_timeout_ms: per-statement timeout enforced on PG via
            ``SET LOCAL statement_timeout``. ``None`` skips the budget
            middleware. Default 2000.
        budget_max_rows: row cap per query. ``None`` skips.
        nplusone_threshold: queries-per-template before an N+1 finding
            fires. ``None`` skips the N+1 middleware.
        nplusone_raise: when True, exceeded thresholds raise inside
            the request. Default False (log-only).
        otel: enable :class:`OTelDormMiddleware`. Default True.
        warmup_pool: open up to this many connections at startup on
            PostgreSQL. ``None`` skips warmup. Default None.
        using: connection alias the middlewares apply to.
    """

    def __init__(
        self,
        *,
        budget_timeout_ms: int | None = 2000,
        budget_max_rows: int | None = None,
        nplusone_threshold: int | None = 10,
        nplusone_raise: bool = False,
        otel: bool = True,
        warmup_pool: int | None = None,
        using: str = "default",
    ) -> None:
        self.budget_timeout_ms = budget_timeout_ms
        self.budget_max_rows = budget_max_rows
        self.nplusone_threshold = nplusone_threshold
        self.nplusone_raise = nplusone_raise
        self.otel = otel
        self.warmup_pool = warmup_pool
        self.using = using

    async def _on_startup(self) -> None:
        if self.warmup_pool is None:
            return
        try:
            from .pool_autoscale import warmup_pool
        except ImportError:
            return
        try:
            warmup_pool(target=self.warmup_pool, using=self.using)
        except Exception:  # pragma: no cover - best-effort warmup
            # A failing warmup must not block the app — the first real
            # request will rediscover the failure.
            import logging

            logging.getLogger("dorm.contrib.litestar").warning(
                "Pool warmup failed for alias %r — continuing",
                self.using,
                exc_info=True,
            )

    async def _on_shutdown(self) -> None:
        try:
            from ..db.connection import close_all, close_all_async
        except ImportError:
            return
        # Async pools must be drained from their own event loop;
        # ``close_all`` only handles sync pools. We invoke both — the
        # sync side is a fast no-op when no sync alias was used.
        try:
            await close_all_async()
        except Exception:  # pragma: no cover
            pass
        try:
            close_all()
        except Exception:  # pragma: no cover
            pass

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        middlewares: list[Any] = list(app_config.middleware or [])
        if self.otel:
            middlewares.append(DefineMiddleware(OTelDormMiddleware))
        if self.budget_timeout_ms is not None or self.budget_max_rows is not None:
            middlewares.append(
                DefineMiddleware(
                    QueryBudgetMiddleware,
                    timeout_ms=self.budget_timeout_ms,
                    max_rows=self.budget_max_rows,
                    using=self.using,
                )
            )
        if self.nplusone_threshold is not None:
            middlewares.append(
                DefineMiddleware(
                    NPlusOneMiddleware,
                    threshold=self.nplusone_threshold,
                    raise_on_detect=self.nplusone_raise,
                )
            )
        app_config.middleware = middlewares

        # Hook into lifespan via on_startup / on_shutdown lists.
        startup = list(app_config.on_startup or [])
        startup.append(self._on_startup)
        app_config.on_startup = startup

        shutdown = list(app_config.on_shutdown or [])
        shutdown.append(self._on_shutdown)
        app_config.on_shutdown = shutdown
        return app_config


def dorm_plugin(**kwargs: Any) -> DormPlugin:
    """Functional shortcut: ``app = Litestar(plugins=[dorm_plugin()])``.

    Accepts the same kwargs as :class:`DormPlugin`."""
    return DormPlugin(**kwargs)


__all__ = ["DormPlugin", "dorm_plugin"]
