"""ASGI middleware for FastAPI / Starlette / Litestar / Quart / Sanic.

Three middlewares ship in this module — all of them framework-agnostic
ASGI-3 wrappers that compose around any inner ``app``:

- :class:`QueryBudgetMiddleware` — opens a :func:`dorm.budget.abudget`
  block per request so a slow handler can't blow past an HTTP SLA.
- :class:`NPlusOneMiddleware` — opens a
  :class:`dorm.contrib.nplusone.NPlusOneDetector` block (in log-only
  mode by default) so accidental N+1 patterns surface in logs / OTel
  spans without breaking production traffic.
- :class:`OTelDormMiddleware` — opens a parent span around each
  request and enriches it with dorm's per-query attributes (when
  ``dorm.contrib.otel`` is wired up). Lets distributed-tracing UIs
  group every DB span under the originating HTTP request.

All three are pure ASGI — they do not depend on FastAPI, Litestar, or
any other framework. Wire them at app-construction time::

    from dorm.contrib.asgi import (
        NPlusOneMiddleware,
        OTelDormMiddleware,
        QueryBudgetMiddleware,
    )

    app = FastAPI()
    app.add_middleware(NPlusOneMiddleware, threshold=10)
    app.add_middleware(QueryBudgetMiddleware, timeout_ms=2000, max_rows=10_000)
    app.add_middleware(OTelDormMiddleware)

Order matters — the outermost middleware (last ``add_middleware``)
sees the request first. OTel goes outermost so every later span
(budget, dorm queries) is parented to the request span.
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

# ASGI type aliases — kept loose so this module imports cleanly without
# pulling Starlette / FastAPI.
_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_App = Callable[[_Scope, _Receive, _Send], Awaitable[None]]

_log = logging.getLogger("dorm.contrib.asgi")


class QueryBudgetMiddleware:
    """ASGI middleware that wraps every HTTP request in a
    :func:`dorm.budget.abudget` block.

    Args:
        app: the inner ASGI application.
        timeout_ms: wall-clock ceiling per statement, in milliseconds.
            Enforced on PostgreSQL via ``SET LOCAL statement_timeout``.
            ``None`` skips the timeout. Default 2000.
        max_rows: cap on rows materialised by any one query. Backend-
            agnostic. ``None`` skips the row cap.
        using: connection alias the budget applies to. Default
            ``"default"``.

    Non-HTTP scopes (``lifespan``, ``websocket``) bypass the middleware
    and forward straight to the inner app — a websocket connection
    typically lives for many minutes, so wrapping it in a per-request
    budget would be wrong.

    Exceptions from inside the budget propagate unchanged; the budget
    cleanup runs in a ``finally`` so a 5xx response still releases the
    DB connection cleanly.

    .. note::

       When *timeout_ms* is set and the configured backend is
       PostgreSQL, :func:`dorm.budget.abudget` opens an implicit
       ``aatomic()`` block so it can ``SET LOCAL statement_timeout``.
       This means **every write inside one request is wrapped in a
       single transaction**: a 5xx response rolls back every write
       together. Set ``timeout_ms=None`` to opt out of the implicit
       transaction (and the statement-timeout feature). SQLite and
       other backends are unaffected — *timeout_ms* is a no-op
       there.
    """

    def __init__(
        self,
        app: _App,
        *,
        timeout_ms: int | None = 2000,
        max_rows: int | None = None,
        using: str = "default",
    ) -> None:
        self.app = app
        self.timeout_ms = timeout_ms
        self.max_rows = max_rows
        self.using = using

    async def __call__(
        self, scope: _Scope, receive: _Receive, send: _Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        from ..budget import abudget

        async with abudget(
            timeout_ms=self.timeout_ms,
            max_rows=self.max_rows,
            using=self.using,
        ):
            await self.app(scope, receive, send)


class NPlusOneMiddleware:
    """ASGI middleware that wraps every HTTP request in an
    :class:`~dorm.contrib.nplusone.NPlusOneDetector` block.

    Args:
        app: the inner ASGI application.
        threshold: a SQL template firing more than this many times in
            one request is flagged. Default 10.
        raise_on_detect: when True, exceeded thresholds raise
            ``NPlusOneError`` and the request returns a 5xx. When
            False (the default — safer for production), every finding
            is logged at WARNING level and the request completes
            normally.
        ignore: optional iterable of SQL substrings the detector
            should skip.
    """

    def __init__(
        self,
        app: _App,
        *,
        threshold: int = 10,
        raise_on_detect: bool = False,
        ignore: tuple[str, ...] | None = None,
    ) -> None:
        self.app = app
        self.threshold = threshold
        self.raise_on_detect = raise_on_detect
        self.ignore = ignore

    async def __call__(
        self, scope: _Scope, receive: _Receive, send: _Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        from .nplusone import NPlusOneDetector

        kwargs: dict[str, Any] = {
            "threshold": self.threshold,
            "raise_on_detect": self.raise_on_detect,
        }
        if self.ignore is not None:
            kwargs["ignore"] = self.ignore
        detector = NPlusOneDetector(**kwargs)
        with detector:
            await self.app(scope, receive, send)
        # Findings outside the ``with`` block — the detector aggregates
        # everything on __exit__, so reading here is safe. Log every
        # finding for the inevitable post-mortem.
        if not self.raise_on_detect and detector.findings:
            method = scope.get("method", "?")
            path = scope.get("path", "?")
            for template, count in detector.findings:
                _log.warning(
                    "N+1 detected on %s %s: %s ran %d times (threshold=%d)",
                    method,
                    path,
                    template,
                    count,
                    self.threshold,
                )


class OTelDormMiddleware:
    """ASGI middleware that opens an OpenTelemetry parent span around
    each HTTP request so every dorm-emitted DB span is parented to the
    originating request span. Composes with
    :func:`dorm.contrib.otel.instrument` (which provides the child
    spans).

    Span name follows ``"HTTP <METHOD> <path>"``; status is
    derived from the response status code (5xx → ``Error``).

    No-op when ``opentelemetry-api`` isn't importable — the
    middleware simply forwards to the inner app, so callers don't
    have to gate it conditionally.
    """

    def __init__(
        self,
        app: _App,
        *,
        tracer_name: str = "dorm.contrib.asgi",
    ) -> None:
        self.app = app
        self.tracer_name = tracer_name
        # Resolve the tracer lazily — at import time the SDK might not
        # be configured yet.
        self._tracer: Any | None = None
        self._tracer_resolved = False

    def _get_tracer(self) -> Any | None:
        if self._tracer_resolved:
            return self._tracer
        self._tracer_resolved = True
        try:
            from opentelemetry import trace  # type: ignore[import-not-found]
        except ImportError:
            self._tracer = None
            return None
        self._tracer = trace.get_tracer(self.tracer_name)
        return self._tracer

    async def __call__(
        self, scope: _Scope, receive: _Receive, send: _Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        tracer = self._get_tracer()
        if tracer is None:
            await self.app(scope, receive, send)
            return

        from opentelemetry.trace import Status, StatusCode  # type: ignore[import-not-found]

        method = scope.get("method", "GET")
        path = scope.get("path", "/")
        span_name = f"HTTP {method} {path}"

        status_holder: dict[str, int] = {}

        async def _send_wrapper(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                status_holder["status"] = int(message.get("status", 200))
            await send(message)

        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("http.method", method)
            span.set_attribute("http.target", path)
            try:
                await self.app(scope, receive, _send_wrapper)
            except Exception as exc:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
                raise
            else:
                status = status_holder.get("status", 200)
                span.set_attribute("http.status_code", status)
                if status >= 500:
                    span.set_status(Status(StatusCode.ERROR))


__all__ = [
    "QueryBudgetMiddleware",
    "NPlusOneMiddleware",
    "OTelDormMiddleware",
]
