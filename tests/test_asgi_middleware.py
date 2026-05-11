"""Tests for the ASGI middleware module — pure ASGI semantics, no
framework needed.

Each test drives the middleware via a minimal ASGI scope dict and a
tiny inner-app coroutine. We focus on:

- Non-HTTP scopes pass straight through.
- Budget / detector context managers actually wrap the inner call.
- Findings get logged when raise_on_detect=False.
- OTel middleware is a no-op when the SDK isn't installed.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

import dorm
from dorm.contrib.asgi import (
    NPlusOneMiddleware,
    OTelDormMiddleware,
    QueryBudgetMiddleware,
)

dorm.configure(
    DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
    INSTALLED_APPS=["tests"],
)


async def _noop_receive() -> dict[str, Any]:
    return {"type": "http.request"}


def _make_send_recorder() -> tuple[list[dict[str, Any]], Any]:
    sent: list[dict[str, Any]] = []

    async def _send(message: dict[str, Any]) -> None:
        sent.append(message)

    return sent, _send


def _http_scope(path: str = "/test", method: str = "GET") -> dict[str, Any]:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
    }


async def _200_app(scope, receive, send) -> None:
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


# ── QueryBudgetMiddleware ────────────────────────────────────────────────────


class TestQueryBudgetMiddleware:
    def test_http_scope_invokes_inner_app(self):
        mw = QueryBudgetMiddleware(_200_app, timeout_ms=None)
        sent, send = _make_send_recorder()
        asyncio.run(mw(_http_scope(), _noop_receive, send))
        assert [m["type"] for m in sent] == [
            "http.response.start",
            "http.response.body",
        ]

    def test_lifespan_scope_bypasses(self):
        calls = {"n": 0}

        async def app(scope, receive, send):
            calls["n"] += 1

        mw = QueryBudgetMiddleware(app, timeout_ms=None)
        asyncio.run(
            mw({"type": "lifespan"}, _noop_receive, _make_send_recorder()[1])
        )
        assert calls["n"] == 1

    def test_websocket_scope_bypasses(self):
        called = {"n": 0}

        async def app(scope, receive, send):
            called["n"] += 1

        mw = QueryBudgetMiddleware(app)
        asyncio.run(
            mw({"type": "websocket"}, _noop_receive, _make_send_recorder()[1])
        )
        assert called["n"] == 1

    def test_budget_state_visible_inside_handler(self):
        seen: dict[str, Any] = {}

        async def app(scope, receive, send):
            from dorm.budget import current

            state = current()
            seen["max_rows"] = state.max_rows if state else None
            seen["timeout_ms"] = state.timeout_ms if state else None
            await _200_app(scope, receive, send)

        mw = QueryBudgetMiddleware(
            app, timeout_ms=None, max_rows=1000
        )
        sent, send = _make_send_recorder()
        asyncio.run(mw(_http_scope(), _noop_receive, send))
        assert seen["max_rows"] == 1000

    def test_inner_exception_propagates(self):
        async def app(scope, receive, send):
            raise RuntimeError("boom")

        mw = QueryBudgetMiddleware(app, timeout_ms=None)
        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(mw(_http_scope(), _noop_receive, _make_send_recorder()[1]))


# ── NPlusOneMiddleware ───────────────────────────────────────────────────────


class TestNPlusOneMiddleware:
    def test_http_scope_invokes_inner_app(self):
        mw = NPlusOneMiddleware(_200_app)
        sent, send = _make_send_recorder()
        asyncio.run(mw(_http_scope(), _noop_receive, send))
        assert len(sent) == 2

    def test_non_http_bypasses(self):
        called = {"n": 0}

        async def app(scope, receive, send):
            called["n"] += 1

        mw = NPlusOneMiddleware(app)
        asyncio.run(
            mw({"type": "lifespan"}, _noop_receive, _make_send_recorder()[1])
        )
        assert called["n"] == 1

    def test_findings_logged_when_not_raising(self, caplog):
        # Emit pre_query signals manually to simulate query traffic.
        from dorm.signals import pre_query

        async def app(scope, receive, send):
            for _ in range(15):
                pre_query.send(sender=None, sql="SELECT * FROM t WHERE x=1", params=())
            await _200_app(scope, receive, send)

        mw = NPlusOneMiddleware(app, threshold=5, raise_on_detect=False)
        with caplog.at_level(logging.WARNING, logger="dorm.contrib.asgi"):
            asyncio.run(mw(_http_scope(), _noop_receive, _make_send_recorder()[1]))
        assert any("N+1 detected" in rec.message for rec in caplog.records)


# ── OTelDormMiddleware ───────────────────────────────────────────────────────


class TestOTelDormMiddleware:
    def test_no_op_when_otel_missing(self, monkeypatch):
        # Force the import lookup to fail so we exercise the fallback.
        import builtins

        real_import = builtins.__import__

        def _fail_otel(name, *args, **kwargs):
            if name.startswith("opentelemetry"):
                raise ImportError("simulated")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fail_otel)
        mw = OTelDormMiddleware(_200_app)
        sent, send = _make_send_recorder()
        asyncio.run(mw(_http_scope(), _noop_receive, send))
        assert len(sent) == 2  # still forwards normally

    def test_lifespan_bypass(self):
        called = {"n": 0}

        async def app(scope, receive, send):
            called["n"] += 1

        mw = OTelDormMiddleware(app)
        asyncio.run(
            mw({"type": "lifespan"}, _noop_receive, _make_send_recorder()[1])
        )
        assert called["n"] == 1
