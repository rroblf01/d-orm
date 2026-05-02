"""Request-scoped query log collector.

Captures every SQL statement executed inside a ``QueryLog`` block,
along with elapsed time, alias, and (optionally) the call-site
stack frame. Useful for:

- Inspecting the query mix per FastAPI / Starlette request via the
  :class:`QueryLogASGIMiddleware` middleware.
- Asserting in tests that a code path doesn't regress on query
  count / duration.
- Surfacing N+1 patterns by grouping captured statements by SQL
  template (placeholders normalised to ``?``).

The collector connects ``post_query`` lazily on first use and uses
a ``ContextVar`` so concurrent ASGI requests / asyncio tasks see
isolated logs.
"""

from __future__ import annotations

import contextvars
import re
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .. import signals

# Per-task list of captured ``QueryRecord``s. ``None`` = no
# ``QueryLog`` is active on this task.
_active_log: contextvars.ContextVar[list[Any] | None] = contextvars.ContextVar(
    "dorm_querylog_active", default=None
)

_listener_attached: bool = False
_listener_lock = threading.Lock()


# Compile templates: replace every literal ``?`` and ``$N`` placeholder
# with a single ``?`` so two queries that differ only in their bound
# values group together. Done in a single pass to keep grouping cheap
# under hot loops.
_PLACEHOLDER_RE = re.compile(r"\$\d+|%s|\?")


def _template(sql: str) -> str:
    return _PLACEHOLDER_RE.sub("?", sql)


class QueryRecord:
    """A single captured SQL statement.

    Attributes:
        sql        — the raw SQL text as emitted by the backend
        params     — tuple of bound parameters (may be ``None``)
        alias      — the DB alias the query went through
        elapsed_ms — float, milliseconds the statement took
        error      — exception raised, or ``None`` on success
    """

    __slots__ = ("sql", "params", "alias", "elapsed_ms", "error")

    def __init__(
        self,
        sql: str,
        params: Any,
        alias: str,
        elapsed_ms: float,
        error: BaseException | None,
    ) -> None:
        self.sql = sql
        self.params = params
        self.alias = alias
        self.elapsed_ms = elapsed_ms
        self.error = error

    def template(self) -> str:
        return _template(self.sql)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "params": list(self.params) if self.params else [],
            "alias": self.alias,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "error": (
                f"{type(self.error).__name__}: {self.error}"
                if self.error is not None
                else None
            ),
        }


def _on_post_query(sender: Any, **kwargs: Any) -> None:
    log = _active_log.get()
    if log is None:
        return
    log.append(
        QueryRecord(
            sql=str(kwargs.get("sql", "")),
            params=kwargs.get("params"),
            alias=str(sender),
            elapsed_ms=float(kwargs.get("elapsed_ms", 0.0)),
            error=kwargs.get("error"),
        )
    )


def _ensure_listener() -> None:
    global _listener_attached
    if _listener_attached:
        return
    with _listener_lock:
        if _listener_attached:
            return
        signals.post_query.connect(_on_post_query, weak=False)
        _listener_attached = True


class QueryLog:
    """Context manager that captures every query in its scope.

    Usage::

        with QueryLog() as log:
            do_work()
        for record in log.records:
            print(record.sql, record.elapsed_ms)
        print(log.summary())

    The ``records`` attribute holds raw entries; ``summary()`` groups
    them by SQL template and computes count / total / p95 timings.
    Raw entries survive after the block exits, so a test can assert
    on them directly.
    """

    def __init__(self) -> None:
        self.records: list[QueryRecord] = []
        self._token: contextvars.Token[Any] | None = None

    def __enter__(self) -> "QueryLog":
        _ensure_listener()
        self._token = _active_log.set(self.records)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _active_log.reset(self._token)
            self._token = None

    @property
    def total_ms(self) -> float:
        return sum(r.elapsed_ms for r in self.records)

    @property
    def count(self) -> int:
        return len(self.records)

    def summary(self) -> list[dict[str, Any]]:
        """Group captured records by SQL template and return a list of
        ``{"template", "count", "total_ms", "p50_ms", "p95_ms"}``
        dicts sorted by descending ``total_ms``."""
        by_tpl: dict[str, list[float]] = {}
        for rec in self.records:
            by_tpl.setdefault(rec.template(), []).append(rec.elapsed_ms)
        out: list[dict[str, Any]] = []
        for tpl, timings in by_tpl.items():
            sorted_t = sorted(timings)
            n = len(sorted_t)
            p50 = sorted_t[n // 2] if n else 0.0
            # p95 with nearest-rank — fine for the ~tens of queries a
            # request typically issues; for richer percentiles use
            # numpy on ``self.records`` directly.
            p95_idx = max(0, min(n - 1, int(round(0.95 * (n - 1)))))
            p95 = sorted_t[p95_idx] if n else 0.0
            out.append(
                {
                    "template": tpl,
                    "count": n,
                    "total_ms": round(sum(timings), 3),
                    "p50_ms": round(p50, 3),
                    "p95_ms": round(p95, 3),
                }
            )
        out.sort(key=lambda d: d["total_ms"], reverse=True)
        return out


@contextmanager
def query_log() -> Iterator[QueryLog]:
    """Function-style alias for :class:`QueryLog`. Useful when type
    hints over ``with QueryLog()`` get awkward."""
    log = QueryLog()
    with log:
        yield log


class QueryLogASGIMiddleware:
    """Minimal ASGI middleware that wraps each request in a
    :class:`QueryLog` and stashes the result on
    ``scope["dorm_querylog"]`` so the downstream handler can read /
    log it.

    Intentionally provider-agnostic — works with FastAPI,
    Starlette, Quart, Sanic, anything that speaks ASGI 3. For sync
    WSGI integrations, wrap the view function with the
    :class:`QueryLog` context manager directly.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):  # noqa: ANN001
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        with QueryLog() as log:
            scope["dorm_querylog"] = log
            await self.app(scope, receive, send)


__all__ = ["QueryLog", "QueryRecord", "query_log", "QueryLogASGIMiddleware"]
