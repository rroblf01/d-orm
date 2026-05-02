"""Request-scoped query log collector.

Captures every SQL statement executed inside a ``QueryLog`` block,
along with elapsed time and DB alias. Used by:

- :class:`QueryLogASGIMiddleware` to expose the per-request log on
  ``scope["dorm_querylog"]`` for downstream handlers.
- Tests that want to assert exact query content / shape, not just count.
- Dev-time profiling — group by SQL template and report p50 / p95 to
  surface the query of the 1% slow.

Per-task isolation via :class:`ScopedCollector` over ``post_query``.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator

from .. import signals
from .._scoped import ScopedCollector

# Compile templates: replace every literal ``?`` and ``$N`` placeholder
# with a single ``?`` so two queries that differ only in their bound
# values group together. Note the regex matches placeholders inside
# string literals too — for grouping purposes that's fine (the SQL
# shape collapses to the same template); it'd only matter to a SQL
# parser, which is overkill for a dev-tool collector.
_PLACEHOLDER_RE = re.compile(r"\$\d+|%s|\?")


def _template(sql: str) -> str:
    return _PLACEHOLDER_RE.sub("?", sql)


@dataclass(slots=True)
class QueryRecord:
    sql: str
    params: Any
    # Vendor / engine name (``"sqlite"``, ``"postgresql"``, ``"libsql"``)
    # — sourced from the ``sender`` of the ``post_query`` signal.
    # We surface ``vendor`` rather than ``alias`` because the
    # signal payload doesn't carry the alias today; if a future
    # release threads ``alias`` through ``log_query`` we can add
    # the field without breaking the existing one.
    vendor: str
    elapsed_ms: float
    error: BaseException | None

    def template(self) -> str:
        return _template(self.sql)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "params": list(self.params) if self.params else [],
            "vendor": self.vendor,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "error": (
                f"{type(self.error).__name__}: {self.error}"
                if self.error is not None
                else None
            ),
        }


@dataclass(slots=True)
class TemplateStats:
    template: str
    count: int
    total_ms: float
    p50_ms: float
    p95_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "template": self.template,
            "count": self.count,
            "total_ms": round(self.total_ms, 3),
            "p50_ms": round(self.p50_ms, 3),
            "p95_ms": round(self.p95_ms, 3),
        }


def _record(state: list[QueryRecord], kwargs: dict[str, Any]) -> None:
    state.append(
        QueryRecord(
            sql=str(kwargs.get("sql", "")),
            params=kwargs.get("params"),
            vendor=str(kwargs.get("sender", "")),
            elapsed_ms=float(kwargs.get("elapsed_ms", 0.0)),
            error=kwargs.get("error"),
        )
    )


_collector: ScopedCollector[list[QueryRecord]] = ScopedCollector(
    signals.post_query, "dorm_querylog_active", _record
)


@dataclass(slots=False)
class QueryLog:
    """Context manager that captures every query in its scope.

    Usage::

        with QueryLog() as log:
            do_work()
        for record in log.records:
            print(record.sql, record.elapsed_ms)
        print(log.summary())
    """

    records: list[QueryRecord] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._token: Any = None

    def __enter__(self) -> "QueryLog":
        self._token = _collector.open(self.records)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _collector.close(self._token)
            self._token = None

    @property
    def total_ms(self) -> float:
        return sum(r.elapsed_ms for r in self.records)

    @property
    def count(self) -> int:
        return len(self.records)

    def summary(self) -> list[TemplateStats]:
        """Group captured records by SQL template; return stats sorted
        by descending total time. p95 uses nearest-rank — fine for the
        ~tens of queries a typical request issues."""
        by_tpl: dict[str, list[float]] = {}
        for rec in self.records:
            by_tpl.setdefault(rec.template(), []).append(rec.elapsed_ms)
        out: list[TemplateStats] = []
        for tpl, timings in by_tpl.items():
            sorted_t = sorted(timings)
            n = len(sorted_t)
            p50 = sorted_t[n // 2] if n else 0.0
            p95_idx = max(0, min(n - 1, int(round(0.95 * (n - 1)))))
            p95 = sorted_t[p95_idx] if n else 0.0
            out.append(
                TemplateStats(
                    template=tpl,
                    count=n,
                    total_ms=sum(timings),
                    p50_ms=p50,
                    p95_ms=p95,
                )
            )
        out.sort(key=lambda s: s.total_ms, reverse=True)
        return out


@contextmanager
def query_log() -> Iterator[QueryLog]:
    """Function-style alias for :class:`QueryLog`."""
    log = QueryLog()
    with log:
        yield log


class QueryLogASGIMiddleware:
    """Minimal ASGI middleware that wraps each request in a
    :class:`QueryLog` and stashes the result on
    ``scope["dorm_querylog"]`` so the downstream handler can read /
    log it.

    Provider-agnostic — works with FastAPI, Starlette, Quart, Sanic,
    anything that speaks ASGI 3."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):  # noqa: ANN001
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        with QueryLog() as log:
            scope["dorm_querylog"] = log
            await self.app(scope, receive, send)


__all__ = [
    "QueryLog",
    "QueryRecord",
    "TemplateStats",
    "query_log",
    "QueryLogASGIMiddleware",
]
