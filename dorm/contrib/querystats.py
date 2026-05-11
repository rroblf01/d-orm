"""Per-template aggregated query statistics.

While :mod:`dorm.contrib.prometheus` exposes per-vendor histograms,
``querystats`` aggregates **per SQL template** so dashboards can rank
the slowest distinct queries. Useful for capacity planning: "which 10
templates account for 90% of total DB time?".

Usage::

    from dorm.contrib.querystats import collector, render_text, render_json

    collector().enable()  # idempotent; connect once at app start

    # Later, expose at /metrics/querystats:
    return render_text()           # Prometheus text format
    # or:
    return json.dumps(render_json())  # JSON for custom dashboards

The collector is opt-in (off by default) — each query carries a small
template-normalisation cost. Disable in production via
``collector().disable()``.
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import Any

from .. import signals

# Literal-stripping regexes — same shape as querylog._template. Splits
# parameter values out of the SQL so two queries with the same shape
# (different bound values) aggregate into one template row.
_STRIP_PATTERNS = [
    (re.compile(r"\b\d+\b"), "?"),
    (re.compile(r"'[^']*'"), "?"),
    (re.compile(r'"[^"]*"'), '"?"'),
    (re.compile(r"\$\d+"), "?"),
    (re.compile(r"%s"), "?"),
]


def _template(sql: str) -> str:
    out = sql
    for pat, repl in _STRIP_PATTERNS:
        out = pat.sub(repl, out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


@dataclass
class TemplateStats:
    template: str
    count: int = 0
    total_ms: float = 0.0
    # Reservoir of timings used for percentile estimation. Bounded to
    # keep memory cost predictable on long-running apps — we use a
    # simple deterministic sliding window (the last ``_RESERVOIR_MAX``
    # samples) rather than a full reservoir sampler, which over-estimates
    # tail latency under bursty traffic. The trade-off is acceptable
    # for an ops dashboard.
    samples: list[float] = field(default_factory=list)

    def add(self, ms: float, *, reservoir_max: int) -> None:
        self.count += 1
        self.total_ms += ms
        if len(self.samples) >= reservoir_max:
            self.samples.pop(0)
        self.samples.append(ms)

    def percentile(self, q: float) -> float:
        """Nearest-rank percentile estimate on the current sample
        window. ``q`` in ``[0, 1]``."""
        if not self.samples:
            return 0.0
        idx = max(0, min(len(self.samples) - 1, int(round(q * (len(self.samples) - 1)))))
        return sorted(self.samples)[idx]


_RESERVOIR_MAX_DEFAULT = 1000


class _Collector:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: dict[str, TemplateStats] = {}
        self._enabled = False
        self._reservoir_max = _RESERVOIR_MAX_DEFAULT
        self._dispatch_uid = f"dorm-querystats-{id(self)}"

    def enable(self, *, reservoir_max: int = _RESERVOIR_MAX_DEFAULT) -> None:
        """Connect the post_query receiver. Idempotent."""
        self._reservoir_max = max(1, int(reservoir_max))
        if self._enabled:
            return
        signals.post_query.connect(
            self._on_query, weak=False, dispatch_uid=self._dispatch_uid
        )
        self._enabled = True

    def disable(self) -> None:
        if not self._enabled:
            return
        try:
            signals.post_query.disconnect(dispatch_uid=self._dispatch_uid)
        except Exception:  # pragma: no cover
            pass
        self._enabled = False

    def reset(self) -> None:
        """Drop every captured template. Useful between test runs and
        between deployment versions when the shape population
        changes."""
        with self._lock:
            self._stats.clear()

    def _on_query(self, sender: Any, **kwargs: Any) -> None:
        sql = str(kwargs.get("sql", ""))
        if not sql:
            return
        elapsed_ms = float(kwargs.get("elapsed_ms", 0.0))
        tpl = _template(sql)
        with self._lock:
            stat = self._stats.get(tpl)
            if stat is None:
                stat = TemplateStats(template=tpl)
                self._stats[tpl] = stat
            stat.add(elapsed_ms, reservoir_max=self._reservoir_max)

    def snapshot(self) -> list[TemplateStats]:
        """Return a copy of the current stats sorted by total_ms desc."""
        with self._lock:
            data = list(self._stats.values())
        return sorted(data, key=lambda s: s.total_ms, reverse=True)


_default_collector = _Collector()


def collector() -> _Collector:
    """Return the module-level singleton collector."""
    return _default_collector


def reset() -> None:
    """Drop every captured template on the default collector."""
    _default_collector.reset()


def render_text() -> str:
    """Render the snapshot in Prometheus text-exposition format.

    Emits one ``dorm_template_count``, ``dorm_template_total_ms``,
    ``dorm_template_p50_ms``, ``dorm_template_p95_ms`` and
    ``dorm_template_p99_ms`` line per template. The template string is
    truncated to 120 chars in the label value to bound metric
    cardinality.
    """
    snapshot = _default_collector.snapshot()
    lines: list[str] = [
        "# HELP dorm_template_count Times a SQL template ran.",
        "# TYPE dorm_template_count counter",
        "# HELP dorm_template_total_ms Cumulative time per SQL template.",
        "# TYPE dorm_template_total_ms counter",
        "# HELP dorm_template_p50_ms Estimated p50 latency per template.",
        "# TYPE dorm_template_p50_ms gauge",
        "# HELP dorm_template_p95_ms Estimated p95 latency per template.",
        "# TYPE dorm_template_p95_ms gauge",
        "# HELP dorm_template_p99_ms Estimated p99 latency per template.",
        "# TYPE dorm_template_p99_ms gauge",
    ]
    for stat in snapshot:
        label = stat.template[:120].replace("\\", "\\\\").replace('"', '\\"')
        suffix = f'{{template="{label}"}}'
        lines.append(f"dorm_template_count{suffix} {stat.count}")
        lines.append(f"dorm_template_total_ms{suffix} {stat.total_ms:.3f}")
        lines.append(
            f"dorm_template_p50_ms{suffix} {stat.percentile(0.50):.3f}"
        )
        lines.append(
            f"dorm_template_p95_ms{suffix} {stat.percentile(0.95):.3f}"
        )
        lines.append(
            f"dorm_template_p99_ms{suffix} {stat.percentile(0.99):.3f}"
        )
    return "\n".join(lines) + "\n"


def render_json() -> list[dict[str, Any]]:
    """Return the snapshot as a list of dicts. Ready to feed
    ``json.dumps`` for a custom dashboard endpoint."""
    return [
        {
            "template": s.template,
            "count": s.count,
            "total_ms": round(s.total_ms, 3),
            "p50_ms": round(s.percentile(0.50), 3),
            "p95_ms": round(s.percentile(0.95), 3),
            "p99_ms": round(s.percentile(0.99), 3),
        }
        for s in _default_collector.snapshot()
    ]


__all__ = [
    "collector",
    "reset",
    "render_text",
    "render_json",
    "TemplateStats",
]
