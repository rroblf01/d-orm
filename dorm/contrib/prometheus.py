"""Prometheus exposition for dorm runtime metrics.

Stdlib-only — no dependency on the official ``prometheus_client``
package. Emits the text exposition format (version 0.0.4) that
Prometheus / VictoriaMetrics / Datadog Agent / Grafana Agent
all consume.

Usage with FastAPI / Starlette / any ASGI app::

    from dorm.contrib.prometheus import metrics_response, install

    install()  # connect counters / histograms to dorm signals

    @app.get("/metrics")
    def metrics():
        return Response(content=metrics_response(), media_type="text/plain; version=0.0.4")

Metrics produced:

- ``dorm_queries_total{vendor,alias,outcome}``     — counter
- ``dorm_query_duration_seconds{vendor,alias}``    — histogram
- ``dorm_pool_size{alias}``                        — gauge (PG only)
- ``dorm_pool_in_use{alias}``                      — gauge (PG only)
- ``dorm_cache_hits_total{alias}``                 — counter (queryset cache)
- ``dorm_cache_misses_total{alias}``               — counter

The implementation is intentionally tiny: histograms use a fixed
bucket layout (1 ms → 5 s, doubling) so the exposition is one short
loop. Apps that need richer / configurable buckets should swap to
``prometheus_client`` and translate from the same dorm signals.
"""

from __future__ import annotations

import threading
from typing import Any

from .. import signals

_lock = threading.Lock()

_HISTOGRAM_BUCKETS_S: tuple[float, ...] = (
    0.001, 0.002, 0.005,
    0.01, 0.02, 0.05,
    0.1, 0.25, 0.5,
    1.0, 2.5, 5.0,
)

# State shared by all collectors. Histograms and counters are
# bucketed by (vendor, outcome) — the ``post_query`` signal carries
# ``sender`` (vendor name) but not the DB alias, so an ``alias``
# label would always be empty. If a future release threads ``alias``
# through ``log_query``, plumb it back in here.
_query_counter: dict[tuple[str, str], int] = {}
_duration_buckets: dict[str, list[int]] = {}
_duration_sum: dict[str, float] = {}
_duration_count: dict[str, int] = {}

_cache_hits: dict[str, int] = {}
_cache_misses: dict[str, int] = {}

_installed: bool = False


def _bump_query(vendor: str, outcome: str) -> None:
    key = (vendor, outcome)
    with _lock:
        _query_counter[key] = _query_counter.get(key, 0) + 1


def _observe_duration(vendor: str, seconds: float) -> None:
    with _lock:
        bucket_counts = _duration_buckets.setdefault(
            vendor, [0] * len(_HISTOGRAM_BUCKETS_S)
        )
        # Each bucket is "<= upper bound": increment every bucket
        # whose upper bound is ≥ the observation. Standard Prometheus
        # cumulative-histogram shape.
        for i, bound in enumerate(_HISTOGRAM_BUCKETS_S):
            if seconds <= bound:
                bucket_counts[i] += 1
        _duration_sum[vendor] = _duration_sum.get(vendor, 0.0) + seconds
        _duration_count[vendor] = _duration_count.get(vendor, 0) + 1


def _on_post_query(sender: Any, **kwargs: Any) -> None:
    vendor = str(sender)
    error = kwargs.get("error")
    outcome = "error" if error is not None else "ok"
    _bump_query(vendor, outcome)
    elapsed_ms = float(kwargs.get("elapsed_ms", 0.0))
    _observe_duration(vendor, elapsed_ms / 1000.0)


def install() -> None:
    """Connect the receiver. Idempotent.

    Called explicitly so projects that don't expose metrics pay no
    overhead from the post-query timing fan-out.
    """
    global _installed
    with _lock:
        if _installed:
            return
        signals.post_query.connect(_on_post_query, weak=False)
        _installed = True


def uninstall() -> None:
    """Disconnect the receiver and reset every counter / histogram."""
    global _installed
    with _lock:
        if _installed:
            signals.post_query.disconnect(_on_post_query)
            _installed = False
        _query_counter.clear()
        _duration_buckets.clear()
        _duration_sum.clear()
        _duration_count.clear()
        _cache_hits.clear()
        _cache_misses.clear()


def record_cache_hit(alias: str = "default") -> None:
    """Optional helper for cache backends — call from your custom
    ``BaseCache.get`` when the read landed a hit. ``RedisCache`` /
    ``LocMemCache`` don't call this automatically yet so apps that
    don't care pay zero overhead."""
    with _lock:
        _cache_hits[alias] = _cache_hits.get(alias, 0) + 1


def record_cache_miss(alias: str = "default") -> None:
    with _lock:
        _cache_misses[alias] = _cache_misses.get(alias, 0) + 1


# ── Exposition ───────────────────────────────────────────────────────────────


def _label_pairs(**labels: str) -> str:
    """Render ``{a="b",c="d"}`` with proper escaping."""
    parts = []
    for k in sorted(labels):
        v = labels[k].replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        parts.append(f'{k}="{v}"')
    return "{" + ",".join(parts) + "}"


def _pool_lines() -> list[str]:
    """Best-effort poll of every active sync alias's pool stats."""
    lines: list[str] = []
    try:
        from ..db.connection import _sync_connections, pool_stats
    except ImportError:
        return lines
    aliases = list(_sync_connections.keys())
    if not aliases:
        return lines
    lines.append("# HELP dorm_pool_size Connections currently open in the pool.")
    lines.append("# TYPE dorm_pool_size gauge")
    for alias in aliases:
        try:
            stats = pool_stats(alias)
        except Exception:
            continue
        if not stats:
            continue
        size = stats.get("pool_size")
        if size is not None:
            lines.append(f"dorm_pool_size{_label_pairs(alias=alias)} {size}")
        in_use = stats.get("requests_num")
        if in_use is None:
            in_use = stats.get("pool_size", 0) - (stats.get("pool_available") or 0)
        if in_use is not None:
            lines.append(
                f"dorm_pool_in_use{_label_pairs(alias=alias)} {in_use}"
            )
    return lines


def metrics_response() -> str:
    """Return the Prometheus text-exposition payload as ``str``.

    Wrap in your framework's ``Response`` helper. Output is a string;
    the canonical content type is ``text/plain; version=0.0.4``.
    """
    out: list[str] = []
    with _lock:
        # Counter: queries
        if _query_counter:
            out.append("# HELP dorm_queries_total Total executed SQL statements.")
            out.append("# TYPE dorm_queries_total counter")
            for (vendor, outcome), n in _query_counter.items():
                labels = _label_pairs(vendor=vendor, outcome=outcome)
                out.append(f"dorm_queries_total{labels} {n}")

        # Histogram: query duration
        if _duration_buckets:
            out.append("# HELP dorm_query_duration_seconds Query duration histogram.")
            out.append("# TYPE dorm_query_duration_seconds histogram")
            for vendor, counts in _duration_buckets.items():
                for i, bound in enumerate(_HISTOGRAM_BUCKETS_S):
                    labels = _label_pairs(le=str(bound), vendor=vendor)
                    out.append(
                        f"dorm_query_duration_seconds_bucket{labels} {counts[i]}"
                    )
                # +Inf bucket equals the total count.
                total = _duration_count.get(vendor, 0)
                labels = _label_pairs(le="+Inf", vendor=vendor)
                out.append(f"dorm_query_duration_seconds_bucket{labels} {total}")
                base = _label_pairs(vendor=vendor)
                out.append(
                    f"dorm_query_duration_seconds_sum{base} "
                    f"{_duration_sum.get(vendor, 0.0)}"
                )
                out.append(f"dorm_query_duration_seconds_count{base} {total}")

        # Counters: cache hit/miss
        if _cache_hits or _cache_misses:
            out.append("# HELP dorm_cache_hits_total Queryset cache hits.")
            out.append("# TYPE dorm_cache_hits_total counter")
            for alias, n in _cache_hits.items():
                out.append(
                    f"dorm_cache_hits_total{_label_pairs(alias=alias)} {n}"
                )
            out.append("# HELP dorm_cache_misses_total Queryset cache misses.")
            out.append("# TYPE dorm_cache_misses_total counter")
            for alias, n in _cache_misses.items():
                out.append(
                    f"dorm_cache_misses_total{_label_pairs(alias=alias)} {n}"
                )

    out.extend(_pool_lines())
    return "\n".join(out) + "\n"


__all__ = [
    "install",
    "uninstall",
    "metrics_response",
    "record_cache_hit",
    "record_cache_miss",
]
