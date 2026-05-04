"""Connection-pool autoscaling helpers.

Reads pool utilisation from the active backend wrapper and resizes
the underlying pool when the in-use / max-size ratio crosses a
configurable threshold. Designed as a building block — callers
schedule the scaling loop themselves (FastAPI startup task, asyncio
task, cron job), since the right cadence depends on traffic shape.

Usage::

    from dorm.contrib.pool_autoscale import autoscale_pool

    # Inside an asyncio task that fires every 10s:
    while True:
        autoscale_pool(target_utilization=0.7, min_floor=2, max_ceiling=20)
        await asyncio.sleep(10)

PostgreSQL (psycopg-pool) is the only backend with a real pool that
supports live resize. SQLite and MySQL return ``None`` from
:func:`autoscale_pool` — there's no shared pool to grow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any


_log = logging.getLogger("dorm.contrib.pool_autoscale")


@dataclass(frozen=True)
class PoolStats:
    """Normalised view of pool occupancy across backends.

    Backend dialects:

    - PostgreSQL (psycopg-pool) — every field is populated.
    - SQLite / MySQL — ``vendor`` is set, ``open`` is the truth,
      everything else is ``0`` / ``None`` (no shared pool).
    """

    vendor: str
    open: bool
    min_size: int = 0
    max_size: int = 0
    pool_size: int = 0
    in_use: int = 0
    available: int = 0
    waiting: int = 0

    @property
    def utilization(self) -> float:
        """Ratio of in-use connections to ``max_size``. ``0.0`` when
        the pool isn't open or ``max_size`` is unknown."""
        if not self.open or self.max_size <= 0:
            return 0.0
        return self.in_use / self.max_size


def read_pool_stats(using: str = "default") -> PoolStats:
    """Snapshot the active sync pool for *using*.

    Calls into the backend's ``pool_stats()``. The PG wrapper returns
    a dict shaped by ``psycopg_pool.ConnectionPool.get_stats``; this
    function normalises the keys we care about into :class:`PoolStats`
    so consumers don't have to feature-check.
    """
    from ..db.connection import get_connection

    conn = get_connection(using)
    raw = getattr(conn, "pool_stats", None)
    if not callable(raw):
        return PoolStats(
            vendor=getattr(conn, "vendor", "unknown"), open=False
        )
    data: dict[str, Any] = raw() or {}
    return _normalise(data)


def _normalise(data: dict[str, Any]) -> PoolStats:
    """Map a backend's ``pool_stats()`` dict to :class:`PoolStats`.

    psycopg-pool keys (subset we read):

    - ``pool_size`` — total open connections
    - ``pool_available`` — idle slots
    - ``requests_waiting`` — queued requests blocked on checkout
    - ``in_use`` — derived as ``pool_size - pool_available`` when
      psycopg-pool doesn't expose it directly (varies by version).
    """
    vendor = data.get("vendor", "unknown")
    open_ = bool(data.get("open", False))
    min_size = int(data.get("min_size", 0) or 0)
    max_size = int(data.get("max_size", 0) or 0)
    pool_size = int(data.get("pool_size", 0) or 0)
    available = int(data.get("pool_available", 0) or 0)
    waiting = int(data.get("requests_waiting", 0) or 0)
    in_use = data.get("in_use")
    if in_use is None:
        # Older psycopg-pool releases don't ship ``in_use`` — derive
        # it from pool_size and available so the metric stays
        # comparable across versions.
        in_use = max(0, pool_size - available)
    return PoolStats(
        vendor=vendor,
        open=open_,
        min_size=min_size,
        max_size=max_size,
        pool_size=pool_size,
        in_use=int(in_use),
        available=available,
        waiting=waiting,
    )


def autoscale_pool(
    *,
    target_utilization: float = 0.7,
    min_floor: int = 2,
    max_ceiling: int = 20,
    step: int = 2,
    using: str = "default",
) -> tuple[int, int] | None:
    """Resize the pool when utilisation crosses *target_utilization*.

    Returns ``(new_min, new_max)`` after the resize, or ``None`` when
    no change was needed (pool not open, non-PG backend, or already
    at the bound).

    Heuristic:

    - **Grow** (``+step`` to ``max_size``) when utilisation is above
      ``target_utilization`` *or* there's at least one queued request.
      Capped at ``max_ceiling``.
    - **Shrink** (``-step`` from ``max_size``) when utilisation is
      below ``target_utilization / 2`` AND there are no queued
      requests. Floored at ``max(min_floor, current min_size)``.
    - Otherwise, no-op.

    The ``min_size`` is left untouched — psycopg-pool keeps a warm
    floor of connections on hand, and tuning that is a different
    concern than scaling the *ceiling*.
    """
    if step < 1:
        raise ValueError("autoscale_pool: step must be >= 1")
    if min_floor > max_ceiling:
        raise ValueError(
            "autoscale_pool: min_floor cannot exceed max_ceiling"
        )

    from ..db.connection import get_connection

    conn = get_connection(using)
    pool = getattr(conn, "_pool", None)
    if pool is None:
        return None
    resize = getattr(pool, "resize", None)
    if not callable(resize):
        return None

    stats = read_pool_stats(using)
    if not stats.open:
        return None

    current_min = stats.min_size
    current_max = stats.max_size
    if current_max <= 0:
        return None

    util = stats.utilization
    new_max = current_max
    if util >= target_utilization or stats.waiting > 0:
        new_max = min(current_max + step, max_ceiling)
    elif util < target_utilization / 2.0 and stats.waiting == 0:
        new_max = max(current_max - step, max(min_floor, current_min))

    if new_max == current_max:
        return None

    try:
        resize(min_size=current_min, max_size=new_max)
    except TypeError:
        # psycopg-pool older versions accept positional args only.
        resize(current_min, new_max)
    except Exception as exc:  # noqa: BLE001 — log + bail, don't crash hot path
        _log.warning("pool resize failed: %r", exc)
        return None

    # Reflect the new ceiling on the wrapper so subsequent
    # ``pool_stats()`` reads see it.
    if hasattr(conn, "_max_size"):
        conn._max_size = new_max
    _log.info(
        "pool resized: util=%.2f waiting=%d max %d -> %d",
        util,
        stats.waiting,
        current_max,
        new_max,
    )
    return current_min, new_max


__all__ = ["PoolStats", "autoscale_pool", "read_pool_stats"]
