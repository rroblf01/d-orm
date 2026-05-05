"""Lag-aware read replica router.

Standard practice for read-scaled PostgreSQL deployments is to ship
write traffic to the primary and fan reads out across one or more
streaming replicas. The risk: when a replica falls behind (lag spike,
network blip, slow recovery), reads from it return *stale* data —
sometimes seconds behind the primary. For data the application
expects to be authoritative (a payment status, a fresh user record),
that is a correctness bug, not a latency bug.

This router checks ``pg_stat_replication.replay_lag`` (or
``pg_stat_wal_receiver`` on the replica side) before sending a read
to the replica. When lag exceeds ``max_lag_seconds`` the router
deflects the read to the primary instead — slower, but correct.

Usage::

    from dorm.contrib.lag_router import LagAwareReadRouter

    DATABASES = {
        "primary": {...},
        "replica_1": {...},
        "replica_2": {...},
    }
    DATABASE_ROUTERS = [
        LagAwareReadRouter(
            primary="primary",
            replicas=["replica_1", "replica_2"],
            max_lag_seconds=2.0,
        ),
    ]

The router caches the last lag check for ``cache_seconds`` so every
single ``filter()`` call doesn't re-query ``pg_stat_replication``.
The default 5 s window is the right tradeoff for a typical web app
— short enough to react to a spike before too many requests are
served stale, long enough to amortise the cost.
"""

from __future__ import annotations

import logging
import random
import threading
import time as _time
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger("dorm.contrib.lag_router")


@dataclass
class _ReplicaState:
    alias: str
    last_lag_seconds: float
    last_check_at: float
    healthy: bool


class LagAwareReadRouter:
    """Database router that consults each replica's lag before
    routing a read.

    Args:
        primary: alias of the writeable primary.
        replicas: list of read-replica aliases.
        max_lag_seconds: replicas with lag above this threshold are
            considered unhealthy and skipped. Default 2.0.
        cache_seconds: how long to cache a replica's lag reading
            before re-checking. Default 5.0.
        rng: optional pre-seeded ``random.Random`` for deterministic
            shuffling in tests.
    """

    def __init__(
        self,
        *,
        primary: str = "default",
        replicas: list[str],
        max_lag_seconds: float = 2.0,
        cache_seconds: float = 5.0,
        rng: random.Random | None = None,
    ) -> None:
        if not replicas:
            raise ValueError("LagAwareReadRouter requires at least one replica")
        if max_lag_seconds <= 0:
            raise ValueError("max_lag_seconds must be > 0")
        if cache_seconds <= 0:
            raise ValueError("cache_seconds must be > 0")
        self.primary = primary
        self.replicas = list(replicas)
        self.max_lag_seconds = max_lag_seconds
        self.cache_seconds = cache_seconds
        self._rng = rng or random.Random()
        self._state: dict[str, _ReplicaState] = {}
        self._lock = threading.Lock()

    # ── Public router protocol ───────────────────────────────────────────────
    def db_for_read(self, model, **hints) -> str:
        """Pick a healthy replica or fall back to primary."""
        candidates = [r for r in self.replicas if self._is_healthy(r)]
        if not candidates:
            _log.warning(
                "lag_router: every replica is over the lag threshold "
                "(%.1fs); deflecting reads to primary %r",
                self.max_lag_seconds,
                self.primary,
            )
            return self.primary
        return self._rng.choice(candidates)

    def db_for_write(self, model, **hints) -> str:
        return self.primary

    def allow_relation(self, obj1, obj2, **hints):
        # Cross-replica relations are fine when both sides eventually
        # converge to the same primary.
        return True

    def allow_migrate(self, db, app_label, **hints):
        # Migrations only run on primary — replicas mirror its state
        # via streaming replication.
        return db == self.primary

    # ── Internals ────────────────────────────────────────────────────────────
    def _is_healthy(self, alias: str) -> bool:
        # Cache lookup under the same lock as the eventual write —
        # otherwise two threads could race the probe + write and end
        # up issuing duplicate probes against the replica.
        now = _time.monotonic()
        with self._lock:
            cached = self._state.get(alias)
            if (
                cached is not None
                and (now - cached.last_check_at) < self.cache_seconds
            ):
                return cached.healthy

        # Probe outside the lock — the network call can take tens of
        # milliseconds and we don't want to serialise every reader on
        # it. Worst case under contention: two callers both probe
        # concurrently and only the second's write wins. Acceptable.
        lag = self._measure_lag(alias)
        with self._lock:
            healthy = lag is not None and lag <= self.max_lag_seconds
            self._state[alias] = _ReplicaState(
                alias=alias,
                last_lag_seconds=lag if lag is not None else float("inf"),
                last_check_at=now,
                healthy=healthy,
            )
        if not healthy:
            _log.info(
                "lag_router: replica %r unhealthy (lag=%s, threshold=%.1fs)",
                alias,
                "?" if lag is None else f"{lag:.2f}s",
                self.max_lag_seconds,
            )
        return healthy

    def _measure_lag(self, alias: str) -> float | None:
        """Query ``pg_last_xact_replay_timestamp()`` against the
        replica and compare to ``now()``. Returns ``None`` when the
        replica is unreachable or the value cannot be parsed.

        On non-PostgreSQL aliases the helper returns 0.0 so the
        replica is treated as healthy — ``LagAwareReadRouter`` is a
        PG-only concept; users wiring it for other backends opt in
        deliberately and accept "always healthy" semantics.
        """
        from ..db.connection import get_connection

        try:
            conn = get_connection(alias)
        except Exception:
            return None
        if getattr(conn, "vendor", "sqlite") != "postgresql":
            return 0.0
        try:
            rows = conn.execute(
                "SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) AS lag"
            )
        except Exception:
            return None
        if not rows:
            return None
        lag = rows[0].get("lag")
        if lag is None:
            # Replica is in standby but not actively replaying — treat
            # as 0 lag (it is fully caught up by definition).
            return 0.0
        try:
            return float(lag)
        except (TypeError, ValueError):
            return None

    # ── Inspection ───────────────────────────────────────────────────────────
    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return a snapshot of every replica's last-known state.

        Useful in a Prometheus exporter or a debug endpoint::

            GET /debug/lag → {"replica_1": {"lag": 0.4, "healthy": true}, ...}
        """
        out: dict[str, dict[str, Any]] = {}
        for alias, st in self._state.items():
            out[alias] = {
                "lag_seconds": st.last_lag_seconds,
                "healthy": st.healthy,
                "checked_at": st.last_check_at,
            }
        return out

    def reset(self) -> None:
        """Drop the lag cache. Forces a fresh probe on the next call."""
        with self._lock:
            self._state.clear()


__all__ = ["LagAwareReadRouter"]
