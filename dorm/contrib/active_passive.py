"""Active / passive (primary / replica) router with auto-detection.

Database router that probes ``pg_is_in_recovery()`` on each alias
periodically and routes reads to whichever node is currently in
recovery (the replica). Writes always go to the alias whose probe
returns ``f`` (the primary). Useful when failover swaps the role of
two clusters and the application config doesn't know which is which
at boot.

Configuration::

    DATABASE_ROUTERS = [
        ActivePassiveRouter(
            aliases=["node_a", "node_b"],
            probe_seconds=10.0,
        ),
    ]

Probes run lazily on the next read / write after the cache expires;
no background thread.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

_log = logging.getLogger("dorm.contrib.active_passive")


class _Cache:
    primary: str | None = None
    replicas: list[str]
    last_probe: float = 0.0

    def __init__(self) -> None:
        self.replicas = []


class ActivePassiveRouter:
    """Database router that auto-discovers primary / replica.

    Args:
        aliases: candidate aliases that may serve traffic.
            Each is probed for ``pg_is_in_recovery()`` on schedule;
            the one returning ``f`` becomes the primary. The rest
            are treated as replicas.
        probe_seconds: cache TTL for the probe result. Default 10s.
        prefer_primary_for_writes: when True (default), writes go
            to the discovered primary. When False, the router
            returns ``None`` for writes (lets a higher-priority
            router decide).
        rng: optional deterministic RNG for replica choice in tests.
    """

    def __init__(
        self,
        *,
        aliases: list[str],
        probe_seconds: float = 10.0,
        prefer_primary_for_writes: bool = True,
        rng: Any = None,
    ) -> None:
        if not aliases or len(aliases) < 2:
            raise ValueError(
                "ActivePassiveRouter requires at least 2 candidate aliases"
            )
        if probe_seconds <= 0:
            raise ValueError("probe_seconds must be > 0")
        self.aliases = list(aliases)
        self.probe_seconds = probe_seconds
        self.prefer_primary_for_writes = prefer_primary_for_writes
        self._cache = _Cache()
        self._lock = threading.Lock()
        import random as _random

        self._rng = rng or _random.Random()

    def db_for_read(self, model, **hints) -> str:
        self._refresh()
        replicas = self._cache.replicas
        if replicas:
            return self._rng.choice(replicas)
        return self._cache.primary or self.aliases[0]

    def db_for_write(self, model, **hints) -> str | None:
        if not self.prefer_primary_for_writes:
            return None
        self._refresh()
        return self._cache.primary or self.aliases[0]

    def allow_relation(self, obj1, obj2, **hints) -> bool:
        return True

    def allow_migrate(self, db, app_label, **hints) -> bool:
        self._refresh()
        return db == (self._cache.primary or self.aliases[0])

    def _refresh(self) -> None:
        now = time.monotonic()
        with self._lock:
            if (now - self._cache.last_probe) < self.probe_seconds and (
                self._cache.primary is not None
            ):
                return
        primary: str | None = None
        replicas: list[str] = []
        for alias in self.aliases:
            try:
                from ..db.connection import get_connection

                conn = get_connection(alias)
                if getattr(conn, "vendor", None) != "postgresql":
                    # Non-PG → treat the first alias as primary.
                    primary = primary or alias
                    continue
                rows = conn.execute("SELECT pg_is_in_recovery() AS rec")
                in_recovery = bool(
                    rows and list(rows[0].values())[0]
                )
                if in_recovery:
                    replicas.append(alias)
                elif primary is None:
                    primary = alias
                else:
                    # Two primaries — split brain. Log and treat the
                    # second as a replica to keep traffic flowing.
                    _log.warning(
                        "ActivePassiveRouter: more than one node reports "
                        "primary status (%r and %r). Treating %r as "
                        "replica until next probe.",
                        primary,
                        alias,
                        alias,
                    )
                    replicas.append(alias)
            except Exception as exc:
                _log.warning(
                    "ActivePassiveRouter: probe of alias %r failed: %s",
                    alias,
                    exc,
                )
        with self._lock:
            self._cache.primary = primary
            self._cache.replicas = replicas
            self._cache.last_probe = now


__all__ = ["ActivePassiveRouter"]
