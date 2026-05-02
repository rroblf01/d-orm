"""In-process LRU cache backend.

A drop-in :class:`BaseCache` implementation that lives in the
process's own memory. Useful for:

- Tests that want to exercise the queryset cache path without a
  Redis dependency.
- Single-process scripts where a process-local cache is good enough
  (one worker, no fan-out across replicas).
- Layered setups where an in-process LRU sits in front of Redis to
  cut hot-key round-trip latency (build a thin
  ``LayeredCache(local, remote)`` on top).

NOT recommended for multi-worker deployments — each gunicorn /
uvicorn worker holds its own copy of the dict, so writes by one
worker are invisible to siblings until the version-counter bump
on the model invalidates them.

Configuration::

    CACHES = {
        "default": {
            "BACKEND": "dorm.cache.locmem.LocMemCache",
            "OPTIONS": {"maxsize": 1024},
            "TTL": 300,
        },
    }

The ``maxsize`` option caps the dict; once full, the least-recently
used entry is evicted on the next ``set``. Async helpers delegate to
the sync ones so the contract matches Redis.
"""

from __future__ import annotations

import fnmatch
import threading
import time
from collections import OrderedDict

from . import BaseCache


class LocMemCache(BaseCache):
    """Thread-safe LRU cache held in a single ``OrderedDict``.

    The instance is shared across the process — :func:`get_cache` in
    ``dorm.cache`` memoises one per alias. Coarse-grained lock around
    every operation; the values it caches are SQL-row payloads, so
    contention is generally bounded by query rate, not lock granularity.
    """

    def __init__(self, cfg: dict | None = None) -> None:
        self._store: OrderedDict[str, tuple[bytes, float | None]] = OrderedDict()
        self._lock = threading.Lock()
        cfg = cfg or {}
        opts = cfg.get("OPTIONS") or {}
        self._maxsize: int = int(opts.get("maxsize", 1024))
        self._default_timeout: int = int(cfg.get("TTL", 300))

    # ── Sync ─────────────────────────────────────────────────────────────────

    def get(self, key: str) -> bytes | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if expires_at is not None and expires_at <= time.monotonic():
                # Lazy expiration: drop and treat as a miss. Avoids a
                # background sweeper for what is meant to be a tiny
                # in-process cache.
                del self._store[key]
                return None
            # LRU touch: move the read entry to the end of the order.
            self._store.move_to_end(key)
            return value

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
        ttl = timeout if timeout is not None else self._default_timeout
        expires_at = time.monotonic() + ttl if ttl else None
        with self._lock:
            self._store[key] = (value, expires_at)
            self._store.move_to_end(key)
            while len(self._store) > self._maxsize:
                # popitem(last=False) drops the least-recently used.
                self._store.popitem(last=False)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def delete_pattern(self, pattern: str) -> int:
        with self._lock:
            victims = [k for k in self._store if fnmatch.fnmatchcase(k, pattern)]
            for k in victims:
                del self._store[k]
            return len(victims)

    def clear(self) -> None:
        """Drop every entry. Test-helper — not part of the
        :class:`BaseCache` contract but handy in fixtures."""
        with self._lock:
            self._store.clear()

    # ── Async (delegate to sync — operations are CPU-only) ───────────────────

    async def aget(self, key: str) -> bytes | None:
        return self.get(key)

    async def aset(self, key: str, value: bytes, timeout: int | None = None) -> None:
        self.set(key, value, timeout)

    async def adelete(self, key: str) -> None:
        self.delete(key)

    async def adelete_pattern(self, pattern: str) -> int:
        return self.delete_pattern(pattern)


__all__ = ["LocMemCache"]
