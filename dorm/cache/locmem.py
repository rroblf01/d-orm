"""In-process LRU cache backend.

Drop-in :class:`BaseCache` implementation that lives in the process's
own memory. Useful for tests, single-process scripts, or as a layer
in front of Redis.

NOT recommended for multi-worker deployments — each gunicorn /
uvicorn worker holds its own copy of the dict, so writes by one
worker are invisible to siblings until the version-counter bump on
the model invalidates them.

Configuration::

    CACHES = {
        "default": {
            "BACKEND": "dorm.cache.locmem.LocMemCache",
            "OPTIONS": {"maxsize": 1024},
            "TTL": 300,
        },
    }
"""

from __future__ import annotations

import fnmatch
import threading
import time
from collections import OrderedDict, defaultdict

from . import BaseCache


class LocMemCache(BaseCache):
    """Thread-safe LRU with a secondary prefix index.

    The primary store is an ``OrderedDict`` (LRU). A secondary
    ``defaultdict(set)`` indexes keys by their first-colon prefix —
    every dorm cache key uses ``namespace:specifics`` shape, so
    ``delete_pattern("dormqs:app.User:*")`` finds matches in
    O(matches) instead of O(n) scanning the whole store. Patterns
    that aren't a literal-prefix-followed-by-glob still fall back to
    the full ``fnmatch`` scan.
    """

    def __init__(self, cfg: dict | None = None) -> None:
        self._store: OrderedDict[str, tuple[bytes, float | None]] = OrderedDict()
        # ``prefix → set of full keys`` for prefix-style invalidation.
        self._by_prefix: defaultdict[str, set[str]] = defaultdict(set)
        self._lock = threading.Lock()
        cfg = cfg or {}
        opts = cfg.get("OPTIONS") or {}
        self._maxsize: int = int(opts.get("maxsize", 1024))
        self._default_timeout: int = int(cfg.get("TTL", 300))

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _prefix_of(key: str) -> str:
        # Last ``:`` carves off the variable suffix (pk, version, hash);
        # everything before it is the model / queryset namespace.
        return key.rsplit(":", 1)[0]

    def _index_add(self, key: str) -> None:
        self._by_prefix[self._prefix_of(key)].add(key)

    def _index_remove(self, key: str) -> None:
        prefix = self._prefix_of(key)
        bucket = self._by_prefix.get(prefix)
        if bucket is None:
            return
        bucket.discard(key)
        if not bucket:
            del self._by_prefix[prefix]

    # ── Sync ─────────────────────────────────────────────────────────────────

    def get(self, key: str) -> bytes | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if expires_at is not None and expires_at <= time.monotonic():
                # Lazy expiration.
                del self._store[key]
                self._index_remove(key)
                return None
            self._store.move_to_end(key)
            return value

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:
        ttl = timeout if timeout is not None else self._default_timeout
        expires_at = time.monotonic() + ttl if ttl else None
        with self._lock:
            already = key in self._store
            self._store[key] = (value, expires_at)
            self._store.move_to_end(key)
            if not already:
                self._index_add(key)
            while len(self._store) > self._maxsize:
                evicted_key, _ = self._store.popitem(last=False)
                self._index_remove(evicted_key)

    def delete(self, key: str) -> None:
        with self._lock:
            if self._store.pop(key, None) is not None:
                self._index_remove(key)

    def delete_pattern(self, pattern: str) -> int:
        with self._lock:
            # Fast path: ``prefix:*`` — most dorm invalidations look
            # like this. Drop the trailing ``*`` and look the bucket
            # up directly. Patterns without a globbed suffix or with
            # globs in the middle fall back to the full scan.
            if pattern.endswith(":*") and "*" not in pattern[:-2] and "?" not in pattern[:-2]:
                prefix = pattern[:-2]
                bucket = self._by_prefix.pop(prefix, None)
                if not bucket:
                    return 0
                for k in bucket:
                    self._store.pop(k, None)
                return len(bucket)
            victims = [k for k in self._store if fnmatch.fnmatchcase(k, pattern)]
            for k in victims:
                del self._store[k]
                self._index_remove(k)
            return len(victims)

    def clear(self) -> None:
        """Drop every entry. Test-helper — not part of the
        :class:`BaseCache` contract."""
        with self._lock:
            self._store.clear()
            self._by_prefix.clear()

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
