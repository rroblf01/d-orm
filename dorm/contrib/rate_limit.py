"""Token-bucket rate limiter for in-process throttling.

Lightweight, dependency-free. For cluster-wide limiting wire Redis
behind it (the bucket state object is plain dict-shaped — substitute
the storage transparently).

Usage::

    from dorm.contrib.rate_limit import TokenBucket, rate_limited

    bucket = TokenBucket(rate_per_second=10, burst=20)

    @rate_limited(bucket, key=lambda *a, **kw: a[0].ip)
    def signup(request, ...):
        ...

    # Or check manually:
    if not bucket.allow("user-42"):
        raise TooManyRequests
"""
from __future__ import annotations

import functools
import threading
import time
from collections import OrderedDict
from typing import Any, Callable


class TooManyRequests(Exception):
    """Raised by :func:`rate_limited` when the bucket is empty."""


class TokenBucket:
    """In-memory token bucket. Each :meth:`allow` call removes one
    token from the bucket if available; refills happen at
    ``rate_per_second`` tokens / sec, capped at ``burst``.

    Per-key state is kept in a dict guarded by a single lock. For
    high-throughput workloads with many distinct keys, swap the
    storage by subclassing.
    """

    def __init__(
        self,
        *,
        rate_per_second: float,
        burst: int,
        max_keys: int = 100_000,
    ) -> None:
        """*max_keys* caps the per-key map so workloads with
        unbounded key cardinality (per-IP, per-token) can't leak
        memory. When the cap is hit, the least-recently-touched key
        is dropped — its bucket implicitly resets to full burst on
        the next ``allow`` call, so the cap is observable as
        occasional "leniency" near saturation rather than denied
        requests."""
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        if max_keys < 1:
            raise ValueError("max_keys must be >= 1")
        self.rate = rate_per_second
        self.burst = burst
        self.max_keys = max_keys
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        """Return True iff the bucket for *key* has at least one token,
        consuming it. False otherwise."""
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(self.burst), now))
            elapsed = max(0.0, now - last)
            tokens = min(self.burst, tokens + elapsed * self.rate)
            granted = tokens >= 1.0
            new_tokens = tokens - 1.0 if granted else tokens
            self._buckets[key] = (new_tokens, now)
            self._buckets.move_to_end(key)
            # Evict LRU when above the cap.
            while len(self._buckets) > self.max_keys:
                self._buckets.popitem(last=False)
            return granted

    def reset(self, key: str | None = None) -> None:
        """Drop one key (when given) or every key (when omitted)."""
        with self._lock:
            if key is None:
                self._buckets.clear()
            else:
                self._buckets.pop(key, None)


def rate_limited(
    bucket: TokenBucket,
    *,
    key: Callable[..., str] = lambda *a, **kw: "default",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that raises :class:`TooManyRequests` when *bucket*
    denies the call. ``key=`` derives the bucket key from the
    function arguments (defaults to a single shared bucket)."""

    def _decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            k = key(*args, **kwargs)
            if not bucket.allow(k):
                raise TooManyRequests(
                    f"rate limit exceeded for key={k!r}"
                )
            return func(*args, **kwargs)

        return _wrap

    return _decorate


__all__ = ["TokenBucket", "rate_limited", "TooManyRequests"]
