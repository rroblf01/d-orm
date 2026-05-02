"""Generic memoised settings resolver.

Wraps the ``settings → env var → default`` resolution pattern shared by
every per-call knob in the codebase (slow-query threshold, retry
attempts, retry backoff, …). Each instance memoises only when the
value comes from an explicit ``configure(...)`` call — env-var and
default branches are re-read on each call so test
``monkeypatch.setenv`` workflows keep observing the current value
without an explicit cache flush.

Why centralise:

- Each knob used to spell out ~40 lines of boilerplate (sentinel,
  resolver, getter with isinstance fallback, invalidator). Adding
  another knob meant copying the lot.
- ``conf.configure(...)`` had to grow an ``if "X" in kwargs: invalidate_X()``
  block per knob. Now a single ``invalidate_all_for(kwargs)`` walks the
  registry.
"""

from __future__ import annotations

import os
import threading
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")

# Sentinel for the unresolved-cache state. ``None`` is a valid resolved
# value (it disables knobs that opt into ``allow_none=True``), so we
# can't reuse ``None`` as the "not yet computed" marker.
_UNSET: object = object()

# Process-global registry so ``configure(...)`` can fan an invalidation
# pulse to every memoised setting that the user touched in the call.
_REGISTRY: dict[str, "MemoizedSetting[Any]"] = {}
_REGISTRY_LOCK = threading.Lock()


class MemoizedSetting(Generic[T]):
    """Lazy reader for a single setting.

    Construct one per knob at module-import time::

        SLOW_QUERY_MS = MemoizedSetting(
            "SLOW_QUERY_MS",
            env_var="DORM_SLOW_QUERY_MS",
            default=500.0,
            parser=float,
            allow_none=True,
        )

    Then call ``SLOW_QUERY_MS.get()`` on the hot path. The resolved
    value is memoised iff it came from ``settings.<NAME>`` — the
    env-var / default branch is read fresh every call (cost: one
    ``os.environ.get``).
    """

    __slots__ = (
        "name",
        "env_var",
        "default",
        "parser",
        "allow_none",
        "_cache",
        "_lock",
    )

    def __init__(
        self,
        name: str,
        *,
        env_var: str | None,
        default: T,
        parser: Callable[[Any], T],
        allow_none: bool = False,
    ) -> None:
        self.name = name
        self.env_var = env_var
        self.default = default
        self.parser = parser
        self.allow_none = allow_none
        # Memoised value or ``_UNSET``. The cache holds either a
        # parser-output of type ``T`` or ``None`` (when allow_none).
        self._cache: T | None | object = _UNSET
        self._lock = threading.Lock()
        with _REGISTRY_LOCK:
            _REGISTRY[name] = self

    # ── Resolution ──────────────────────────────────────────────────────────

    def _resolve(self) -> tuple[T | None, bool]:
        """Return ``(value, cacheable)``. Cacheable iff the value came
        from an explicit ``configure(...)`` override."""
        try:
            from .conf import settings

            explicit = getattr(settings, "_explicit_settings", set())
            if self.name in explicit:
                raw = getattr(settings, self.name, _UNSET)
                if raw is not _UNSET:
                    if raw is None and self.allow_none:
                        return None, True
                    try:
                        return self.parser(raw), True
                    except (TypeError, ValueError):
                        pass
        except Exception:
            pass

        if self.env_var is not None:
            raw_env = os.environ.get(self.env_var)
            if raw_env is not None:
                try:
                    return self.parser(raw_env), False
                except (TypeError, ValueError):
                    pass

        return self.default, False

    def get(self) -> T | None:
        cached = self._cache
        if cached is not _UNSET:
            # Defensive narrow: if external code mutated the cache to a
            # bogus type, drop it and re-resolve. ``isinstance`` is a
            # real runtime check (unlike ``assert``) so ``python -O``
            # doesn't disarm it.
            if cached is None or isinstance(cached, type(self.default)):
                return cached  # type: ignore[return-value]
            self._cache = _UNSET
        val, cacheable = self._resolve()
        if cacheable:
            self._cache = val
        return val

    def invalidate(self) -> None:
        with self._lock:
            self._cache = _UNSET


def invalidate_all_for(kwargs: dict[str, Any]) -> None:
    """Invalidate every registered memoised setting whose name appears
    in *kwargs*. Called by ``conf.configure(...)`` so a runtime swap
    takes effect on the next ``get()``."""
    if not kwargs:
        return
    with _REGISTRY_LOCK:
        snapshot = dict(_REGISTRY)
    for name, ms in snapshot.items():
        if name in kwargs:
            ms.invalidate()


def get_registered(name: str) -> MemoizedSetting[Any] | None:
    return _REGISTRY.get(name)


__all__ = [
    "MemoizedSetting",
    "invalidate_all_for",
    "get_registered",
]
