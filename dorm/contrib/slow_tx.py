"""Slow-transaction detector.

Wraps :func:`dorm.transaction.atomic` so atomic blocks that exceed
``settings.SLOW_TX_MS`` (default 1000 ms) emit a WARNING and attach a
``dorm.slow_tx`` event to the active OTel span.

Activate by calling :func:`install` once at process start. The
detector is opt-in because every atomic exit pays a small timing
cost; production deployments can flip it on without code changes.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from .. import transaction

_log = logging.getLogger("dorm.contrib.slow_tx")
_installed: list[Any] = []


def install(threshold_ms: float | None = None) -> None:
    """Wrap :func:`dorm.transaction.atomic` so each atomic exit
    records its duration. Idempotent — repeat calls preserve the
    first wrapping.

    *threshold_ms* overrides the setting ``SLOW_TX_MS`` for the
    current process.
    """
    if _installed:
        return
    if threshold_ms is None:
        try:
            from ..conf import settings

            threshold_ms = float(getattr(settings, "SLOW_TX_MS", 1000.0))
        except Exception:
            threshold_ms = 1000.0
    original = transaction.atomic

    class _TimedAtomicWrapper:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._inner = original(*args, **kwargs)
            self._start: float = 0.0

        def __enter__(self) -> Any:
            self._start = time.perf_counter()
            return self._inner.__enter__()

        def __exit__(self, exc_type, exc, tb) -> bool:
            try:
                return bool(self._inner.__exit__(exc_type, exc, tb))
            finally:
                _record_duration(time.perf_counter() - self._start, threshold_ms)

        # Decorator usage: ``@atomic`` without parens — delegate to
        # the original.
        def __call__(self, func: Any) -> Any:
            return self._inner(func)

    def _patched(*args: Any, **kwargs: Any) -> Any:
        # Distinguish decorator (``@atomic``) vs ctx manager call.
        if args and callable(args[0]) and not isinstance(args[0], str):
            return original(*args, **kwargs)
        return _TimedAtomicWrapper(*args, **kwargs)

    transaction.atomic = _patched  # type: ignore[assignment]  # ty:ignore[invalid-assignment]
    _installed.append((original, threshold_ms))


def uninstall() -> None:
    """Restore the original :func:`atomic`. Idempotent."""
    if not _installed:
        return
    original, _ = _installed.pop(0)
    transaction.atomic = original  # type: ignore[assignment]


def _record_duration(elapsed_s: float, threshold_ms: float) -> None:
    elapsed_ms = elapsed_s * 1000.0
    if elapsed_ms < threshold_ms:
        return
    _log.warning(
        "slow transaction (%.2fms >= %.0fms)", elapsed_ms, threshold_ms
    )
    try:
        from opentelemetry import trace  # type: ignore[import-not-found]
    except ImportError:
        return
    span = trace.get_current_span()
    if span and span.is_recording():
        try:
            span.add_event(
                "dorm.slow_tx",
                {"elapsed_ms": elapsed_ms, "threshold_ms": threshold_ms},
            )
        except Exception:  # pragma: no cover
            pass


__all__ = ["install", "uninstall"]
