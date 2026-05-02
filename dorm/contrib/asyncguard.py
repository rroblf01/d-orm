"""Detect sync ORM calls inside a running event loop.

When you accidentally call ``Model.objects.get(...)`` (sync) from an
``async def`` view, the call blocks the event loop for the duration
of the query — every other request on the worker stalls. The bug is
silent: the call works, just slowly.

``enable_async_guard()`` connects to the ``pre_query`` signal and
checks whether the calling thread has a running ``asyncio`` loop. If
it does *and* the call came through the synchronous backend path,
the guard reacts according to its mode:

- ``"warn"`` (default) — emits a single ``WARNING`` per offending
  call site (logger ``dorm.asyncguard``).
- ``"raise"`` — raises ``SyncCallInAsyncContext`` immediately,
  surfacing the bug as a 500 instead of as latent slowness.
- ``"raise_first"`` — raise on the first occurrence, then downgrade
  to warning to avoid spamming.

Off in production by default; enable in tests / dev to catch the
pattern early. The guard is a no-op when no event loop is running
on the calling thread.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from typing import Any, Literal

from .. import signals

# CPython sets this flag on a code object that defines an ``async def``
# function. Walking up the call stack and checking ``co_flags`` is the
# cheapest reliable way to tell whether the SQL came in through dorm's
# async path (no warning needed) or its sync path (the bug we want to
# surface).
_CO_COROUTINE = 0x100

_logger = logging.getLogger("dorm.asyncguard")

Mode = Literal["warn", "raise", "raise_first"]


class SyncCallInAsyncContext(BaseException):
    """Raised when ``enable_async_guard(mode="raise")`` (or
    ``"raise_first"``) detects a sync ORM call inside a running event
    loop.

    Inherits from :class:`BaseException` (not :class:`Exception`) so
    that the dispatcher inside ``Signal.send`` — which catches
    ``Exception`` to keep one bad receiver from sinking the whole
    save — propagates this one to the caller anyway. That's exactly
    what we want for a programming-error guard: surface as a 500,
    don't get logged-and-swallowed."""


_state: dict[str, Any] = {
    "attached": False,
    "mode": "warn",
    "first_raised": False,
    "warned_locations": set(),
    "lock": threading.Lock(),
}


def _running_loop_in_current_thread() -> bool:
    """``True`` if an asyncio loop is currently running on this thread."""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _called_from_async_dorm_path() -> bool:
    """Walk the stack from this frame; ``True`` if any ``async def``
    frame inside the ``dorm`` package is on the way down. That's the
    legitimate async path (``acount``, ``aget``, ``aexecute``, …) —
    no warning needed.

    The sync ORM path between the user's coroutine and ``log_query``
    is plain ``def`` only, so the user's own ``async def view``
    frame doesn't count: we require the coroutine frame to live
    inside dorm itself.
    """
    frame: Any = sys._getframe(1)
    while frame is not None:
        code = frame.f_code
        if code.co_flags & _CO_COROUTINE:
            filename = (code.co_filename or "").replace("\\", "/")
            if "/dorm/" in filename or filename.endswith("/dorm"):
                return True
        frame = frame.f_back
    return False


def _on_pre_query(sender: Any, **kwargs: Any) -> None:
    if not _running_loop_in_current_thread():
        return
    if _called_from_async_dorm_path():
        return
    sql = str(kwargs.get("sql", ""))[:120]
    mode: Mode = _state["mode"]

    if mode == "raise" or (mode == "raise_first" and not _state["first_raised"]):
        _state["first_raised"] = True
        raise SyncCallInAsyncContext(
            "Synchronous dorm query executed inside a running event "
            f"loop. Use the async API (a*) instead. SQL: {sql!r}"
        )

    # ``warn`` mode (and ``raise_first`` after the first raise): log
    # once per (vendor, sql template) pair so a hot offender doesn't
    # flood the log.
    key = (str(sender), sql.split(" WHERE ", 1)[0])
    if key in _state["warned_locations"]:
        return
    _state["warned_locations"].add(key)
    _logger.warning(
        "Sync dorm query inside async event loop (vendor=%s): %s",
        sender,
        sql,
    )


def enable_async_guard(mode: Mode = "warn") -> None:
    """Activate the guard. Idempotent — calling twice does not attach
    a second receiver. Call :func:`disable_async_guard` to turn off.

    *mode*: ``"warn"`` (log once per offender), ``"raise"`` (always
    raise), or ``"raise_first"`` (raise once, then degrade to warn)."""
    if mode not in ("warn", "raise", "raise_first"):
        raise ValueError(f"Unknown async-guard mode: {mode!r}")
    with _state["lock"]:
        _state["mode"] = mode
        _state["first_raised"] = False
        _state["warned_locations"] = set()
        if not _state["attached"]:
            signals.pre_query.connect(_on_pre_query, weak=False)
            _state["attached"] = True


def disable_async_guard() -> None:
    """Disconnect the guard. After this call, sync ORM operations
    inside a running event loop are no longer flagged."""
    with _state["lock"]:
        if not _state["attached"]:
            return
        signals.pre_query.disconnect(_on_pre_query)
        _state["attached"] = False
        _state["warned_locations"] = set()
        _state["first_raised"] = False


__all__ = [
    "enable_async_guard",
    "disable_async_guard",
    "SyncCallInAsyncContext",
]
