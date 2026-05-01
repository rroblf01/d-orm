from __future__ import annotations

import asyncio
import inspect
import logging
import weakref
from typing import Any, Awaitable, Callable

# Receivers that raise are logged here at ERROR level. Apps can route this
# logger to Sentry / their alert pipeline so a broken ``post_save`` hook
# doesn't fail silently. Set ``raise_exceptions=True`` on a Signal to
# propagate instead — useful in tests, where silently-swallowed errors
# mask broken receivers.
_logger = logging.getLogger("dorm.signals")


def _is_async_receiver(fn: Callable[..., Any]) -> bool:
    """True if *fn* (or, for bound methods, its underlying function) is a
    coroutine function. Plain ``inspect.iscoroutinefunction`` already
    handles bound methods on CPython 3.11+, but we keep the unwrap step
    for ``functools.partial`` wrappers people sometimes connect.
    """
    if inspect.iscoroutinefunction(fn):
        return True
    inner = getattr(fn, "func", None)  # functools.partial
    if inner is not None and inspect.iscoroutinefunction(inner):
        return True
    return False


class Signal:
    """Minimal signal/event dispatcher (pre/post save/delete).

    Receivers may be either regular functions or ``async def`` coroutine
    functions:

    - :meth:`send` invokes synchronous receivers in order. Coroutine
      receivers are skipped with a single ``WARNING`` log line per call,
      since there is no event loop on the synchronous path to await them
      on. Connect them via :meth:`asend` (typically from
      :meth:`Model.asave` / :meth:`Model.adelete`) instead.
    - :meth:`asend` invokes both kinds: synchronous receivers are called
      directly, coroutine receivers are awaited sequentially in the order
      they were connected. The dispatch order matches :meth:`send` so
      receivers that depend on each other behave the same way under both
      entry points.
    """

    def __init__(self, *, raise_exceptions: bool = False) -> None:
        self._receivers: list[tuple[Any, Any, type | None, bool]] = []
        # When True, ``send()`` re-raises any exception a receiver throws
        # after logging it. The default (False) preserves the historical
        # behaviour where one bad receiver doesn't break the save path,
        # but exceptions are now logged instead of silently swallowed.
        self.raise_exceptions = raise_exceptions

    # ── Registration ─────────────────────────────────────────────────────────

    def connect(
        self,
        receiver: Callable[..., Any] | Callable[..., Awaitable[Any]],
        sender: type | None = None,
        weak: bool = True,
        dispatch_uid: str | None = None,
    ) -> None:
        """Register *receiver* to be called when this signal fires.

        *receiver* may be a regular callable or an ``async def``
        coroutine function. Coroutine receivers only fire from
        :meth:`asend`; on the sync :meth:`send` path they are logged-and-
        skipped, so registering one does not silently turn synchronous
        ``Model.save()`` calls into a no-op for that receiver.
        """
        # Stable id for bound methods: ``id(obj.method)`` returns the
        # id of a *temporary* bound-method object that gets GC'd as
        # soon as ``connect`` returns. CPython recycles those ids
        # freely, so a subsequent ``connect(other_obj.method)`` could
        # produce the same id and silently disconnect the first
        # receiver. Build a stable composite uid out of the bound
        # instance + underlying function instead.
        if dispatch_uid is not None:
            uid: Any = dispatch_uid
        else:
            self_obj = getattr(receiver, "__self__", None)
            func = getattr(receiver, "__func__", None)
            if self_obj is not None and func is not None:
                uid = (id(self_obj), id(func))
            else:
                uid = id(receiver)
        self._receivers = [r for r in self._receivers if r[0] != uid]
        if weak:
            try:
                ref: Any = weakref.WeakMethod(receiver)  # type: ignore[arg-type]
            except TypeError:
                ref = weakref.ref(receiver)
        else:
            ref = receiver
        self._receivers.append((uid, ref, sender, weak))

    def disconnect(
        self,
        receiver: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
        sender: type | None = None,
        dispatch_uid: str | None = None,
    ) -> bool:
        # Mirror the same composite-uid scheme used by ``connect``
        # so disconnection of a bound method targets the same row
        # in ``_receivers``.
        if dispatch_uid is not None:
            uid: Any = dispatch_uid
        elif receiver is not None:
            self_obj = getattr(receiver, "__self__", None)
            func = getattr(receiver, "__func__", None)
            if self_obj is not None and func is not None:
                uid = (id(self_obj), id(func))
            else:
                uid = id(receiver)
        else:
            uid = None
        before = len(self._receivers)
        if uid is not None:
            self._receivers = [r for r in self._receivers if r[0] != uid]
        elif sender is not None:
            self._receivers = [r for r in self._receivers if r[2] is not sender]
        return len(self._receivers) < before

    # ── Dispatch helpers ─────────────────────────────────────────────────────

    def _live_receivers(
        self, sender: Any
    ) -> tuple[list[Callable[..., Any]], list[tuple[Any, Any, type | None, bool]]]:
        """Materialise weak refs into hard callable references and prune
        garbage-collected entries. Returns ``(callables, kept_records)``
        in the original connect order.
        """
        callables: list[Callable[..., Any]] = []
        live: list[tuple[Any, Any, type | None, bool]] = []
        for uid, ref, filt_sender, is_weak in self._receivers:
            if filt_sender is not None and filt_sender is not sender:
                live.append((uid, ref, filt_sender, is_weak))
                continue
            if is_weak:
                fn = ref()
                if fn is None:
                    continue  # garbage-collected
            else:
                fn = ref
            live.append((uid, ref, filt_sender, is_weak))
            callables.append(fn)
        return callables, live

    # ── Dispatch ─────────────────────────────────────────────────────────────

    def send(self, sender: Any, **kwargs: Any) -> list[tuple[Callable[..., Any], Any]]:
        responses: list[tuple[Callable[..., Any], Any]] = []
        callables, live = self._live_receivers(sender)
        skipped_async = 0
        for fn in callables:
            if _is_async_receiver(fn):
                # Sync send path can't await a coroutine without an event
                # loop, so we skip async receivers and surface it via the
                # signals logger. Fire via ``asend`` (typically from the
                # ``asave`` / ``adelete`` paths) to invoke them.
                skipped_async += 1
                continue
            try:
                responses.append((fn, fn(sender=sender, **kwargs)))
            except Exception:
                # Always log the receiver failure so it isn't lost. Most
                # production Signals (pre/post_save, etc.) keep the legacy
                # "one bad receiver doesn't sink the whole save" semantics
                # by leaving ``raise_exceptions=False``; opt in to re-raise
                # for stricter dispatchers (tests, custom user signals).
                _logger.exception(
                    "Signal receiver %r raised while handling sender=%r",
                    fn,
                    sender,
                )
                if self.raise_exceptions:
                    self._receivers = live
                    raise
        if skipped_async:
            _logger.warning(
                "Signal.send skipped %d async receiver(s); "
                "use asend() (e.g. via Model.asave/adelete) to dispatch them.",
                skipped_async,
            )
        self._receivers = live
        return responses

    async def asend(
        self, sender: Any, **kwargs: Any
    ) -> list[tuple[Callable[..., Any], Any]]:
        """Async dispatch. Awaits coroutine receivers; calls sync ones
        directly. Order matches :meth:`send`.

        Receivers are awaited *sequentially*, not concurrently, so two
        receivers that share state (e.g. both write to a buffer) behave
        the same as on the sync path. If you want concurrency, fan out
        to ``asyncio.gather`` from inside one receiver.
        """
        responses: list[tuple[Callable[..., Any], Any]] = []
        callables, live = self._live_receivers(sender)
        for fn in callables:
            try:
                if _is_async_receiver(fn):
                    result = await fn(sender=sender, **kwargs)
                else:
                    result = fn(sender=sender, **kwargs)
                    # Sync receiver might still return a coroutine if the
                    # user wrapped an async helper without making the
                    # outer callable async. Await it transparently rather
                    # than letting it dangle (which would warn at GC).
                    if inspect.isawaitable(result):
                        result = await result
                responses.append((fn, result))
            except Exception:
                _logger.exception(
                    "Signal receiver %r raised while handling sender=%r",
                    fn,
                    sender,
                )
                if self.raise_exceptions:
                    self._receivers = live
                    raise
        self._receivers = live
        return responses

    def __repr__(self) -> str:
        return f"<Signal receivers={len(self._receivers)}>"


# Asyncio is imported for completeness even if not directly referenced —
# users sometimes inspect ``dorm.signals.asyncio`` from REPL. Keeping the
# import explicit avoids "where did this come from" surprises.
_ = asyncio


pre_save = Signal()
post_save = Signal()
pre_delete = Signal()
post_delete = Signal()

# Query observability — fired around every SQL statement so users can
# wire metrics, distributed tracing (OpenTelemetry, Datadog), or custom
# diagnostics. Receivers should be cheap; heavy work belongs elsewhere.
#
# pre_query  receivers: ``def recv(sender=<vendor str>, sql, params): ...``
# post_query receivers: ``def recv(sender=<vendor str>, sql, params, elapsed_ms, error): ...``
#
# ``error`` is the raised exception (or None if the statement succeeded).
# Query signals are dispatched from the synchronous SQL log context, so
# only sync receivers fire on them; async receivers connected here are
# skipped with a warning. Wire async tracing via the ``asend``-driven
# model signals or do the work inside a sync receiver that schedules a
# task on the running loop.
pre_query = Signal()
post_query = Signal()
