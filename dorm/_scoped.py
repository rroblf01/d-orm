"""ContextVar-based per-task signal collector.

Common shape used by ``contrib.querycount``, ``test.assertNumQueries``
and ``contrib.querylog``: each opens a per-task scope on enter, every
matching signal mutates the scope's state in place, and the scope's
final value is read on exit.

State is held in a :class:`contextvars.ContextVar` so concurrent ASGI
requests / asyncio tasks see independent scopes — a query in request A
doesn't bleed into a counter / log opened in request B.

The state object is **mutable** — callers receive a list / dict /
custom container and the receiver mutates index ``[0]`` / appends to
the list. This avoids ``ContextVar.set`` per signal (every ``set``
allocates a Token), so a guard around 1000 queries pays one ``set`` /
``reset`` pair instead of 1000.
"""

from __future__ import annotations

import contextvars
import threading
from typing import Any, Callable, Generic, TypeVar

from .signals import Signal

S = TypeVar("S")  # The shape of the per-scope mutable state.


class ScopedCollector(Generic[S]):
    """One instance per signal × use-case.

    ``signal`` is the Signal to listen on. ``on_event`` is called for
    every ``signal.send`` while a scope is active and receives
    ``(state, **kwargs)`` — it mutates *state* directly.

    The listener is attached lazily on first ``open()`` and stays
    attached for the life of the process. Idempotent: subsequent
    ``open()`` calls reuse it.
    """

    __slots__ = ("_var", "_signal", "_on_event", "_attached", "_lock")

    def __init__(
        self,
        signal: Signal,
        var_name: str,
        on_event: Callable[[S, dict[str, Any]], None],
    ) -> None:
        self._var: contextvars.ContextVar[S | None] = contextvars.ContextVar(
            var_name, default=None
        )
        self._signal = signal
        self._on_event = on_event
        self._attached = False
        self._lock = threading.Lock()

    def _ensure_attached(self) -> None:
        if self._attached:
            return
        with self._lock:
            if self._attached:
                return

            def _receiver(sender: Any, **kwargs: Any) -> None:
                state = self._var.get()
                if state is None:
                    return
                # ``Signal.send`` peels ``sender`` off as a positional
                # arg before calling the receiver, so it never lands
                # in ``kwargs``. Stitch it back in so ``on_event``
                # consumers (querylog, prometheus, …) can read the
                # vendor / model name without a separate parameter.
                merged = dict(kwargs)
                merged["sender"] = sender
                self._on_event(state, merged)

            self._signal.connect(_receiver, weak=False)
            self._attached = True

    # ── Scope lifecycle ─────────────────────────────────────────────────────

    def open(self, state: S) -> contextvars.Token[S | None]:
        """Begin a scope with *state* as the per-task container."""
        self._ensure_attached()
        return self._var.set(state)

    def close(self, token: contextvars.Token[S | None]) -> None:
        self._var.reset(token)

    def current(self) -> S | None:
        return self._var.get()


__all__ = ["ScopedCollector"]
