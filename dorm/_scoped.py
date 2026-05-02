"""ContextVar-based per-task signal collector.

Common shape used by ``contrib.querycount``, ``test.assertNumQueries``
and ``contrib.querylog``: each opens a per-task scope on enter, every
matching signal mutates the scope's state in place, and the scope's
final value is read on exit.

State is held in a :class:`contextvars.ContextVar` so concurrent ASGI
requests / asyncio tasks see independent scopes — a query in request A
doesn't bleed into a counter / log opened in request B.

Nested scopes accumulate: when ``open()`` is called twice, both
scopes' states get updated on every signal. Outer scopes therefore
see the queries fired inside inner blocks (matches Django's
``assertNumQueries`` semantics), while inner scopes track only their
own slice of the work. The ContextVar holds an immutable tuple of
states; each ``open()`` pushes one entry, ``close()`` pops back via
the saved token.

The state objects passed to ``open()`` are **mutable** — callers
hand a list / dict / custom container and the receiver mutates it in
place. Avoids ``ContextVar.set`` per signal: a guard around 1000
queries pays exactly one ``set`` / ``reset`` pair, not 1000.
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
    every ``signal.send`` while at least one scope is active, and
    receives ``(state, kwargs)`` for every active scope (innermost
    last) — it mutates *state* directly.

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
        # The ContextVar carries the active stack as an immutable
        # tuple. ``None`` (default) means "no scope is open on this
        # task" — receivers short-circuit cheaply.
        self._var: contextvars.ContextVar[tuple[S, ...] | None] = (
            contextvars.ContextVar(var_name, default=None)
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
                stack = self._var.get()
                if not stack:
                    return
                # ``Signal.send`` peels ``sender`` off as a positional
                # arg before calling the receiver, so it never lands
                # in ``kwargs``. Stitch it back in so ``on_event``
                # consumers (querylog, prometheus, …) can read the
                # vendor / model name without a separate parameter.
                merged = dict(kwargs)
                merged["sender"] = sender
                # Update every active scope so an outer guard counts
                # queries fired inside a nested inner one — matches
                # Django's ``assertNumQueries`` semantics.
                for state in stack:
                    self._on_event(state, merged)

            self._signal.connect(_receiver, weak=False)
            self._attached = True

    # ── Scope lifecycle ─────────────────────────────────────────────────────

    def open(
        self, state: S
    ) -> contextvars.Token[tuple[S, ...] | None]:
        """Push *state* onto the active scope stack and return the
        token for :meth:`close`."""
        self._ensure_attached()
        current = self._var.get() or ()
        return self._var.set(current + (state,))

    def close(
        self, token: contextvars.Token[tuple[S, ...] | None]
    ) -> None:
        """Pop the scope stack back to whatever was active when
        :meth:`open` returned *token*."""
        self._var.reset(token)

    def current(self) -> S | None:
        """Innermost active state, or ``None`` if no scope is open.

        Returning the *innermost* matches the prior single-state
        semantics for the existing query-count consumers; an outer
        scope is reachable via ``self._var.get()`` if a future
        consumer needs the full stack.
        """
        stack = self._var.get()
        if not stack:
            return None
        return stack[-1]


__all__ = ["ScopedCollector"]
