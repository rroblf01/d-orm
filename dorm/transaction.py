from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import logging
import threading
from typing import Any, Awaitable, Callable, Union


_log = logging.getLogger("dorm.transaction")


# ── on_commit infrastructure ─────────────────────────────────────────────────
#
# Each running atomic() block owns a list of "deferred" callbacks. The list
# lives at the depth of the *current* atomic frame; when an inner block commits
# (``RELEASE SAVEPOINT``), its callbacks are merged into the parent frame's
# list so they only actually fire when the **outermost** transaction commits.
# Rollback at any depth discards that frame's callbacks (and any merged from
# its children — they're already in the parent's list at that point, so we
# track per-depth lists independently and merge only on success).
#
# Sync uses thread-local; async uses a ContextVar so concurrent tasks don't
# share state. Both expose the same per-alias stack.

_SYNC_STATE = threading.local()

# Async stack is a per-task list of (using_alias, callbacks) frames.
_ASYNC_STACK: contextvars.ContextVar[list[tuple[str, list[Callable[[], Any]]]] | None] = (
    contextvars.ContextVar("dorm_on_commit_async_stack", default=None)
)


def _sync_stack(using: str) -> list[list[Callable[[], Any]]]:
    """Return (and lazily create) the per-thread, per-alias frame stack."""
    stacks = getattr(_SYNC_STATE, "stacks", None)
    if stacks is None:
        stacks = {}
        _SYNC_STATE.stacks = stacks
    return stacks.setdefault(using, [])


def _push_sync_frame(using: str) -> None:
    _sync_stack(using).append([])


def _pop_sync_frame(using: str, *, committed: bool) -> None:
    """Pop the innermost frame.

    If the surrounding ``atomic()`` block committed, the frame's callbacks
    are merged into the parent frame so they fire when the parent commits
    (or chained again into an even-outer frame). If the block rolled back,
    the callbacks are discarded — exactly what you want for "fire only on
    commit" semantics.
    """
    stack = _sync_stack(using)
    frame = stack.pop()
    if not committed:
        return
    if stack:
        stack[-1].extend(frame)
        return
    # Outermost commit — fire everything now, swallowing exceptions per
    # callback. A failing post-commit hook is *not* a database error: the
    # transaction has already been written.
    for cb in frame:
        try:
            cb()
        except Exception:
            _log.exception("on_commit callback %r raised", cb)


def on_commit(
    callback: Callable[[], Any],
    using: str = "default",
) -> None:
    """Schedule *callback* to run when the current transaction commits.

    Use this for side effects that must happen **only after** the surrounding
    write actually lands — sending email, enqueueing a Celery / RQ job,
    publishing to a message bus, calling an external API — so you never
    leak an effect for a transaction that ends up rolling back.

    If called outside an :func:`atomic` block, the callback runs immediately
    (Django's behaviour). Inside nested atomics, callbacks are deferred all
    the way up to the outermost commit; a rollback at any depth discards
    callbacks scheduled inside that block.

    Exceptions from a callback are logged on the ``dorm.transaction`` logger
    but do **not** roll back the (already-committed) transaction — by the
    time the callback fires, the DB state is durable. Wire that logger to
    your alerting if you depend on these for correctness.
    """
    stack = _sync_stack(using)
    if not stack:
        # No active atomic — degrade to direct call. Django does the same.
        callback()
        return
    stack[-1].append(callback)


# ── async on_commit ──────────────────────────────────────────────────────────


def _aon_commit_stack() -> list[tuple[str, list[Callable[[], Any]]]]:
    """Return the per-task async on-commit stack, creating one if needed.

    The ContextVar default is ``None`` so we can detect "first use" and set
    a fresh per-task list (otherwise concurrent tasks would share the
    default mutable list — a classic gotcha).
    """
    stack = _ASYNC_STACK.get()
    if stack is None:
        stack = []
        _ASYNC_STACK.set(stack)
    return stack


def _push_async_frame(using: str) -> None:
    _aon_commit_stack().append((using, []))


async def _pop_async_frame(using: str, *, committed: bool) -> None:
    stack = _aon_commit_stack()
    if not stack:
        return
    frame_using, frame = stack.pop()
    if not committed:
        return
    if stack:
        # Merge into parent frame so it fires when *that* layer commits.
        stack[-1][1].extend(frame)
        return
    # Outermost commit: dispatch each callback. ``await`` coroutines and
    # awaitables; call sync callables directly (and await the result if
    # they happened to return a coroutine, e.g. a fire-and-forget wrapper).
    for cb in frame:
        try:
            result = cb()
            if inspect.isawaitable(result):
                await result
        except Exception:
            _log.exception("aon_commit callback %r raised", cb)


def aon_commit(
    callback: Union[Callable[[], Any], Callable[[], Awaitable[Any]]],
    using: str = "default",
) -> None:
    """Async counterpart of :func:`on_commit`.

    Accepts both regular callables and coroutine functions / awaitables.
    Outside an :func:`aatomic` block, the callback fires immediately — if
    it returns an awaitable, a task is scheduled with
    ``asyncio.ensure_future`` so the call site doesn't have to await.

    Inside ``aatomic``, callbacks are deferred to the outermost commit
    just like the sync variant. Rolled-back blocks discard their pending
    callbacks.
    """
    stack = _aon_commit_stack()
    if not stack:
        # No active aatomic — fire now. Schedule coroutines so callers
        # don't have to await us.
        result = callback()
        if inspect.isawaitable(result):
            try:
                asyncio.ensure_future(result)
            except RuntimeError:
                # No running loop — best-effort: drop the coroutine.
                # This path is hard to reach in practice (aatomic implies
                # a running loop) but we log it so a misuse is visible.
                _log.warning(
                    "aon_commit called with no running loop; "
                    "coroutine result was not awaited"
                )
        return
    stack[-1][1].append(callback)


# ── atomic() / aatomic() context managers ────────────────────────────────────


class _AtomicContextManager:
    """Backs :func:`atomic`. Supports both ``with atomic():`` and ``@atomic``."""

    def __init__(self, using: str = "default") -> None:
        self.using = using
        self._cm: Any = None
        self._rollback_requested: bool = False

    def __enter__(self):
        from .db.connection import get_connection

        self._cm = get_connection(self.using).atomic()
        self._cm.__enter__()
        _push_sync_frame(self.using)
        return self

    def __exit__(self, exc_type, exc, tb):
        # If set_rollback() was called, force a rollback by raising an
        # exception that the underlying atomic() will catch and convert
        # into a rollback. Use a private sentinel exception so it doesn't
        # collide with anything user code might catch.
        if self._rollback_requested and exc_type is None:
            try:
                self._cm.__exit__(_RollbackForce, _RollbackForce(), None)
            except _RollbackForce:
                pass
            _pop_sync_frame(self.using, committed=False)
            return True

        # Normal path: propagate the result up to the inner atomic, which
        # decides commit vs rollback based on the exception.
        suppressed = self._cm.__exit__(exc_type, exc, tb)
        committed = exc_type is None and not suppressed
        _pop_sync_frame(self.using, committed=committed)
        return suppressed

    def set_rollback(self, flag: bool = True) -> None:
        """Mark the transaction to roll back when the block exits, even if
        no exception was raised. Mirrors Django's ``transaction.set_rollback``
        and is the canonical way for a test fixture to undo work without
        having to fake an exception. The block still exits normally; on
        exit, the rollback is performed and pending ``on_commit`` callbacks
        from this frame are discarded.
        """
        self._rollback_requested = flag

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            with self.__class__(self.using):
                return func(*args, **kwargs)

        return wrapper


class _AsyncAtomicContextManager:
    """Backs :func:`aatomic`. Supports both ``async with aatomic():`` and ``@aatomic``."""

    def __init__(self, using: str = "default") -> None:
        self.using = using
        self._cm: Any = None
        self._rollback_requested: bool = False

    async def __aenter__(self):
        from .db.connection import get_async_connection

        self._cm = get_async_connection(self.using).aatomic()
        await self._cm.__aenter__()
        _push_async_frame(self.using)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._rollback_requested and exc_type is None:
            try:
                await self._cm.__aexit__(_RollbackForce, _RollbackForce(), None)
            except _RollbackForce:
                pass
            await _pop_async_frame(self.using, committed=False)
            return True
        suppressed = await self._cm.__aexit__(exc_type, exc, tb)
        committed = exc_type is None and not suppressed
        await _pop_async_frame(self.using, committed=committed)
        return suppressed

    def set_rollback(self, flag: bool = True) -> None:
        """Async counterpart of :meth:`_AtomicContextManager.set_rollback`."""
        self._rollback_requested = flag

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any):
            async with self.__class__(self.using):
                return await func(*args, **kwargs)

        return wrapper


class _RollbackForce(Exception):
    """Internal sentinel: raised inside ``__exit__`` / ``__aexit__`` to force
    the underlying atomic() to roll back when ``set_rollback(True)`` was
    requested without a user exception.

    Must be an ``Exception`` subclass (not ``BaseException``) because the
    backend ``atomic()`` context managers catch ``Exception`` to trigger
    their rollback path — a ``BaseException`` would slip through and the
    rollback would never run. We only ever raise this from inside
    ``__exit__``, *after* user code has finished, so user-side ``except
    Exception:`` blocks have no chance to intercept it.
    """


def atomic(using: str | Callable[..., Any] = "default"):
    """Wrap a block of code in a database transaction.

    Usable as a context manager or as a decorator::

        with dorm.transaction.atomic():
            ...

        @dorm.transaction.atomic
        def update_balance(...):
            ...

        @dorm.transaction.atomic("replica")
        def report(...):
            ...

    On success the transaction is committed; on exception it is rolled back.
    Nested calls create savepoints so only the inner block is rolled back on
    inner failure. The returned context manager exposes :meth:`set_rollback`
    so test fixtures (or generic cleanup helpers) can force a rollback
    without having to raise an exception."""
    # @atomic (no parens) — `using` is the function being decorated.
    if callable(using) and not isinstance(using, str):
        return _AtomicContextManager("default")(using)
    return _AtomicContextManager(using)


def aatomic(using: str | Callable[..., Any] = "default"):
    """Async counterpart of :func:`atomic`. Same usage as ``atomic``: works
    as ``async with`` context manager or as a decorator on async functions."""
    if callable(using) and not isinstance(using, str):
        return _AsyncAtomicContextManager("default")(using)
    return _AsyncAtomicContextManager(using)
