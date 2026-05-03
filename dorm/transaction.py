from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import logging
import re as _re
import secrets as _secrets
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

# Async stack is a per-task list of (using_alias, commit_callbacks,
# rollback_callbacks) frames. Sync uses a parallel pair of dicts on
# ``_SYNC_STATE`` (commit / rollback) keyed by alias.
_ASYNC_STACK: contextvars.ContextVar[
    list[tuple[str, list[Callable[[], Any]], list[Callable[[], Any]]]] | None
] = contextvars.ContextVar("dorm_on_commit_async_stack", default=None)


def _sync_stack(using: str) -> list[list[Callable[[], Any]]]:
    """Return (and lazily create) the per-thread, per-alias commit frame
    stack. Each entry is a list of pending ``on_commit`` callbacks for
    one nesting level."""
    stacks = getattr(_SYNC_STATE, "stacks", None)
    if stacks is None:
        stacks = {}
        _SYNC_STATE.stacks = stacks
    return stacks.setdefault(using, [])


def _sync_rollback_stack(using: str) -> list[list[Callable[[], Any]]]:
    """Per-thread rollback-callback stack, parallel to ``_sync_stack``.

    Maintained as a separate dict on ``_SYNC_STATE`` so the existing
    ``on_commit`` path stays untouched — the ``_pop_sync_frame``
    coordinator drains both stacks atomically."""
    stacks = getattr(_SYNC_STATE, "rb_stacks", None)
    if stacks is None:
        stacks = {}
        _SYNC_STATE.rb_stacks = stacks
    return stacks.setdefault(using, [])


def _push_sync_frame(using: str) -> None:
    _sync_stack(using).append([])
    _sync_rollback_stack(using).append([])


def _pop_sync_frame(using: str, *, committed: bool) -> None:
    """Pop the innermost frame.

    On commit: ``on_commit`` callbacks are merged into the parent (so
    they fire when the *outermost* tx commits) and ``on_rollback``
    callbacks are discarded — we did commit, no cleanup needed.

    On rollback: ``on_rollback`` callbacks fire **now** (the inner work
    is genuinely undone, so its compensations should run as soon as the
    rollback completes) and ``on_commit`` callbacks are discarded.

    Each callback is fired in its own ``try``/``except`` so a buggy
    cleanup hook can't take down the transaction-management bookkeeping.
    """
    stack = _sync_stack(using)
    rb_stack = _sync_rollback_stack(using)
    frame = stack.pop()
    rb_frame = rb_stack.pop()

    if not committed:
        # Discard commit callbacks; fire rollback ones immediately.
        for cb in rb_frame:
            try:
                cb()
            except Exception:
                _log.exception("on_rollback callback %r raised", cb)
        return

    # Committed — discard rollback frame, propagate commit frame.
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


def on_rollback(
    callback: Callable[[], Any],
    using: str = "default",
) -> None:
    """Schedule *callback* to run when the surrounding transaction rolls
    back. The mirror image of :func:`on_commit` — used to undo
    *non-transactional* side effects when the underlying DB work didn't
    stick. The canonical user is :class:`dorm.FileField`, which queues
    a ``storage.delete`` so a file written inside an ``atomic()`` that
    later rolls back doesn't survive as an orphan.

    Behaviour matches ``on_commit``'s semantics in reverse:

    - Outside any ``atomic()`` block, there is nothing to roll back, so
      the callback is **dropped**. (``on_commit`` runs the callback
      immediately because the "transaction" already committed; the
      symmetric "transaction already committed" answer for rollback is
      "nothing to undo.")
    - Inside nested ``atomic()`` blocks, callbacks fire when **their**
      block rolls back — savepoint rollbacks fire only inner callbacks,
      outer commit-or-rollback decides the outer ones.
    - If the surrounding block commits, queued rollback callbacks are
      discarded.
    - When this is called from inside an ``aatomic()`` block, the
      callback is registered on the async stack and fires from the
      async pop path (``aon_rollback`` is a thin alias for callers
      who want to make the async intent explicit).
    """
    # Prefer the sync stack (tracks per-thread atomic frames). Fall back
    # to the active async stack so the same helper works inside both
    # ``atomic()`` and ``aatomic()`` — which matters for ``FileField``,
    # whose ``pre_save`` is sync even on the async ORM path.
    sync_stack = _sync_rollback_stack(using)
    if sync_stack:
        sync_stack[-1].append(callback)
        return
    async_stack = _aon_commit_stack()
    for frame in reversed(async_stack):
        frame_using, _commit, rollback_cbs = frame
        if frame_using == using:
            rollback_cbs.append(callback)
            return
    # Outside any active transaction → nothing to roll back, drop.
    return


# ── async on_commit ──────────────────────────────────────────────────────────


def _aon_commit_stack() -> list[
    tuple[str, list[Callable[[], Any]], list[Callable[[], Any]]]
]:
    """Return the per-task async stack, creating one if needed.

    Each frame is ``(using_alias, commit_callbacks, rollback_callbacks)``.
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
    _aon_commit_stack().append((using, [], []))


async def _pop_async_frame(using: str, *, committed: bool) -> None:
    stack = _aon_commit_stack()
    if not stack:
        return
    frame_using, frame, rb_frame = stack.pop()
    del frame_using  # only used for parity with ``aon_rollback``'s scoping

    if not committed:
        # Discard commit callbacks; fire rollback callbacks now. Both
        # sync and async rollback callables are accepted — coroutines
        # are awaited, sync calls run inline.
        for cb in rb_frame:
            try:
                result = cb()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                _log.exception("aon_rollback callback %r raised", cb)
        return

    # Committed — discard rollback frame, propagate commit frame.
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
    # ``stack[-1]`` is ``(using, commit_cbs, rollback_cbs)`` — index 1
    # is the commit list, matching the original layout.
    stack[-1][1].append(callback)


def aon_rollback(
    callback: Union[Callable[[], Any], Callable[[], Awaitable[Any]]],
    using: str = "default",
) -> None:
    """Async counterpart of :func:`on_rollback`.

    Accepts both regular callables and coroutine functions. The
    rollback path awaits coroutines so cleanup that itself needs the
    event loop (deleting a remote object via ``aiobotocore``, for
    example) Just Works.

    Outside an :func:`aatomic` block, the callback is dropped — the
    rollback symmetry of the no-op ``on_commit`` path.
    """
    stack = _aon_commit_stack()
    for frame in reversed(stack):
        frame_using, _commit, rollback_cbs = frame
        if frame_using == using:
            rollback_cbs.append(callback)
            return
    # Outside any active aatomic — nothing to roll back. Drop.
    return


# ── atomic() / aatomic() context managers ────────────────────────────────────


class _AtomicContextManager:
    """Backs :func:`atomic`. Supports both ``with atomic():`` and ``@atomic``."""

    def __init__(self, using: str = "default", *, durable: bool = False) -> None:
        self.using = using
        self.durable = durable
        self._cm: Any = None
        self._rollback_requested: bool = False

    def __enter__(self):
        from .db.connection import get_connection

        conn = get_connection(self.using)
        if self.durable and getattr(conn, "_atomic_depth", 0) > 0:
            raise RuntimeError(
                "atomic(durable=True) was nested inside another atomic() "
                "block — durable atomics must be top-level so they map "
                "to a real COMMIT, not a savepoint."
            )
        self._cm = conn.atomic()
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

    def __init__(self, using: str = "default", *, durable: bool = False) -> None:
        self.using = using
        self.durable = durable
        self._cm: Any = None
        self._rollback_requested: bool = False

    async def __aenter__(self):
        from .db.connection import get_async_connection

        conn = get_async_connection(self.using)
        if self.durable and getattr(conn, "_atomic_depth", 0) > 0:
            raise RuntimeError(
                "aatomic(durable=True) was nested inside another aatomic() "
                "block — durable atomics must be top-level so they map "
                "to a real COMMIT, not a savepoint."
            )
        self._cm = conn.aatomic()
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


def atomic(
    using: str | Callable[..., Any] = "default", *, durable: bool = False
):
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
    without having to raise an exception.

    Pass ``durable=True`` (Django 3.2+) to assert that *this* atomic
    block is the outermost one — i.e. the surrounding code is NOT
    already inside another ``atomic()``. The block raises
    :class:`RuntimeError` immediately if it would silently degrade
    to a savepoint instead of a top-level transaction. Use this on
    work that MUST land in its own COMMIT (write-then-publish
    patterns where the publish step waits on a real fsync).
    """
    # @atomic (no parens) — `using` is the function being decorated.
    if callable(using) and not isinstance(using, str):
        return _AtomicContextManager("default", durable=durable)(using)
    return _AtomicContextManager(using, durable=durable)


def aatomic(
    using: str | Callable[..., Any] = "default", *, durable: bool = False
):
    """Async counterpart of :func:`atomic`. Same usage as ``atomic``: works
    as ``async with`` context manager or as a decorator on async functions.
    ``durable=True`` is enforced the same way as the sync version."""
    if callable(using) and not isinstance(using, str):
        return _AsyncAtomicContextManager("default", durable=durable)(using)
    return _AsyncAtomicContextManager(using, durable=durable)


# ── Manual savepoint API (3.1+) ──────────────────────────────────────────────
#
# ``atomic()`` already nests via savepoints automatically. The functions
# below expose the SQL-level primitives directly for users who need to
# branch / rollback inside a single ``atomic()`` block without unwinding
# the whole transaction. Mirrors Django's
# ``django.db.transaction.savepoint`` family.

# Savepoint IDs minted by :func:`savepoint` are ``s_<hex>`` — anything
# else gets rejected to keep arbitrary user input out of the SQL we
# splice into ``SAVEPOINT`` / ``RELEASE`` / ``ROLLBACK TO``.
_SAVEPOINT_RE = _re.compile(r"^s_[0-9a-f]+$")


def _connection_for(using: str = "default"):
    from .db.connection import get_connection

    return get_connection(using)


def savepoint(using: str = "default") -> str:
    """Create a savepoint inside the current transaction. Returns the
    savepoint ID (a unique-per-process token suitable for SQL
    splicing); pass it to :func:`savepoint_commit` or
    :func:`savepoint_rollback`.

    Must be called inside an ``atomic()`` block. Without an outer
    transaction the savepoint emits unmatched DDL that the backend
    rejects.
    """
    sid = "s_" + _secrets.token_hex(8)
    _connection_for(using).execute_script(f"SAVEPOINT {sid}")
    return sid


def savepoint_commit(sid: str, using: str = "default") -> None:
    """Release *sid* — its writes stay part of the outer transaction."""
    if not _SAVEPOINT_RE.match(sid):
        # Defence-in-depth: callers should only pass IDs returned
        # by :func:`savepoint`. Reject anything that wouldn't
        # round-trip through ``token_hex`` to avoid SQL splicing
        # of arbitrary user input.
        raise ValueError(f"invalid savepoint id: {sid!r}")
    _connection_for(using).execute_script(f"RELEASE SAVEPOINT {sid}")


def savepoint_rollback(sid: str, using: str = "default") -> None:
    """Roll back every write made since *sid* without aborting the
    outer transaction. The savepoint is then released automatically
    by the rollback (no need to call :func:`savepoint_commit`)."""
    if not _SAVEPOINT_RE.match(sid):
        raise ValueError(f"invalid savepoint id: {sid!r}")
    _connection_for(using).execute_script(f"ROLLBACK TO SAVEPOINT {sid}")
