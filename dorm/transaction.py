from __future__ import annotations

import functools
from typing import Any, Callable


class _AtomicContextManager:
    """Backs :func:`atomic`. Supports both ``with atomic():`` and ``@atomic``."""

    def __init__(self, using: str = "default") -> None:
        self.using = using
        self._cm: Any = None

    def __enter__(self):
        from .db.connection import get_connection

        self._cm = get_connection(self.using).atomic()
        return self._cm.__enter__()

    def __exit__(self, exc_type, exc, tb):
        return self._cm.__exit__(exc_type, exc, tb)

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

    async def __aenter__(self):
        from .db.connection import get_async_connection

        self._cm = get_async_connection(self.using).aatomic()
        return await self._cm.__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        return await self._cm.__aexit__(exc_type, exc, tb)

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any):
            async with self.__class__(self.using):
                return await func(*args, **kwargs)

        return wrapper


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
    inner failure."""
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
