from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager


@contextmanager
def atomic(using: str = "default"):
    """Wrap a block of code in a database transaction.

    On success the transaction is committed; on exception it is rolled back.
    Nested calls create savepoints so only the inner block is rolled back on
    inner failure.

    Usage::

        with dorm.transaction.atomic():
            Author.objects.create(name="Alice", age=30)
            Book.objects.create(title="...", author_id=1)
    """
    from .db.connection import get_connection

    with get_connection(using).atomic():
        yield


@asynccontextmanager
async def aatomic(using: str = "default"):
    """Async version of :func:`atomic`.

    Usage::

        async with dorm.transaction.aatomic():
            await Author.objects.acreate(name="Alice", age=30)
    """
    from .db.connection import get_async_connection

    async with get_async_connection(using).aatomic():
        yield
