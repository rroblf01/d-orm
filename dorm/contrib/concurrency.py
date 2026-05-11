"""High-level concurrency primitives built on existing dorm helpers.

- :func:`named_lock` — distributed mutex keyed by a string name.
  Wraps :func:`dorm.contrib.advisory.advisory_lock` on PG, falls
  back to an in-process :class:`threading.Lock` keyed by name on
  every other backend (best-effort; not cluster-safe outside PG).
- :class:`SerializableSnapshot` — context manager that opens an
  ``atomic()`` block with ``SERIALIZABLE`` isolation on PG and
  auto-retries on serialization failure (SQLSTATE ``40001``).
- :func:`with_optimistic_lock` — instance-level helper for
  optimistic concurrency control on a ``version`` integer column.
"""
from __future__ import annotations

import contextlib
import threading
from typing import Any, Callable, TypeVar

from .. import transaction
from .cockroach import (
    retry_on_serialization,
    aretry_on_serialization,
)

_T = TypeVar("_T")
_IN_PROC_LOCKS: dict[str, threading.Lock] = {}
_REGISTRY_LOCK = threading.Lock()


def _get_inproc(key: str) -> threading.Lock:
    with _REGISTRY_LOCK:
        return _IN_PROC_LOCKS.setdefault(key, threading.Lock())


@contextlib.contextmanager
def named_lock(name: str, *, using: str = "default"):
    """Acquire a mutex named *name*.

    On PostgreSQL, delegates to :func:`advisory_lock` — the mutex is
    process-pool-shared and survives within a single connection's
    lifetime. On other vendors, falls back to an in-process
    :class:`threading.Lock` registered under *name*; **not**
    cluster-safe but lets the same calling pattern work in dev /
    SQLite tests."""
    from ..db.connection import get_connection

    conn = get_connection(using)
    if getattr(conn, "vendor", None) == "postgresql":
        from .advisory import advisory_lock

        with advisory_lock(name, using=using):
            yield
        return
    lock = _get_inproc(name)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class SerializableSnapshot:
    """``atomic()`` block with SERIALIZABLE isolation + auto-retry.

    The wrapped block runs at SERIALIZABLE isolation; on the first
    serialization-failure raise (PG SQLSTATE ``40001``) the block
    rolls back and retries up to *max_attempts* with exponential
    backoff. Useful for "transfer money between two accounts"-style
    invariants where READ COMMITTED would let phantom rows slip in.

    Falls back to a plain ``atomic()`` on non-PG vendors (their
    default isolation already covers most read-write patterns the
    helper targets).
    """

    def __init__(
        self,
        *,
        using: str = "default",
        max_attempts: int = 5,
        base_backoff: float = 0.05,
        max_backoff: float = 2.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.using = using
        self.max_attempts = max_attempts
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff

    def run(self, fn: Callable[[], _T]) -> _T:
        """Execute *fn* under SERIALIZABLE + auto-retry."""
        from ..db.connection import get_connection

        conn = get_connection(self.using)
        is_pg = getattr(conn, "vendor", None) == "postgresql"

        def _body() -> _T:
            with transaction.atomic(using=self.using):
                if is_pg:
                    conn.execute_write(
                        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                    )
                return fn()

        if not is_pg:
            return _body()
        return retry_on_serialization(
            _body,
            max_attempts=self.max_attempts,
            base_backoff=self.base_backoff,
            max_backoff=self.max_backoff,
        )

    async def arun(self, fn: Callable[[], Any]) -> Any:
        """Async counterpart of :meth:`run`. ``fn`` must be a zero-arg
        callable returning a coroutine."""
        from ..db.connection import get_async_connection

        conn = get_async_connection(self.using)
        is_pg = getattr(conn, "vendor", None) == "postgresql"

        async def _body() -> Any:
            async with transaction.aatomic(using=self.using):
                if is_pg:
                    await conn.execute_write(
                        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                    )
                return await fn()

        if not is_pg:
            return await _body()
        return await aretry_on_serialization(
            _body,
            max_attempts=self.max_attempts,
            base_backoff=self.base_backoff,
            max_backoff=self.max_backoff,
        )


class OptimisticLockError(Exception):
    """Raised when a concurrent write wins the race and the
    optimistic-locked save would silently overwrite it."""


def with_optimistic_lock(
    instance: Any,
    *,
    version_field: str = "version",
    update_fields: list[str] | None = None,
) -> None:
    """Save *instance* iff the database row's ``version_field``
    equals the in-memory value, then bump the version by 1.

    Raises :class:`OptimisticLockError` when the row no longer
    matches — the caller can re-read, re-merge, and retry.

    The model must declare the version column as an integer field
    (initial ``default=0`` recommended).
    """
    cls = type(instance)
    pk = cls._meta.pk.attname  # type: ignore[attr-defined]
    pk_val = instance.__dict__.get(pk)
    if pk_val is None:
        raise OptimisticLockError(
            "with_optimistic_lock: instance has no PK; only saved rows can "
            "participate in optimistic-lock writes."
        )
    cur_version = instance.__dict__.get(version_field, 0)
    new_version = cur_version + 1

    qs = cls.objects.filter(  # type: ignore[attr-defined]
        **{pk: pk_val, version_field: cur_version}
    )
    # Build the update payload from update_fields (or every non-PK
    # field when omitted).
    if update_fields is None:
        update_fields = [
            f.name
            for f in cls._meta.fields  # type: ignore[attr-defined]
            if f.name != pk and not getattr(f, "many_to_many", False)
        ]
    payload = {f: instance.__dict__.get(f) for f in update_fields if f != version_field}
    payload[version_field] = new_version
    rowcount = qs.update(**payload)
    if rowcount == 0:
        raise OptimisticLockError(
            f"row pk={pk_val!r} has been modified since the last read "
            f"(expected {version_field}={cur_version}). Re-read and retry."
        )
    instance.__dict__[version_field] = new_version


__all__ = [
    "named_lock",
    "SerializableSnapshot",
    "with_optimistic_lock",
    "OptimisticLockError",
]
