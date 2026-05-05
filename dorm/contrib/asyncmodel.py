"""``AsyncModel`` — strict async-only Model variant.

For codebases that run exclusively under ``asyncio`` (FastAPI,
Litestar, Starlette, aiohttp), accidental sync ORM calls are a silent
disaster: ``Author.objects.create(...)`` inside an ``async def``
handler blocks the event loop, freezes every other connection, and
appears as random latency spikes in production. The
``dorm.contrib.asyncguard`` module already detects them at *runtime*;
this module catches them at *import time* by exposing a Model that
**rejects** sync access altogether.

Usage::

    from dorm.contrib.asyncmodel import AsyncModel

    class Author(AsyncModel):
        name = dorm.CharField(max_length=100)
        age = dorm.IntegerField()

    # Sync paths raise — the wrong API for this Model.
    Author.objects.create(name="x")
    # AsyncOnlyError: AsyncModel forbids sync access. Use acreate(...).

    # Async paths work as usual.
    author = await Author.objects.acreate(name="x")
    authors = [a async for a in Author.objects.afilter(active=True)]

The class swaps the default :class:`dorm.Manager` for
:class:`AsyncOnlyManager`, which raises :class:`AsyncOnlyError` from
every sync method. The async ``a*`` methods are passed through
unchanged. Save / delete on instances are similarly gated: callers
get ``asave()`` / ``adelete()`` but ``save()`` / ``delete()`` raise.

The error subclasses :class:`RuntimeError` so handlers that already
catch generic runtime errors do something reasonable, but the
distinct class lets you write tests that assert *exactly* this
failure mode.
"""

from __future__ import annotations

from typing import Any

from ..manager import BaseManager, Manager
from ..models import Model


class AsyncOnlyError(RuntimeError):
    """Raised when sync ORM API is called on an :class:`AsyncModel`.

    Carries the offending method name for clearer test assertions and
    log messages. Subclasses :class:`RuntimeError` so generic
    error handlers degrade gracefully."""

    def __init__(self, method: str) -> None:
        super().__init__(
            f"AsyncModel forbids sync access. Method {method!r} is "
            f"sync-only — use the async equivalent (typically "
            f"prefixed with 'a': a{method.lstrip('_')}, "
            f"asave, adelete, acreate, aget, afilter, ...)."
        )
        self.method = method


_SYNC_FORBIDDEN: frozenset[str] = frozenset(
    {
        "all",
        "filter",
        "exclude",
        "get",
        "create",
        "update_or_create",
        "get_or_create",
        "first",
        "last",
        "exists",
        "count",
        "delete",
        "update",
        "bulk_create",
        "bulk_update",
        "in_bulk",
        "iterator",
        "values",
        "values_list",
        "earliest",
        "latest",
        "aggregate",
    }
)


class AsyncOnlyManager(Manager):
    """Manager that forwards async methods and rejects sync ones.

    Async methods (``aall``, ``afilter``, ``acreate``, ``aget``,
    etc.) hit the parent implementation untouched. Every sync method
    raises :class:`AsyncOnlyError`.
    """

    def __getattribute__(self, name: str) -> Any:
        # ``__getattribute__`` is called for every attr access, so we
        # need to short-circuit the dunders / private internals to
        # avoid a recursion explosion when the parent class's own
        # methods walk our attribute set.
        if name.startswith("_") or name.startswith("a") or name in (
            "model", "name", "creation_counter", "auto_created",
            "use_in_migrations", "contribute_to_class", "db_manager",
            "using", "get_queryset", "from_queryset",
        ):
            return super().__getattribute__(name)
        if name in _SYNC_FORBIDDEN:
            raise AsyncOnlyError(name)
        return super().__getattribute__(name)


class AsyncModel(Model):
    """Subclass this instead of :class:`dorm.Model` to forbid sync
    ORM access on the resulting class.

    The async-only contract is enforced two ways:

    1. The default ``objects`` manager is replaced with an
       :class:`AsyncOnlyManager` — any sync class-level call (e.g.
       ``MyModel.objects.create(...)``) raises
       :class:`AsyncOnlyError`.
    2. Instance-level :meth:`save` and :meth:`delete` raise the same
       error — callers must use :meth:`asave` and :meth:`adelete`.

    Sub-classing rules: ``AsyncModel`` itself stays abstract (no
    table). Concrete subclasses get the strict-async behaviour
    automatically; you can still override ``objects`` if you want a
    custom async-only Manager subclass.
    """

    objects = AsyncOnlyManager()

    class Meta:
        abstract = True

    def save(self, *args: Any, **kwargs: Any) -> Any:
        raise AsyncOnlyError("save")

    def delete(self, *args: Any, **kwargs: Any) -> Any:
        raise AsyncOnlyError("delete")


__all__ = [
    "AsyncModel",
    "AsyncOnlyError",
    "AsyncOnlyManager",
]


# Silence the unused-import linter — ``BaseManager`` is referenced in
# the docstring for IDE hovers.
_ = BaseManager
