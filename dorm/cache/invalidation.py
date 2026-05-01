"""Auto-invalidation hooks for queryset result caches.

Connected lazily on first :meth:`QuerySet.cache` call so projects
that never opt into caching pay zero cost.

Invalidation timing
-------------------

Bumping the version + wiping ``delete_pattern`` runs on
**transaction commit**, not on the raw ``post_save`` /
``post_delete`` signal:

- ``post_save`` fires AFTER the INSERT/UPDATE statement but
  BEFORE the surrounding ``atomic()`` block commits. Doing the
  invalidation there opens a stale-read race against a
  concurrent reader on a separate connection: the reader sees
  no committed update yet (snapshot isolation), populates the
  cache with PRE-write data under the bumped version, and the
  cache stays stale for the full TTL.
- The fix is to defer the bump + ``delete_pattern`` call to
  ``transaction.on_commit`` (and ``aon_commit`` on the async
  side). Outside an ``atomic()`` block ``on_commit`` runs the
  callback immediately — the connection has already
  auto-committed — so single-statement saves stay correct.
- On rollback, the deferred callback never fires; cache stays
  populated with pre-rollback (i.e. live) state. No spurious
  invalidation work.

Sync vs async dispatch
----------------------

Only ONE handler is connected per signal:

- The sync handler (``_drop_model_sync``) runs on
  ``Signal.send()`` — invoked by ``Model.save()`` /
  ``Model.delete()``.
- The async handler (``_drop_model_async``) is registered as a
  *coroutine receiver* on the same signal but routed through
  ``transaction.aon_commit``. ``Signal.asend()`` runs both sync
  and async receivers (see :mod:`dorm.signals`); to avoid a
  double bump on async saves we install only the sync handler
  and route to the right ``on_commit`` variant from inside
  based on ``asyncio.get_running_loop()``.

Trade-off
---------

- Coarse-grained: a single save invalidates *every* cached
  queryset for the model, including ones that wouldn't have
  matched the new row.
- Per-alias scoping: the cache key includes the queryset's
  ``self._db`` so two saves on different aliases do NOT clobber
  each other's caches.
- Cross-model writes (saving an ``Author`` while a queryset on
  ``Book`` is cached) are NOT auto-invalidated — only the
  saved model.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

from . import get_cache, model_cache_namespace

_signals_connected: bool = False
_lock = threading.Lock()


def _do_drop(sender: Any) -> None:
    """Bump the model's version + ``delete_pattern`` every cache.

    Shared body for the sync + async invalidation paths. The
    version bump MUST happen *inside* this function so it lands
    on transaction commit, not earlier — otherwise a concurrent
    reader can race-fill a stale entry under the bumped key
    before the writer's transaction is durable (see module
    docstring).
    """
    from . import bump_model_cache_version

    try:
        bump_model_cache_version(sender)
        namespace = model_cache_namespace(sender)
        from ..conf import settings

        caches = getattr(settings, "CACHES", {}) or {}
    except Exception:
        return
    pattern = f"{namespace}:*"
    for alias in caches:
        try:
            backend = get_cache(alias)
            backend.delete_pattern(pattern)
        except Exception:
            # Cache is best-effort; a Redis outage must NEVER
            # take down a save.
            pass


async def _ado_drop(sender: Any) -> None:
    """Async counterpart of :func:`_do_drop`. Same contract;
    routed through the cache backend's async API."""
    from . import bump_model_cache_version

    try:
        bump_model_cache_version(sender)
        namespace = model_cache_namespace(sender)
        from ..conf import settings

        caches = getattr(settings, "CACHES", {}) or {}
    except Exception:
        return
    pattern = f"{namespace}:*"
    for alias in caches:
        try:
            backend = get_cache(alias)
            await backend.adelete_pattern(pattern)
        except Exception:
            pass


def _drop_model(sender: Any, **kwargs: Any) -> None:
    """Single signal receiver — both ``post_save.send()`` and
    ``post_save.asend()`` fan out through here.

    Defers the bump + ``delete_pattern`` call to
    ``transaction.on_commit`` so the invalidation lands AFTER
    the writer's transaction commits — closes the stale-read
    race against a concurrent reader on a separate connection
    (snapshot isolation lets that reader see PRE-write rows
    while the writer's UPDATE is pending; if invalidation fired
    pre-commit, the reader would re-populate the cache with
    stale rows under the bumped version, surviving for the full
    TTL).

    ``on_commit`` runs the callback immediately when no atomic
    block is active (the writer's connection has already
    auto-committed), so single-statement saves stay correct
    without an explicit ``atomic()`` wrapper. On rollback the
    callback never fires — no spurious invalidation work.

    The drop body is sync regardless of the originating
    ``send`` vs ``asend`` path. ``RedisCache.delete_pattern`` is
    a single Redis round-trip; running it sync from inside a
    coroutine is acceptable, and avoids the dual-handler
    double-bump that would otherwise happen if we registered an
    async receiver alongside this one (``asend`` runs both).

    ``using`` is taken from the signal kwargs; ``Model.save`` /
    ``delete`` always forward it. Fallback ``"default"``.
    """
    using = kwargs.get("using") or "default"

    # Pick the right commit hook. Async context (``Model.asave``
    # called from a coroutine) uses ``aon_commit`` so callbacks
    # registered inside an ``aatomic()`` block stay deferred to
    # the outermost commit. Sync context uses ``on_commit``. Both
    # accept a sync callable: ``_do_drop`` returns ``None`` so
    # neither helper schedules a coroutine.
    try:
        asyncio.get_running_loop()
        async_ctx = True
    except RuntimeError:
        async_ctx = False

    cb = lambda: _do_drop(sender)  # noqa: E731 — single-line by design
    try:
        if async_ctx:
            from ..transaction import aon_commit

            aon_commit(cb, using=using)
        else:
            from ..transaction import on_commit

            on_commit(cb, using=using)
    except Exception:
        # Last-ditch: the on-commit machinery may itself reject
        # the callback (broken transaction state). Run inline so
        # the cache gets wiped regardless.
        try:
            _do_drop(sender)
        except Exception:
            pass


def invalidate_model(sender: Any, using: str = "default") -> None:
    """Schedule a cache invalidation for *sender* on commit.

    Public API used by bulk write operations (``QuerySet.update``,
    ``QuerySet.delete``, ``bulk_create``, ``bulk_update`` and the
    async equivalents) which DON'T fire ``post_save`` /
    ``post_delete`` per row. Without this hook every cached
    queryset on the model would survive bulk writes — silent
    stale data until TTL.

    Same routing as ``_drop_model``: defers to ``on_commit`` /
    ``aon_commit`` so the bump+wipe lands AFTER the bulk
    statement's transaction commits.
    """
    _drop_model(sender, using=using)


async def ainvalidate_model(sender: Any, using: str = "default") -> None:
    """Async counterpart of :func:`invalidate_model`. Scheduling
    happens through ``aon_commit`` because we're already inside a
    coroutine — the receiver detects the running loop and routes
    appropriately."""
    _drop_model(sender, using=using)


def ensure_signals_connected() -> None:
    """Wire up post_save / post_delete invalidation handlers once
    per process. Safe to call from every ``qs.cache()`` site —
    repeat calls are no-ops.

    A single sync receiver is registered on each signal; it
    routes to the sync or async commit hook internally based on
    the running event loop. Registering an async receiver on
    these signals would cause:

    - Sync ``Model.save()`` (``Signal.send``) to log a WARNING
      per call (async receivers get skipped on sync send).
    - Async ``Model.asave()`` (``Signal.asend``) to fire BOTH
      receivers (asend runs sync + async ones), double-bumping
      the version counter.

    The single-handler design avoids both pitfalls.
    """
    global _signals_connected
    if _signals_connected:
        return
    with _lock:
        if _signals_connected:
            return
        from ..signals import post_delete, post_save

        post_save.connect(
            _drop_model, dispatch_uid="dorm.cache.invalidation.save"
        )
        post_delete.connect(
            _drop_model, dispatch_uid="dorm.cache.invalidation.delete"
        )
        _signals_connected = True
