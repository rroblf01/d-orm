"""Query-count guard for development and tests.

Counts every SQL statement executed inside a context-manager block and
emits a single ``WARNING`` when the count exceeds a configurable
threshold. Useful as a lightweight N+1 guard around HTTP request
handlers, RPC entry points, or hot loops:

.. code-block:: python

    from dorm.contrib.querycount import query_count_guard

    with query_count_guard(warn_above=20, label="GET /articles"):
        return [article_dict(a) for a in Article.objects.all()]

Implementation:

- Per-block counter lives in a ``contextvars.ContextVar`` so async
  code paths get isolated counters per task.
- A single ``pre_query`` listener is connected on first use and
  re-used by every guard — the listener is a no-op when no guard is
  active so projects that never enter a guard pay only the per-signal
  empty-receiver dispatch cost (already paid for any other receiver).

The default ``warn_above`` is taken from
``settings.QUERY_COUNT_WARN``. ``None`` (the default) leaves the
guard inert — it counts but never warns. ``0`` warns on the first
query.
"""

from __future__ import annotations

import contextvars
import logging
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .. import signals
from ..conf import settings

_logger = logging.getLogger("dorm.querycount")

# Per-task counter. ``None`` = no guard is active on this task; an
# integer = the running count of queries since the most recent guard
# was entered. Nested guards are supported via the saved-token return
# of ``ContextVar.set``.
_count: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "dorm_querycount", default=None
)

_listener_attached: bool = False
_listener_lock = threading.Lock()


def _on_pre_query(sender: Any, **kwargs: Any) -> None:
    """Increment the active per-task counter, if any."""
    n = _count.get()
    if n is None:
        return
    _count.set(n + 1)


def _ensure_listener() -> None:
    """Attach the ``pre_query`` listener on first use. Idempotent."""
    global _listener_attached
    if _listener_attached:
        return
    with _listener_lock:
        if _listener_attached:
            return
        signals.pre_query.connect(_on_pre_query, weak=False)
        _listener_attached = True


@contextmanager
def query_count_guard(
    warn_above: int | None = None,
    *,
    label: str | None = None,
) -> Iterator["QueryCount"]:
    """Count queries executed inside the block.

    *warn_above* — emit a WARNING on exit if the count exceeds this
    threshold. Falls back to ``settings.QUERY_COUNT_WARN`` when not
    given; ``None`` means "count but never warn".

    *label* — included in the warning to identify the call-site.
    Useful when the guard wraps a request handler.

    Yields a :class:`QueryCount` whose ``count`` attribute holds the
    running total — useful in tests to assert exact numbers without
    relying on log capture.
    """
    _ensure_listener()
    if warn_above is None:
        warn_above = getattr(settings, "QUERY_COUNT_WARN", None)

    state = QueryCount()
    token = _count.set(0)
    try:
        yield state
    finally:
        state.count = _count.get() or 0
        _count.reset(token)
        if warn_above is not None and state.count > warn_above:
            tag = f" [{label}]" if label else ""
            _logger.warning(
                "query count exceeded threshold%s: %d > %d",
                tag,
                state.count,
                warn_above,
            )


class QueryCount:
    """Live handle returned by :func:`query_count_guard`.

    The ``count`` attribute is updated when the guard exits — reading
    it inside the ``with`` block returns 0 because the guard hasn't
    finalised yet. Tests typically read it after the block.
    """

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0


__all__ = ["query_count_guard", "QueryCount"]
