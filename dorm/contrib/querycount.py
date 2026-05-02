"""Query-count guard for development and tests.

Counts every SQL statement executed inside a context-manager block and
emits a single ``WARNING`` when the count exceeds a configurable
threshold. Useful as a lightweight N+1 guard around HTTP request
handlers, RPC entry points, or hot loops:

.. code-block:: python

    from dorm.contrib.querycount import query_count_guard

    with query_count_guard(warn_above=20, label="GET /articles"):
        return [article_dict(a) for a in Article.objects.all()]

The default ``warn_above`` is taken from ``settings.QUERY_COUNT_WARN``.
``None`` (default) leaves the guard inert — it counts but never warns.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator

from .. import signals
from .._scoped import ScopedCollector
from ..conf import settings

_logger = logging.getLogger("dorm.querycount")


def _bump(state: list[int], _kwargs: dict[str, Any]) -> None:
    state[0] += 1


# Listener attached on first guard. State is a single-element list so
# the receiver mutates ``state[0]`` in place — avoids one
# ``ContextVar.set`` (and the Token allocation it implies) per query.
_collector: ScopedCollector[list[int]] = ScopedCollector(
    signals.pre_query, "dorm_querycount", _bump
)


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

    Yields a :class:`QueryCount` whose ``count`` attribute holds the
    final total after the block exits. Tests that need an exact-count
    assertion can read it directly without grepping the log.
    """
    if warn_above is None:
        warn_above = getattr(settings, "QUERY_COUNT_WARN", None)

    handle = QueryCount()
    state: list[int] = [0]
    token = _collector.open(state)
    try:
        yield handle
    finally:
        handle.count = state[0]
        _collector.close(token)
        if warn_above is not None and handle.count > warn_above:
            tag = f" [{label}]" if label else ""
            _logger.warning(
                "query count exceeded threshold%s: %d > %d",
                tag,
                handle.count,
                warn_above,
            )


class QueryCount:
    """Live handle returned by :func:`query_count_guard`. ``count``
    is finalised on context exit; reading it inside the ``with`` block
    returns 0."""

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0


__all__ = ["query_count_guard", "QueryCount"]
