"""OpenTelemetry auto-instrumentation for djanorm.

Hooks the ``pre_query`` / ``post_query`` signals so every query becomes
a span without any per-call-site changes. Optional dependency on
``opentelemetry-api`` — call :func:`instrument` from your application
startup; if OTel isn't installed, raises ``ImportError`` with a hint.

Usage::

    from dorm.contrib.otel import instrument

    instrument()                      # default tracer name "dorm"
    # instrument(tracer_name="myapp.dorm")

After this, every ORM query produces a span with attributes:

- ``db.system`` = ``"postgresql"`` / ``"sqlite"``
- ``db.statement`` = the SQL text
- ``db.dorm.elapsed_ms`` = post-query elapsed (set on the span before
  it ends, so trace exporters pick it up)

If a query raises, the span is marked with status ``ERROR`` and the
exception's ``__class__.__name__`` lands in ``db.dorm.error``.

Call :func:`uninstrument` to detach. Idempotent — calling
:func:`instrument` twice replaces the previous wiring.
"""

from __future__ import annotations

import logging
from typing import Any

from .. import signals


_log = logging.getLogger("dorm.contrib.otel")

# Module-level so :func:`uninstrument` can disconnect the same
# receivers we connected. Stored as a tuple ``(pre, post)``.
_RECEIVERS: tuple[Any, Any] | None = None
_SPAN_BY_QUERY: dict[int, Any] = {}


def _import_otel():
    try:
        from opentelemetry import trace
    except ImportError as exc:  # pragma: no cover — exercised only without OTel
        raise ImportError(
            "dorm.contrib.otel requires opentelemetry-api. "
            "Install with: pip install opentelemetry-api opentelemetry-sdk"
        ) from exc
    return trace


def instrument(tracer_name: str = "dorm") -> None:
    """Wire ``pre_query`` / ``post_query`` to OpenTelemetry spans.

    *tracer_name* is what shows up in the exporter as the instrument
    library name; default ``"dorm"`` is fine for most apps. Calling
    twice replaces the previous wiring (no double-spans).
    """
    global _RECEIVERS

    trace = _import_otel()
    tracer = trace.get_tracer(tracer_name)

    def _on_pre_query(sender: str, **kwargs: Any) -> None:
        sql = kwargs.get("sql", "")
        # Key by the id() of the sql+params tuple — pre_query and
        # post_query are emitted from the same scope so id() stability
        # holds for the duration of the query.
        # Note: tracking by id() of the SQL string isn't perfect (same
        # SQL string in different threads could collide) but in practice
        # we use the params tuple too to disambiguate. A thread-local
        # would be cleaner but signals don't expose call_id.
        span = tracer.start_span(
            name=f"db.{sender}",
            attributes={
                "db.system": sender,
                "db.statement": sql[:1024],  # truncate giant SQL
            },
        )
        # Stash the span keyed by the (sender, id(sql), id(params)) tuple
        # so post_query can find it.
        params = kwargs.get("params")
        key = (sender, id(sql), id(params))
        _SPAN_BY_QUERY[hash(key)] = span

    def _on_post_query(sender: str, **kwargs: Any) -> None:
        sql = kwargs.get("sql", "")
        params = kwargs.get("params")
        elapsed_ms = kwargs.get("elapsed_ms", 0.0)
        error = kwargs.get("error")

        key = (sender, id(sql), id(params))
        span = _SPAN_BY_QUERY.pop(hash(key), None)
        if span is None:
            return
        try:
            span.set_attribute("db.dorm.elapsed_ms", float(elapsed_ms))
            if error is not None:
                span.set_attribute("db.dorm.error", type(error).__name__)
                # StatusCode.ERROR is the one we want; pull lazily so
                # the import is cached.
                from opentelemetry.trace import (
                    Status,
                    StatusCode,
                )

                span.set_status(Status(StatusCode.ERROR, str(error)))
        finally:
            span.end()

    # Replace any previous wiring so calls are idempotent.
    uninstrument()
    signals.pre_query.connect(_on_pre_query, weak=False)
    signals.post_query.connect(_on_post_query, weak=False)
    _RECEIVERS = (_on_pre_query, _on_post_query)
    _log.info("dorm.contrib.otel: instrumented (tracer=%r)", tracer_name)


def uninstrument() -> None:
    """Disconnect the OTel receivers installed by :func:`instrument`.
    Idempotent: safe to call when nothing is wired up."""
    global _RECEIVERS
    if _RECEIVERS is None:
        return
    pre, post = _RECEIVERS
    try:
        signals.pre_query.disconnect(pre)
    except Exception:  # pragma: no cover — defensive
        pass
    try:
        signals.post_query.disconnect(post)
    except Exception:  # pragma: no cover — defensive
        pass
    _RECEIVERS = None
    _SPAN_BY_QUERY.clear()


__all__ = ["instrument", "uninstrument"]
