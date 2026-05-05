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
import re
from typing import Any

from .. import signals


_OP_RE = re.compile(
    r"^\s*(?:--[^\n]*\n|/\*.*?\*/|\s)*"  # leading comments + whitespace
    r"(SELECT|INSERT|UPDATE|DELETE|MERGE|UPSERT|TRUNCATE|CREATE|ALTER|DROP|COPY)",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("SELECT", re.compile(r"\bFROM\s+\"?([\w$.]+)\"?", re.IGNORECASE)),
    ("INSERT", re.compile(r"\bINTO\s+\"?([\w$.]+)\"?", re.IGNORECASE)),
    ("UPDATE", re.compile(r"\bUPDATE\s+\"?([\w$.]+)\"?", re.IGNORECASE)),
    ("DELETE", re.compile(r"\bFROM\s+\"?([\w$.]+)\"?", re.IGNORECASE)),
    ("COPY", re.compile(r"\bCOPY\s+\"?([\w$.]+)\"?", re.IGNORECASE)),
]


def _classify_operation(sql: str) -> str | None:
    """Return the SQL verb at the head of *sql* in upper case.

    Returns ``None`` when nothing matches — anonymous DDL / vendor
    extensions get a generic span name instead of ``UNKNOWN``."""
    if not sql:
        return None
    m = _OP_RE.match(sql)
    return m.group(1).upper() if m else None


def _extract_table(sql: str, operation: str | None) -> str | None:
    """Extract the primary table name targeted by *sql*.

    Best-effort: only the first match is returned, so multi-table
    statements (UPDATE … FROM, joined SELECTs) drop additional table
    names. Good enough for span naming."""
    if operation is None or not sql:
        return None
    for op, pattern in _TABLE_PATTERNS:
        if op != operation:
            continue
        m = pattern.search(sql)
        if m:
            return m.group(1)
    return None


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
        # Operation classification (SELECT / INSERT / UPDATE / DELETE)
        # follows the OTel semantic convention ``db.operation``. Walk
        # past leading whitespace and comments to find the verb.
        operation = _classify_operation(sql)
        # Best-effort table-name extraction so the span name carries
        # the high-cardinality bit operators actually want to filter
        # on. Failure is graceful — span name falls back to ``db.<sys>``.
        table = _extract_table(sql, operation)

        attrs: dict[str, Any] = {
            "db.system": sender,
            "db.statement": sql[:1024],
        }
        if operation:
            attrs["db.operation"] = operation
        if table:
            attrs["db.sql.table"] = table

        # Adopt the v1.20+ stable attribute names alongside the legacy
        # ones — exporters that already migrated pick up
        # ``db.collection.name``; older ones still see ``db.sql.table``.
        if table:
            attrs["db.collection.name"] = table

        # Connection alias — visible in the span so multi-DB apps can
        # filter on it. ``alias`` is added to signal kwargs by every
        # backend wrapper that emits these signals; default to "default".
        alias = kwargs.get("alias")
        if alias:
            attrs["db.dorm.alias"] = alias

        span_name = (
            f"{operation} {table}" if (operation and table) else f"db.{sender}"
        )
        span = tracer.start_span(name=span_name, attributes=attrs)
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
