"""Streaming serialisation primitives for very large querysets.

Yields encoded ``bytes`` so the caller can hand them straight to a
``StreamingResponse`` (FastAPI / Starlette / Litestar), an
``aiohttp.StreamResponse``, an open file, or an S3 multipart upload —
without materialising the whole result set in memory.

Five formats:

- :func:`stream_json` — single ``[{...}, {...}]`` JSON array. Memory-
  bounded chunked emission; each row is encoded incrementally with
  comma separators.
- :func:`stream_jsonl` — newline-delimited JSON. One row per line.
  De-facto standard for log-shaped exports and streaming ingestion.
- :func:`stream_csv` — RFC-4180 CSV with a header row built from the
  first row's keys.
- :func:`stream_bytes` — pass-through for callers that already
  serialise rows themselves.
- :func:`stream_ndjson_pretty` — pretty-printed JSONL for humans
  reading the export with ``less``.

All helpers accept either a plain ``QuerySet`` (sync or async) or
any iterable / async iterable of dicts. They route through
:meth:`QuerySet.iterator` / :meth:`aiterator` with a configurable
``chunk_size`` so memory stays flat regardless of result-set size.

Example::

    from dorm.contrib.streaming import astream_jsonl

    async def export(qs):
        async for chunk in astream_jsonl(qs, chunk_size=1000):
            yield chunk  # bytes — pass to StreamingResponse / send_bytes

The helpers are framework-agnostic. They never import FastAPI,
Starlette, or any web framework.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any, AsyncIterator, Iterable, Iterator


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Coerce a queryset row into a dict.

    - dict: passed through.
    - dorm Model instance: emits the concrete-field columns as
      ``{attname: value}``. Skips relations (avoids O(N) extra queries
      mid-stream); use ``values()`` upstream when you need joins.
    - tuple/list: returned as ``{f"col_{i}": value}``.
    """
    if isinstance(row, dict):
        return row
    meta = getattr(row, "_meta", None)
    if meta is not None:
        out: dict[str, Any] = {}
        for f in meta.fields:
            if not f.column:
                continue
            out[f.attname] = row.__dict__.get(f.attname)
        return out
    if isinstance(row, (tuple, list)):
        return {f"col_{i}": v for i, v in enumerate(row)}
    return {"value": row}


class _JSONEncoder(json.JSONEncoder):
    """JSON encoder with sensible fallbacks for ORM-shaped values:
    datetime → ISO-8601, Decimal → str, UUID / Enum → ``str(value)``.
    """

    def default(self, o):
        try:
            from datetime import date, datetime, time, timedelta
            from decimal import Decimal
            from enum import Enum
            from uuid import UUID
        except Exception:
            return super().default(o)
        if isinstance(o, (datetime, date, time)):
            return o.isoformat()
        if isinstance(o, timedelta):
            return o.total_seconds()
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, (bytes, bytearray)):
            return o.hex()
        return super().default(o)


def _dump(obj: Any, *, indent: int | None = None) -> str:
    return json.dumps(
        obj, ensure_ascii=False, separators=(",", ":") if indent is None else None,
        cls=_JSONEncoder, indent=indent,
    )


# ── Sync primitives ─────────────────────────────────────────────────────────


def _iterator_accepts_chunk_size(it: Any) -> bool:
    """Return True when ``it`` is an ``iterator()`` method that
    accepts a ``chunk_size`` kwarg.

    Inspecting the signature instead of catching a blanket
    ``TypeError`` from a failed call avoids swallowing real errors
    that happen *inside* the iterator body (a misbehaving
    ``__next__`` bubbling ``TypeError`` would otherwise be silently
    retried with no chunk size and either succeed differently or
    fail with a different message)."""
    import inspect

    try:
        sig = inspect.signature(it)
    except (TypeError, ValueError):
        return False
    if "chunk_size" in sig.parameters:
        return True
    # ``**kwargs`` accepts anything — assume yes.
    return any(
        p.kind is inspect.Parameter.VAR_KEYWORD
        for p in sig.parameters.values()
    )


def _iter_rows(source: Any, chunk_size: int) -> Iterator[Any]:
    """Adapter that pulls rows out of *source*: either a QuerySet
    (calls ``iterator(chunk_size)``) or any iterable."""
    iterator = getattr(source, "iterator", None)
    if callable(iterator):
        if _iterator_accepts_chunk_size(iterator):
            yield from iterator(chunk_size=chunk_size)
        else:
            yield from iterator()
        return
    yield from iter(source)


def stream_json(source: Any, *, chunk_size: int = 1000) -> Iterator[bytes]:
    """Yield bytes of a single ``[{...}, {...}]`` JSON array.

    Suitable for clients that expect a JSON document; the encoded
    bytes are streamed comma-separated so memory stays flat.
    """
    yield b"["
    first = True
    for row in _iter_rows(source, chunk_size):
        sep = b"" if first else b","
        first = False
        yield sep + _dump(_row_to_dict(row)).encode("utf-8")
    yield b"]"


def stream_jsonl(source: Any, *, chunk_size: int = 1000) -> Iterator[bytes]:
    """Yield newline-delimited JSON. One full record per line."""
    for row in _iter_rows(source, chunk_size):
        yield _dump(_row_to_dict(row)).encode("utf-8") + b"\n"


def stream_ndjson_pretty(source: Any, *, chunk_size: int = 1000) -> Iterator[bytes]:
    """Yield indented JSON records, one record per multi-line block.

    Use for ``less``-grade human inspection only — the format is not
    a valid JSONL document for downstream parsers."""
    for row in _iter_rows(source, chunk_size):
        yield _dump(_row_to_dict(row), indent=2).encode("utf-8") + b"\n"


def stream_csv(
    source: Any,
    *,
    chunk_size: int = 1000,
    columns: list[str] | None = None,
) -> Iterator[bytes]:
    """Yield RFC-4180 CSV bytes. The header row is built from
    *columns* when provided, otherwise from the first row's keys.

    Subsequent rows that lack a column emit an empty cell; extra
    keys are dropped (CSV's grid shape is non-negotiable).
    """
    rows = _iter_rows(source, chunk_size)
    first_row = None
    for first_row in rows:
        first_row = _row_to_dict(first_row)
        break
    if first_row is None:
        # Empty queryset — emit just the header if columns specified.
        if columns:
            buf = io.StringIO()
            csv.writer(buf, lineterminator="\n").writerow(columns)
            yield buf.getvalue().encode("utf-8")
        return

    cols = columns or list(first_row.keys())

    def _write(record: dict[str, Any]) -> bytes:
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(_csv_value(record.get(c)) for c in cols)
        return buf.getvalue().encode("utf-8")

    # Header + first row.
    head_buf = io.StringIO()
    csv.writer(head_buf, lineterminator="\n").writerow(cols)
    yield head_buf.getvalue().encode("utf-8")
    yield _write(first_row)

    for row in rows:
        yield _write(_row_to_dict(row))


def _csv_value(v: Any) -> str:
    if v is None:
        return ""
    from datetime import date, datetime, time
    from decimal import Decimal
    from uuid import UUID
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, (Decimal, UUID)):
        return str(v)
    if isinstance(v, (dict, list, tuple)):
        return _dump(v)
    return str(v)


def stream_bytes(source: Iterable[bytes]) -> Iterator[bytes]:
    """Pass-through helper for callers that already serialise rows
    themselves. Only useful as a uniform import surface."""
    yield from source


# ── Async primitives ────────────────────────────────────────────────────────


async def _aiter_rows(source: Any, chunk_size: int) -> AsyncIterator[Any]:
    aiterator = getattr(source, "aiterator", None)
    if callable(aiterator):
        if _iterator_accepts_chunk_size(aiterator):
            async for row in aiterator(chunk_size=chunk_size):
                yield row
        else:
            async for row in aiterator():
                yield row
        return
    if hasattr(source, "__aiter__"):
        async for row in source:
            yield row
        return
    for row in source:
        yield row


async def astream_json(
    source: Any, *, chunk_size: int = 1000
) -> AsyncIterator[bytes]:
    yield b"["
    first = True
    async for row in _aiter_rows(source, chunk_size):
        sep = b"" if first else b","
        first = False
        yield sep + _dump(_row_to_dict(row)).encode("utf-8")
    yield b"]"


async def astream_jsonl(
    source: Any, *, chunk_size: int = 1000
) -> AsyncIterator[bytes]:
    async for row in _aiter_rows(source, chunk_size):
        yield _dump(_row_to_dict(row)).encode("utf-8") + b"\n"


async def astream_csv(
    source: Any,
    *,
    chunk_size: int = 1000,
    columns: list[str] | None = None,
) -> AsyncIterator[bytes]:
    """Async CSV stream. See :func:`stream_csv` for the contract."""
    rows_iter = _aiter_rows(source, chunk_size)
    first_row = None
    async for r in rows_iter:
        first_row = _row_to_dict(r)
        break
    if first_row is None:
        if columns:
            buf = io.StringIO()
            csv.writer(buf, lineterminator="\n").writerow(columns)
            yield buf.getvalue().encode("utf-8")
        return

    cols = columns or list(first_row.keys())

    def _write(record: dict[str, Any]) -> bytes:
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\n").writerow(
            _csv_value(record.get(c)) for c in cols
        )
        return buf.getvalue().encode("utf-8")

    head_buf = io.StringIO()
    csv.writer(head_buf, lineterminator="\n").writerow(cols)
    yield head_buf.getvalue().encode("utf-8")
    yield _write(first_row)

    async for row in rows_iter:
        yield _write(_row_to_dict(row))


__all__ = [
    "astream_csv",
    "astream_json",
    "astream_jsonl",
    "stream_bytes",
    "stream_csv",
    "stream_json",
    "stream_jsonl",
    "stream_ndjson_pretty",
]
