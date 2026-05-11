"""Optional serializers for dorm rows: msgpack + Avro + OpenAPI.

Each helper degrades gracefully when its underlying library isn't
installed — raises :class:`ImportError` with a clear hint pointing at
the optional extra.
"""
from __future__ import annotations

from typing import Any, Iterable, Iterator


def _row_to_dict(row: Any) -> dict[str, Any]:
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
    return {"value": row}


# ── MessagePack ─────────────────────────────────────────────────────────────


def stream_msgpack(source: Iterable[Any]) -> Iterator[bytes]:
    """Yield MessagePack bytes — one packed object per row.

    Streamed; memory consumption is bounded by row size. Requires
    ``msgpack`` (``pip install msgpack``)."""
    try:
        import msgpack  # type: ignore[import-not-found]  # ty:ignore[unresolved-import]
    except ImportError as e:
        raise ImportError(
            "stream_msgpack requires the 'msgpack' package. "
            "Install with: pip install msgpack"
        ) from e
    packer = msgpack.Packer(use_bin_type=True, default=_msgpack_default)
    iterator = getattr(source, "iterator", None)
    rows: Iterable[Any]
    if callable(iterator):
        rows = iterator()
    else:
        rows = source
    for row in rows:
        yield packer.pack(_row_to_dict(row))


def _msgpack_default(obj: Any) -> Any:
    from datetime import date, datetime, time
    from decimal import Decimal
    from uuid import UUID

    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj)
    raise TypeError(f"unserialisable type: {type(obj).__name__}")


# ── Avro ────────────────────────────────────────────────────────────────────


def avro_schema_for(model_cls: type) -> dict[str, Any]:
    """Generate an Avro schema dict from *model_cls*'s field meta.

    Lossless for primitives + strings + datetimes; complex types
    (JSON, ranges) fall back to ``string`` with a logical-type
    annotation. Returns a plain dict — feed to ``fastavro.parse_schema``
    or ``avro.schema.parse(json.dumps(...))`` downstream."""
    from .. import fields as _fields

    meta = getattr(model_cls, "_meta", None)
    if meta is None:
        raise TypeError(f"{model_cls!r} is not a dorm Model")
    avro_fields: list[dict[str, Any]] = []
    for f in meta.fields:
        if getattr(f, "many_to_many", False):
            continue
        if isinstance(f, _fields.IntegerField):
            t: Any = "long"
        elif isinstance(f, _fields.FloatField):
            t = "double"
        elif isinstance(f, _fields.BooleanField):
            t = "boolean"
        elif isinstance(f, _fields.DateTimeField):
            t = {"type": "long", "logicalType": "timestamp-micros"}
        elif isinstance(f, _fields.DateField):
            t = {"type": "int", "logicalType": "date"}
        elif isinstance(f, _fields.UUIDField):
            t = {"type": "string", "logicalType": "uuid"}
        elif isinstance(f, _fields.BinaryField):
            t = "bytes"
        elif isinstance(f, _fields.DecimalField):
            t = {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": f.max_digits or 18,
                "scale": f.decimal_places or 0,
            }
        else:
            t = "string"
        if getattr(f, "null", False):
            t = ["null", t]
        avro_fields.append({"name": f.name, "type": t})
    return {
        "type": "record",
        "name": model_cls.__name__,
        "namespace": getattr(meta, "app_label", "dorm"),
        "fields": avro_fields,
    }


# ── OpenAPI ────────────────────────────────────────────────────────────────


def openapi_schema_for(
    model_cls: type, *, include_pk: bool = True
) -> dict[str, Any]:
    """Generate an OpenAPI 3.1 schema dict from *model_cls*.

    Maps dorm field types to JSON-Schema-style entries with a
    ``"format"`` hint where applicable (``"date-time"``, ``"uuid"``,
    ``"email"``). Result is a plain dict — splice into an OpenAPI
    spec's ``components.schemas`` section.
    """
    from .. import fields as _fields

    meta = getattr(model_cls, "_meta", None)
    if meta is None:
        raise TypeError(f"{model_cls!r} is not a dorm Model")
    props: dict[str, dict[str, Any]] = {}
    required: list[str] = []
    for f in meta.fields:
        if getattr(f, "many_to_many", False):
            continue
        if f.primary_key and not include_pk:
            continue
        entry: dict[str, Any] = {}
        if isinstance(f, _fields.IntegerField):
            entry["type"] = "integer"
        elif isinstance(f, _fields.FloatField):
            entry["type"] = "number"
        elif isinstance(f, _fields.DecimalField):
            entry["type"] = "string"
            entry["format"] = "decimal"
        elif isinstance(f, _fields.BooleanField):
            entry["type"] = "boolean"
        elif isinstance(f, _fields.DateTimeField):
            entry["type"] = "string"
            entry["format"] = "date-time"
        elif isinstance(f, _fields.DateField):
            entry["type"] = "string"
            entry["format"] = "date"
        elif isinstance(f, _fields.UUIDField):
            entry["type"] = "string"
            entry["format"] = "uuid"
        elif isinstance(f, _fields.EmailField):
            entry["type"] = "string"
            entry["format"] = "email"
        elif isinstance(f, _fields.URLField):
            entry["type"] = "string"
            entry["format"] = "uri"
        elif isinstance(f, _fields.BinaryField):
            entry["type"] = "string"
            entry["format"] = "byte"
        elif isinstance(f, _fields.JSONField):
            entry["type"] = "object"
        else:
            entry["type"] = "string"
            if getattr(f, "max_length", None):
                entry["maxLength"] = f.max_length
        if not getattr(f, "null", False) and not getattr(f, "blank", False):
            required.append(f.name)
        props[f.name] = entry
    return {
        "type": "object",
        "title": model_cls.__name__,
        "properties": props,
        "required": required,
    }


__all__ = [
    "stream_msgpack",
    "avro_schema_for",
    "openapi_schema_for",
]
