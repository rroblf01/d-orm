"""JSON fixtures: dump model rows to JSON and load them back.

The format mirrors Django's ``dumpdata`` / ``loaddata`` output so users
porting from Django can keep their existing fixtures::

    [
      {"model": "blog.Author", "pk": 1, "fields": {"name": "Alice"}},
      {"model": "blog.Article", "pk": 7, "fields": {
          "title": "Hello", "author": 1, "tags": [3, 5]
      }}
    ]

Loading bypasses the ``save()`` path and signals on purpose — fixtures
restore a known state and the per-row roundtrip cost matters when
seeding a multi-thousand-row test database. Use :meth:`Model.save` (or
:meth:`Model.objects.create`) when you do want pre-save hooks to fire.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import enum
import json
import uuid
from typing import Any, Iterable, Iterator

from .exceptions import FieldDoesNotExist


def _serialize_value(value: Any) -> Any:
    """Convert *value* into something :mod:`json` can dump natively.

    The chosen wire forms round-trip through :func:`_deserialize_value`
    when paired with the right :class:`Field`. Types not listed here
    fall through unchanged: ``None``, ``bool``, ``int``, ``float``,
    ``str``, ``list`` and ``dict`` are already JSON-native.
    """
    if value is None:
        return None
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        # Microseconds — round-trips losslessly through
        # :meth:`DurationField.from_db_value`.
        return (
            value.days * 86_400 * 10 ** 6
            + value.seconds * 10 ** 6
            + value.microseconds
        )
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__bytes__": base64.b64encode(bytes(value)).decode("ascii")}
    # Range field value — flatten to a dict the loader recognises.
    if all(hasattr(value, attr) for attr in ("lower", "upper", "bounds")):
        return {
            "__range__": True,
            "lower": _serialize_value(value.lower),
            "upper": _serialize_value(value.upper),
            "bounds": value.bounds,
        }
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    return value


def _row_to_dict(instance: Any) -> dict[str, Any]:
    """Build the ``{"model", "pk", "fields"}`` envelope for one row."""
    meta = instance._meta
    label = f"{meta.app_label}.{instance.__class__.__name__}"
    fields_out: dict[str, Any] = {}
    for field in meta.fields:
        if field.primary_key:
            continue
        if getattr(field, "many_to_many", False):
            # Emit M2M relations as a list of related-object PKs so the
            # fixture round-trips. We read the through table directly to
            # avoid hydrating the full target rows.
            try:
                manager = getattr(instance, field.name)
                pks = [obj.pk for obj in manager.all()]
            except Exception:
                pks = []
            fields_out[field.name] = pks
            continue
        if not field.column:
            continue
        # FKs serialize as the underlying ``<name>_id`` value, matching
        # Django's natural form. Plain fields read straight from the
        # instance dict, then go through ``_serialize_value`` so types
        # like Decimal / UUID / timedelta survive the JSON round-trip.
        attname = field.attname
        raw = instance.__dict__.get(attname)
        fields_out[field.name] = _serialize_value(raw)
    return {"model": label, "pk": instance.pk, "fields": fields_out}


def serialize(querysets_or_iterables: Iterable[Any]) -> list[dict[str, Any]]:
    """Serialize the union of *querysets_or_iterables* into a list of dicts.

    Each element may be a model class (treated as ``Model.objects.all()``),
    a queryset, or any iterable of model instances. The order of the
    output matches the iteration order of the inputs concatenated.
    """
    out: list[dict[str, Any]] = []
    for source in querysets_or_iterables:
        if isinstance(source, type):
            iterable: Iterable[Any] = source.objects.all()
        elif hasattr(source, "all") and callable(source.all):
            iterable = source.all() if not hasattr(source, "__iter__") else source
        else:
            iterable = source
        for instance in iterable:
            out.append(_row_to_dict(instance))
    return out


def dumps(querysets_or_iterables: Iterable[Any], *, indent: int | None = None) -> str:
    """Serialize and JSON-encode in one step."""
    return json.dumps(
        serialize(querysets_or_iterables), indent=indent, ensure_ascii=False
    )


# ── loaddata ──────────────────────────────────────────────────────────────────


def _deserialize_value(field: Any, value: Any) -> Any:
    """Reverse of :func:`_serialize_value` for a single field/value pair.

    The field's :meth:`to_python` does most of the heavy lifting (it's
    the same logic ``Model.__init__`` uses); we only pre-process the
    custom envelope shapes the dumper emits — bytes and ranges — so
    they reach ``to_python`` already typed.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if "__bytes__" in value:
            return base64.b64decode(value["__bytes__"].encode("ascii"))
        if value.get("__range__"):
            from .fields import Range

            return Range(value.get("lower"), value.get("upper"), bounds=value.get("bounds", "[)"))
    return field.to_python(value)


def _resolve_model(label: str) -> Any:
    """Find a model class by ``app.ModelName`` or ``ModelName`` lookup."""
    from .models import _model_registry

    if label in _model_registry:
        return _model_registry[label]
    # Bare class name fallback — match Django's tolerance for fixtures
    # written without app prefixes.
    if "." in label:
        bare = label.split(".", 1)[1]
        if bare in _model_registry:
            return _model_registry[bare]
    raise LookupError(
        f"loaddata: model {label!r} not found in INSTALLED_APPS. "
        "Make sure the app is registered before calling load()."
    )


def _build_insert_record(model: Any, record: dict[str, Any]) -> tuple[
    list[Any], list[Any], dict[str, list[Any]]
]:
    """Translate one fixture record into ``(fields, values, m2m_targets)``.

    ``m2m_targets`` is keyed by M2M field name with a list of related
    PKs to insert into the junction table after the row itself lands.
    """
    pk_value = record.get("pk")
    field_values = record.get("fields", {})
    meta = model._meta
    fields_out: list[Any] = []
    values_out: list[Any] = []
    m2m_targets: dict[str, list[Any]] = {}

    pk_field = meta.pk
    if pk_value is not None and pk_field is not None:
        fields_out.append(pk_field)
        values_out.append(pk_field.get_db_prep_value(pk_field.to_python(pk_value)))

    for fname, raw in field_values.items():
        try:
            field = meta.get_field(fname)
        except FieldDoesNotExist:
            # Forward-compat: ignore unknown columns rather than failing
            # the whole load — fixtures get reused across schema versions
            # and a migration in flight is the common case.
            continue
        if getattr(field, "many_to_many", False):
            m2m_targets[fname] = list(raw or [])
            continue
        if not field.column:
            continue
        py_value = _deserialize_value(field, raw)
        fields_out.append(field)
        values_out.append(field.get_db_prep_value(py_value))
    return fields_out, values_out, m2m_targets


def load(
    text: str,
    *,
    using: str = "default",
) -> int:
    """Load JSON-encoded fixture *text* into the database.

    Returns the number of rows inserted. The whole load runs inside an
    :func:`atomic` block, so a malformed record rolls back the partial
    state instead of half-loading. M2M relations are inserted after all
    parent rows exist.
    """
    from .db.connection import get_connection
    from .query import SQLQuery
    from .transaction import atomic

    records = json.loads(text)
    if not isinstance(records, list):
        raise ValueError(
            "loaddata: fixture root must be a JSON array of "
            "{model, pk, fields} objects."
        )

    conn = get_connection(using)
    inserted = 0

    vendor = getattr(conn, "vendor", "sqlite")

    with atomic(using=using):
        # Defer FK validation for the duration of the load so
        # records can reference rows that appear later in the
        # fixture (self-referential FKs and cyclic graphs are the
        # canonical cases — Django's ``loaddata`` does the same).
        # Without deferral a ``Category[parent_id=2]`` row inserted
        # before its parent ``Category[pk=2]`` failed with
        # ``IntegrityError: FK violates``.
        if vendor == "postgresql":
            try:
                conn.execute_script("SET CONSTRAINTS ALL DEFERRED")
            except Exception:
                # Constraint may not be DEFERRABLE — fall back to
                # in-order insert; user gets the same FK error
                # they would have without this fix.
                pass
        elif vendor == "sqlite":
            try:
                conn.execute_script("PRAGMA defer_foreign_keys=ON")
            except Exception:
                pass

        # Phase 1 — base rows.
        deferred_m2m: list[tuple[Any, Any, dict[str, list[Any]]]] = []
        for record in records:
            label = record.get("model")
            if not label:
                continue
            model = _resolve_model(label)
            fields, values, m2m_targets = _build_insert_record(model, record)
            if not fields:
                continue
            query = SQLQuery(model)
            sql, params = query.as_insert(fields, values, conn)
            pk_col = model._meta.pk.column if model._meta.pk else "id"
            new_pk = conn.execute_insert(sql, params, pk_col=pk_col)
            effective_pk = record.get("pk") if record.get("pk") is not None else new_pk
            inserted += 1
            if m2m_targets:
                deferred_m2m.append((model, effective_pk, m2m_targets))

        # Phase 2 — junction rows. We use the M2M field's helpers to
        # discover the through table + column names so a custom
        # ``through=`` model is handled correctly.
        for model, source_pk, m2m_targets in deferred_m2m:
            for fname, target_pks in m2m_targets.items():
                if not target_pks:
                    continue
                m2m = model._meta.get_field(fname)
                table = m2m._get_through_table()
                src_col, tgt_col = m2m._get_through_columns()
                for target_pk in target_pks:
                    conn.execute_write(
                        f'INSERT INTO "{table}" ("{src_col}", "{tgt_col}") '
                        "VALUES (%s, %s)",
                        [source_pk, target_pk],
                    )
    return inserted


def deserialize(text: str) -> Iterator[Any]:
    """Yield model instances reconstructed from *text*, without inserting.

    Useful when callers want to inspect or transform fixture rows
    before persisting them. Each instance's ``pk`` is set from the
    fixture record; FK columns are filled with the raw PK value the
    fixture stores, matching what :meth:`Model._from_db_row` produces.
    """
    records = json.loads(text)
    for record in records:
        label = record.get("model")
        if not label:
            continue
        model = _resolve_model(label)
        instance = model.__new__(model)
        instance.__dict__ = {}
        meta = model._meta
        if meta.pk is not None and record.get("pk") is not None:
            instance.__dict__[meta.pk.attname] = meta.pk.to_python(record["pk"])
        for fname, raw in (record.get("fields") or {}).items():
            try:
                field = meta.get_field(fname)
            except FieldDoesNotExist:
                continue
            if getattr(field, "many_to_many", False) or not field.column:
                continue
            instance.__dict__[field.attname] = _deserialize_value(field, raw)
        yield instance
