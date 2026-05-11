"""System-versioned (temporal) tables for point-in-time queries.

Wraps a model so every write also lands in a sibling history table
with ``valid_from`` / ``valid_to`` timestamps. Beyond what
``@track_history`` (audit log) offers, the temporal layer answers
**"what did this row look like at time T?"** in one query.

Usage::

    from dorm.contrib.temporal import temporal

    @temporal
    class Article(dorm.Model):
        title = dorm.CharField(max_length=200)
        body  = dorm.TextField()

    # At runtime:
    Article.objects.create(title="v1", body="...")
    article = Article.objects.first()
    article.title = "v2"
    article.save()

    # Point-in-time query:
    snapshot = Article.objects.as_of(t)              # all rows valid at t
    one      = Article.objects.as_of(t).get(pk=42)   # single row
    history  = Article.history.filter(pk=42)          # every version

Implementation notes:

- The history mirror is a sibling model
  ``<Name>Temporal`` with the same field shape plus ``valid_from`` /
  ``valid_to`` / ``operation`` columns.
- Writes go through ``post_save`` / ``post_delete`` so the temporal
  row reflects the **committed** state — async paths fire through
  the async signal variants.
- ``as_of()`` filters on ``valid_from <= T < valid_to`` so the
  semi-open interval matches the SQL standard's system-time
  conventions.
- Unlike ``@track_history``, the temporal manager surfaces the
  history under ``Model.objects.as_of()`` as a normal queryset —
  no separate API to learn.

.. warning::

   ``bulk_create`` / ``bulk_update`` / ``queryset.update`` /
   ``queryset.delete`` do **not** fire per-row ``post_save`` /
   ``post_delete`` signals — their writes will not land in the
   temporal mirror automatically. Either avoid bulk operations on
   temporal-tracked models, fire the signals manually with
   :func:`record_history_for`-style helpers, or accept the gap.
"""
from __future__ import annotations

import copy
import datetime as _dt
from typing import Any

from .. import fields as _fields
from .. import signals
from ..models import Model


def _build_temporal_model(model_cls: type) -> type:
    """Construct the sibling ``<Name>Temporal`` model with the source
    fields demoted (no PK uniqueness) + valid_from / valid_to /
    operation columns."""
    src_meta = model_cls._meta  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    attrs: dict[str, Any] = {}
    for f in src_meta.fields:
        if getattr(f, "many_to_many", False):
            continue
        clone = copy.deepcopy(f)
        if clone.primary_key:
            clone.primary_key = False
            clone.db_index = True
        clone.unique = False
        if hasattr(clone, "auto_now"):
            clone.auto_now = False
        if hasattr(clone, "auto_now_add"):
            clone.auto_now_add = False
        attrs[f.name] = clone
    attrs["temporal_id"] = _fields.BigAutoField(primary_key=True)
    attrs["valid_from"] = _fields.DateTimeField(db_index=True)
    # Open-ended versions use ``valid_to=None`` (current row); closed
    # versions stamp the supersede / delete timestamp here.
    attrs["valid_to"] = _fields.DateTimeField(null=True, blank=True, db_index=True)
    # Single-char op so the column stays narrow even on tables with
    # millions of history rows (``+`` insert, ``~`` update, ``-`` delete).
    attrs["operation"] = _fields.CharField(max_length=1)
    attrs["__module__"] = model_cls.__module__

    class _Meta:
        db_table = f"{src_meta.db_table}_temporal"
        app_label = src_meta.app_label
        managed = True
        ordering = ["-valid_from"]

    attrs["Meta"] = _Meta
    return type(f"{model_cls.__name__}Temporal", (Model,), attrs)


def _snapshot(instance: Any, src_meta: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for f in src_meta.fields:
        if getattr(f, "many_to_many", False):
            continue
        out[f.name] = instance.__dict__.get(f.attname)
    return out


def _close_open_version(
    temporal_cls: type, source_pk_val: Any, ts: _dt.datetime, pk_name: str
) -> None:
    """Stamp ``valid_to=ts`` on the still-open temporal row for this
    source PK so the timeline stays continuous."""
    open_rows = temporal_cls.objects.filter(  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        **{pk_name: source_pk_val, "valid_to__isnull": True}
    )
    open_rows.update(valid_to=ts)


async def _aclose_open_version(
    temporal_cls: type, source_pk_val: Any, ts: _dt.datetime, pk_name: str
) -> None:
    open_rows = temporal_cls.objects.filter(  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        **{pk_name: source_pk_val, "valid_to__isnull": True}
    )
    await open_rows.aupdate(valid_to=ts)


def temporal(model_cls: type) -> type:
    """Class decorator that turns *model_cls* into a system-versioned
    (temporal) model. Idempotent — second application is a no-op.

    The decorator:

    1. Builds the ``<Name>Temporal`` sibling model.
    2. Connects ``post_save`` / ``post_delete`` receivers to mirror
       every write.
    3. Exposes ``Model.objects.as_of(ts)`` (point-in-time query)
       and ``Model.history`` (full version stream).
    """
    if getattr(model_cls, "_temporal_model", None) is not None:
        return model_cls

    temporal_cls = _build_temporal_model(model_cls)
    model_cls._temporal_model = temporal_cls  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    model_cls.history = temporal_cls.objects  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

    src_pk = model_cls._meta.pk.name  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

    def _now() -> _dt.datetime:
        return _dt.datetime.now(_dt.timezone.utc)

    def _running_loop() -> bool:
        import asyncio

        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    def _on_post_save(sender, instance, created, **_kwargs):
        if _running_loop():
            return
        ts = _now()
        pk_val = instance.__dict__.get(model_cls._meta.pk.attname)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        if pk_val is not None and not created:
            _close_open_version(temporal_cls, pk_val, ts, src_pk)
        kwargs = _snapshot(instance, model_cls._meta)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        kwargs["valid_from"] = ts
        kwargs["valid_to"] = None
        kwargs["operation"] = "+" if created else "~"
        temporal_cls.objects.create(**kwargs)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

    def _on_post_delete(sender, instance, **_kwargs):
        if _running_loop():
            return
        ts = _now()
        pk_val = instance.__dict__.get(model_cls._meta.pk.attname)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        if pk_val is not None:
            _close_open_version(temporal_cls, pk_val, ts, src_pk)
        kwargs = _snapshot(instance, model_cls._meta)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        kwargs["valid_from"] = ts
        kwargs["valid_to"] = ts
        kwargs["operation"] = "-"
        temporal_cls.objects.create(**kwargs)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

    async def _aon_post_save(sender, instance, created, **_kwargs):
        ts = _now()
        pk_val = instance.__dict__.get(model_cls._meta.pk.attname)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        if pk_val is not None and not created:
            await _aclose_open_version(temporal_cls, pk_val, ts, src_pk)
        kwargs = _snapshot(instance, model_cls._meta)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        kwargs["valid_from"] = ts
        kwargs["valid_to"] = None
        kwargs["operation"] = "+" if created else "~"
        await temporal_cls.objects.acreate(**kwargs)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

    async def _aon_post_delete(sender, instance, **_kwargs):
        ts = _now()
        pk_val = instance.__dict__.get(model_cls._meta.pk.attname)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        if pk_val is not None:
            await _aclose_open_version(temporal_cls, pk_val, ts, src_pk)
        kwargs = _snapshot(instance, model_cls._meta)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        kwargs["valid_from"] = ts
        kwargs["valid_to"] = ts
        kwargs["operation"] = "-"
        await temporal_cls.objects.acreate(**kwargs)  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]

    signals.post_save.connect(
        _on_post_save,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"temporal.sync.save.{id(model_cls)}",
    )
    signals.post_delete.connect(
        _on_post_delete,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"temporal.sync.delete.{id(model_cls)}",
    )
    signals.post_save.connect(
        _aon_post_save,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"temporal.async.save.{id(model_cls)}",
    )
    signals.post_delete.connect(
        _aon_post_delete,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"temporal.async.delete.{id(model_cls)}",
    )
    return model_cls


def as_of(model_cls: type, ts: _dt.datetime):
    """Free-function equivalent of ``Model.objects.as_of(ts)``.

    Returns a queryset against ``<Model>Temporal`` filtered to the
    rows whose validity interval covers *ts*.
    """
    temporal_cls = getattr(model_cls, "_temporal_model", None)
    if temporal_cls is None:
        raise TypeError(
            f"{model_cls.__name__} is not @temporal-tracked. Apply "
            "@temporal first."
        )
    from ..expressions import Q

    return temporal_cls.objects.filter(  # type: ignore[attr-defined]
        Q(valid_to__isnull=True) | Q(valid_to__gt=ts),
        valid_from__lte=ts,
    )


__all__ = ["temporal", "as_of"]
