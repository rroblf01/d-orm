"""Audit-trail / history mixin — track every INSERT, UPDATE and
DELETE against a model into a parallel ``<table>_history`` table.

Usage::

    import dorm
    from dorm.contrib.history import track_history

    @track_history
    class Article(dorm.Model):
        title = dorm.CharField(max_length=200)
        body = dorm.TextField()

    Article.objects.create(title="hello", body="world")
    Article.history.all()        # one row, history_type='+'

    art = Article.objects.get(pk=1)
    art.title = "hi"
    art.save()                   # adds a '~' row

    art.delete()                 # adds a '-' row

The decorator builds a sibling ``ArticleHistorical`` model class with
every original field plus four audit columns:

- ``history_id``   — surrogate BigAutoField primary key
- ``history_date`` — UTC timestamp of the change
- ``history_type`` — single-char tag: ``"+"`` (insert), ``"~"`` (update),
  ``"-"`` (delete)
- ``history_user_id`` — optional integer; populate via
  :func:`set_history_user` from middleware so request-scoped writes
  carry the actor's id

Caveats — by design, the v3.2 mixin does **not** capture:

- Queryset-level updates (``Model.objects.filter(...).update(...)``):
  these don't fire ``post_save``. Use individual ``save()`` calls when
  audit coverage matters, or call :func:`record_history_for` manually.
- Bulk operations (``bulk_create``, ``bulk_update``): same reason.
- Schema changes — the history model is registered with dorm's model
  registry, so ``dorm makemigrations`` picks up ``CREATE TABLE
  <table>_history`` on the next run. Migration order is automatic.
"""

from __future__ import annotations

import contextvars
import copy
import datetime as _dt
from typing import Any

from .. import fields as _fields
from .. import signals
from ..models import Model


# Per-task / per-thread current actor. Set this from middleware to
# attribute history rows to the user that triggered the change.
_active_user: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "dorm_history_user", default=None
)


def set_history_user(user_id: int | None):
    """Set the actor id stamped on subsequent history rows. Returns the
    contextvar reset token so callers can restore the previous value::

        token = set_history_user(request.user.id)
        try:
            ...
        finally:
            reset_history_user(token)
    """
    return _active_user.set(user_id)


def reset_history_user(token) -> None:
    """Restore the actor id from the token returned by
    :func:`set_history_user`."""
    _active_user.reset(token)


def current_history_user() -> int | None:
    """Return the actor id currently active in this task / thread, or
    ``None`` if :func:`set_history_user` was never called."""
    return _active_user.get()


def _build_history_model(model_cls: type) -> type:
    """Build the sibling ``<Name>Historical`` model class.

    Same fields as *model_cls* with PKs demoted to indexed regular
    columns (the history table has its own ``history_id`` surrogate
    PK), plus four audit columns. Registered with dorm's model
    registry just like a normal user model — so the migration
    autodetector picks it up.
    """
    src_meta = model_cls._meta
    history_attrs: dict[str, Any] = {}

    for f in src_meta.fields:
        copy_f = copy.deepcopy(f)
        # Demote PK + unique constraints — the same row can appear
        # multiple times in history (one row per change).
        if copy_f.primary_key:
            copy_f.primary_key = False
            copy_f.db_index = True
        copy_f.unique = False
        # ``auto_now`` / ``auto_now_add`` on the source would re-stamp
        # the history row's snapshot of the original column, which
        # destroys the audit trail. Strip both flags from the copy.
        if hasattr(copy_f, "auto_now"):
            copy_f.auto_now = False
        if hasattr(copy_f, "auto_now_add"):
            copy_f.auto_now_add = False
        history_attrs[f.name] = copy_f

    history_attrs["history_id"] = _fields.BigAutoField(primary_key=True)
    history_attrs["history_date"] = _fields.DateTimeField()
    history_attrs["history_type"] = _fields.CharField(max_length=1)
    history_attrs["history_user_id"] = _fields.IntegerField(null=True, blank=True)
    history_attrs["__module__"] = model_cls.__module__

    class _HistMeta:
        db_table = f"{src_meta.db_table}_history"
        app_label = src_meta.app_label
        managed = True
        ordering = ["-history_date"]

    history_attrs["Meta"] = _HistMeta
    return type(f"{model_cls.__name__}Historical", (Model,), history_attrs)


def _snapshot_kwargs(instance, src_meta) -> dict[str, Any]:
    """Pull every field's current Python value off *instance* into a
    plain dict. Used to seed the history row's ``create()`` call so
    the row captures the post-save / pre-delete state."""
    out: dict[str, Any] = {}
    for f in src_meta.fields:
        # Skip M2M (no column on the source row anyway).
        if getattr(f, "many_to_many", False):
            continue
        out[f.name] = instance.__dict__.get(f.attname)
    return out


def record_history_for(instance, kind: str, *, user_id: int | None = None) -> None:
    """Manually write a history row for *instance*. ``kind`` must be
    ``"+"`` (insert), ``"~"`` (update), or ``"-"`` (delete).

    Useful when bypassing :meth:`Model.save` (queryset ``.update()`` /
    ``bulk_create``) and you still want the audit row written. The
    automatic post_save / post_delete hooks already cover normal
    save / delete paths.
    """
    if kind not in ("+", "~", "-"):
        raise ValueError(
            f"record_history_for: kind must be '+', '~' or '-', got {kind!r}"
        )
    cls = type(instance)
    hist_cls = getattr(cls, "_history_model", None)
    if hist_cls is None:
        raise TypeError(
            f"{cls.__name__} is not history-tracked. Apply "
            "@track_history (or HistoricalModelMixin) first."
        )
    fields = _snapshot_kwargs(instance, cls._meta)
    fields["history_type"] = kind
    fields["history_date"] = _dt.datetime.now(_dt.timezone.utc)
    fields["history_user_id"] = (
        user_id if user_id is not None else current_history_user()
    )
    hist_cls.objects.create(**fields)


async def arecord_history_for(
    instance, kind: str, *, user_id: int | None = None
) -> None:
    """Async counterpart of :func:`record_history_for`."""
    if kind not in ("+", "~", "-"):
        raise ValueError(
            f"arecord_history_for: kind must be '+', '~' or '-', got {kind!r}"
        )
    cls = type(instance)
    hist_cls = getattr(cls, "_history_model", None)
    if hist_cls is None:
        raise TypeError(
            f"{cls.__name__} is not history-tracked. Apply "
            "@track_history (or HistoricalModelMixin) first."
        )
    fields = _snapshot_kwargs(instance, cls._meta)
    fields["history_type"] = kind
    fields["history_date"] = _dt.datetime.now(_dt.timezone.utc)
    fields["history_user_id"] = (
        user_id if user_id is not None else current_history_user()
    )
    await hist_cls.objects.acreate(**fields)


def track_history(model_cls: type) -> type:
    """Class decorator that turns *model_cls* into a history-tracked
    model. Idempotent — second application is a no-op.

    Side effects:
    - Registers a sibling ``<Name>Historical`` model in dorm's
      registry, so ``makemigrations`` produces ``CREATE TABLE
      <table>_history`` on the next run.
    - Connects sync receivers to ``post_save`` / ``post_delete`` and
      async receivers for the ``asave`` / ``adelete`` paths.
    - Exposes ``model_cls.history`` as a manager on the history
      model — write ``MyModel.history.filter(history_type='-')``.
    """
    if getattr(model_cls, "_history_model", None) is not None:
        return model_cls

    hist_cls = _build_history_model(model_cls)
    model_cls._history_model = hist_cls
    model_cls.history = hist_cls.objects

    def _running_loop() -> bool:
        # Both sync and async receivers fire under ``asend``. To avoid
        # double-recording under ``asave`` / ``adelete``, the sync
        # receiver bails out when an event loop is already running —
        # the async receiver will write the row using the async
        # connection. ``send`` from synchronous code has no running
        # loop, so the sync receiver fires alone.
        import asyncio

        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    def _on_post_save(sender, instance, created, **_kwargs):
        if _running_loop():
            return
        record_history_for(instance, "+" if created else "~")

    def _on_post_delete(sender, instance, **_kwargs):
        if _running_loop():
            return
        record_history_for(instance, "-")

    async def _aon_post_save(sender, instance, created, **_kwargs):
        await arecord_history_for(instance, "+" if created else "~")

    async def _aon_post_delete(sender, instance, **_kwargs):
        await arecord_history_for(instance, "-")

    # weak=False: receivers are local closures, would otherwise be
    # GC'd immediately after this decorator returns.
    signals.post_save.connect(
        _on_post_save,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"history.sync.save.{id(model_cls)}",
    )
    signals.post_delete.connect(
        _on_post_delete,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"history.sync.delete.{id(model_cls)}",
    )
    signals.post_save.connect(
        _aon_post_save,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"history.async.save.{id(model_cls)}",
    )
    signals.post_delete.connect(
        _aon_post_delete,
        sender=model_cls,
        weak=False,
        dispatch_uid=f"history.async.delete.{id(model_cls)}",
    )
    return model_cls


__all__ = [
    "track_history",
    "record_history_for",
    "arecord_history_for",
    "set_history_user",
    "reset_history_user",
    "current_history_user",
]
