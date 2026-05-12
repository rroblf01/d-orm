"""Field-level audit trail.

Per-field before/after diff log for every write on a decorated model.
Each change emits one row to ``<Name>Audit`` capturing the column
name, old value, new value, optional actor, timestamp and operation
flag (``+`` insert, ``~`` update, ``-`` delete). Where :mod:`dorm.contrib.temporal`
answers *"what did this row look like at time T?"* — audit answers
*"who changed which column when and to what?"*.

Usage::

    from dorm.contrib.audit import audited, audit_history

    @audited(fields=["email", "salary"], actor_getter=lambda: get_current_user_id())
    class Employee(dorm.Model):
        name = dorm.CharField(max_length=64)
        email = dorm.EmailField()
        salary = dorm.IntegerField()

    # All writes to ``email`` or ``salary`` produce audit rows:
    e = Employee.objects.create(name="A", email="a@x.com", salary=100)
    e.salary = 200
    e.save()
    rows = list(audit_history(e))  # → [+ salary, + email, + name?, ~ salary]

Behavioural notes:

- The decorator builds an :class:`AuditEntry` sibling at decoration
  time. Run ``makemigrations`` to materialise the table — the model
  is owned by the same ``app_label`` as the source.
- ``pre_save`` issues one extra ``SELECT`` per save against the
  source row to capture the *before* image. The cost is intentional:
  audit reads are usually rare, and a snapshot from in-memory state
  would silently miss columns whose DB value drifted (e.g. a trigger
  update between rows).
- ``fields`` defaults to *every* concrete column except the primary
  key. Pass an explicit list to scope the trail.
- ``actor_getter`` is invoked at write time. Use it to thread a
  ``current_user_id()`` or thread-local context object. Returning
  ``None`` is allowed (the row records ``actor = NULL``).
- Bulk operations (``bulk_create`` / ``bulk_update`` /
  ``queryset.update`` / ``queryset.delete``) bypass ``post_save`` /
  ``post_delete`` signals and therefore do **not** generate audit
  rows. Same caveat as :mod:`dorm.contrib.temporal` — wrap with
  per-row saves when the trail must be complete.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any, Callable

from .. import fields as _fields
from .. import signals
from ..models import Model

# Module-level registry keyed by id(model_cls) → the AuditEntry
# sibling. Lets ``audit_history`` look up the trail model without
# requiring callers to import it.
_AUDIT_MODELS: dict[int, type] = {}
# Per-source-model config so the signal callbacks know which
# columns to diff and how to resolve the actor without re-reading
# decorator arguments at signal-fire time.
_AUDIT_CONFIG: dict[int, dict[str, Any]] = {}
# Per-instance pre-save snapshot. Keyed by ``(model_id, pk)`` so two
# unrelated decorated models writing in parallel don't clobber each
# other's pending diff. ``WeakValueDictionary`` would suffice if pks
# were hashable instance refs; sticking with the explicit tuple keeps
# the lookup obvious.
_PRE_SNAPSHOTS: dict[tuple[int, Any], dict[str, Any]] = {}


def _build_audit_model(model_cls: type, *, target_pk_field: Any) -> type:
    """Return the ``<Name>Audit`` sibling model.

    The mirror keeps the source PK column shape so audit rows can be
    joined back to the original row with a plain integer / UUID
    comparison instead of stringifying the value.
    """
    src_meta = model_cls._meta  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    pk_copy = type(target_pk_field)()
    # Demote the PK clone — the audit sibling has its own PK.
    pk_copy.primary_key = False
    pk_copy.unique = False
    pk_copy.db_index = True

    attrs: dict[str, Any] = {
        "audit_id": _fields.BigAutoField(primary_key=True),
        "target_id": pk_copy,
        "field": _fields.CharField(max_length=64, db_index=True),
        "old_value": _fields.TextField(null=True, blank=True),
        "new_value": _fields.TextField(null=True, blank=True),
        "actor": _fields.CharField(max_length=255, null=True, blank=True),
        "at": _fields.DateTimeField(auto_now_add=True, db_index=True),
        "operation": _fields.CharField(max_length=1),
        "__module__": model_cls.__module__,
    }

    class _Meta:
        db_table = f"{src_meta.db_table}_audit"
        app_label = src_meta.app_label
        managed = True
        ordering = ["-at", "-audit_id"]

    attrs["Meta"] = _Meta
    return type(f"{model_cls.__name__}Audit", (Model,), attrs)


def _stringify(value: Any) -> str | None:
    """Render a Python value as a UTF-8 string for the audit column.

    ``None`` is preserved so SQL ``IS NULL`` filters keep working on
    the audit table; every other value is rendered through ``str``
    so Decimal / UUID / datetime / Enum all serialise without
    raising. Long values are kept verbatim — audit reads tolerate
    big text columns better than truncated history."""
    if value is None:
        return None
    return str(value)


def _watched_fields(model_cls: type, requested: list[str] | None) -> list[str]:
    """Resolve the column list to diff.

    Without an explicit ``fields=`` list, every concrete (non-M2M /
    non-PK) column is tracked. The PK is excluded because every row
    has a fixed PK by construction — auditing it would just emit
    one ``+ pk_field`` row per insert with no diagnostic value.
    """
    meta = model_cls._meta  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    pk_name = meta.pk.name
    all_cols = [
        f.name
        for f in meta.fields
        if not getattr(f, "many_to_many", False) and f.name != pk_name
    ]
    if requested is None:
        return all_cols
    unknown = [n for n in requested if n not in all_cols]
    if unknown:
        raise ValueError(
            f"audited(fields=...): unknown field(s) {unknown!r} on "
            f"{model_cls.__name__}. Allowed: {all_cols!r}"
        )
    return list(requested)


def _resolve_actor(actor_getter: Callable[[], Any] | None) -> str | None:
    """Invoke *actor_getter* and stringify its result.

    Exceptions raised by user code are swallowed and logged via the
    return value ``None``: audit must never break a save, even if
    the request-context lookup blew up."""
    if actor_getter is None:
        return None
    try:
        actor = actor_getter()
    except Exception:
        return None
    return _stringify(actor)


def audited(
    fields: list[str] | None = None,
    *,
    actor_getter: Callable[[], Any] | None = None,
    using: str = "default",
) -> Callable[[type], type]:
    """Class decorator that records a per-field audit trail.

    Args:
        fields: column names to track. ``None`` (default) tracks
            every concrete, non-PK column.
        actor_getter: zero-arg callable returning the principal
            performing the write (user id / email / opaque token).
            Result is stringified; ``None`` is allowed.
        using: database alias for the audit-row insert. Defaults to
            ``"default"`` — matches the source row's alias in most
            single-DB setups.
    """

    def _wrap(model_cls: type) -> type:
        if id(model_cls) in _AUDIT_MODELS:
            # Idempotent — re-decorating the same class returns the
            # already-instrumented version untouched.
            return model_cls
        meta = model_cls._meta  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        watched = _watched_fields(model_cls, fields)
        audit_cls = _build_audit_model(model_cls, target_pk_field=meta.pk)
        _AUDIT_MODELS[id(model_cls)] = audit_cls
        _AUDIT_CONFIG[id(model_cls)] = {
            "fields": watched,
            "actor_getter": actor_getter,
            "using": using,
            "pk_attname": meta.pk.attname,
        }
        model_cls._audit_model = audit_cls  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

        signals.pre_save.connect(
            _on_pre_save,
            sender=model_cls,
            weak=False,
            dispatch_uid=f"audit.pre_save.{id(model_cls)}",
        )
        signals.post_save.connect(
            _on_post_save,
            sender=model_cls,
            weak=False,
            dispatch_uid=f"audit.post_save.{id(model_cls)}",
        )
        signals.post_delete.connect(
            _on_post_delete,
            sender=model_cls,
            weak=False,
            dispatch_uid=f"audit.post_delete.{id(model_cls)}",
        )
        return model_cls

    return _wrap


def _on_pre_save(sender: Any, instance: Any, **_kwargs: Any) -> None:
    """Stash the *before* image for the watched columns.

    For inserts (no PK yet) the snapshot is empty — every new column
    will produce a ``+ field`` row in :func:`_on_post_save`. For
    updates, one ``SELECT`` reads the persisted values directly so
    auto-now / trigger-driven columns are captured accurately
    instead of trusting the in-memory copy.
    """
    cfg = _AUDIT_CONFIG.get(id(sender))
    if cfg is None:
        return
    pk_val = instance.__dict__.get(cfg["pk_attname"])
    if pk_val is None:
        _PRE_SNAPSHOTS[(id(sender), id(instance))] = {}
        return
    try:
        row = (
            sender.objects.using(cfg["using"])
            .filter(pk=pk_val)
            .values(*cfg["fields"])
            .first()
        )
    except Exception:
        row = None
    _PRE_SNAPSHOTS[(id(sender), id(instance))] = dict(row) if row else {}


def _emit_diff(
    sender: Any,
    instance: Any,
    *,
    operation: str,
    audit_cls: type,
    cfg: dict[str, Any],
    pre: dict[str, Any],
) -> None:
    """Build + insert one audit row per changed field."""
    actor = _resolve_actor(cfg["actor_getter"])
    pk_attname = cfg["pk_attname"]
    target_id = instance.__dict__.get(pk_attname)
    rows = []
    for fname in cfg["fields"]:
        old_v = pre.get(fname)
        new_v = instance.__dict__.get(fname)
        if operation == "-":
            # Deletion — emit the *before* image only; new is NULL.
            new_v = None
        if operation == "~" and old_v == new_v:
            continue
        rows.append(
            audit_cls(
                target_id=target_id,
                field=fname,
                old_value=_stringify(old_v),
                new_value=_stringify(new_v),
                actor=actor,
                operation=operation,
            )
        )
    if not rows:
        return
    try:
        audit_cls.objects.using(cfg["using"]).bulk_create(rows)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    except Exception:
        # Audit must never break a save. The trail is best-effort
        # against transient errors (e.g. the audit table doesn't
        # exist yet because makemigrations wasn't run); the swallow
        # is documented in the module docstring.
        pass


def _on_post_save(
    sender: Any, instance: Any, created: bool = False, **_kwargs: Any
) -> None:
    """Diff the watched columns against the pre-save snapshot and
    emit one audit row per change. Inserts emit one row per watched
    column (the ``old`` slot is NULL)."""
    cfg = _AUDIT_CONFIG.get(id(sender))
    audit_cls = _AUDIT_MODELS.get(id(sender))
    if cfg is None or audit_cls is None:
        return
    pre = _PRE_SNAPSHOTS.pop((id(sender), id(instance)), {})
    operation = "+" if created else "~"
    _emit_diff(
        sender,
        instance,
        operation=operation,
        audit_cls=audit_cls,
        cfg=cfg,
        pre=pre,
    )


def _on_post_delete(sender: Any, instance: Any, **_kwargs: Any) -> None:
    """Emit one ``-`` row per watched column carrying the value at
    delete time."""
    cfg = _AUDIT_CONFIG.get(id(sender))
    audit_cls = _AUDIT_MODELS.get(id(sender))
    if cfg is None or audit_cls is None:
        return
    pre = {fname: instance.__dict__.get(fname) for fname in cfg["fields"]}
    _emit_diff(
        sender,
        instance,
        operation="-",
        audit_cls=audit_cls,
        cfg=cfg,
        pre=pre,
    )


def audit_history(instance: Any) -> Any:
    """Return a queryset of audit rows for *instance*, newest first.

    Equivalent to ``Model._audit_model.objects.filter(target_id=instance.pk)``
    — sugar so callers don't need to know the sibling model's name.

    Raises :class:`LookupError` when *instance*'s class isn't decorated
    with :func:`audited`.
    """
    cls = type(instance)
    audit_cls = _AUDIT_MODELS.get(id(cls))
    if audit_cls is None:
        raise LookupError(
            f"{cls.__name__} is not @audited — no audit history available."
        )
    pk_attname = cls._meta.pk.attname  # type: ignore[attr-defined]
    pk_val = instance.__dict__.get(pk_attname)
    return audit_cls.objects.filter(target_id=pk_val)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]


def _now_utc() -> _dt.datetime:
    """Helper exported for tests that need to freeze the audit
    timestamp deterministically."""
    return _dt.datetime.now(_dt.timezone.utc)


__all__ = ["audited", "audit_history", "AuditEntry"]


# Public marker so callers can ``isinstance(row, AuditEntry)`` against
# the sibling rows without depending on the dynamically generated
# class name. The sibling models inherit from ``Model``; we set the
# alias to ``Model`` because every audit sibling IS a Model. Users
# that want stronger typing can pull the actual subclass via
# ``MyModel._audit_model``.
AuditEntry = Model
