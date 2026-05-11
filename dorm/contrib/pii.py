"""PII (personally-identifiable information) helpers.

Fields declared with ``pii=True`` opt into a registry that exposes
helpers for compliance tooling — GDPR right-to-be-forgotten endpoints,
audit-log redaction, structured-data export filtering. The flag itself
does nothing to the SQL schema; this module wraps the introspection
and bulk-mutation primitives on top.

Example::

    class User(dorm.Model):
        email = dorm.EmailField(pii=True)
        full_name = dorm.CharField(max_length=120, pii=True)
        username = dorm.CharField(max_length=40, unique=True)

    # Get a list of PII fields for a model:
    from dorm.contrib.pii import pii_fields, mask_instance, anonymize_row

    pii_fields(User)
    # [<EmailField: email>, <CharField: full_name>]

    # Mask an in-memory instance (mutates):
    mask_instance(user)
    # user.email == "[REDACTED]", user.full_name == "[REDACTED]"

    # Anonymise persisted rows:
    anonymize_row(user)   # writes the masked values via .save()

The module also enables the audit-log redaction path in
``dorm.contrib.history`` via ``settings.HISTORY_MASK_PII = True``.
"""
from __future__ import annotations

from typing import Any

__all__ = [
    "pii_fields",
    "has_pii_fields",
    "mask_instance",
    "mask_dict",
    "anonymize_row",
    "aanonymize_row",
]


def _redacted_for(field: Any) -> Any:
    """Return the masked replacement value for *field*. Strings get
    ``"[REDACTED]"`` so eyeballing redacted rows is unambiguous; all
    other types fall back to ``None`` so the DB type-checker doesn't
    reject the assignment (a masked integer column can't legally hold
    a string sentinel)."""
    from ..fields import (
        CharField,
        EmailField,
        SlugField,
        TextField,
        URLField,
    )

    if isinstance(field, (CharField, TextField, EmailField, SlugField, URLField)):
        return "[REDACTED]"
    return None


def pii_fields(model_cls: type) -> list[Any]:
    """Return the list of fields on *model_cls* flagged with ``pii=True``.

    Walks ``Meta.fields`` directly, so M2M / reverse descriptors are
    excluded — only concrete columns surface."""
    meta = getattr(model_cls, "_meta", None)
    if meta is None:
        return []
    return [f for f in meta.fields if getattr(f, "pii", False)]


def has_pii_fields(model_cls: type) -> bool:
    """Cheap True/False probe — useful in middleware that wants to
    short-circuit the redaction pass on models without any PII."""
    return bool(pii_fields(model_cls))


def mask_instance(instance: Any) -> None:
    """Mutate *instance* in place, replacing every ``pii=True`` field's
    value with the masked sentinel. Does NOT persist — call
    :func:`anonymize_row` for the write-through variant."""
    for f in pii_fields(type(instance)):
        if instance.__dict__.get(f.attname) is None:
            continue
        instance.__dict__[f.attname] = _redacted_for(f)


def mask_dict(model_cls: type, row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *row* with every PII column masked. Useful when
    serialising raw query rows (``values()`` / ``values_list``) to a
    response payload where you want to redact in flight."""
    if not row:
        return dict(row)
    pii_names = {f.attname for f in pii_fields(model_cls)} | {
        f.name for f in pii_fields(model_cls)
    }
    out: dict[str, Any] = {}
    fields_by_name = {f.name: f for f in pii_fields(model_cls)}
    for key, value in row.items():
        if key in pii_names and value is not None:
            f = fields_by_name.get(key)
            if f is None:
                # Lookup by attname when caller used column-style keys.
                for cand in pii_fields(model_cls):
                    if cand.attname == key:
                        f = cand
                        break
            out[key] = _redacted_for(f) if f is not None else None
        else:
            out[key] = value
    return out


def anonymize_row(instance: Any) -> None:
    """Mask every PII field on *instance* and ``save()`` the row. Use
    inside a transaction when anonymising in batch — the per-row save
    is otherwise un-atomic with the rest of your workflow."""
    mask_instance(instance)
    instance.save()


async def aanonymize_row(instance: Any) -> None:
    """Async counterpart of :func:`anonymize_row`."""
    mask_instance(instance)
    await instance.asave()
