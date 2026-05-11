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

import functools
from typing import Any

__all__ = [
    "pii_fields",
    "has_pii_fields",
    "mask_instance",
    "mask_dict",
    "anonymize_row",
    "aanonymize_row",
    "reset_cache",
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


@functools.lru_cache(maxsize=512)
def _pii_fields_cached(model_cls: type) -> tuple[Any, ...]:
    """Cache the PII-field walk per model class. Model registration is
    permanent — a class either has PII fields or it doesn't, decided
    at class-creation time. Caching avoids re-walking ``Meta.fields``
    on every :func:`mask_dict` / :func:`mask_instance` call (the hot
    path in serialisation middleware that runs once per row).
    Returns a tuple for hashability + cheap iteration."""
    meta = getattr(model_cls, "_meta", None)
    if meta is None:
        return ()
    return tuple(f for f in meta.fields if getattr(f, "pii", False))


def pii_fields(model_cls: type) -> list[Any]:
    """Return the list of fields on *model_cls* flagged with ``pii=True``.

    Walks ``Meta.fields`` directly, so M2M / reverse descriptors are
    excluded — only concrete columns surface. The walk is cached per
    class (LRU(512)); :func:`reset_cache` flushes it when needed."""
    return list(_pii_fields_cached(model_cls))


def reset_cache() -> None:
    """Drop the cached PII-field lookup. Useful in test suites that
    monkey-patch a field's ``pii`` flag mid-run, or after dynamically
    registering a new model whose class object was already cached."""
    _pii_fields_cached.cache_clear()


def has_pii_fields(model_cls: type) -> bool:
    """Cheap True/False probe — useful in middleware that wants to
    short-circuit the redaction pass on models without any PII."""
    return bool(pii_fields(model_cls))


def mask_instance(instance: Any) -> None:
    """Mutate *instance* in place, replacing every ``pii=True`` field's
    value with the masked sentinel. Does NOT persist — call
    :func:`anonymize_row` for the write-through variant.

    .. warning::

       Non-string PII fields (``IntegerField``, ``DateTimeField``, …)
       are replaced with ``None``. If the column is declared
       ``null=False`` and the caller later persists the instance, the
       database will reject the row. Either declare PII columns
       nullable, or build a per-field replacement strategy of your
       own — :func:`mask_instance` doesn't know what a "safe"
       non-null replacement looks like for an arbitrary domain.
    """
    for f in pii_fields(type(instance)):
        if instance.__dict__.get(f.attname) is None:
            continue
        instance.__dict__[f.attname] = _redacted_for(f)


def mask_dict(model_cls: type, row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *row* with every PII column masked. Useful when
    serialising raw query rows (``values()`` / ``values_list``) to a
    response payload where you want to redact in flight.

    Accepts ``row`` keyed by either field *name* or column *attname* —
    the lookup walks both. Non-PII keys are passed through untouched.
    """
    if not row:
        return dict(row)
    # Resolve the PII set once — the previous implementation called
    # ``pii_fields(model_cls)`` four times per invocation, which is
    # O(N_fields × N_pii_calls) per row.
    pii_list = pii_fields(model_cls)
    if not pii_list:
        return dict(row)
    fields_by_lookup: dict[str, Any] = {}
    for f in pii_list:
        fields_by_lookup[f.name] = f
        fields_by_lookup[f.attname] = f
    out: dict[str, Any] = {}
    for key, value in row.items():
        f = fields_by_lookup.get(key)
        if f is not None and value is not None:
            out[key] = _redacted_for(f)
        else:
            out[key] = value
    return out


def anonymize_row(instance: Any) -> None:
    """Mask every PII field on *instance* and ``save()`` the row.

    Use inside a transaction when anonymising in batch — the per-row
    save is otherwise un-atomic with the rest of your workflow. See
    :func:`mask_instance` for the NOT NULL caveat that applies to
    non-string PII fields.
    """
    mask_instance(instance)
    instance.save()


async def aanonymize_row(instance: Any) -> None:
    """Async counterpart of :func:`anonymize_row`."""
    mask_instance(instance)
    await instance.asave()
