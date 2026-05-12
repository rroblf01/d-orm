"""Right-to-erasure helpers for GDPR Art. 17 workflows.

The toolbox bridges the gap between :mod:`dorm.contrib.anonymizer`
(batch table rewrites) and the legal reality of an individual
subject request: erase *one* person's data + cascade across every
table that holds an FK to that subject's row, atomically, with an
audit-friendly summary.

Usage::

    from dorm.contrib.gdpr import erase_subject

    summary = erase_subject(
        User,
        user_id,
        rules={"email": "random_email", "phone": "random_phone"},
        cascade=[Order, Comment],
    )
    # → {"User": 1, "Order": 17, "Comment": 4}

Defaults: when ``rules`` is omitted, every ``CharField`` /
``TextField`` / ``EmailField`` on the subject is redacted; numeric +
date columns are left untouched (mass-zeroing them often breaks
foreign-key joins downstream and is rarely required by Art. 17).

Cascade behaviour: *cascade* is an explicit allowlist of related
models. For each, every row that references the subject (via any
``ForeignKey`` whose target is the subject model) is rewritten with
the same ``rules``. The cascade is intentionally non-recursive — a
GDPR erasure should be a deliberate decision per related model, not
an automated walk that risks erasing more than the regulator asked
for.

All writes happen inside a single :func:`dorm.transaction.atomic`
block so a mid-erasure crash rolls back to the pre-erasure state.
Re-running the helper is idempotent against the random_* strategies
(deterministic on the source value) and a no-op against ``redact``
(the redacted sentinel is rewritten to the same sentinel).
"""
from __future__ import annotations

from typing import Any, Callable

from .. import fields as _fields
from .. import transaction
from .anonymizer import _resolve as _resolve_strategy
from .anonymizer import redact

_Strategy = Callable[[Any], Any]

# Field classes considered "PII-shaped" when ``rules`` is omitted.
# Numeric / date / boolean columns are left alone — regulators
# typically ask for identifiable text and leave aggregate metrics
# alone, and overwriting a date column risks breaking downstream
# joins that depend on it.
_PII_FIELD_TYPES: tuple[type, ...] = (
    _fields.CharField,
    _fields.TextField,
    _fields.EmailField,
)


def _default_rules(model_cls: type) -> dict[str, _Strategy]:
    """Build ``{field_name: redact}`` for every CharField / TextField /
    EmailField on *model_cls*, excluding the primary key.

    The primary key is preserved so existing references (audit
    trails, payment receipts, regulator-mandated retention rows)
    keep their join key intact. Only the *data* gets erased, not
    the identity of the record."""
    meta = model_cls._meta  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    pk_name = meta.pk.name
    rules: dict[str, _Strategy] = {}
    for f in meta.fields:
        if f.name == pk_name:
            continue
        if getattr(f, "many_to_many", False):
            continue
        if isinstance(f, _PII_FIELD_TYPES):
            rules[f.name] = redact
    return rules


def _resolve_rules(
    model_cls: type, rules: dict[str, str | _Strategy] | None
) -> dict[str, _Strategy]:
    """Validate + normalise *rules*. ``None`` → :func:`_default_rules`.

    Raises ``ValueError`` if any rule references a field that
    doesn't exist on *model_cls* — silently no-oping on a typo is
    the worst possible failure mode for a compliance feature."""
    if rules is None:
        return _default_rules(model_cls)
    meta = model_cls._meta  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    known = {f.name for f in meta.fields if not getattr(f, "many_to_many", False)}
    unknown = [n for n in rules if n not in known]
    if unknown:
        raise ValueError(
            f"erase_subject: rules reference unknown field(s) {unknown!r} "
            f"on {model_cls.__name__}. Known: {sorted(known)!r}"
        )
    return {name: _resolve_strategy(s) for name, s in rules.items()}


def _fks_pointing_at(target: type, model_cls: type) -> list[str]:
    """Return the names of ``ForeignKey`` fields on *model_cls* that
    reference *target*. Used by the cascade walk to find the join
    column for each related model.

    Walks ``meta.fields`` only — reverse-FK / M2M relations are not
    cascaded automatically. The point is to give the operator
    explicit control over which related tables get rewritten.
    """
    matches: list[str] = []
    meta = model_cls._meta  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    for f in meta.fields:
        if not isinstance(f, _fields.ForeignKey):
            continue
        # ``ForeignKey.remote_field_to`` holds the resolved target
        # model class (string-based lazy references are upgraded to
        # the actual class at registry-link time).
        if getattr(f, "remote_field_to", None) is target:
            matches.append(f.attname or f.name)
    return matches


def _apply_strategies(instance: Any, rules: dict[str, _Strategy]) -> None:
    """Mutate ``instance.__dict__`` with the strategy output for
    every rule-mapped field. The caller is responsible for the
    ``.save(update_fields=...)`` that persists the change."""
    for fname, fn in rules.items():
        instance.__dict__[fname] = fn(instance.__dict__.get(fname))


def erase_subject(
    model_cls: type,
    pk: Any,
    *,
    rules: dict[str, str | _Strategy] | None = None,
    cascade: list[type] | None = None,
    using: str = "default",
) -> dict[str, int]:
    """Erase the row identified by *pk* and (optionally) every row
    in *cascade* that references it via a ForeignKey.

    Args:
        model_cls: target model holding the subject's record.
        pk: primary key of the row to erase.
        rules: ``{field_name: strategy}`` — strategy is a built-in
            anonymizer key ("redact" / "random_email" /
            "random_phone") or a callable. ``None`` (default)
            redacts every CharField / TextField / EmailField on the
            model and any cascade target.
        cascade: explicit list of related model classes whose rows
            pointing at the subject should also be rewritten. The
            walk is non-recursive — pass each level you want
            covered.
        using: database alias for both the subject and cascade
            writes.

    Returns a dict mapping each touched model's class name to the
    number of rows erased.
    """
    subject_rules = _resolve_rules(model_cls, rules)
    cascade_models = list(cascade or [])
    cascade_rules: dict[type, dict[str, _Strategy]] = {
        m: _resolve_rules(m, rules) for m in cascade_models
    }
    summary: dict[str, int] = {}

    with transaction.atomic(using=using):
        # Subject row — fetch, apply, save. ``filter().first()``
        # tolerates a missing pk by returning ``None``; we surface
        # that as ``0`` in the summary so callers can detect the
        # "already erased / never existed" case without an
        # exception (Art. 17 § 3 lets the controller decline if
        # there is no matching data).
        manager = model_cls.objects.using(using)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        inst = manager.filter(pk=pk).first()
        if inst is not None and subject_rules:
            _apply_strategies(inst, subject_rules)
            inst.save(update_fields=list(subject_rules.keys()), using=using)
            summary[model_cls.__name__] = 1
        else:
            summary[model_cls.__name__] = 0

        # Cascade — for each related model find FK columns pointing
        # at *model_cls*, filter rows that reference *pk* on any of
        # them, then apply the rules.
        for related_cls in cascade_models:
            fk_cols = _fks_pointing_at(model_cls, related_cls)
            if not fk_cols:
                summary[related_cls.__name__] = 0
                continue
            related_mgr = related_cls.objects.using(using)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            # Each FK column emits a separate ``filter()`` — most
            # models only have one FK toward a given target, so the
            # outer loop is single-pass. The dedup set keeps a join
            # table (two FKs to the same model) from double-erasing
            # the same row.
            rules_for_related = cascade_rules[related_cls]
            seen_pks: set[Any] = set()
            count = 0
            for col in fk_cols:
                for inst in related_mgr.filter(**{col: pk}):
                    if inst.pk in seen_pks:
                        continue
                    seen_pks.add(inst.pk)
                    _apply_strategies(inst, rules_for_related)
                    if rules_for_related:
                        inst.save(
                            update_fields=list(rules_for_related.keys()),
                            using=using,
                        )
                    count += 1
            summary[related_cls.__name__] = count

    return summary


__all__ = ["erase_subject"]
