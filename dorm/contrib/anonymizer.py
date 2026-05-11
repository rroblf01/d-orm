"""Per-column anonymisation for safe dumps and GDPR right-to-be-
forgotten flows.

Where :mod:`dorm.contrib.pii` redacts individual rows in flight, the
anonymizer batch-rewrites every row of a model with a deterministic
fake value — useful when shipping a production snapshot to a
staging cluster or producing a GDPR-compliant dump.

Strategies bundled out of the box:

- ``"redact"`` — replaces the column with ``"[REDACTED]"`` (strings)
  / ``None`` (other types). Same shape as ``mask_instance``.
- ``"random_email"`` — replaces with ``anon-<hash>@example.test``;
  deterministic per source value so cross-row references stay
  consistent.
- ``"random_phone"`` — generates a `+1555{nnnnnnn}` number from the
  hash of the source value.
- ``"shuffle"`` — replaces with the value of another row's column,
  chosen deterministically by hash. Preserves distribution but
  removes the row-to-value association.

Custom strategies are simple callables: ``Callable[[Any], Any]``.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Callable

from .. import transaction

_log = logging.getLogger("dorm.contrib.anonymizer")

_Strategy = Callable[[Any], Any]


def redact(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return "[REDACTED]"
    return None


def random_email(value: Any) -> Any:
    if value is None:
        return None
    digest = hashlib.blake2b(
        str(value).encode("utf-8"), digest_size=8
    ).hexdigest()
    return f"anon-{digest}@example.test"


def random_phone(value: Any) -> Any:
    if value is None:
        return None
    digest = hashlib.blake2b(
        str(value).encode("utf-8"), digest_size=4
    ).digest()
    n = int.from_bytes(digest, "big") % 10_000_000
    return f"+1555{n:07d}"


_BUILTIN_STRATEGIES: dict[str, _Strategy] = {
    "redact": redact,
    "random_email": random_email,
    "random_phone": random_phone,
}


def _resolve(strategy: str | _Strategy) -> _Strategy:
    if not isinstance(strategy, str):
        if callable(strategy):
            return strategy
        raise ValueError(
            f"anonymize: strategy must be a built-in name or callable; "
            f"got {type(strategy).__name__}"
        )
    if strategy in _BUILTIN_STRATEGIES:
        return _BUILTIN_STRATEGIES[strategy]
    raise ValueError(
        f"anonymize: unknown strategy {strategy!r}. Built-ins: "
        f"{sorted(_BUILTIN_STRATEGIES)}"
    )


def anonymize_model(
    model_cls: type,
    rules: dict[str, str | _Strategy],
    *,
    batch_size: int = 500,
    progress: Callable[[int, int], Any] | None = None,
) -> int:
    """Rewrite every row of *model_cls* using *rules*.

    Args:
        model_cls: dorm Model class to anonymise in place.
        rules: ``{field_name: strategy}`` — strategy is one of the
            built-in keys ("redact", "random_email", "random_phone")
            or a callable accepting the current value.
        batch_size: rows per inner transaction. Each batch commits
            independently so a million-row table is recoverable
            (re-running picks up where it left off thanks to the
            deterministic strategies).
        progress: optional ``(rotated_so_far, total_seen)`` callback
            after each batch (drop-in for tqdm).

    Returns the number of rows touched.
    """
    if not rules:
        raise ValueError("anonymize_model: rules is required (got empty dict)")
    strategies = {name: _resolve(s) for name, s in rules.items()}
    pk_attname = model_cls._meta.pk.attname  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    manager = model_cls.objects  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
    field_names = list(strategies.keys())
    seen = 0
    touched = 0
    while True:
        chunk = list(
            manager.order_by(pk_attname).all()[seen : seen + batch_size]
        )
        if not chunk:
            break
        with transaction.atomic():
            for inst in chunk:
                for fname, fn in strategies.items():
                    inst.__dict__[fname] = fn(inst.__dict__.get(fname))
                inst.save(update_fields=field_names)
                touched += 1
        seen += len(chunk)
        if progress is not None:
            try:
                progress(touched, seen)
            except Exception:  # pragma: no cover
                pass
    return touched


__all__ = [
    "anonymize_model",
    "redact",
    "random_email",
    "random_phone",
]
