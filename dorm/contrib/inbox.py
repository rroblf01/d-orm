"""Inbox pattern — idempotent message processing.

Counterpart to :mod:`dorm.contrib.outbox`. Where the outbox guarantees
"at-least-once" delivery, the inbox guarantees "exactly-once"
processing on the receiving side by recording every message id and
refusing to handle it twice.

Usage::

    from dorm.contrib.inbox import InboxRecord, idempotent

    class Inbox(InboxRecord):
        class Meta:
            db_table = "inbox"

    @idempotent(Inbox)
    def handle_order_paid(message_id: str, payload: dict) -> None:
        ...

    handle_order_paid("msg-abc-123", {"order_id": 42})  # runs
    handle_order_paid("msg-abc-123", {"order_id": 42})  # no-op (logged)
"""
from __future__ import annotations

import functools
import logging
from typing import Any, Callable

from .. import fields as _fields
from .. import transaction
from ..exceptions import IntegrityError
from ..models import Model

_log = logging.getLogger("dorm.contrib.inbox")


class InboxRecord(Model):
    """Abstract inbox row. Subclass with a concrete ``Meta.db_table``
    to materialise.

    Schema:

    - ``message_id`` — application-supplied unique identifier
      (event-source id, broker message id, etc.).
    - ``handler_name`` — handler that processed the message.
      Lets multiple handlers register against the same source.
    - ``processed_at`` — UTC timestamp.
    """

    message_id = _fields.CharField(max_length=255, db_index=True)
    handler_name = _fields.CharField(max_length=255, db_index=True)
    processed_at = _fields.DateTimeField()

    class Meta:
        abstract = True


def idempotent(
    model_cls: type[InboxRecord], *, handler_name: str | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that wraps *func* in an inbox lookup.

    The decorated function must accept ``message_id`` as its first
    positional argument. On every call:

    1. Check the inbox for ``(message_id, handler_name)``.
    2. If found, log and return ``None`` (no-op).
    3. Otherwise run *func* inside an ``atomic()`` block and insert
       the inbox row before commit. The UNIQUE constraint serialises
       concurrent invocations safely — a concurrent duplicate raises
       :class:`IntegrityError` which the decorator swallows.
    """
    import datetime as _dt

    def _decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        hname = handler_name or (
            f"{func.__module__}.{getattr(func, '__qualname__', repr(func))}"
        )

        @functools.wraps(func)
        def _wrapper(message_id: str, *args: Any, **kwargs: Any) -> Any:
            qs = model_cls.objects.filter(  # type: ignore[attr-defined]
                message_id=message_id, handler_name=hname
            )
            if qs.exists():
                _log.info(
                    "Inbox skip: handler=%r message_id=%r already processed",
                    hname,
                    message_id,
                )
                return None
            try:
                with transaction.atomic():
                    result = func(message_id, *args, **kwargs)
                    model_cls.objects.create(  # type: ignore[attr-defined]
                        message_id=message_id,
                        handler_name=hname,
                        processed_at=_dt.datetime.now(_dt.timezone.utc),
                    )
                    return result
            except IntegrityError:
                # Concurrent duplicate — another worker won the race.
                # Treat as a successful skip.
                _log.info(
                    "Inbox concurrent-skip: handler=%r message_id=%r",
                    hname,
                    message_id,
                )
                return None

        return _wrapper

    return _decorate


__all__ = ["InboxRecord", "idempotent"]
