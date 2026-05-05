"""Transactional outbox pattern helper.

The outbox pattern solves the dual-write problem in a microservice
architecture: a single business action needs to update the database
*and* publish an event. Doing both directly creates a window where the
DB write commits but the broker publish fails — leaving the system
inconsistent.

The fix: write the event to an *outbox* table inside the same DB
transaction as the business write. A separate worker drains the
outbox and publishes to the broker, marking each row as published
once the broker acks. Because the business write and the outbox
insert share a transaction, they atomically commit or roll back
together; events are guaranteed to exist for every committed write.

This module provides:

- :class:`OutboxEvent` — base ``Model`` for the outbox table. Subclass
  it to add your own columns; the migration is the user's responsibility.
- :func:`record_event` / :func:`arecord_event` — write a row inside
  the active transaction.
- :class:`OutboxRelay` — minimal worker that polls the outbox with
  ``SELECT ... FOR UPDATE SKIP LOCKED`` and dispatches each row to a
  caller-supplied handler. The handler decides whether to publish to
  Kafka, RabbitMQ, NATS, etc.; this helper has no broker dependency.

Example::

    from dorm.contrib.outbox import OutboxEvent, OutboxRelay, record_event

    class Outbox(OutboxEvent):
        class Meta:
            db_table = "outbox"

    # Inside a request handler, after creating an Order:
    with transaction.atomic():
        order = Order.objects.create(...)
        record_event(Outbox, "order.created", {"order_id": order.id})

    # Standalone worker process:
    relay = OutboxRelay(Outbox, batch_size=100)
    relay.run(handler=publish_to_kafka)  # blocks; SIGTERM to stop
"""

from __future__ import annotations

import json
import logging
import signal
import time
import uuid
from typing import Any, Callable

from .. import fields
from ..models import Model
from ..transaction import atomic

_log = logging.getLogger("dorm.contrib.outbox")


class OutboxEvent(Model):
    """Minimal abstract model for the outbox table.

    Subclass with a concrete ``Meta.db_table`` and run ``makemigrations``
    to materialise. Override the field definitions if you need different
    types — the ``OutboxRelay`` only depends on the column *names*
    (``id``, ``event_type``, ``payload``, ``status``, ``created_at``).
    """

    id = fields.UUIDField(primary_key=True, default=uuid.uuid4)
    event_type = fields.CharField(max_length=128)
    payload = fields.JSONField(default=dict)
    status = fields.CharField(
        max_length=16, default="pending", db_index=True
    )
    created_at = fields.DateTimeField(auto_now_add=True)
    published_at = fields.DateTimeField(null=True, blank=True)
    last_error = fields.TextField(null=True, blank=True)
    attempts = fields.IntegerField(default=0)

    class Meta:
        abstract = True


def record_event(
    model: type[OutboxEvent],
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    using: str | None = None,
) -> OutboxEvent:
    """Insert an outbox row inside the active transaction.

    Calling without an active ``atomic()`` works but defeats the
    pattern — the whole point is to share a transaction with the
    business write. The helper logs a warning in that case.
    """
    from ..db.connection import get_connection

    alias = using or "default"
    conn = get_connection(alias)
    if getattr(conn, "_atomic_depth", 0) == 0:
        _log.warning(
            "record_event(%s) called outside a transaction; the outbox "
            "row will commit independently of any business write — defeats "
            "the dual-write guarantee.",
            event_type,
        )
    obj = model(event_type=event_type, payload=payload or {})
    obj.save(using=alias)
    return obj


async def arecord_event(
    model: type[OutboxEvent],
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    using: str | None = None,
) -> OutboxEvent:
    """Async counterpart of :func:`record_event`.

    Mirrors the sync version's tx-detection: logs a warning when
    called outside an active ``aatomic()`` block so the dual-write
    invariant isn't silently broken.
    """
    from ..db.utils import ASYNC_ATOMIC_STATE
    from ..db.connection import get_async_connection

    alias = using or "default"
    state = ASYNC_ATOMIC_STATE.get()
    aconn = get_async_connection(alias)
    in_atomic = state is not None and state[0] is aconn
    if not in_atomic:
        _log.warning(
            "arecord_event(%s) called outside an aatomic() block; the "
            "outbox row will commit independently of any business write "
            "— defeats the dual-write guarantee.",
            event_type,
        )
    obj = model(event_type=event_type, payload=payload or {})
    await obj.asave(using=alias)
    return obj


class OutboxRelay:
    """Polling worker that drains an outbox table.

    Each iteration:

    1. Opens a transaction.
    2. ``SELECT ... FOR UPDATE SKIP LOCKED`` of up to ``batch_size``
       pending rows. Other relay workers running against the same
       outbox naturally pick up disjoint rows — horizontal scaling is
       free.
    3. Calls *handler(row)* for each. The handler returns a truthy
       value on success, falsy on failure. Any raised exception is
       treated as a failure.
    4. Successful rows are marked ``status='published'`` with
       ``published_at = now()``. Failures bump ``attempts`` and store
       ``last_error`` for inspection.
    5. After ``max_attempts`` failures, the row is moved to
       ``status='dead'`` so it stops blocking the queue. Operators
       drain the dead-letter pile manually.

    The relay is a plain blocking loop. Run it in its own process
    (``python -m worker``) or under supervisor/systemd. Async users
    can call :meth:`arun` instead.
    """

    def __init__(
        self,
        model: type[OutboxEvent],
        *,
        batch_size: int = 100,
        poll_interval_s: float = 1.0,
        max_attempts: int = 5,
        using: str = "default",
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be > 0")
        self.model = model
        self.batch_size = batch_size
        self.poll_interval_s = poll_interval_s
        self.max_attempts = max_attempts
        self.using = using
        self._stop = False

    def stop(self) -> None:
        """Request the relay loop to exit after the current batch."""
        self._stop = True

    def _process_one(self, row: OutboxEvent, handler: Callable[[Any], Any]) -> bool:
        try:
            ok = handler(row)
        except Exception as exc:
            row.attempts += 1
            row.last_error = f"{type(exc).__name__}: {exc}"[:1024]
            if row.attempts >= self.max_attempts:
                row.status = "dead"
            row.save(using=self.using)
            _log.warning(
                "outbox handler raised on event %s (%s/%s attempts): %s",
                row.id,
                row.attempts,
                self.max_attempts,
                exc,
            )
            return False
        if ok:
            from datetime import datetime, timezone

            row.status = "published"
            row.published_at = datetime.now(timezone.utc)
            row.last_error = None
            row.save(using=self.using)
            return True
        row.attempts += 1
        if row.attempts >= self.max_attempts:
            row.status = "dead"
        row.save(using=self.using)
        return False

    def drain_once(self, handler: Callable[[Any], Any]) -> int:
        """Run a single batch. Returns the number of successfully
        published events. Useful to drive the relay from a test or
        from an external scheduler instead of the blocking loop.

        Concurrency model: ``SELECT ... FOR UPDATE SKIP LOCKED`` on
        PostgreSQL — multiple relays naturally pick disjoint rows.
        On backends without ``SKIP LOCKED`` support (SQLite, MySQL <
        8.0) the helper falls back to a plain SELECT inside the same
        transaction; running multiple relay processes against those
        backends will sometimes process the same event twice — make
        the handler idempotent.
        """
        from ..db.connection import get_connection

        conn = get_connection(self.using)
        supports_skip = getattr(conn, "vendor", "sqlite") == "postgresql"

        published = 0
        with atomic(using=self.using):
            base = (
                self.model.objects.using(self.using)
                .filter(status="pending")
                .order_by("created_at")
            )
            if supports_skip:
                qs = base.select_for_update(skip_locked=True)[: self.batch_size]
            else:
                qs = base[: self.batch_size]
            rows = list(qs)
            for row in rows:
                if self._process_one(row, handler):
                    published += 1
        return published

    def run(self, handler: Callable[[Any], Any]) -> None:
        """Block forever, calling ``handler`` for each pending row.

        Installs a SIGTERM / SIGINT handler that calls ``self.stop``
        so the loop exits cleanly between batches; partial work is
        already committed by ``atomic()`` and won't be lost. The
        handler returns ``True`` to mark a row published or ``False``
        to retry it later.
        """
        prev_term = signal.getsignal(signal.SIGTERM)
        prev_int = signal.getsignal(signal.SIGINT)

        def _on_signal(_signum, _frame):
            self.stop()

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
        try:
            while not self._stop:
                published = self.drain_once(handler)
                if published == 0:
                    time.sleep(self.poll_interval_s)
        finally:
            signal.signal(signal.SIGTERM, prev_term)
            signal.signal(signal.SIGINT, prev_int)

    async def _aprocess_one(self, row, handler: Callable[[Any], Any]) -> bool:
        """Async counterpart of :meth:`_process_one`.

        Accepts both sync and async handlers — when *handler* returns
        a coroutine we await it; otherwise we treat the return value
        as the success flag the same way the sync path does.
        """
        import inspect

        try:
            result = handler(row)
            if inspect.iscoroutine(result):
                ok = await result
            else:
                ok = result
        except Exception as exc:
            row.attempts += 1
            row.last_error = f"{type(exc).__name__}: {exc}"[:1024]
            if row.attempts >= self.max_attempts:
                row.status = "dead"
            await row.asave(using=self.using)
            _log.warning(
                "outbox handler raised on event %s (%s/%s attempts): %s",
                row.id,
                row.attempts,
                self.max_attempts,
                exc,
            )
            return False
        if ok:
            from datetime import datetime, timezone

            row.status = "published"
            row.published_at = datetime.now(timezone.utc)
            row.last_error = None
            await row.asave(using=self.using)
            return True
        row.attempts += 1
        if row.attempts >= self.max_attempts:
            row.status = "dead"
        await row.asave(using=self.using)
        return False

    async def adrain_once(self, handler: Callable[[Any], Any]) -> int:
        """Async equivalent of :meth:`drain_once`. Drains a single
        batch via the async ORM path; await the handler when it is
        a coroutine function."""
        from ..db.connection import get_async_connection
        from ..transaction import aatomic

        conn = get_async_connection(self.using)
        supports_skip = getattr(conn, "vendor", "sqlite") == "postgresql"

        published = 0
        async with aatomic(using=self.using):
            base = (
                self.model.objects.using(self.using)
                .filter(status="pending")
                .order_by("created_at")
            )
            if supports_skip:
                qs = base.select_for_update(skip_locked=True)[: self.batch_size]
            else:
                qs = base[: self.batch_size]
            rows = [row async for row in qs]
            for row in rows:
                if await self._aprocess_one(row, handler):
                    published += 1
        return published

    async def arun(self, handler: Callable[[Any], Any]) -> None:
        """Async equivalent of :meth:`run`. Loops via
        :meth:`adrain_once`, sleeping between empty batches.

        Stop with ``self.stop()`` from another task — there is no
        SIGTERM hook here because async stacks typically register
        their own; mirroring the sync helper would surprise FastAPI
        / Litestar / aiohttp users that already wired a graceful-
        shutdown lifespan.
        """
        import asyncio

        while not self._stop:
            published = await self.adrain_once(handler)
            if published == 0:
                await asyncio.sleep(self.poll_interval_s)


def serialize_payload(payload: Any) -> str:
    """Helper: deterministic JSON encoding for outbox payloads.

    Sort keys + UTF-8 default + tuple-as-list make the output stable
    across Python versions, which matters when an idempotent
    consumer dedupes by hash."""
    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


__all__ = [
    "OutboxEvent",
    "OutboxRelay",
    "record_event",
    "arecord_event",
    "serialize_payload",
]
