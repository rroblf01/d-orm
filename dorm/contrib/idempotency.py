"""Idempotency-key primitive — reuses the v3.4 outbox infrastructure.

Most APIs that handle non-idempotent writes (payments, order creation,
external resource provisioning) need a way to safely retry: the
client sends the same request twice — perhaps because the network
flapped between the original request and the response — and the
server must either replay the prior response *or* execute the request
exactly once. The standard pattern is the ``Idempotency-Key`` HTTP
header (Stripe-style): client picks a UUID per logical operation, the
server keys a side-table on it.

This module provides the *primitive*: a context manager that checks
the table, returns the cached response on a hit, and stores the new
response on a miss. Wiring it into a specific framework's request
pipeline is the caller's job (one or two lines of middleware /
dependency wiring), keeping core dorm framework-agnostic.

Usage::

    class IdempotencyEntry(IdempotencyRecord):
        class Meta:
            db_table = "idempotency_entries"


    with idempotency_key("abc-123", model=IdempotencyEntry) as ctx:
        if ctx.replay:
            return ctx.cached_response
        result = do_payment(...)
        ctx.store(result)
        return result

The wrapping ``atomic()`` block guarantees the cache row commits
together with the business write; a partial failure rolls both back
so the next retry sees a clean slate.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone
from typing import Any, Optional, Type

from .. import fields
from ..models import Model
from ..transaction import atomic


class IdempotencyRecord(Model):
    """Abstract base for the idempotency table. Subclass with a
    concrete ``Meta.db_table`` (and any extra columns your app
    needs) and run ``makemigrations``.

    Columns:

    - ``key`` — caller-supplied identifier (the HTTP header value);
      uniqueness is the contract.
    - ``response`` — JSON blob of the cached payload.
    - ``status_code`` — optional integer the caller can use to mirror
      an HTTP status. Free-form.
    - ``created_at`` — fingerprint timestamp; useful for TTL purges.
    """

    key = fields.CharField(max_length=200, unique=True)
    response = fields.JSONField(default=dict)
    status_code = fields.IntegerField(null=True, blank=True)
    created_at = fields.DateTimeField(auto_now_add=True)

    class Meta:
        abstract = True


class _IdempotencyContext:
    """Yielded object inside the ``with`` block.

    Attributes:

    - ``replay`` — True when an earlier request with the same key
      already wrote a response; the caller should return
      ``cached_response`` instead of executing the operation.
    - ``cached_response`` — the prior payload (None on first call).
    - ``cached_status_code`` — the prior status (None on first call).
    """

    def __init__(
        self,
        key: str,
        model: Type[IdempotencyRecord],
        existing: Optional[IdempotencyRecord],
        using: str,
    ) -> None:
        self.key = key
        self.model = model
        self.using = using
        self.replay = existing is not None
        self.cached_response: Any = existing.response if existing else None
        self.cached_status_code: Optional[int] = (
            existing.status_code if existing else None
        )
        self._stored = self.replay
        self._existing = existing

    def store(self, response: Any, *, status_code: Optional[int] = None) -> None:
        """Persist *response* under the active key.

        Idempotent — calling twice in the same context is a no-op
        on the second call. Raises if the caller forgot to call
        ``store`` on a non-replay path (handled by the surrounding
        context manager's ``__exit__``).
        """
        if self._stored:
            return
        # Round-trip through JSON to validate the payload is
        # serialisable up-front — failing here is much friendlier
        # than failing inside the DB driver later. ``default=None``
        # leaves out the lenient fallback so genuinely-unserialisable
        # values (custom classes without ``__dict__`` etc.) surface
        # at the boundary instead of hiding behind ``str(obj)``.
        try:
            json.dumps(response)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"idempotency_key().store(): response is not JSON-serialisable: {exc}"
            ) from exc

        record = self.model(
            key=self.key,
            response=response,
            status_code=status_code,
        )
        record.save(using=self.using)
        self._stored = True
        self._existing = record


@contextlib.contextmanager
def idempotency_key(
    key: str,
    *,
    model: Type[IdempotencyRecord],
    using: str = "default",
):
    """Acquire an idempotency-key context.

    The block runs inside an ``atomic()`` transaction so the
    side-table row and any business writes commit (or roll back)
    together. On a replay hit the block still executes; the caller
    chooses what to do via ``ctx.replay``.

    Concurrency: a tiny race window exists between the lookup and
    the eventual ``store()``. Two simultaneous requests with the
    same key will both see ``replay=False``, both run the work, and
    one will fail with an ``IntegrityError`` on commit (the unique
    constraint). The losing transaction's surrounding ``atomic()``
    rolls back its work; the caller can retry and pick up the
    cached row. For higher-throughput needs, wrap the block in a
    ``select_for_update(skip_locked=True)`` row-level lock on the
    key.
    """
    if not key:
        raise ValueError("idempotency_key(): key must be a non-empty string")

    with atomic(using=using):
        existing = (
            model.objects.using(using).filter(key=key).first()
        )
        ctx = _IdempotencyContext(key=key, model=model, existing=existing, using=using)
        try:
            yield ctx
        finally:
            if not ctx._stored and not ctx.replay:
                # Caller forgot to call ``store`` on a fresh path —
                # we can't know what the response would have been,
                # so we let the transaction commit without storing.
                # The next retry will re-run the work (which is
                # arguably more correct than persisting a half-baked
                # answer).
                pass


def purge_expired(
    model: Type[IdempotencyRecord],
    *,
    older_than_seconds: int,
    using: str = "default",
) -> int:
    """Delete idempotency rows older than *older_than_seconds*.

    Returns the number of rows deleted. Wire this into a periodic
    job (cron / Celery beat / APScheduler) to keep the table
    bounded — idempotency rows are only useful for the brief window
    a client might retry; keeping them forever just wastes disk."""
    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
    qs = model.objects.using(using).filter(created_at__lt=cutoff)
    n = qs.count()
    qs.delete()
    return n


__all__ = [
    "IdempotencyRecord",
    "idempotency_key",
    "purge_expired",
]
