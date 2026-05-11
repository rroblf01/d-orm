"""Celery-lite task runner built on the outbox + LISTEN/NOTIFY.

For workloads that need persistent background jobs but don't justify
pulling in Celery / RQ / Dramatiq, this module stitches together two
features dorm already ships:

- :mod:`dorm.contrib.outbox` — the durable, ``atomic()``-friendly job
  queue.
- :mod:`dorm.contrib.listen_notify` — PostgreSQL pub/sub for instant
  wake-up (workers don't poll when a NOTIFY is in flight).

Usage::

    from dorm.contrib.tasks import TaskQueue, task

    class TaskTable(OutboxEvent):
        class Meta:
            db_table = "background_tasks"

    queue = TaskQueue(model=TaskTable, channel="tasks")

    @task(queue, name="send-welcome")
    def send_welcome(user_id: int) -> None:
        ...

    # Enqueue inside a transaction (atomic write + task creation):
    with dorm.transaction.atomic():
        user = User.objects.create(email="x@y")
        send_welcome.delay(user.id)

    # Worker process:
    queue.run()                # blocking; consumes until SIGTERM
    queue.drain_once()         # single-pass; useful in tests

Falls back to polling on backends without ``LISTEN`` / ``NOTIFY``
(SQLite, libsql) — the queue still works, just less responsive.
"""
from __future__ import annotations

import json
import logging
import signal as _signal
import time
from dataclasses import dataclass
from typing import Any, Callable

from .outbox import OutboxEvent, OutboxRelay, record_event

_log = logging.getLogger("dorm.contrib.tasks")


_TASK_REGISTRY: dict[str, "Task"] = {}


@dataclass
class Task:
    """Metadata for a single registered task. Bound to a queue at
    declaration time so ``.delay()`` doesn't need an explicit queue
    reference at every call site."""

    name: str
    func: Callable[..., Any]
    queue: "TaskQueue"
    max_attempts: int = 5

    def delay(self, *args: Any, **kwargs: Any) -> Any:
        """Enqueue an invocation of this task.

        Must run inside an :func:`dorm.transaction.atomic` block when
        composability with surrounding writes matters — the outbox
        row commits with the business write or both roll back.
        """
        payload = {
            "args": list(args),
            "kwargs": kwargs,
            "max_attempts": self.max_attempts,
        }
        return record_event(self.queue.model, self.name, payload)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Direct call invokes the underlying function — useful for
        tests and synchronous unit calls."""
        return self.func(*args, **kwargs)


class TaskQueue:
    """Workers and registry for a single task table.

    Args:
        model: subclass of :class:`OutboxEvent` materialising the
            queue's storage table.
        channel: PostgreSQL ``NOTIFY`` channel. Workers listen on it
            for wake-ups so they don't have to poll. ``None`` falls
            back to polling-only (also automatic on non-PG backends).
        poll_interval_s: idle wait between polls when no notification
            is in flight.
        max_attempts: default retry budget per task. Per-task
            overrides go through :func:`task`.
        using: connection alias.
    """

    def __init__(
        self,
        *,
        model: type[OutboxEvent],
        channel: str | None = "dorm_tasks",
        poll_interval_s: float = 5.0,
        max_attempts: int = 5,
        using: str = "default",
    ) -> None:
        self.model = model
        self.channel = channel
        self.poll_interval_s = poll_interval_s
        self.max_attempts = max_attempts
        self.using = using
        self._relay: OutboxRelay | None = None

    # ── Worker entry points ────────────────────────────────────────────────

    def _build_handler(self) -> Callable[[OutboxEvent], None]:
        def _handle(event: OutboxEvent) -> None:
            task_ = _TASK_REGISTRY.get(event.event_type)
            if task_ is None:
                raise RuntimeError(
                    f"No task registered for event_type={event.event_type!r}. "
                    "Did you import the module that calls @task?"
                )
            payload = event.payload
            if isinstance(payload, str):
                payload = json.loads(payload)
            args = payload.get("args", []) or []
            kwargs = payload.get("kwargs", {}) or {}
            task_.func(*args, **kwargs)

        return _handle

    def _get_relay(self) -> OutboxRelay:
        if self._relay is None:
            self._relay = OutboxRelay(
                model=self.model,
                using=self.using,
                max_attempts=self.max_attempts,
            )
        return self._relay

    def drain_once(self) -> int:
        """Single-pass drain — process every PENDING row exactly once.
        Returns the count of rows handled. Test-friendly: synchronous,
        no signals wired."""
        return self._get_relay().drain_once(self._build_handler())

    def run(self, *, stop_after_idle_seconds: float | None = None) -> None:
        """Blocking worker loop. Listens on the NOTIFY channel when
        available, polls ``poll_interval_s`` otherwise. Stops on
        SIGTERM / SIGINT. Optional *stop_after_idle_seconds* makes
        the loop exit after that many seconds without new work —
        useful in Kubernetes Job manifests."""

        stop = {"flag": False}

        def _on_signal(_signum, _frame):  # noqa: ANN001
            stop["flag"] = True

        prev_term = _signal.signal(_signal.SIGTERM, _on_signal)
        prev_int = _signal.signal(_signal.SIGINT, _on_signal)
        handler = self._build_handler()
        relay = self._get_relay()

        last_work = time.monotonic()
        try:
            while not stop["flag"]:
                processed = relay.drain_once(handler)
                if processed:
                    last_work = time.monotonic()
                elif stop_after_idle_seconds is not None:
                    if time.monotonic() - last_work >= stop_after_idle_seconds:
                        break
                if processed:
                    continue
                # Idle: either LISTEN on the channel or sleep.
                if self.channel:
                    try:
                        self._wait_for_notify()
                    except Exception:  # pragma: no cover - vendor fallback
                        time.sleep(self.poll_interval_s)
                else:
                    time.sleep(self.poll_interval_s)
        finally:
            _signal.signal(_signal.SIGTERM, prev_term)
            _signal.signal(_signal.SIGINT, prev_int)

    def _wait_for_notify(self) -> None:
        """Block on the configured NOTIFY channel for at most
        ``poll_interval_s``. PG-only — silently falls back to a sleep
        on other vendors so the loop keeps making forward progress."""
        from ..db.connection import get_connection

        conn = get_connection(self.using)
        if getattr(conn, "vendor", None) != "postgresql":
            time.sleep(self.poll_interval_s)
            return
        try:
            import select  # stdlib

            # Use a raw psycopg connection to wait on notifications.
            with conn._get_pool().connection() as raw:
                with raw.cursor() as cur:
                    cur.execute(f"LISTEN {self.channel}")
                raw.commit()
                # ``raw.notifies()`` is a generator; we want a
                # bounded wait.
                fd = raw.pgconn.socket
                select.select([fd], [], [], self.poll_interval_s)
                # Drain anything that arrived so the wakeup is a
                # one-shot per cycle.
                while raw.notifies():
                    pass
        except Exception:  # pragma: no cover
            time.sleep(self.poll_interval_s)


def task(
    queue: TaskQueue,
    *,
    name: str | None = None,
    max_attempts: int | None = None,
) -> Callable[[Callable[..., Any]], Task]:
    """Decorator that registers *func* as a task on *queue*.

    The decorated function gains a ``.delay(*args, **kwargs)`` method
    that enqueues a deferred invocation. Calling the function directly
    still runs synchronously — useful in tests.
    """

    def _decorate(func: Callable[..., Any]) -> Task:
        tname = name or f"{func.__module__}.{getattr(func, '__qualname__', repr(func))}"
        if tname in _TASK_REGISTRY:
            raise RuntimeError(
                f"Task {tname!r} already registered. Pass an explicit "
                "``name=`` to differentiate."
            )
        t = Task(
            name=tname,
            func=func,
            queue=queue,
            max_attempts=max_attempts or queue.max_attempts,
        )
        _TASK_REGISTRY[tname] = t
        return t

    return _decorate


def reset_registry() -> None:
    """Drop every registered task. Test helper — production code
    never needs this."""
    _TASK_REGISTRY.clear()


__all__ = ["TaskQueue", "Task", "task", "reset_registry"]
