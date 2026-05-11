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

import datetime as _dt
import json
import logging
import random
import signal as _signal
import time
from dataclasses import dataclass
from typing import Any, Callable

from .outbox import OutboxEvent, OutboxRelay, record_event

_log = logging.getLogger("dorm.contrib.tasks")


class _TaskNotReady(Exception):
    """Raised by the worker handler when an event's ``eta`` is in the
    future. Surfaces as a normal handler failure to the relay — the
    relay's retry-with-backoff path then re-checks on the next pass."""


_TASK_REGISTRY: dict[str, "Task"] = {}


@dataclass
class Task:
    """Metadata for a single registered task. Bound to a queue at
    declaration time so ``.delay()`` doesn't need an explicit queue
    reference at every call site.

    Args:
        name: registry key.
        func: synchronous Python callable that does the work.
        queue: :class:`TaskQueue` owning this task.
        max_attempts: per-task retry budget. ``OutboxRelay`` enforces
            it via the ``attempts`` column.
        priority: integer priority — workers drain low values first
            when multiple priorities are queued. Default 0.
        cron: optional cron expression (5-field, minute granularity)
            evaluated by :meth:`TaskQueue.run_cron_loop`. Enqueues a
            fresh invocation on every matching minute.
    """

    name: str
    func: Callable[..., Any]
    queue: "TaskQueue"
    max_attempts: int = 5
    priority: int = 0
    cron: str | None = None

    def delay(
        self,
        *args: Any,
        eta: _dt.datetime | None = None,
        delay_seconds: float | None = None,
        priority: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Enqueue an invocation of this task.

        Args:
            *args / **kwargs: arguments forwarded to the task body.
            eta: schedule the run at this UTC timestamp; workers
                skip rows whose ``eta`` is still in the future.
            delay_seconds: shortcut for ``eta = now + N seconds``.
            priority: per-invocation override of the task's default.

        Must run inside an :func:`dorm.transaction.atomic` block when
        composability with surrounding writes matters — the outbox
        row commits with the business write or both roll back.
        """
        if eta is None and delay_seconds is not None:
            eta = _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(
                seconds=delay_seconds
            )
        payload: dict[str, Any] = {
            "args": list(args),
            "kwargs": kwargs,
            "max_attempts": self.max_attempts,
            "priority": self.priority if priority is None else priority,
        }
        if eta is not None:
            payload["eta"] = eta.isoformat()
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
            # Respect ``eta`` written by ``Task.delay(eta=…)``. The
            # OutboxRelay scans pending rows without knowing about
            # ETA semantics, so the gate fires here: skip rows whose
            # ETA is still in the future and raise so the relay
            # bumps the attempt count + leaves the row pending. The
            # relay's exponential-backoff retry path then naturally
            # serves as the rescheduler.
            eta_raw = payload.get("eta") if isinstance(payload, dict) else None
            if eta_raw:
                try:
                    eta = _dt.datetime.fromisoformat(eta_raw)
                except ValueError:
                    eta = None
                if eta is not None:
                    now = _dt.datetime.now(_dt.timezone.utc)
                    # Normalise naive datetimes to UTC.
                    if eta.tzinfo is None:
                        eta = eta.replace(tzinfo=_dt.timezone.utc)
                    if eta > now:
                        raise _TaskNotReady(
                            f"task {event.event_type!r} scheduled for "
                            f"{eta.isoformat()}; current {now.isoformat()}"
                        )
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
    priority: int = 0,
    cron: str | None = None,
) -> Callable[[Callable[..., Any]], Task]:
    """Decorator that registers *func* as a task on *queue*.

    The decorated function gains a ``.delay(*args, **kwargs)`` method
    that enqueues a deferred invocation. Calling the function directly
    still runs synchronously — useful in tests.

    Args:
        priority: integer priority. Workers drain lowest first.
        cron: optional 5-field cron expression. Drives
            :meth:`TaskQueue.run_cron_loop`.
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
            priority=priority,
            cron=cron,
        )
        _TASK_REGISTRY[tname] = t
        return t

    return _decorate


def _parse_cron_field(spec: str, lo: int, hi: int) -> set[int]:
    """Parse one cron field into the set of matching values.

    Supports: ``*``, ``a,b,c``, ``a-b``, ``*/N`` and the
    combinations Python users typically reach for. Ranges + step
    sizes only — no fancy macros (``@daily`` etc.).
    """
    out: set[int] = set()
    for part in spec.split(","):
        step = 1
        had_step = False
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = int(step_s)
            if step < 1:
                raise ValueError(f"cron step must be >= 1, got {step}")
            part = base
            had_step = True
        if part in ("*", ""):
            rng = range(lo, hi + 1, step)
        elif "-" in part:
            a, b = part.split("-", 1)
            rng = range(int(a), int(b) + 1, step)
        elif had_step:
            # Standard cron: ``a/N`` means "from a to hi in steps of
            # N", e.g. ``0/5`` in a minute field matches
            # ``0, 5, 10, …, 55``.
            rng = range(int(part), hi + 1, step)
        else:
            n = int(part)
            rng = range(n, n + 1)
        for v in rng:
            if not (lo <= v <= hi):
                raise ValueError(
                    f"cron field value {v} outside [{lo}, {hi}]"
                )
            out.add(v)
    return out


def cron_matches(expr: str, when: _dt.datetime) -> bool:
    """Return True iff *when* matches the 5-field cron *expr*
    (minute, hour, day-of-month, month, day-of-week).

    Sunday is ``0`` (Python's :meth:`datetime.weekday()` returns Monday=0,
    so we shift accordingly).
    """
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(
            f"cron expression must have 5 fields; got {len(parts)} in {expr!r}"
        )
    mins = _parse_cron_field(parts[0], 0, 59)
    hrs = _parse_cron_field(parts[1], 0, 23)
    doms = _parse_cron_field(parts[2], 1, 31)
    months = _parse_cron_field(parts[3], 1, 12)
    dows = _parse_cron_field(parts[4], 0, 6)
    dow_py = (when.weekday() + 1) % 7
    return (
        when.minute in mins
        and when.hour in hrs
        and when.day in doms
        and when.month in months
        and dow_py in dows
    )


def reset_registry() -> None:
    """Drop every registered task. Test helper — production code
    never needs this."""
    _TASK_REGISTRY.clear()


def run_cron_tick(
    queue: "TaskQueue", *, now: _dt.datetime | None = None
) -> int:
    """Walk every registered task with a ``cron`` expression and
    enqueue a ``.delay()`` for those matching *now* (defaults to UTC
    current minute). Returns the count enqueued.

    Wire this into the scheduler of your choice — APScheduler,
    cron-on-the-host, or a dedicated dorm worker. Re-run every minute;
    duplicate-suppression on the destination is the caller's problem
    (use :func:`dorm.contrib.inbox.idempotent` when at-most-once
    matters).
    """
    when = now or _dt.datetime.now(_dt.timezone.utc).replace(
        second=0, microsecond=0
    )
    enqueued = 0
    for t in _TASK_REGISTRY.values():
        if t.queue is not queue or not t.cron:
            continue
        if cron_matches(t.cron, when):
            t.delay()
            enqueued += 1
    return enqueued


def retry_with_backoff(
    queue: "TaskQueue",
    event: OutboxEvent,
    *,
    base_seconds: float = 5.0,
    max_seconds: float = 3600.0,
    jitter: bool = True,
) -> _dt.datetime:
    """Return the ETA for *event*'s next attempt using exponential
    backoff + optional jitter. Calling code stamps the event row
    with this ETA so the worker skips it until ready."""
    n = max(0, event.attempts)
    delay = min(max_seconds, base_seconds * (2 ** n))
    if jitter:
        delay *= 0.5 + random.random()
    return _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(seconds=delay)


def dead_letters(queue: "TaskQueue") -> Any:
    """Return a queryset over the dead-letter rows on *queue*'s
    model. ``status='dead'`` is set by :class:`OutboxRelay` when an
    event's ``attempts`` exceeds its ``max_attempts``."""
    return queue.model.objects.filter(status="dead")  # type: ignore[attr-defined]


__all__ = [
    "TaskQueue",
    "Task",
    "task",
    "reset_registry",
    "cron_matches",
    "run_cron_tick",
    "retry_with_backoff",
    "dead_letters",
]
