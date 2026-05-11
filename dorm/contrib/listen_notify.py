"""High-level ``LISTEN`` / ``NOTIFY`` helper for PostgreSQL.

PostgreSQL exposes a publish/subscribe primitive over the database
connection: any session can ``NOTIFY <channel>, '<payload>'`` and any
session that previously ran ``LISTEN <channel>`` receives the payload.
It is typed, durable for the lifetime of the listening connection and
needs no broker — the typical use case is to fan out cache-invalidation
messages, queue wake-ups, or low-volume real-time updates without
introducing Redis / NATS / Kafka.

This module wraps that low-level mechanism into an async iterator:

    from dorm.contrib.listen_notify import listen, notify

    async with listen("orders") as channel:
        # Optionally publish from another task / connection.
        await notify("orders", '{"id": 42}')
        async for message in channel:
            handle(message.payload)
            break  # leaving the ``async with`` block tears down LISTEN

Each ``Notification`` exposes ``channel``, ``payload`` and ``pid``
(the originating server backend pid). Heartbeat / keep-alive is the
caller's responsibility — this helper does not retry on connection
loss because the right policy depends on the workload.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any, AsyncIterator

from ..db.connection import get_async_connection, get_connection


@dataclass(frozen=True, slots=True)
class Notification:
    """A single ``NOTIFY`` payload as observed by a ``LISTEN`` session.

    *channel* matches the channel name the listener subscribed to.
    *payload* is whatever the publisher sent (PG ≥ 9.0 allows up to
    8000 bytes; longer payloads must be sliced by the publisher).
    *pid* is the backend pid of the publishing session — useful for
    self-message filtering.
    """

    channel: str
    payload: str
    pid: int


def _ensure_postgres(conn) -> None:
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            "LISTEN/NOTIFY is PostgreSQL-only — other backends do not "
            "expose an equivalent primitive over the connection."
        )


def _quote_ident(name: str) -> str:
    """Quote a channel name as a SQL identifier.

    PG ``LISTEN`` / ``NOTIFY`` accept identifiers, not parameter
    placeholders — so we have to inline the channel. A double-quoted
    identifier with internal ``"`` doubled is the safe form against
    accidental SQL injection from caller-supplied channel names.
    """
    return '"' + name.replace('"', '""') + '"'


def notify(channel: str, payload: str = "", *, using: str = "default") -> None:
    """Synchronously publish a ``NOTIFY`` message.

    The message is delivered when the surrounding transaction commits;
    if no transaction is active it is delivered immediately. Empty
    payloads are valid and useful as bare wake-up signals.

    Implemented via the ``pg_notify(text, text)`` function rather than
    the ``NOTIFY`` statement so caller-supplied payloads pass through
    psycopg's parameter binding instead of being string-formatted.
    """
    conn = get_connection(using)
    _ensure_postgres(conn)
    conn.execute_write("SELECT pg_notify(%s, %s)", [channel, payload])


async def anotify(channel: str, payload: str = "", *, using: str = "default") -> None:
    """Async counterpart of :func:`notify`."""
    conn = get_async_connection(using)
    _ensure_postgres(conn)
    await conn.execute_write("SELECT pg_notify(%s, %s)", [channel, payload])


class _ListenChannel:
    """Async iterator yielded by :func:`listen` / :func:`alisten`."""

    def __init__(self, channels: tuple[str, ...], conn, raw_conn) -> None:
        self._channels = channels
        self._conn = conn
        self._raw = raw_conn
        self._closed = False

    async def __aiter__(self) -> AsyncIterator[Notification]:
        if self._closed:
            return
        # ``raw_conn.notifies()`` is the psycopg3 async generator that
        # yields incoming notifications. It blocks the connection until
        # a NOTIFY arrives or the connection closes.
        gen = self._raw.notifies()
        try:
            async for n in gen:
                yield Notification(channel=n.channel, payload=n.payload, pid=n.pid)
                if self._closed:
                    break
        except (asyncio.CancelledError, GeneratorExit):
            # Caller broke out of the loop or task got cancelled — let
            # the surrounding ``async with`` clean up the connection.
            return

    async def aclose(self) -> None:
        self._closed = True


@contextlib.asynccontextmanager
async def listen(*channels: str, using: str = "default"):
    """Async context manager that ``LISTEN``s on one or more *channels*.

    Yields a :class:`_ListenChannel` that's also an async iterator —
    iterate it to receive :class:`Notification` instances. On exit
    the helper ``UNLISTEN``s and returns the dedicated connection
    back to the pool::

        async with listen("orders") as ch:
            async for n in ch:
                process(n.payload)
                if some_condition:
                    break

    A dedicated connection is checked out for the lifetime of the
    block — LISTEN ties the subscription to a single backend, so the
    pool's connection-recycling logic must not swap it out from under
    the iterator.
    """
    if not channels:
        raise ValueError("listen() requires at least one channel name.")
    conn = get_async_connection(using)
    _ensure_postgres(conn)

    pool = await conn._get_pool()
    # ``connection()`` returns the raw psycopg3 AsyncConnection — we
    # hold onto it for the lifetime of the iterator so the pool does
    # not reclaim the backend that owns our LISTEN subscription.
    cm = pool.connection()
    raw = await cm.__aenter__()
    try:
        # psycopg3 routes NOTIFY only when the connection is in
        # autocommit; otherwise the notifications queue up but the
        # async iterator never wakes. Make sure we're not in a tx.
        await raw.set_autocommit(True)
        for ch in channels:
            await raw.execute(f"LISTEN {_quote_ident(ch)}")
        channel = _ListenChannel(channels, conn, raw)
        try:
            yield channel
        finally:
            await channel.aclose()
            for ch in channels:
                with contextlib.suppress(Exception):
                    await raw.execute(f"UNLISTEN {_quote_ident(ch)}")
    finally:
        with contextlib.suppress(Exception):
            await cm.__aexit__(None, None, None)


class Broadcaster:
    """Multiplexer for LISTEN/NOTIFY: one dedicated connection,
    many in-process subscribers.

    The standalone :func:`listen` helper pins a connection to a single
    iterator — fine for one consumer per channel, awkward when many
    async tasks want to react to the same PG event. The broadcaster
    keeps a single LISTEN connection open for the lifetime of the
    process (or context) and fans every arriving notification out
    across an :class:`asyncio.Queue` per subscription.

    Usage::

        from dorm.contrib.listen_notify import Broadcaster

        async def task_a(bcast):
            async with bcast.subscribe("orders") as queue:
                async for n in queue:
                    handle_a(n.payload)

        async def main():
            async with Broadcaster(["orders"]) as bcast:
                await asyncio.gather(
                    task_a(bcast), task_b(bcast), task_c(bcast)
                )

    Each subscriber gets its own queue: a slow consumer can lag
    without dropping notifications for the others. Queue overflow is
    bounded by ``maxsize`` (default 100) — older items are discarded
    once full so memory pressure stays predictable; emit a NOTIFY
    that re-syncs from authoritative state if your protocol requires
    every event.
    """

    def __init__(
        self,
        channels: list[str],
        *,
        using: str = "default",
        maxsize: int = 100,
    ) -> None:
        if not channels:
            raise ValueError("Broadcaster requires at least one channel")
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self._channels = list(channels)
        self._using = using
        self._maxsize = maxsize
        # ``_subs`` maps channel → list of subscriber queues. Mutation
        # under ``_lock`` so subscribe/unsubscribe doesn't race the
        # dispatcher task.
        self._subs: dict[str, list[asyncio.Queue[Notification]]] = {
            ch: [] for ch in channels
        }
        self._lock = asyncio.Lock()
        self._listen_ctx: Any = None
        self._listen_channel: _ListenChannel | None = None
        self._dispatcher_task: asyncio.Task[None] | None = None
        self._started = False

    async def __aenter__(self) -> "Broadcaster":
        self._listen_ctx = listen(*self._channels, using=self._using)
        self._listen_channel = await self._listen_ctx.__aenter__()
        self._started = True

        async def _dispatch() -> None:
            assert self._listen_channel is not None
            async for n in self._listen_channel:
                async with self._lock:
                    queues = list(self._subs.get(n.channel, ()))
                for q in queues:
                    if q.full():
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:  # pragma: no cover
                            pass
                    q.put_nowait(n)

        self._dispatcher_task = asyncio.create_task(_dispatch())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._dispatcher_task is not None:
            self._dispatcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._dispatcher_task
        if self._listen_ctx is not None:
            await self._listen_ctx.__aexit__(exc_type, exc, tb)
        # Notify any remaining subscribers via sentinel so their
        # iterators terminate cleanly instead of hanging forever.
        # Queues hold Notification, so we cast the sentinel via Any to
        # bypass the type-checker — the iterator detects ``is None``
        # and raises StopAsyncIteration.
        async with self._lock:
            for queues in self._subs.values():
                for q in queues:
                    with contextlib.suppress(Exception):
                        sentinel: Any = None
                        q.put_nowait(sentinel)

    @contextlib.asynccontextmanager
    async def subscribe(self, channel: str):
        """Subscribe to *channel*. Yields an async iterator that
        yields :class:`Notification` instances and terminates when the
        broadcaster shuts down."""
        if channel not in self._subs:
            raise KeyError(
                f"Broadcaster: channel {channel!r} not registered "
                f"(known: {sorted(self._subs)})"
            )
        queue: asyncio.Queue[Notification] = asyncio.Queue(self._maxsize)
        async with self._lock:
            self._subs[channel].append(queue)
        try:
            yield _SubscriberStream(queue)
        finally:
            async with self._lock:
                try:
                    self._subs[channel].remove(queue)
                except ValueError:  # pragma: no cover
                    pass


class _SubscriberStream:
    def __init__(self, queue: "asyncio.Queue[Notification]") -> None:
        self._queue = queue

    def __aiter__(self) -> "_SubscriberStream":
        return self

    async def __anext__(self) -> Notification:
        item = await self._queue.get()
        if item is None:  # broadcaster shutdown sentinel
            raise StopAsyncIteration
        return item


__all__ = [
    "Notification",
    "listen",
    "notify",
    "anotify",
    "Broadcaster",
]
