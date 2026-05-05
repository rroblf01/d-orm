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
from typing import AsyncIterator

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


__all__ = [
    "Notification",
    "listen",
    "notify",
    "anotify",
]
