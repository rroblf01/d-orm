"""Broker-bound publishers for :class:`OutboxRelay`.

Where :mod:`dorm.contrib.outbox` provides the **transactional**
producer (writes events to a DB table inside the business
transaction), this module supplies the *consumer* — the handler
that drains the outbox and forwards each event to an external
broker. The split keeps the core feature broker-agnostic; the
adapters here pick up the OS-level dependency on a per-installation
basis.

Adapters ship for the brokers most dorm users have asked for:

- :class:`KafkaPublisher` — requires the optional ``aiokafka`` or
  ``kafka-python`` dependency. Sync version uses
  :mod:`kafka-python`; the async version uses :mod:`aiokafka`.
- :class:`NatsPublisher` — requires ``nats-py``.
- :class:`RedisStreamPublisher` — requires ``redis`` (sync) or
  ``redis.asyncio`` (async).
- :class:`LoggingPublisher` — no external dep; writes JSON lines to
  a logger / file. Useful for local dev + CI smoke tests that don't
  want to spin up a broker.

Each publisher exposes a ``__call__(row) -> bool`` synchronous
interface (and ``acall(row)`` async equivalent where applicable) so
it plugs straight into ``OutboxRelay.run(handler=publisher)`` /
``arun(handler=publisher)`` without any glue code.

Failure semantics: a publisher returns ``True`` when the broker
acked the message, ``False`` to defer + retry (matches the existing
relay protocol). Any raised exception is treated as a failure by
the relay and bumps ``row.attempts``.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable

_log = logging.getLogger("dorm.contrib.outbox_cdc")


def _row_to_json(row: Any) -> bytes:
    """Encode an outbox row as the bytes payload most brokers want.

    The shape is intentionally stable: ``id``, ``event_type``,
    ``payload``, ``created_at`` (ISO 8601). Consumers depending on
    extra columns can subclass the publisher and override
    :meth:`encode`. The bytes form (UTF-8 JSON) plays nicely with
    Kafka / Redis Streams / NATS without coupling the relay to a
    specific schema registry.
    """
    body = {
        "id": str(row.id),
        "event_type": row.event_type,
        "payload": row.payload,
    }
    created = getattr(row, "created_at", None)
    if created is not None:
        body["created_at"] = (
            created.isoformat() if hasattr(created, "isoformat") else str(created)
        )
    return json.dumps(body, sort_keys=True, default=str).encode("utf-8")


class _BasePublisher:
    """Common scaffolding for the broker adapters.

    *topic_resolver* picks the destination topic / subject / stream
    for each row. By default we use the row's ``event_type`` —
    rename via ``topic_resolver=lambda row: f"dorm.{row.event_type}"``
    if your broker conventions differ.
    """

    def __init__(
        self,
        *,
        topic_resolver: Callable[[Any], str] | None = None,
    ) -> None:
        self._resolve_topic = topic_resolver or (lambda row: row.event_type)

    def encode(self, row: Any) -> bytes:
        """Hook for subclasses that want custom message bodies."""
        return _row_to_json(row)


class LoggingPublisher(_BasePublisher):
    """Drop-in publisher that just writes the encoded row to a
    logger. Useful for end-to-end tests + local dev workflows that
    don't want to spin up a real broker.

    The handler always returns ``True`` so the relay marks the row
    as published — the assumption is "if logging fails, you have
    bigger problems than a stuck outbox row".
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        level: int = logging.INFO,
        topic_resolver: Callable[[Any], str] | None = None,
    ) -> None:
        super().__init__(topic_resolver=topic_resolver)
        self._logger = logger or _log
        self._level = level

    def __call__(self, row: Any) -> bool:
        topic = self._resolve_topic(row)
        body = self.encode(row).decode("utf-8")
        self._logger.log(self._level, "outbox_cdc topic=%s body=%s", topic, body)
        return True


class KafkaPublisher(_BasePublisher):
    """Sync Kafka publisher backed by :mod:`kafka-python`.

    Args:
        bootstrap_servers: comma-separated host:port list or list of
            tuples — passed straight to ``KafkaProducer``.
        topic_resolver: optional row-to-topic callable. Defaults to
            using ``row.event_type`` as the topic.
        producer_kwargs: extra kwargs forwarded to ``KafkaProducer``
            (e.g. ``acks="all"`` for stronger durability).
    """

    def __init__(
        self,
        bootstrap_servers: Any,
        *,
        topic_resolver: Callable[[Any], str] | None = None,
        producer_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(topic_resolver=topic_resolver)
        try:
            from kafka import KafkaProducer  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]
        except ImportError as exc:  # pragma: no cover - optional dep
            raise ImportError(
                "KafkaPublisher requires the 'kafka-python' package. "
                "Install with `pip install kafka-python`."
            ) from exc
        kwargs = {"bootstrap_servers": bootstrap_servers}
        if producer_kwargs:
            kwargs.update(producer_kwargs)
        self._producer = KafkaProducer(**kwargs)

    def __call__(self, row: Any) -> bool:
        topic = self._resolve_topic(row)
        body = self.encode(row)
        try:
            future = self._producer.send(topic, value=body)
            # Force the per-message future to surface broker errors
            # synchronously — without this the producer would
            # buffer and silently drop on misconfig.
            future.get(timeout=10)
            return True
        except Exception as exc:  # pragma: no cover - integration
            _log.warning("KafkaPublisher: send failed on %s: %s", topic, exc)
            raise

    def close(self) -> None:  # pragma: no cover - integration
        """Close the underlying producer's sockets. Call before
        process exit to flush any buffered records."""
        self._producer.flush()
        self._producer.close()


class NatsPublisher(_BasePublisher):
    """Async NATS publisher backed by :mod:`nats-py`.

    Connection is lazy + reused across calls — the first ``__call__``
    establishes the connection. Use :meth:`aclose` from the saga's
    teardown to release the socket.

    Designed to plug into ``OutboxRelay.arun()``.
    """

    def __init__(
        self,
        servers: list[str] | str,
        *,
        topic_resolver: Callable[[Any], str] | None = None,
    ) -> None:
        super().__init__(topic_resolver=topic_resolver)
        try:
            import nats  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]  # noqa: F401
        except ImportError as exc:  # pragma: no cover - optional dep
            raise ImportError(
                "NatsPublisher requires the 'nats-py' package. "
                "Install with `pip install nats-py`."
            ) from exc
        self._servers = servers if isinstance(servers, list) else [servers]
        self._nc: Any = None

    async def _connect(self) -> Any:  # pragma: no cover - integration
        import nats  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]

        if self._nc is None:
            self._nc = await nats.connect(self._servers)
        return self._nc

    async def __call__(self, row: Any) -> bool:  # pragma: no cover - integration
        nc = await self._connect()
        subject = self._resolve_topic(row)
        body = self.encode(row)
        await nc.publish(subject, body)
        await nc.flush()
        return True

    async def aclose(self) -> None:  # pragma: no cover - integration
        if self._nc is not None:
            await self._nc.close()
            self._nc = None


class RedisStreamPublisher(_BasePublisher):
    """Sync Redis Streams publisher backed by :mod:`redis`.

    Each event becomes one entry on a stream whose key is the
    resolved topic. Use a single-shard stream for strict ordering
    or stream-per-tenant for parallelism.
    """

    def __init__(
        self,
        client: Any,
        *,
        topic_resolver: Callable[[Any], str] | None = None,
        max_len: int | None = None,
    ) -> None:
        """*client* is a connected ``redis.Redis`` instance.

        ``max_len`` (optional) caps the stream length using
        ``MAXLEN ~`` — the *approximate* trim that Redis can do
        without scanning the whole stream. Pass it to enforce
        retention without an external cron.
        """
        super().__init__(topic_resolver=topic_resolver)
        self._client = client
        self._max_len = max_len

    def __call__(self, row: Any) -> bool:
        topic = self._resolve_topic(row)
        body = self.encode(row)
        kwargs: dict[str, Any] = {}
        if self._max_len is not None:
            kwargs["maxlen"] = self._max_len
            kwargs["approximate"] = True
        # ``xadd`` returns the auto-generated stream id; truthy ack.
        return bool(
            self._client.xadd(
                topic, {"body": body, "event_type": row.event_type}, **kwargs
            )
        )


__all__ = [
    "LoggingPublisher",
    "KafkaPublisher",
    "NatsPublisher",
    "RedisStreamPublisher",
]
