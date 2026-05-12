"""Unit tests for ``dorm.contrib.outbox_cdc``.

The Kafka / NATS adapters need the broker libraries + a running
broker for an end-to-end test; here we exercise the row-encoder,
the topic resolver, the logging publisher (no external deps) and
the Redis stream publisher against an in-memory fake.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any


class _FakeRow:
    """Minimal duck-typed outbox row — matches the columns the
    publisher reads."""

    def __init__(
        self,
        event_type: str = "order.created",
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.id = uuid.uuid4()
        self.event_type = event_type
        self.payload = payload or {"k": "v"}
        self.created_at: Any = datetime(2026, 5, 12, tzinfo=timezone.utc)


class TestRowEncoder:
    def test_encodes_to_stable_json(self):
        from dorm.contrib.outbox_cdc import _row_to_json

        row = _FakeRow(payload={"b": 2, "a": 1})
        body = _row_to_json(row)
        decoded = json.loads(body)
        # Stable key ordering — sort_keys=True
        assert list(decoded.keys()) == ["created_at", "event_type", "id", "payload"]
        assert decoded["event_type"] == "order.created"
        assert decoded["payload"] == {"a": 1, "b": 2}
        assert decoded["created_at"].startswith("2026-05-12")

    def test_handles_missing_created_at(self):
        from dorm.contrib.outbox_cdc import _row_to_json

        row = _FakeRow()
        row.created_at = None
        decoded = json.loads(_row_to_json(row))
        assert "created_at" not in decoded


class TestLoggingPublisher:
    def test_logs_topic_and_body(self, caplog):
        from dorm.contrib.outbox_cdc import LoggingPublisher

        publisher = LoggingPublisher()
        with caplog.at_level(logging.INFO, logger="dorm.contrib.outbox_cdc"):
            row = _FakeRow(event_type="order.created", payload={"id": 1})
            ok = publisher(row)
        assert ok is True
        text = "\n".join(rec.message for rec in caplog.records)
        assert "topic=order.created" in text
        # JSON encoded with spaces by ``json.dumps`` default.
        assert '"id": 1' in text or '"id":1' in text

    def test_topic_resolver_override(self, caplog):
        from dorm.contrib.outbox_cdc import LoggingPublisher

        publisher = LoggingPublisher(
            topic_resolver=lambda r: f"dorm.{r.event_type}"
        )
        with caplog.at_level(logging.INFO, logger="dorm.contrib.outbox_cdc"):
            publisher(_FakeRow(event_type="user.changed"))
        assert any(
            "topic=dorm.user.changed" in r.message for r in caplog.records
        )

    def test_custom_logger(self):
        from dorm.contrib.outbox_cdc import LoggingPublisher

        seen: list[str] = []

        class _Cap(logging.Handler):
            def emit(self, record):
                seen.append(record.getMessage())

        logger = logging.getLogger("test_outbox_cdc_custom")
        logger.handlers.clear()
        logger.addHandler(_Cap())
        logger.setLevel(logging.DEBUG)
        publisher = LoggingPublisher(logger=logger, level=logging.DEBUG)
        publisher(_FakeRow())
        assert seen and "topic=order.created" in seen[0]


class TestRedisStreamPublisher:
    def test_xadd_called_with_body_and_event_type(self):
        from dorm.contrib.outbox_cdc import RedisStreamPublisher

        seen: list[tuple[Any, ...]] = []

        class _FakeClient:
            def xadd(self, key, fields, **kwargs):
                seen.append((key, fields, kwargs))
                return b"1-0"  # truthy stream id

        publisher = RedisStreamPublisher(_FakeClient())
        ok = publisher(_FakeRow(event_type="evt", payload={"x": 1}))
        assert ok is True
        key, fields, _kwargs = seen[0]
        assert key == "evt"
        assert fields["event_type"] == "evt"
        assert json.loads(fields["body"])["event_type"] == "evt"

    def test_maxlen_threaded_through(self):
        from dorm.contrib.outbox_cdc import RedisStreamPublisher

        captured: dict[str, Any] = {}

        class _FakeClient:
            def xadd(self, key, fields, **kwargs):
                captured.update(kwargs)
                return b"1-0"

        publisher = RedisStreamPublisher(_FakeClient(), max_len=1000)
        publisher(_FakeRow())
        assert captured == {"maxlen": 1000, "approximate": True}

    def test_falsy_xadd_returns_false(self):
        from dorm.contrib.outbox_cdc import RedisStreamPublisher

        class _FakeClient:
            def xadd(self, *_a, **_kw):
                return None  # broker hiccup — falsy

        publisher = RedisStreamPublisher(_FakeClient())
        assert publisher(_FakeRow()) is False


class TestKafkaPublisherDependencyError:
    def test_clear_error_when_kafka_python_missing(self, monkeypatch):
        """When ``kafka-python`` isn't installed the constructor must
        raise an ``ImportError`` with an actionable message — not a
        bare ``ModuleNotFoundError``."""
        import builtins

        from dorm.contrib import outbox_cdc

        real_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "kafka":
                raise ImportError("simulated missing kafka")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)
        try:
            outbox_cdc.KafkaPublisher("localhost:9092")
        except ImportError as exc:
            assert "kafka-python" in str(exc)
        else:
            raise AssertionError("expected ImportError")


class TestNatsPublisherDependencyError:
    def test_clear_error_when_nats_py_missing(self, monkeypatch):
        import builtins

        from dorm.contrib import outbox_cdc

        real_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "nats":
                raise ImportError("simulated missing nats")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)
        try:
            outbox_cdc.NatsPublisher(["nats://localhost:4222"])
        except ImportError as exc:
            assert "nats-py" in str(exc)
        else:
            raise AssertionError("expected ImportError")
