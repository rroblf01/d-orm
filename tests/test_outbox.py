"""Tests for ``dorm.contrib.outbox``.

The outbox model is a concrete subclass defined inline in the test
file; the test fixture creates / drops its table per-test so the
suite stays self-contained. Backend-agnostic — works on SQLite and PG.
"""

from __future__ import annotations

import pytest

from dorm import transaction
from dorm.contrib.outbox import (
    OutboxEvent,
    OutboxRelay,
    record_event,
    serialize_payload,
)
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


class _Outbox(OutboxEvent):
    class Meta:
        db_table = "outbox_events"
        app_label = "tests"


@pytest.fixture(autouse=True)
def _outbox_table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "outbox_events"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Outbox._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "outbox_events" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "outbox_events"{cascade}')


def test_record_event_inside_atomic_persists():
    with transaction.atomic():
        evt = record_event(_Outbox, "order.created", {"order_id": 1})
    assert evt.id is not None
    assert _Outbox.objects.count() == 1


def test_record_event_rolls_back_with_atomic():
    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        with transaction.atomic():
            record_event(_Outbox, "x", {"a": 1})
            raise _Boom()
    assert _Outbox.objects.count() == 0


def test_record_event_outside_atomic_warns(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="dorm.contrib.outbox"):
        record_event(_Outbox, "warn-me", {})
    assert any("outside a transaction" in r.message for r in caplog.records)


def test_relay_drain_marks_published():
    with transaction.atomic():
        record_event(_Outbox, "x", {"i": 1})
        record_event(_Outbox, "x", {"i": 2})

    seen: list[dict] = []

    def _handler(row):
        seen.append(row.payload)
        return True

    relay = OutboxRelay(_Outbox, batch_size=10)
    n = relay.drain_once(_handler)
    assert n == 2
    assert sorted(p["i"] for p in seen) == [1, 2]
    assert _Outbox.objects.filter(status="published").count() == 2
    assert _Outbox.objects.filter(status="pending").count() == 0


def test_relay_handler_failure_increments_attempts():
    with transaction.atomic():
        record_event(_Outbox, "boom", {"i": 1})

    def _handler(row):
        raise RuntimeError("nope")

    relay = OutboxRelay(_Outbox, max_attempts=3)
    n = relay.drain_once(_handler)
    assert n == 0
    row = _Outbox.objects.get(event_type="boom")
    assert row.attempts == 1
    assert row.status == "pending"
    assert row.last_error is not None
    assert "RuntimeError" in row.last_error


def test_relay_dead_letter_after_max_attempts():
    with transaction.atomic():
        record_event(_Outbox, "deadletter", {})

    def _bad(_row):
        return False  # silent failure path

    relay = OutboxRelay(_Outbox, max_attempts=2)
    relay.drain_once(_bad)
    relay.drain_once(_bad)
    row = _Outbox.objects.get(event_type="deadletter")
    assert row.status == "dead"
    assert row.attempts == 2

    # Subsequent drains skip dead rows.
    n = relay.drain_once(_bad)
    assert n == 0
    row = _Outbox.objects.get(event_type="deadletter")
    assert row.attempts == 2  # unchanged


def test_relay_validation():
    with pytest.raises(ValueError):
        OutboxRelay(_Outbox, batch_size=0)
    with pytest.raises(ValueError):
        OutboxRelay(_Outbox, poll_interval_s=0)


def test_serialize_payload_is_deterministic():
    a = serialize_payload({"a": 1, "b": [1, 2]})
    b = serialize_payload({"b": [1, 2], "a": 1})
    assert a == b
