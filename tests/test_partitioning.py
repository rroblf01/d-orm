"""Tests for declarative partitioning ops.

Covers ``CreatePartitionedTable`` / ``CreatePartition`` /
``AttachPartition`` / ``DetachPartition``. PostgreSQL-only — non-PG
backends raise NotImplementedError when the op runs.
"""

from __future__ import annotations

import pytest

from dorm.db.connection import get_connection
from dorm.migrations.operations import (
    AttachPartition,
    CreatePartition,
    CreatePartitionedTable,
    DetachPartition,
)


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


def test_invalid_method_raises_at_construction():
    with pytest.raises(ValueError, match="RANGE"):
        CreatePartitionedTable(
            "events", columns_sql="id INT", method="INVALID", key="id"
        )


def test_partitioning_not_supported_on_sqlite(db_config):
    if _is_postgres(db_config):
        pytest.skip("test targets non-PG path")
    op = CreatePartitionedTable(
        "p_events",
        columns_sql="id BIGSERIAL, occurred_at TIMESTAMP NOT NULL",
        method="RANGE",
        key="occurred_at",
    )
    conn = get_connection()
    with pytest.raises(NotImplementedError):
        op.database_forwards("tests", conn, None, None)


def test_range_partitioning_end_to_end(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    # Clean up if previous test left state.
    conn.execute_script('DROP TABLE IF EXISTS "p_events_q1" CASCADE')
    conn.execute_script('DROP TABLE IF EXISTS "p_events_q2" CASCADE')
    conn.execute_script('DROP TABLE IF EXISTS "p_events" CASCADE')

    parent_op = CreatePartitionedTable(
        "p_events",
        columns_sql='id BIGSERIAL, occurred_at TIMESTAMP NOT NULL, payload TEXT, PRIMARY KEY (id, occurred_at)',
        method="RANGE",
        key="occurred_at",
    )
    parent_op.database_forwards("tests", conn, None, None)
    try:
        q1 = CreatePartition(
            parent="p_events",
            name="p_events_q1",
            for_values="FROM ('2025-01-01') TO ('2025-04-01')",
        )
        q1.database_forwards("tests", conn, None, None)

        q2 = CreatePartition(
            parent="p_events",
            name="p_events_q2",
            for_values="FROM ('2025-04-01') TO ('2025-07-01')",
        )
        q2.database_forwards("tests", conn, None, None)

        conn.execute_script(
            'INSERT INTO "p_events" (occurred_at, payload) '
            "VALUES ('2025-02-01', 'a'), ('2025-05-01', 'b')"
        )
        rows_q1 = conn.execute('SELECT payload FROM "p_events_q1"')
        rows_q2 = conn.execute('SELECT payload FROM "p_events_q2"')
        assert {r["payload"] for r in rows_q1} == {"a"}
        assert {r["payload"] for r in rows_q2} == {"b"}

        # Reverse: drop child via op.
        q1.database_backwards("tests", conn, None, None)
        rows = conn.execute(
            "SELECT 1 FROM pg_class WHERE relname = %s", ["p_events_q1"]
        )
        assert not rows
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "p_events_q2" CASCADE')
        parent_op.database_backwards("tests", conn, None, None)


def test_attach_detach_partition(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only")
    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "p_evt_solo" CASCADE')
    conn.execute_script('DROP TABLE IF EXISTS "p_evt" CASCADE')

    CreatePartitionedTable(
        "p_evt",
        columns_sql='id BIGSERIAL, region TEXT NOT NULL, PRIMARY KEY (id, region)',
        method="LIST",
        key="region",
    ).database_forwards("tests", conn, None, None)
    try:
        # Build a standalone table with a matching shape.
        conn.execute_script(
            'CREATE TABLE "p_evt_solo" (id BIGSERIAL, region TEXT NOT NULL, PRIMARY KEY (id, region))'
        )
        attach = AttachPartition(
            parent="p_evt", name="p_evt_solo", for_values="IN ('eu-west-1')"
        )
        attach.database_forwards("tests", conn, None, None)
        conn.execute_script(
            "INSERT INTO \"p_evt\" (region) VALUES ('eu-west-1')"
        )
        rows = conn.execute('SELECT region FROM "p_evt_solo"')
        assert rows and rows[0]["region"] == "eu-west-1"

        # Detach via dedicated op.
        detach = DetachPartition(
            parent="p_evt", name="p_evt_solo", for_values="IN ('eu-west-1')"
        )
        detach.database_forwards("tests", conn, None, None)
        rows = conn.execute(
            "SELECT 1 FROM pg_inherits WHERE inhrelid = 'p_evt_solo'::regclass"
        )
        assert not rows
        # Reverse re-attaches.
        detach.database_backwards("tests", conn, None, None)
        rows = conn.execute(
            "SELECT 1 FROM pg_inherits WHERE inhrelid = 'p_evt_solo'::regclass"
        )
        assert rows
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "p_evt_solo" CASCADE')
        conn.execute_script('DROP TABLE IF EXISTS "p_evt" CASCADE')
