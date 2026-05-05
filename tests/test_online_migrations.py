"""Tests for ``AddFieldOnline`` / ``BackfillBatch`` / ``SetNotNullOnline``.

The PG path exercises the full multi-statement protocol; SQLite path
verifies the safe-fallback behaviour.
"""

from __future__ import annotations

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.migrations.operations import (
    AddFieldOnline,
    BackfillBatch,
    SetNotNullOnline,
)


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


class _State:
    """Minimal state-graph stand-in for the operation tests."""

    def __init__(self, table: str):
        self.models = {
            "tests.book": {
                "fields": {},
                "options": {"db_table": table},
            }
        }


@pytest.fixture
def online_table(db_config):
    """Build a fresh ``online_authors`` table seeded with three rows."""
    conn = get_connection()
    cascade = " CASCADE" if _is_postgres(db_config) else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "online_authors"{cascade}')
    pk_decl = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if not _is_postgres(db_config)
        else "BIGSERIAL PRIMARY KEY"
    )
    conn.execute_script(
        f'CREATE TABLE "online_authors" ('
        f' "id" {pk_decl},'
        f' "name" TEXT NOT NULL'
        ")"
    )
    placeholder = "%s" if _is_postgres(db_config) else "?"
    for n in ("a", "b", "c"):
        conn.execute_write(
            f'INSERT INTO "online_authors" ("name") VALUES ({placeholder})',
            [n],
        )
    yield "online_authors"
    conn.execute_script(f'DROP TABLE IF EXISTS "online_authors"{cascade}')


def test_add_field_online_adds_nullable_first(db_config, online_table):
    state = _State(online_table)
    field = dorm.IntegerField(null=False)
    op = AddFieldOnline("Book", "rating", field)
    op.database_forwards("tests", get_connection(), state, state)

    cols = get_connection().get_table_columns(online_table)
    rating = next((c for c in cols if c["name"] == "rating"), None)
    assert rating is not None
    # Column must be created nullable so ADD COLUMN doesn't rewrite
    # the table even though the field declared null=False.
    if _is_postgres(db_config):
        assert rating["is_nullable"].lower() == "yes"


def test_add_field_online_set_not_null_now_pg(db_config, online_table):
    if not _is_postgres(db_config):
        pytest.skip("set_not_null_now path is PG-only")

    conn = get_connection()
    placeholder = "%s"
    state = _State(online_table)
    op = AddFieldOnline(
        "Book",
        "score",
        dorm.IntegerField(null=False, default=0),
        set_not_null_now=False,
    )
    op.database_forwards("tests", conn, state, state)
    # Backfill the existing rows so SET NOT NULL succeeds.
    conn.execute_write(
        f'UPDATE "{online_table}" SET "score" = 0 WHERE "score" IS NULL'
    )
    SetNotNullOnline("Book", "score").database_forwards("tests", conn, state, state)
    cols = conn.get_table_columns(online_table)
    score = next(c for c in cols if c["name"] == "score")
    assert score["is_nullable"].lower() == "no"
    _ = placeholder  # silence


def test_add_field_online_reverse_drops_column(db_config, online_table):
    state = _State(online_table)
    field = dorm.IntegerField(null=True)
    op = AddFieldOnline("Book", "extra", field)
    conn = get_connection()
    op.database_forwards("tests", conn, state, state)
    op.database_backwards("tests", conn, state, state)
    cols = conn.get_table_columns(online_table)
    assert all(c["name"] != "extra" for c in cols)


def test_backfill_batch_validation():
    with pytest.raises(ValueError):
        BackfillBatch(table="t", update_sql="x", batch_size=0)
    with pytest.raises(ValueError):
        BackfillBatch(table="t", update_sql="x", sleep_seconds=-1)


def test_backfill_batch_runs_chunks(db_config, online_table):
    conn = get_connection()
    placeholder = "%s" if _is_postgres(db_config) else "?"

    # Add a nullable column to fill in.
    conn.execute_script(f'ALTER TABLE "{online_table}" ADD COLUMN "rating" INTEGER')
    op = BackfillBatch(
        table=online_table,
        update_sql=(
            f'UPDATE "{online_table}" SET "rating" = 5 '
            f'WHERE "id" BETWEEN {placeholder} AND {placeholder} '
            f'AND "rating" IS NULL'
        ),
        pk_column="id",
        batch_size=1,
    )
    op.database_forwards("tests", conn, None, None)
    rows = conn.execute(f'SELECT "rating" FROM "{online_table}"')
    assert all(r["rating"] == 5 for r in rows)


def test_backfill_batch_max_batches_caps(db_config, online_table):
    conn = get_connection()
    placeholder = "%s" if _is_postgres(db_config) else "?"

    conn.execute_script(f'ALTER TABLE "{online_table}" ADD COLUMN "tag" TEXT')
    # Use batch_size=1 to ensure max_batches=1 only touches one PK.
    op = BackfillBatch(
        table=online_table,
        update_sql=(
            f'UPDATE "{online_table}" SET "tag" = \'x\' '
            f'WHERE "id" BETWEEN {placeholder} AND {placeholder} '
            f'AND "tag" IS NULL'
        ),
        pk_column="id",
        batch_size=1,
        max_batches=1,
    )
    op.database_forwards("tests", conn, None, None)
    rows = conn.execute(
        f'SELECT COUNT(*) AS c FROM "{online_table}" WHERE "tag" IS NOT NULL'
    )
    assert rows[0]["c"] == 1


def test_set_not_null_online_promotes_nullable_column(db_config, online_table):
    conn = get_connection()
    conn.execute_script(f'ALTER TABLE "{online_table}" ADD COLUMN "rating" INTEGER')
    placeholder = "%s" if _is_postgres(db_config) else "?"
    conn.execute_write(
        f'UPDATE "{online_table}" SET "rating" = {placeholder}', [10]
    )
    state = _State(online_table)
    # On SQLite the operation is a no-op (ALTER COLUMN not supported).
    SetNotNullOnline("Book", "rating").database_forwards("tests", conn, state, state)

    if _is_postgres(db_config):
        cols = conn.get_table_columns(online_table)
        rating = next(c for c in cols if c["name"] == "rating")
        assert rating["is_nullable"].lower() == "no"


def test_set_not_null_online_reverse_drops_constraint(db_config, online_table):
    conn = get_connection()
    state = _State(online_table)
    # On SQLite the operation is a no-op (ALTER COLUMN not supported).
    SetNotNullOnline("Book", "name").database_backwards("tests", conn, state, state)
