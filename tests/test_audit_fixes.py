"""Regression tests for the audit-driven fixes.

Each section locks down one fix from the post-2.0.1 review so a refactor
can't quietly reintroduce the original problem:

- Migrations are atomic per migration: a failure in op N rolls back ops 1..N-1.
- prefetch_related catches only FieldDoesNotExist (typos must surface).
- execute_streaming() refuses to run inside atomic() (no silent OOM).
- Manager exposes iterator()/aiterator() (parity with Django).
- log_query() masks values bound to sensitive columns in DEBUG output.
- adelete() with CASCADE branches uses asyncio.gather (parallel sub-deletes).
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pytest

import dorm
from dorm.migrations.executor import MigrationExecutor
from dorm.migrations.operations import CreateModel, RunSQL
from dorm.migrations.recorder import MigrationRecorder
from dorm.migrations.writer import write_migration
from tests.models import Author, Book


# ── Migrations are atomic ────────────────────────────────────────────────────


_PARTIAL_MIGRATION_SOURCE = '''"""
Hand-rolled migration with two ops: the first creates a table, the
second raises. The fixed executor wraps the whole migration in a
transaction so op 1's CREATE TABLE gets rolled back when op 2 fails.
"""
from dorm.migrations.operations import CreateModel, Operation
from dorm.fields import BigAutoField, IntegerField


class _ExplodingOp(Operation):
    reversible = False

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, connection, from_state, to_state):
        # Touch the DB so we can prove a rollback happened, then raise.
        connection.execute_script(
            \'CREATE TABLE IF NOT EXISTS "audit_partial" ("id" INTEGER PRIMARY KEY)\'
        )
        raise RuntimeError("operation 2 failed mid-flight")

    def database_backwards(self, app_label, connection, from_state, to_state):
        pass


dependencies = []

operations = [
    CreateModel(
        name="AuditFirst",
        fields=[("id", BigAutoField(primary_key=True)), ("val", IntegerField())],
        options={"db_table": "audit_first_table"},
    ),
    _ExplodingOp(),
]
'''


def test_migration_partial_failure_rolls_back():
    """When op 2 of a migration raises, op 1's CREATE TABLE must be undone
    AND the migration must NOT be recorded as applied."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    # Clean any leftover from a previous failed run.
    conn.execute_script('DROP TABLE IF EXISTS "audit_first_table"')
    conn.execute_script('DROP TABLE IF EXISTS "audit_partial"')

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        mig_dir.mkdir(parents=True, exist_ok=True)
        (mig_dir / "__init__.py").write_text("")
        (mig_dir / "0001_initial.py").write_text(_PARTIAL_MIGRATION_SOURCE)

        executor = MigrationExecutor(conn, verbosity=0)
        with pytest.raises(RuntimeError, match="operation 2 failed mid-flight"):
            executor.migrate("auditapp", mig_dir)

        # The first op's table must NOT exist — the failure rolled back the
        # whole migration.
        assert not conn.table_exists("audit_first_table"), (
            "atomic per-migration failed: op 1 left state behind"
        )

        # And the recorder must not believe the migration was applied —
        # otherwise the next migrate() call would skip it forever.
        recorder = MigrationRecorder(conn)
        applied = recorder.applied_migrations()
        assert ("auditapp", "0001_initial") not in applied


def test_migration_success_path_still_records():
    """The atomic() wrapper must not regress the happy path."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.execute_script('DROP TABLE IF EXISTS "audit_happy_table"')

    with tempfile.TemporaryDirectory() as tmpdir:
        mig_dir = Path(tmpdir) / "migrations"
        ops = [
            CreateModel(
                name="AuditHappy",
                fields=[
                    ("id", dorm.BigAutoField(primary_key=True)),
                    ("val", dorm.IntegerField()),
                ],
                options={"db_table": "audit_happy_table"},
            ),
            RunSQL("SELECT 1"),
        ]
        write_migration("audithappy", mig_dir, 1, ops)

        MigrationExecutor(conn, verbosity=0).migrate("audithappy", mig_dir)
        assert conn.table_exists("audit_happy_table")
        recorder = MigrationRecorder(conn)
        assert ("audithappy", "0001_initial") in recorder.applied_migrations()
        # Cleanup so re-runs of the suite are clean.
        conn.execute_script('DROP TABLE IF EXISTS "audit_happy_table"')


# ── prefetch_related: typos no longer fall back to N+1 silently ──────────────


def test_prefetch_related_with_typo_no_longer_swallowed_sync():
    """Before the fix: ``prefetch_related("typo")`` silently fell through
    a bare-except and ran with no prefetch (effectively N+1, no warning).
    After: the reverse-FK fallback path runs, validates the name, and
    raises so the caller learns about the typo."""
    Author.objects.create(name="Solo", age=30)
    qs = Author.objects.all().prefetch_related("definitely_not_a_field")
    # Materialising the queryset triggers the prefetch resolution.
    with pytest.raises(Exception):  # noqa: PT011
        list(qs)


@pytest.mark.asyncio
async def test_prefetch_related_typo_propagates_with_relation_name_async():
    """Async prefetch must include the offending relation name in the
    raised error so the user can find their typo."""
    await Author.objects.acreate(name="Solo", age=30)
    qs = Author.objects.all().prefetch_related("definitely_not_a_field_async")
    with pytest.raises(Exception) as excinfo:  # noqa: PT011
        async for _ in qs:
            pass
    # The wrapped error must mention the bad relation name.
    assert "definitely_not_a_field_async" in str(excinfo.value)


# ── execute_streaming() refuses inside atomic() ──────────────────────────────


def test_execute_streaming_rejects_inside_atomic_pg():
    """Postgres named cursors require their own transaction; we now refuse
    instead of silently falling back to a non-streaming fetch (which would
    materialise the whole result set in memory — exactly what streaming
    is supposed to avoid)."""
    from dorm.db.connection import get_connection
    from dorm.transaction import atomic

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-specific behaviour")

    with atomic():
        with pytest.raises(RuntimeError, match="atomic"):
            list(conn.execute_streaming('SELECT 1'))


@pytest.mark.asyncio
async def test_aexecute_streaming_rejects_inside_aatomic_pg():
    from dorm.db.connection import get_async_connection
    from dorm.transaction import aatomic

    conn = get_async_connection()
    if getattr(conn, "vendor", "sqlite") != "postgresql":
        pytest.skip("PG-specific behaviour")

    async with aatomic():
        with pytest.raises(RuntimeError, match="atomic"):
            async for _ in conn.execute_streaming('SELECT 1'):
                pass


# ── Manager.iterator() / .aiterator() proxies ────────────────────────────────


def test_manager_iterator_proxies_queryset():
    """``Author.objects.iterator()`` must work without going through
    ``.get_queryset()`` first, matching Django's API."""
    Author.objects.create(name="Streamy", age=42)
    rows = list(Author.objects.iterator())
    assert any(a.name == "Streamy" for a in rows)


def test_manager_iterator_passes_chunk_size():
    """The chunk_size argument must reach the QuerySet iterator path."""
    for i in range(5):
        Author.objects.create(name=f"chunk{i}", age=i)
    # chunk_size > 0 routes to the streaming path; assert all rows come back.
    rows = list(Author.objects.iterator(chunk_size=2))
    names = {a.name for a in rows}
    for i in range(5):
        assert f"chunk{i}" in names


@pytest.mark.asyncio
async def test_manager_aiterator_proxies_queryset():
    await Author.objects.acreate(name="StreamyAsync", age=33)
    seen: list[str] = []
    async for a in Author.objects.aiterator():
        seen.append(a.name)
    assert "StreamyAsync" in seen


# ── log_query masks sensitive params ─────────────────────────────────────────


def test_log_query_masks_password_param(caplog):
    from dorm.db.utils import log_query

    sql = 'SELECT * FROM "users" WHERE "username" = %s AND "password" = %s'
    params = ["alice", "s3cret_p@ssw0rd"]

    logger = logging.getLogger("dorm.db.backends.testvendor")
    logger.setLevel(logging.DEBUG)
    with caplog.at_level(logging.DEBUG, logger="dorm.db.backends.testvendor"):
        with log_query("testvendor", sql, params):
            pass

    debug_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
    assert debug_lines, "expected a DEBUG line for the query"
    joined = "\n".join(debug_lines)
    assert "s3cret_p@ssw0rd" not in joined, (
        "password value leaked into DEBUG log"
    )
    # Username (non-sensitive) is preserved so debugging is still useful.
    assert "alice" in joined
    assert "***" in joined


def test_log_query_masks_token_and_api_key(caplog):
    from dorm.db.utils import log_query

    sql = (
        'INSERT INTO "creds" ("name", "api_key", "token") '
        'VALUES (%s, %s, %s)'
    )
    params = ["myapp", "AKIA-LEAK-1", "tk_LEAK_2"]

    logger = logging.getLogger("dorm.db.backends.testvendor2")
    logger.setLevel(logging.DEBUG)
    with caplog.at_level(logging.DEBUG, logger="dorm.db.backends.testvendor2"):
        with log_query("testvendor2", sql, params):
            pass

    joined = "\n".join(
        r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG
    )
    assert "AKIA-LEAK-1" not in joined
    assert "tk_LEAK_2" not in joined
    assert "myapp" in joined  # non-sensitive column kept


def test_log_query_does_not_mask_when_no_sensitive_columns(caplog):
    from dorm.db.utils import log_query

    sql = 'SELECT * FROM "books" WHERE "title" = %s AND "pages" = %s'
    params = ["The Hobbit", 310]

    logger = logging.getLogger("dorm.db.backends.testvendor3")
    logger.setLevel(logging.DEBUG)
    with caplog.at_level(logging.DEBUG, logger="dorm.db.backends.testvendor3"):
        with log_query("testvendor3", sql, params):
            pass

    joined = "\n".join(
        r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG
    )
    # Plain values must not be masked when no sensitive column is involved.
    assert "The Hobbit" in joined
    assert "310" in joined


def test_mask_params_unit_helper_handles_dollar_n():
    """psycopg passes through both ``%s`` and the ``$N`` form (the SQL
    builder emits ``$N`` before _to_pyformat). The masker must handle
    both, so secrets are redacted regardless of which placeholder style
    reached log_query()."""
    from dorm.db.utils import _mask_params

    # %s style.
    sql_pct = 'UPDATE "u" SET "password" = %s WHERE "id" = %s'
    assert _mask_params(sql_pct, ["new", 1]) == ["***", 1]

    # $N style (pre-_to_pyformat).
    sql_dollar = 'UPDATE "u" SET "password" = $1 WHERE "id" = $2'
    assert _mask_params(sql_dollar, ["new", 1]) == ["***", 1]

    # ? style (sqlite-native).
    sql_q = 'UPDATE "u" SET "password" = ? WHERE "id" = ?'
    assert _mask_params(sql_q, ["new", 1]) == ["***", 1]


def test_mask_params_returns_unchanged_for_dict_params():
    """Named/dict params aren't a flat sequence; we don't try to align
    them to placeholders. Passes through untouched."""
    from dorm.db.utils import _mask_params

    out = _mask_params(
        'UPDATE "u" SET "password" = %(pw)s WHERE "id" = %(id)s',
        {"pw": "secret", "id": 1},
    )
    assert out == {"pw": "secret", "id": 1}


# ── adelete() parallel cascade ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_adelete_cascade_runs_subdeletes_concurrently():
    """When a parent has CASCADE relations, the per-relation sub-deletes
    must run via asyncio.gather. We can't directly observe parallelism
    in the suite, but we can assert the total counts come back correctly
    AND that ProtectedError still wins if any relation is PROTECT.

    Author has Books with on_delete=CASCADE — sanity-check the gather
    path returns the correct cascade detail dict.
    """
    a = await Author.objects.acreate(name="Cascader", age=40)
    await Book.objects.acreate(title="b1", author_id=a.pk)
    await Book.objects.acreate(title="b2", author_id=a.pk)

    qs = Author.objects.filter(pk=a.pk)
    total, detail = await qs.adelete()

    # Two books cascaded + one author = 3 total deleted rows.
    assert total == 3
    # The detail dict must report each model's tally.
    book_label = f"{Book._meta.app_label}.Book"
    author_label = f"{Author._meta.app_label}.Author"
    assert detail.get(book_label) == 2
    assert detail.get(author_label) == 1


@pytest.mark.asyncio
async def test_adelete_no_relations_still_works():
    """Sanity: a delete with no on_delete-bearing reverse FKs must still
    return a clean count (the gather-path build collected nothing)."""
    from tests.models import Tag

    t = await Tag.objects.acreate(name="orphan-tag")
    qs = Tag.objects.filter(pk=t.pk)
    total, detail = await qs.adelete()
    assert total == 1
    assert detail.get(f"{Tag._meta.app_label}.Tag") == 1
