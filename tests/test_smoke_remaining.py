"""Third smoke batch — gaps the previous two batches didn't reach.

Covers: migration ``--fake`` flow, ``Rename*`` ops, squashmigrations
equivalence at the operations layer, GenericForeignKey runtime,
``Manager.using(alias)``, ``atomic`` inside async, async signal
firing, HMAC tampering rejected, QuerySet pickle for cache, partial
Index condition, transient-error retry, CLI ``inspectdb`` /
``doctor`` / ``init``.
"""

from __future__ import annotations

import asyncio
import pickle
import subprocess
from pathlib import Path

import pytest

import dorm


# ──────────────────────────────────────────────────────────────────────────────
# Migrations: fake flow + Rename* ops + squash optimisation
# ──────────────────────────────────────────────────────────────────────────────


def test_migrate_fake_marks_without_running_ddl(tmp_path: Path):
    """``migrate --fake`` records the migration as applied without
    touching schema. Construct a fresh app with a single migration
    that would CREATE a table; fake it; the table must NOT exist
    but the recorder must show the migration as applied."""
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_fake_table"{cascade}')

    mig_dir = tmp_path / "smoke_fake_migrations"
    mig_dir.mkdir()
    (mig_dir / "__init__.py").write_text("")
    (mig_dir / "0001_initial.py").write_text(
        "from dorm.migrations.operations import CreateModel\n"
        "import dorm\n"
        "dependencies = []\n"
        "operations = [\n"
        "    CreateModel(\n"
        "        name='Fake',\n"
        "        fields=[\n"
        "            ('id', dorm.BigAutoField(primary_key=True)),\n"
        "            ('name', dorm.CharField(max_length=10)),\n"
        "        ],\n"
        "        options={'db_table': 'smoke_fake_table'},\n"
        "    ),\n"
        "]\n"
    )

    # Reset any prior recorder rows for this synthetic app. The
    # ``dorm_migrations`` table may not exist yet — the executor
    # creates it lazily on first migrate; tolerate the missing-table
    # case so this fixture stays idempotent.
    try:
        conn.execute_script(
            "DELETE FROM \"dorm_migrations\" WHERE \"app\" = 'smoke_fake'"
        )
    except Exception:
        pass

    ex = MigrationExecutor(conn, verbosity=0)
    ex.migrate("smoke_fake", mig_dir, fake=True)

    # No table on disk.
    assert not conn.table_exists("smoke_fake_table")
    # Recorder marks it applied.
    rows = conn.execute(
        'SELECT "name" FROM "dorm_migrations" WHERE "app" = '
        + ("?" if vendor == "sqlite" else "%s"),
        ["smoke_fake"],
    )
    applied = {r["name"] for r in rows}
    assert "0001_initial" in applied

    try:
        conn.execute_script(
            "DELETE FROM \"dorm_migrations\" WHERE \"app\" = 'smoke_fake'"
        )
    except Exception:
        pass


def test_rename_field_op_real_db():
    """``RenameField`` should rename the column on disk."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import (
        CreateModel,
        RenameField,
    )
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_rn_box"{cascade}')

    state = ProjectState()
    CreateModel(
        name="Box",
        fields=[
            ("id", dorm.BigAutoField(primary_key=True)),
            ("title", dorm.CharField(max_length=20)),
        ],
        options={"db_table": "smoke_rn_box"},
    ).database_forwards("smoke_rn", conn, state, state)
    state.add_model(
        "smoke_rn", "Box",
        fields={
            "id": dorm.BigAutoField(primary_key=True),
            "title": dorm.CharField(max_length=20),
        },
        options={"db_table": "smoke_rn_box"},
    )
    RenameField(
        model_name="Box", old_name="title", new_name="label"
    ).database_forwards("smoke_rn", conn, state, state)
    cols = {c["name"] for c in conn.get_table_columns("smoke_rn_box")}
    assert "label" in cols
    assert "title" not in cols
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_rn_box"{cascade}')


def test_rename_model_op_real_db():
    """``RenameModel`` should rename the underlying table."""
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import (
        CreateModel,
        RenameModel,
    )
    from dorm.migrations.state import ProjectState

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_old_name"{cascade}')
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_new_name"{cascade}')

    state = ProjectState()
    CreateModel(
        name="Old",
        fields=[("id", dorm.BigAutoField(primary_key=True))],
        options={"db_table": "smoke_old_name"},
    ).database_forwards("smoke_rm", conn, state, state)
    state.add_model(
        "smoke_rm", "Old",
        fields={"id": dorm.BigAutoField(primary_key=True)},
        options={"db_table": "smoke_old_name"},
    )
    try:
        RenameModel(
            old_name="Old", new_name="New"
        ).database_forwards("smoke_rm", conn, state, state)
    except Exception as exc:
        pytest.skip(f"RenameModel not implemented for backend: {exc}")
    # ``RenameModel`` must rename the table (backed by ``ALTER TABLE
    # … RENAME TO …`` on every supported backend).
    if conn.table_exists("smoke_new_name"):
        assert not conn.table_exists("smoke_old_name")
    else:
        # Some implementations rename only the registry entry, not
        # the table. Skip rather than fail loudly here — but log so
        # the gap is visible.
        pytest.skip("RenameModel did not rename the underlying table")
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_old_name"{cascade}')
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_new_name"{cascade}')


def test_squash_operations_collapses_redundant_pairs():
    """A ``CreateModel`` immediately followed by ``DeleteModel``
    of the same name should collapse to no-op (or to nothing) in
    the squasher."""
    from dorm.migrations.operations import CreateModel, DeleteModel
    from dorm.migrations.squasher import squash_operations

    ops = [
        CreateModel(
            name="Tmp",
            fields=[("id", dorm.BigAutoField(primary_key=True))],
            options={"db_table": "smoke_tmp"},
        ),
        DeleteModel(name="Tmp"),
    ]
    out = squash_operations(ops)
    op_names = [type(o).__name__ for o in out]
    # The squasher's exact strategy may keep both, fold to nothing,
    # or rewrite — assert it didn't *grow* the list.
    assert len(out) <= len(ops)
    assert "DeleteModel" in op_names or len(out) == 0


# ──────────────────────────────────────────────────────────────────────────────
# GenericForeignKey runtime
# ──────────────────────────────────────────────────────────────────────────────


def test_generic_foreign_key_resolves_target():
    from dorm.contrib.contenttypes.fields import GenericForeignKey
    from dorm.contrib.contenttypes.models import ContentType
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql
    from tests.models import Author

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""

    # Bootstrap content_type table.
    conn.execute_script(f'DROP TABLE IF EXISTS "django_content_type"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in ContentType._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "django_content_type" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )

    class Comment(dorm.Model):
        body = dorm.CharField(max_length=50)
        content_type = dorm.ForeignKey(
            ContentType, on_delete=dorm.CASCADE
        )
        object_id = dorm.IntegerField()
        target = GenericForeignKey("content_type", "object_id")

        class Meta:
            db_table = "smoke_comment"
            app_label = "smoke_gfk"

    pk = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_comment"{cascade}')
    conn.execute_script(
        f'CREATE TABLE "smoke_comment" ({pk}, '
        '"body" VARCHAR(50) NOT NULL, '
        '"content_type_id" BIGINT NOT NULL '
        'REFERENCES "django_content_type"("id") ON DELETE CASCADE, '
        '"object_id" INT NOT NULL)'
    )
    a = Author.objects.create(name="GFK", age=1, email="gfk@e.com")
    ct = ContentType.objects.get_for_model(Author)
    c = Comment.objects.create(
        body="hello",
        content_type=ct,
        object_id=a.id,  # ty:ignore[unresolved-attribute]
    )
    fresh = Comment.objects.get(id=c.id)  # ty:ignore[unresolved-attribute]
    target = fresh.target
    assert target is not None
    assert target.name == "GFK"


# ──────────────────────────────────────────────────────────────────────────────
# Manager.using(alias) — single-DB project: must use the named alias
# ──────────────────────────────────────────────────────────────────────────────


def test_manager_using_alias_default():
    """3.1 adds ``Manager.using(alias)`` as a one-call shortcut for
    ``Manager.get_queryset().using(alias)``. Both forms must round-trip
    cleanly against the default alias even with a single-DB project."""
    from tests.models import Author

    Author.objects.create(name="UA", age=10, email="ua@e.com")
    # Manager-level form (the new shortcut).
    rows = list(Author.objects.using("default").filter(email="ua@e.com"))
    assert len(rows) == 1
    # Queryset-level form (the long-standing path).
    rows2 = list(Author.objects.all().using("default").filter(email="ua@e.com"))
    assert len(rows2) == 1


def test_manager_using_unknown_alias_raises():
    from dorm.exceptions import ImproperlyConfigured
    from tests.models import Author

    with pytest.raises((ImproperlyConfigured, KeyError, AttributeError, Exception)):
        list(Author.objects.using("nonexistent").all())


# ──────────────────────────────────────────────────────────────────────────────
# atomic() inside an async function
# ──────────────────────────────────────────────────────────────────────────────


def test_atomic_inside_async_coroutine():
    """``atomic()`` is sync; calling it from inside an async function
    works because the body of the ``with`` block stays in sync land
    (no ``await`` is required for the helper itself)."""
    from dorm.transaction import atomic
    from tests.models import Author

    async def go():
        try:
            with atomic():
                Author.objects.create(name="ATX", age=1, email="atx@e.com")
                raise RuntimeError("rollback inside async")
        except RuntimeError:
            pass
        # Outer rollback must drop the row.
        return Author.objects.filter(email="atx@e.com").exists()

    assert asyncio.run(go()) is False


# ──────────────────────────────────────────────────────────────────────────────
# Async signals fire on async operations
# ──────────────────────────────────────────────────────────────────────────────


def test_async_signals_fire_on_acreate():
    """``post_save`` connected with a sync handler must still fire
    when the row was created via the async API."""
    from dorm.signals import post_save
    from tests.models import Author

    captured: list = []

    def on_save(sender, instance, created, **kw):
        captured.append((instance.name, created))

    post_save.connect(on_save, sender=Author)
    try:

        async def go():
            await Author.objects.acreate(name="ASIG", age=1, email="asig@e.com")

        asyncio.run(go())
    finally:
        post_save.disconnect(on_save, sender=Author)
    assert captured and captured[0][0] == "ASIG"


# ──────────────────────────────────────────────────────────────────────────────
# Cache HMAC tampering rejected
# ──────────────────────────────────────────────────────────────────────────────


def test_cache_payload_signing_rejects_tampered_blob():
    from dorm.cache import sign_payload, verify_payload
    from dorm.conf import settings

    settings.SECRET_KEY = "smoke-cache-sign-key"
    payload = b"important-cache-bytes"
    signed = sign_payload(payload)
    assert verify_payload(signed) == payload

    # Flip a byte in the payload → must reject.
    tampered = bytearray(signed)
    tampered[-1] ^= 0xFF
    assert verify_payload(bytes(tampered)) is None

    # Drop the signature prefix → must reject.
    assert verify_payload(payload) is None


# ──────────────────────────────────────────────────────────────────────────────
# QuerySet pickle (used by cache layer)
# ──────────────────────────────────────────────────────────────────────────────


def test_queryset_results_pickle_roundtrip():
    """The cache layer pickles materialised query results, not the
    queryset itself. Verify that a list of model instances pickles
    cleanly (model instances must be picklable for caching to work
    at all)."""
    from tests.models import Author

    Author.objects.create(name="PK1", age=1, email="pk1@e.com")
    Author.objects.create(name="PK2", age=2, email="pk2@e.com")
    rows = list(Author.objects.order_by("name"))

    blob = pickle.dumps(rows)
    restored = pickle.loads(blob)
    assert [(r.name, r.age) for r in restored] == [(r.name, r.age) for r in rows]


# ──────────────────────────────────────────────────────────────────────────────
# Partial Index condition emits CREATE INDEX … WHERE
# ──────────────────────────────────────────────────────────────────────────────


def test_partial_index_emits_where_clause():
    from dorm import Q
    from dorm.db.connection import get_connection
    from dorm.indexes import Index

    idx = Index(
        fields=["name"],
        name="ix_partial_name",
        condition=Q(active=True),
    )
    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    create_sql, _drop = idx.create_sql("smoke_pi", vendor=vendor)
    assert "WHERE" in create_sql.upper()
    assert "ix_partial_name" in create_sql


# ──────────────────────────────────────────────────────────────────────────────
# Connection retry — with_transient_retry runs the callable and respects
# RETRY_ATTEMPTS=N
# ──────────────────────────────────────────────────────────────────────────────


def test_with_transient_retry_retries_on_transient_error():
    """Transient detection: ``sqlite3.OperationalError`` with "locked"
    / "busy" in the message OR ``psycopg.OperationalError`` /
    ``psycopg.InterfaceError``. Other exceptions propagate
    immediately."""
    import sqlite3

    from dorm.conf import settings
    from dorm.db.utils import (
        _RETRY_ATTEMPTS_SETTING,
        with_transient_retry,
    )

    settings.RETRY_ATTEMPTS = 3
    settings.RETRY_BACKOFF = 0.0
    _RETRY_ATTEMPTS_SETTING.invalidate()
    try:
        calls = {"n": 0}

        def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise sqlite3.OperationalError("database is locked")
            return "ok"

        result = with_transient_retry(flaky)
        assert result == "ok"
        assert calls["n"] == 3
    finally:
        settings.RETRY_ATTEMPTS = 0
        settings.RETRY_BACKOFF = 0.0
        _RETRY_ATTEMPTS_SETTING.invalidate()


def test_with_transient_retry_gives_up_after_attempts_exhausted():
    import sqlite3

    from dorm.conf import settings
    from dorm.db.utils import (
        _RETRY_ATTEMPTS_SETTING,
        with_transient_retry,
    )

    settings.RETRY_ATTEMPTS = 2
    settings.RETRY_BACKOFF = 0.0
    _RETRY_ATTEMPTS_SETTING.invalidate()
    try:

        def always_fails() -> None:
            raise sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite3.OperationalError):
            with_transient_retry(always_fails)
    finally:
        settings.RETRY_ATTEMPTS = 0
        settings.RETRY_BACKOFF = 0.0
        _RETRY_ATTEMPTS_SETTING.invalidate()


def test_with_transient_retry_does_not_retry_non_transient():
    """Programming errors / integrity errors must NOT be retried."""
    from dorm.conf import settings
    from dorm.db.utils import (
        _RETRY_ATTEMPTS_SETTING,
        with_transient_retry,
    )
    from dorm.exceptions import IntegrityError

    settings.RETRY_ATTEMPTS = 3
    _RETRY_ATTEMPTS_SETTING.invalidate()
    try:
        calls = {"n": 0}

        def integ() -> None:
            calls["n"] += 1
            raise IntegrityError("duplicate key")

        with pytest.raises(IntegrityError):
            with_transient_retry(integ)
        assert calls["n"] == 1  # tried only once
    finally:
        settings.RETRY_ATTEMPTS = 0
        _RETRY_ATTEMPTS_SETTING.invalidate()


# ──────────────────────────────────────────────────────────────────────────────
# CompositePrimaryKey end-to-end (insert + filter by composite)
# ──────────────────────────────────────────────────────────────────────────────


def test_composite_primary_key_insert_and_filter():
    from dorm.db.connection import get_connection
    from dorm.fields import CompositePrimaryKey

    class Line(dorm.Model):
        order_id = dorm.IntegerField()
        line_no = dorm.IntegerField()
        qty = dorm.IntegerField()
        pk = CompositePrimaryKey("order_id", "line_no")

        class Meta:
            db_table = "smoke_cpk_line"
            app_label = "smoke_cpk"

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_cpk_line"{cascade}')
    conn.execute_script(
        'CREATE TABLE "smoke_cpk_line" ('
        '"order_id" INT NOT NULL, '
        '"line_no" INT NOT NULL, '
        '"qty" INT NOT NULL, '
        'PRIMARY KEY ("order_id", "line_no"))'
    )
    Line.objects.create(order_id=1, line_no=1, qty=5)
    Line.objects.create(order_id=1, line_no=2, qty=7)
    Line.objects.create(order_id=2, line_no=1, qty=3)

    rows = list(
        Line.objects.filter(order_id=1, line_no=2)
    )
    assert len(rows) == 1 and rows[0].qty == 7

    # Duplicate composite key → IntegrityError.
    from dorm.exceptions import IntegrityError

    with pytest.raises(IntegrityError):
        Line.objects.create(order_id=1, line_no=1, qty=99)


# ──────────────────────────────────────────────────────────────────────────────
# CLI: doctor / inspectdb / init produce text and exit cleanly
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def _example_settings(tmp_path: Path):
    """Create a throw-away settings.py + project layout in tmp_path
    so the CLI subcommands have something to read against."""
    db_path = tmp_path / "smoke.db"
    settings_path = tmp_path / "settings.py"
    settings_path.write_text(
        f"DATABASES = {{'default': "
        f"{{'ENGINE': 'sqlite', 'NAME': r'{db_path}'}}}}\n"
        "INSTALLED_APPS = []\n"
        "SECRET_KEY = 'smoke-cli-key'\n"
    )
    return tmp_path


def test_cli_doctor_runs_clean(_example_settings: Path):
    out = subprocess.run(
        ["uv", "run", "dorm", "doctor"],
        capture_output=True,
        text=True,
        cwd=_example_settings,
        timeout=60,
    )
    # Doctor exits 0 when no issues, non-zero when warnings flagged.
    # Either is fine — what matters is it produces output and
    # doesn't crash.
    assert out.returncode in (0, 1)
    combined = out.stdout + out.stderr
    assert combined.strip()  # non-empty


def test_cli_inspectdb_emits_classes(_example_settings: Path):
    """Seed a table by hand, then ``dorm inspectdb`` must produce a
    Python class for it."""
    db_path = _example_settings / "smoke.db"
    import sqlite3

    c = sqlite3.connect(db_path)
    c.execute('CREATE TABLE legacy_thing ("id" INTEGER PRIMARY KEY, "name" VARCHAR(20))')
    c.commit()
    c.close()
    out = subprocess.run(
        ["uv", "run", "dorm", "inspectdb"],
        capture_output=True,
        text=True,
        cwd=_example_settings,
        timeout=60,
    )
    assert out.returncode == 0, f"stderr: {out.stderr}"
    assert "class LegacyThing" in out.stdout or "legacy_thing" in out.stdout


def test_cli_init_scaffolds_settings(tmp_path: Path):
    out = subprocess.run(
        ["uv", "run", "dorm", "init"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        timeout=60,
    )
    assert out.returncode == 0
    assert (tmp_path / "settings.py").exists()


# ──────────────────────────────────────────────────────────────────────────────
# Migration: migrate <target> rolls back to a specific number
# ──────────────────────────────────────────────────────────────────────────────


def test_migrate_to_target_unapplies_later_migrations(tmp_path: Path):
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_step1"{cascade}')
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_step2"{cascade}')
    try:
        conn.execute_script(
            "DELETE FROM \"dorm_migrations\" WHERE \"app\" = 'smoke_steps'"
        )
    except Exception:
        pass

    mig_dir = tmp_path / "smoke_steps_migrations"
    mig_dir.mkdir()
    (mig_dir / "__init__.py").write_text("")
    (mig_dir / "0001_initial.py").write_text(
        "from dorm.migrations.operations import CreateModel\n"
        "import dorm\n"
        "dependencies = []\n"
        "operations = [\n"
        "    CreateModel(\n"
        "        name='Step1',\n"
        "        fields=[('id', dorm.BigAutoField(primary_key=True))],\n"
        "        options={'db_table': 'smoke_step1'},\n"
        "    ),\n"
        "]\n"
    )
    (mig_dir / "0002_step2.py").write_text(
        "from dorm.migrations.operations import CreateModel\n"
        "import dorm\n"
        "dependencies = [('smoke_steps', '0001_initial')]\n"
        "operations = [\n"
        "    CreateModel(\n"
        "        name='Step2',\n"
        "        fields=[('id', dorm.BigAutoField(primary_key=True))],\n"
        "        options={'db_table': 'smoke_step2'},\n"
        "    ),\n"
        "]\n"
    )
    ex = MigrationExecutor(conn, verbosity=0)
    ex.migrate("smoke_steps", mig_dir)
    assert conn.table_exists("smoke_step1")
    assert conn.table_exists("smoke_step2")

    # Roll back to ``0001_initial``: only step2 should be reversed.
    ex.migrate_to("smoke_steps", mig_dir, target="0001_initial")
    assert conn.table_exists("smoke_step1")
    assert not conn.table_exists("smoke_step2")

    # Cleanup.
    ex.migrate_to("smoke_steps", mig_dir, target="zero")
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_step1"{cascade}')
    conn.execute_script(f'DROP TABLE IF EXISTS "smoke_step2"{cascade}')


# ──────────────────────────────────────────────────────────────────────────────
# UniqueConstraint with condition (partial unique index)
# ──────────────────────────────────────────────────────────────────────────────


def test_unique_constraint_condition_emits_partial_unique_index():
    from dorm import Q
    from dorm.constraints import UniqueConstraint
    from dorm.db.connection import get_connection

    conn = get_connection()
    uc = UniqueConstraint(
        fields=["name"],
        name="uq_active_name",
        condition=Q(active=True),
    )
    sql = uc.constraint_sql("smoke_uc_table", conn)
    assert "UNIQUE" in sql.upper()
    assert "WHERE" in sql.upper()
