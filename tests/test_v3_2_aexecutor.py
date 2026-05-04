"""Coverage for v3.2 :class:`AsyncMigrationExecutor`.

Async-friendly façade over the sync executor. Verifies every method
delegates correctly via :func:`asyncio.to_thread` and produces the
same observable behaviour as the sync path.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from dorm.db.connection import get_connection
from dorm.migrations.aexecutor import AsyncMigrationExecutor
from dorm.migrations.executor import MigrationExecutor


def _scaffold_app(tmp: Path, app_label: str = "shop") -> Path:
    """Write an INSTALLED_APPS-style package with a single migration on
    disk that creates one table. Returns the migrations directory."""
    pkg = tmp / app_label
    pkg.mkdir()
    (pkg / "__init__.py").touch()
    (pkg / "models.py").write_text(
        "import dorm\n\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n"
        f"    class Meta:\n"
        f"        db_table = '{app_label}_product'\n"
        f"        app_label = '{app_label}'\n"
    )
    mig = pkg / "migrations"
    mig.mkdir()
    (mig / "__init__.py").touch()
    (mig / "0001_initial.py").write_text(
        textwrap.dedent(
            f"""
            from dorm.migrations.operations import CreateModel
            from dorm.fields import BigAutoField, CharField

            dependencies = []

            operations = [
                CreateModel(
                    name='Product',
                    fields=[
                        ('id', BigAutoField(primary_key=True)),
                        ('name', CharField(max_length=80)),
                    ],
                    options={{'db_table': '{app_label}_product'}},
                ),
            ]
            """
        ).strip()
    )
    return mig


@pytest.fixture
def aexec_env(tmp_path: Path):
    """Per-test sandbox: tmp dir holds the app + migrations. Tear down
    any tables we created against the session-wide DB so the next test
    starts clean."""
    app = "v3_2_aexec"
    mig_dir = _scaffold_app(tmp_path, app_label=app)
    table = f"{app}_product"
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""

    yield mig_dir, app

    conn.execute_script(f'DROP TABLE IF EXISTS "{table}"{cascade}')
    # Wipe recorder rows for this app so re-runs see "pending".
    try:
        conn.execute_write(
            'DELETE FROM "dorm_migrations" WHERE "app" = %s', [app]
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Construction + property forwarding
# ─────────────────────────────────────────────────────────────────────────────


def test_aexecutor_exposes_inner_loader_recorder():
    conn = get_connection()
    aexec = AsyncMigrationExecutor(conn)
    assert aexec.connection is conn
    # The wrapper should not allocate a parallel loader / recorder
    # — it shares the inner sync executor's state so an outer
    # ``MigrationExecutor`` instance could see the same applied set.
    assert aexec.loader is aexec._inner.loader
    assert aexec.recorder is aexec._inner.recorder


# ─────────────────────────────────────────────────────────────────────────────
# amigrate — happy path
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_amigrate_applies_initial_migration(aexec_env):
    mig_dir, app = aexec_env
    aexec = AsyncMigrationExecutor(get_connection())
    await aexec.amigrate(app, mig_dir)

    conn = get_connection()
    rows = conn.execute(
        'SELECT "name" FROM "dorm_migrations" WHERE "app" = %s', [app]
    )
    names = [r["name"] for r in rows]
    assert "0001_initial" in names


@pytest.mark.asyncio
async def test_amigrate_dry_run_returns_sql_without_applying(aexec_env):
    """``dry_run=True`` should NOT apply the migration — recorder stays
    empty and the captured SQL list comes back from the await."""
    mig_dir, app = aexec_env
    aexec = AsyncMigrationExecutor(get_connection())
    captured = await aexec.amigrate(app, mig_dir, dry_run=True)

    assert captured is not None
    # Each entry is (sql, params) — at least one CREATE TABLE statement.
    joined = " ".join(sql for sql, _params in captured)
    assert "CREATE TABLE" in joined.upper()

    conn = get_connection()
    rows = conn.execute(
        'SELECT "name" FROM "dorm_migrations" WHERE "app" = %s', [app]
    )
    assert list(rows) == []


# ─────────────────────────────────────────────────────────────────────────────
# Equivalence with the sync executor
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_amigrate_observable_equivalent_to_sync_migrate(aexec_env):
    """Apply via the async wrapper, then load via the sync executor —
    the recorder must agree on what's applied."""
    mig_dir, app = aexec_env
    aexec = AsyncMigrationExecutor(get_connection())
    await aexec.amigrate(app, mig_dir)

    sync_exec = MigrationExecutor(get_connection())
    sync_exec.loader.load(mig_dir, app)
    sync_exec.loader.load_applied(sync_exec.recorder)
    assert (app, "0001_initial") in sync_exec.loader.applied


# ─────────────────────────────────────────────────────────────────────────────
# arollback round-trip
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_arollback_undoes_a_prior_amigrate(aexec_env):
    mig_dir, app = aexec_env
    conn = get_connection()
    aexec = AsyncMigrationExecutor(conn)
    await aexec.amigrate(app, mig_dir)

    rows_before = conn.execute(
        'SELECT "name" FROM "dorm_migrations" WHERE "app" = %s', [app]
    )
    assert any(r["name"] == "0001_initial" for r in rows_before)

    await aexec.arollback(app, mig_dir, "zero")

    rows_after = conn.execute(
        'SELECT "name" FROM "dorm_migrations" WHERE "app" = %s', [app]
    )
    assert list(rows_after) == []
