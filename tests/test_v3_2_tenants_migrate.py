"""Coverage for v3.2 per-tenant migration runner.

The runner depends on PostgreSQL ``search_path`` switching, so the
end-to-end paths skip on non-PG backends. Pure unit tests for the
helpers (validation, registry, error messages) run on every backend.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

import dorm
from dorm.contrib.tenants import (
    _registered_tenants,
    ensure_schema,
    migrate_all_tenants,
    migrate_tenant,
    register_tenant,
)


def _is_pg() -> bool:
    from dorm.db.connection import get_connection

    return getattr(get_connection(), "vendor", "sqlite") == "postgresql"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers refuse non-PG backends loudly
# ─────────────────────────────────────────────────────────────────────────────


def test_ensure_schema_rejects_non_pg_backend():
    if _is_pg():
        pytest.skip("This test targets the non-PG branch.")
    with pytest.raises(NotImplementedError, match="ensure_schema"):
        ensure_schema("tenant_a")


def test_migrate_tenant_rejects_non_pg_backend(tmp_path: Path):
    if _is_pg():
        pytest.skip("This test targets the non-PG branch.")
    with pytest.raises(NotImplementedError):
        migrate_tenant("tenant_a")


def test_ensure_schema_validates_name():
    """Schema names go straight into DDL, so the validator must reject
    anything that isn't a SQL identifier — even on PG, where the
    DDL would otherwise execute."""
    with pytest.raises(ValueError, match="Tenant schema name"):
        ensure_schema("drop schema public; --")


# ─────────────────────────────────────────────────────────────────────────────
# Tenant registry
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Snapshot + restore the tenant registry around every test in this
    module so cross-test pollution can't leak."""
    snapshot = set(_registered_tenants)
    yield
    _registered_tenants.clear()
    _registered_tenants.update(snapshot)


def test_register_tenant_rejects_invalid_name():
    with pytest.raises(ValueError, match="Tenant schema name"):
        register_tenant("9badstart")


def test_register_tenant_is_idempotent():
    register_tenant("tenant_alpha")
    register_tenant("tenant_alpha")
    register_tenant("tenant_alpha")
    from dorm.contrib.tenants import registered_tenants

    assert "tenant_alpha" in registered_tenants()


def test_migrate_all_tenants_returns_per_tenant_status():
    """Run against an empty registry — must return an empty dict (not
    raise) so a bare CI invocation behaves predictably."""
    _registered_tenants.clear()
    out = migrate_all_tenants()
    assert out == {}


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end on PostgreSQL — applies migrations against a real schema
# ─────────────────────────────────────────────────────────────────────────────


def _scaffold_app(tmp: Path, app_label: str) -> Path:
    """Same shape as the aexecutor tests: write a single-table app with
    one initial migration on disk."""
    pkg = tmp / app_label
    pkg.mkdir()
    (pkg / "__init__.py").touch()
    (pkg / "models.py").write_text(
        f"import dorm\n\n"
        f"class Widget(dorm.Model):\n"
        f"    label = dorm.CharField(max_length=40)\n"
        f"    class Meta:\n"
        f"        db_table = '{app_label}_widget'\n"
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
                    name='Widget',
                    fields=[
                        ('id', BigAutoField(primary_key=True)),
                        ('label', CharField(max_length=40)),
                    ],
                    options={{'db_table': '{app_label}_widget'}},
                ),
            ]
            """
        ).strip()
    )
    return pkg


@pytest.fixture
def pg_tenant_env(tmp_path: Path, monkeypatch):
    """PG-only fixture: write a temp app on disk, scrub any leftover
    schema between tests. Skips the test cleanly when not on PG."""
    if not _is_pg():
        pytest.skip("Per-tenant runner targets PostgreSQL only.")

    app_label = "v3_2_tenant_app"
    schema = "v3_2_tenant_test"
    pkg_dir = _scaffold_app(tmp_path, app_label)

    import sys
    sys.path.insert(0, str(tmp_path))

    # Wire INSTALLED_APPS so migrate_tenant's app loop sees us.
    from dorm.conf import settings

    saved_apps = list(settings.INSTALLED_APPS)
    settings.INSTALLED_APPS = saved_apps + [app_label]
    # Ensure model is in the registry.
    import importlib
    importlib.import_module(f"{app_label}.models")

    from dorm.db.connection import get_connection

    conn = get_connection()
    conn.execute_script(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

    yield schema, app_label

    conn.execute_script(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    settings.INSTALLED_APPS = saved_apps
    sys.path.remove(str(tmp_path))
    for mod in list(sys.modules):
        if mod.startswith(app_label):
            del sys.modules[mod]


def test_migrate_tenant_creates_schema_and_table(pg_tenant_env):
    schema, app_label = pg_tenant_env
    migrate_tenant(schema)

    from dorm.db.connection import get_connection

    conn = get_connection()
    rows = conn.execute(
        "SELECT schemaname FROM pg_tables "
        "WHERE schemaname = %s AND tablename = %s",
        [schema, f"{app_label}_widget"],
    )
    assert any(r["schemaname"] == schema for r in rows)


def test_migrate_all_tenants_processes_registry(pg_tenant_env):
    schema, _app_label = pg_tenant_env
    register_tenant(schema)
    out = migrate_all_tenants()
    assert out.get(schema) == "ok"


def test_migrate_all_tenants_summarises_failures(pg_tenant_env, monkeypatch):
    schema, _app_label = pg_tenant_env
    register_tenant(schema)

    # Force an error inside migrate_tenant so we exercise the
    # per-tenant exception capture branch.
    from dorm.contrib import tenants as mod

    def _boom(*_a, **_kw):
        raise RuntimeError("simulated tenant failure")

    monkeypatch.setattr(mod, "migrate_tenant", _boom)
    out = mod.migrate_all_tenants()
    assert out[schema].startswith("error: ")
    assert "simulated tenant failure" in out[schema]
