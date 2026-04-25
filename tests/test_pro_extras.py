"""Tests for the Tier-A/B/C "professional polish" features:

  - migrate --dry-run + executor.migrate(dry_run=True)
  - QuerySet.explain() / aexplain()
  - dorm sql <Model>
  - ArrayField (PG only)
  - Array / JSON-aware lookup names (array_contains, json_has_key, ...)
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import dorm
from tests.models import Author


REPO_ROOT = Path(__file__).resolve().parent.parent


def _run_cli(args: list[str], cwd: Path):
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + pp if pp else "")
    return subprocess.run(
        [sys.executable, "-m", "dorm", *args],
        cwd=str(cwd), env=env,
        capture_output=True, text=True, timeout=30,
    )


# ── A1: migrate --dry-run ─────────────────────────────────────────────────────


def test_migrate_dry_run_does_not_apply(tmp_path: Path):
    """``migrate --dry-run`` must print the SQL but NOT create the
    table — a second invocation without ``--dry-run`` finds the same
    pending migration ready to apply."""
    db_path = tmp_path / "dry.sqlite3"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["dryapp"]\n'
    )
    app = tmp_path / "dryapp"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class Widget(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    class Meta:\n"
        "        db_table = 'widgets'\n"
        "        app_label = 'dryapp'\n"
    )

    mk = _run_cli(["makemigrations"], cwd=tmp_path)
    assert mk.returncode == 0, mk.stdout + mk.stderr

    # Dry run prints SQL containing CREATE TABLE.
    dr = _run_cli(["migrate", "--dry-run"], cwd=tmp_path)
    assert dr.returncode == 0, dr.stdout + dr.stderr
    assert "CREATE TABLE" in dr.stdout
    assert "widgets" in dr.stdout

    # Real migrate still has the pending migration to apply.
    mg = _run_cli(["migrate"], cwd=tmp_path)
    assert mg.returncode == 0, mg.stdout + mg.stderr
    assert "Applying dryapp" in mg.stdout

    # And the second real run is a no-op.
    mg2 = _run_cli(["migrate"], cwd=tmp_path)
    assert "No migrations to apply" in mg2.stdout


def test_migrate_dry_run_returns_captured_sql_via_executor():
    """Programmatic API: ``executor.migrate(dry_run=True)`` returns the
    captured SQL list."""
    from dorm.migrations.executor import _DryRunConnection

    fake_real = type("X", (), {
        "vendor": "sqlite",
        "settings": {},
        "execute_script": lambda self, sql: None,
        "table_exists": lambda self, name: False,
    })()
    capturing = _DryRunConnection(fake_real)
    capturing.execute_script("CREATE TABLE foo (id INTEGER)")
    capturing.execute("INSERT INTO foo VALUES (1)")
    captured = [sql for sql, _ in capturing.captured]
    assert any("CREATE TABLE foo" in s for s in captured)
    assert any("INSERT INTO foo" in s for s in captured)


# ── A2: QuerySet.explain / aexplain ───────────────────────────────────────────


def test_explain_returns_query_plan_string():
    plan = Author.objects.filter(age__gte=18).explain()
    assert isinstance(plan, str)
    assert plan  # non-empty
    # Both backends mention the target table somewhere in the plan.
    assert "authors" in plan.lower() or "author" in plan.lower()


def test_explain_with_analyze_postgres_only():
    """analyze=True is meaningful only on PG; on SQLite it's accepted but
    ignored. Either way the call should succeed."""
    plan = Author.objects.filter(age__gte=18).explain(analyze=True)
    assert isinstance(plan, str)


async def test_aexplain_returns_plan():
    plan = await Author.objects.filter(age__gte=18).aexplain()
    assert isinstance(plan, str)
    assert plan


# ── B4: dorm sql <Model> ──────────────────────────────────────────────────────


def test_dorm_sql_prints_create_table(tmp_path: Path):
    db_path = tmp_path / "x.db"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["sqlapp"]\n'
    )
    app = tmp_path / "sqlapp"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class Widget(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    qty = dorm.IntegerField(default=0)\n"
        "    class Meta:\n"
        "        db_table = 'widgets'\n"
        "        app_label = 'sqlapp'\n"
    )

    res = _run_cli(["sql", "Widget"], cwd=tmp_path)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "CREATE TABLE" in res.stdout
    assert '"widgets"' in res.stdout
    assert '"name"' in res.stdout
    assert '"qty"' in res.stdout


def test_dorm_sql_all_dumps_every_model(tmp_path: Path):
    db_path = tmp_path / "x.db"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["multapp"]\n'
    )
    app = tmp_path / "multapp"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class A(dorm.Model):\n"
        "    name = dorm.CharField(max_length=10)\n"
        "    class Meta:\n"
        "        db_table = 't_a'\n"
        "        app_label = 'multapp'\n"
        "class B(dorm.Model):\n"
        "    name = dorm.CharField(max_length=10)\n"
        "    class Meta:\n"
        "        db_table = 't_b'\n"
        "        app_label = 'multapp'\n"
    )

    res = _run_cli(["sql", "--all"], cwd=tmp_path)
    assert res.returncode == 0, res.stdout + res.stderr
    assert '"t_a"' in res.stdout
    assert '"t_b"' in res.stdout


def test_dorm_sql_unknown_model_errors(tmp_path: Path):
    db_path = tmp_path / "x.db"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = []\n'
    )
    res = _run_cli(["sql", "DoesNotExist"], cwd=tmp_path)
    assert res.returncode != 0
    assert "not found" in res.stdout.lower() or "not found" in res.stderr.lower()


# ── C9: ArrayField ────────────────────────────────────────────────────────────


def test_array_field_to_python_normalizes_values():
    af = dorm.ArrayField(dorm.IntegerField())
    assert af.to_python(None) is None
    assert af.to_python([1, 2, 3]) == [1, 2, 3]
    assert af.to_python((1, 2)) == [1, 2]
    assert af.to_python(iter([1, 2])) == [1, 2]


def test_array_field_db_type_postgres_only():
    from dorm.db.connection import get_connection

    conn = get_connection()
    af = dorm.ArrayField(dorm.CharField(max_length=50))
    if conn.vendor == "postgresql":
        assert af.db_type(conn).endswith("[]")
        assert "VARCHAR" in af.db_type(conn).upper() or "TEXT" in af.db_type(conn).upper()
    else:
        with pytest.raises(NotImplementedError):
            af.db_type(conn)


def test_array_field_round_trip_postgres():
    """Smoke-test ArrayField against a real PG instance: create a model
    that uses it, push a row through the SQL builder, read back."""
    from dorm.db.connection import get_connection

    conn = get_connection()
    if conn.vendor != "postgresql":
        pytest.skip("ArrayField only works on PostgreSQL")

    class _Tagged(dorm.Model):
        name = dorm.CharField(max_length=50)
        tags = dorm.ArrayField(dorm.CharField(max_length=20), null=True)

        class Meta:
            db_table = "test_array_tagged"
            app_label = "tests"

    try:
        conn.execute_script('DROP TABLE IF EXISTS "test_array_tagged"')
    except Exception:
        pass
    conn.execute_script(
        'CREATE TABLE "test_array_tagged" ('
        '"id" SERIAL PRIMARY KEY, '
        '"name" VARCHAR(50) NOT NULL, '
        '"tags" VARCHAR(20)[]'
        ')'
    )
    try:
        obj = _Tagged.objects.create(name="post", tags=["python", "orm"])
        fetched = _Tagged.objects.get(pk=obj.pk)
        assert fetched.tags == ["python", "orm"]
    finally:
        conn.execute_script('DROP TABLE IF EXISTS "test_array_tagged"')


# ── New lookups (array_contains, json_has_key, ...) ───────────────────────────


def test_new_lookups_registered():
    """The new vendor-specific lookups land in the LOOKUPS table so
    parse_lookup_key recognizes them."""
    from dorm.lookups import VALID_LOOKUPS

    for name in (
        "array_contains",
        "array_overlap",
        "json_has_key",
        "json_has_any",
        "json_has_all",
    ):
        assert name in VALID_LOOKUPS


def test_array_contains_lookup_renders_pg_operator():
    from dorm.lookups import build_lookup_sql

    sql, params = build_lookup_sql('"tags"', "array_contains", ["python"])
    assert "@>" in sql
    assert params == [["python"]]


def test_json_has_key_lookup_renders_pg_operator():
    from dorm.lookups import build_lookup_sql

    sql, params = build_lookup_sql('"data"', "json_has_key", "user")
    assert "?" in sql
    assert params == ["user"]
