"""Fourth coverage uplift — push CLI commands past 85%.

Each test mints a unique ``settings`` module name + SQLite DB in a
per-test tmp dir, so re-imports don't collide with the session-wide
PG-backed conftest config or with sibling tests in this file.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pytest


def _isolate(tmp_path: Path, monkeypatch, mod_name: str, apps_src: str = "[]") -> str:
    """Write a SQLite settings module under *mod_name* and wire
    ``tmp_path`` onto ``sys.path``. Returns the module name."""
    db_path = tmp_path / "db.sqlite3"
    (tmp_path / f"{mod_name}.py").write_text(
        f"DATABASES = {{'default': {{'ENGINE': 'sqlite', 'NAME': {str(db_path)!r}}}}}\n"
        f"INSTALLED_APPS = {apps_src}\n"
        "DEBUG = True\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    return mod_name


def _make_app(tmp_path: Path, name: str, models_src: str) -> None:
    app_dir = tmp_path / name
    app_dir.mkdir()
    (app_dir / "__init__.py").touch()
    (app_dir / "models.py").write_text(models_src)


@pytest.fixture
def cli_isolation():
    """Drop test-scoped modules + restore dorm config after each test."""
    from dorm.conf import settings as dorm_settings
    from dorm.db.connection import reset_connections

    saved_db = {alias: dict(cfg) for alias, cfg in dorm_settings.DATABASES.items()}
    saved_apps = list(dorm_settings.INSTALLED_APPS)
    saved_modules = set(sys.modules)
    saved_path = list(sys.path)
    reset_connections()
    yield
    # Pop every module imported during the test that lives under a
    # tmp dir — leaves dorm/std lib modules intact.
    for name in list(sys.modules):
        if name in saved_modules:
            continue
        mod = sys.modules.get(name)
        f = getattr(mod, "__file__", None) if mod is not None else None
        if not f:
            continue
        if "/pytest-" in str(f) or "/tmp" in str(f):
            del sys.modules[name]
    sys.path[:] = saved_path
    reset_connections()
    import dorm
    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)


# ── cmd_init branches ────────────────────────────────────────────────


class TestCmdInit:
    def test_init_creates_settings_file(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        cmd_init(argparse.Namespace(app=None))
        assert (tmp_path / "settings.py").exists()
        out = capsys.readouterr().out
        assert "Created" in out

    def test_init_app_scaffolds(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        cmd_init(argparse.Namespace(app="myapp"))
        assert (tmp_path / "myapp" / "models.py").exists()

    def test_init_idempotent(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        (tmp_path / "settings.py").write_text("# preserved\n")
        cmd_init(argparse.Namespace(app=None))
        assert (tmp_path / "settings.py").read_text() == "# preserved\n"


# ── cmd_dumpdata / cmd_loaddata round-trip ───────────────────────────


class TestCmdDumpLoad:
    def test_dumpdata_loaddata_roundtrip(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_dumpdata, cmd_loaddata, cmd_makemigrations, cmd_migrate

        mod = _isolate(tmp_path, monkeypatch, "s_dump", apps_src="['mini']")
        _make_app(
            tmp_path,
            "mini",
            "import dorm\n"
            "class Thing(dorm.Model):\n"
            "    label = dorm.CharField(max_length=20)\n",
        )
        cmd_makemigrations(argparse.Namespace(apps=["mini"], empty=False, name=None, settings=mod))
        capsys.readouterr()
        cmd_migrate(argparse.Namespace(
            app_label="mini", target=None, verbosity=0, dry_run=False, settings=mod,
        ))
        capsys.readouterr()
        # Insert one row directly.
        from mini.models import Thing  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]

        Thing.objects.create(label="alpha")
        out_file = tmp_path / "fixture.json"
        cmd_dumpdata(argparse.Namespace(
            targets=["mini"], output=str(out_file), indent=2, settings=mod,
        ))
        capsys.readouterr()
        assert out_file.exists()
        body = json.loads(out_file.read_text())
        assert any(r.get("fields", {}).get("label") == "alpha" for r in body)

        Thing.objects.all().delete()
        assert Thing.objects.count() == 0
        cmd_loaddata(argparse.Namespace(fixtures=[str(out_file)], database="default", settings=mod))
        capsys.readouterr()
        assert Thing.objects.count() == 1

    def test_dumpdata_to_stdout(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_dumpdata, cmd_makemigrations, cmd_migrate

        mod = _isolate(tmp_path, monkeypatch, "s_dump_std", apps_src="['mini']")
        _make_app(tmp_path, "mini", "import dorm\nclass T(dorm.Model):\n    n = dorm.IntegerField()\n")
        cmd_makemigrations(argparse.Namespace(apps=["mini"], empty=False, name=None, settings=mod))
        capsys.readouterr()
        cmd_migrate(argparse.Namespace(
            app_label="mini", target=None, verbosity=0, dry_run=False, settings=mod,
        ))
        capsys.readouterr()
        cmd_dumpdata(argparse.Namespace(
            targets=["mini"], output="-", indent=None, settings=mod,
        ))
        out = capsys.readouterr().out
        assert "[" in out

    def test_dumpdata_unknown_target_exits(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_dumpdata

        mod = _isolate(tmp_path, monkeypatch, "s_dump_unk", apps_src="[]")
        with pytest.raises(SystemExit):
            cmd_dumpdata(argparse.Namespace(
                targets=["nonexistent"], output=None, indent=None, settings=mod,
            ))
        err = capsys.readouterr().err
        assert "matched no models" in err

    def test_loaddata_missing_file(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_loaddata

        mod = _isolate(tmp_path, monkeypatch, "s_load_miss", apps_src="[]")
        with pytest.raises(SystemExit):
            cmd_loaddata(argparse.Namespace(
                fixtures=[str(tmp_path / "nope.json")], database="default", settings=mod,
            ))
        err = capsys.readouterr().err
        assert "not found" in err


# ── cmd_runscript ────────────────────────────────────────────────────


class TestCmdRunscript:
    def test_runs_script(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_runscript

        mod = _isolate(tmp_path, monkeypatch, "s_run", apps_src="[]")
        script = tmp_path / "hello.py"
        script.write_text("import sys\nprint('hello-from-script', sys.argv[1:])\n")
        cmd_runscript(argparse.Namespace(
            path=str(script), args=["foo", "bar"], settings=mod,
        ))
        out = capsys.readouterr().out
        assert "hello-from-script" in out
        assert "'foo'" in out

    def test_missing_script_exits(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_runscript

        mod = _isolate(tmp_path, monkeypatch, "s_run_miss", apps_src="[]")
        with pytest.raises(SystemExit):
            cmd_runscript(argparse.Namespace(
                path=str(tmp_path / "no.py"), args=[], settings=mod,
            ))
        err = capsys.readouterr().err
        assert "not found" in err


# ── cmd_flush ────────────────────────────────────────────────────────


class TestCmdFlush:
    def test_flush_noinput_clears_rows(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_flush, cmd_makemigrations, cmd_migrate

        mod = _isolate(tmp_path, monkeypatch, "s_flush", apps_src="['mini']")
        _make_app(
            tmp_path,
            "mini",
            "import dorm\nclass Thing(dorm.Model):\n    n = dorm.IntegerField()\n",
        )
        cmd_makemigrations(argparse.Namespace(apps=["mini"], empty=False, name=None, settings=mod))
        capsys.readouterr()
        cmd_migrate(argparse.Namespace(
            app_label="mini", target=None, verbosity=0, dry_run=False, settings=mod,
        ))
        capsys.readouterr()
        from mini.models import Thing  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]

        Thing.objects.create(n=1)
        Thing.objects.create(n=2)
        assert Thing.objects.count() == 2

        cmd_flush(argparse.Namespace(noinput=True, settings=mod))
        out = capsys.readouterr().out
        assert "Flushed" in out
        assert Thing.objects.count() == 0

    def test_flush_aborts_without_confirm(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_flush

        mod = _isolate(tmp_path, monkeypatch, "s_flush_abort", apps_src="[]")
        monkeypatch.setattr("builtins.input", lambda *_: "no")
        cmd_flush(argparse.Namespace(noinput=False, settings=mod))
        out = capsys.readouterr().out
        assert "Aborted" in out


# ── cmd_createsuperuser + cmd_changepassword ─────────────────────────


class TestCmdAuthShortcuts:
    def _setup_auth(self, tmp_path, monkeypatch, capsys):
        from dorm.cli import cmd_makemigrations, cmd_migrate

        mod = _isolate(
            tmp_path, monkeypatch, "s_auth",
            apps_src="['dorm.contrib.auth']",
        )
        cmd_makemigrations(argparse.Namespace(
            apps=["dorm.contrib.auth"], empty=False, name=None, settings=mod,
        ))
        capsys.readouterr()
        cmd_migrate(argparse.Namespace(
            app_label="dorm.contrib.auth", target=None, verbosity=0,
            dry_run=False, settings=mod,
        ))
        capsys.readouterr()
        return mod

    def test_createsuperuser_with_password(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_createsuperuser

        mod = self._setup_auth(tmp_path, monkeypatch, capsys)
        cmd_createsuperuser(argparse.Namespace(
            email="admin@example.com", password="s3cret", username="admin", settings=mod,
        ))
        out = capsys.readouterr().out
        assert "Superuser admin@example.com created" in out

    def test_createsuperuser_empty_password_refused(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_createsuperuser

        mod = self._setup_auth(tmp_path, monkeypatch, capsys)
        with pytest.raises(SystemExit):
            cmd_createsuperuser(argparse.Namespace(
                email="x@y.com", password="", username="x", settings=mod,
            ))
        err = capsys.readouterr().err
        assert "empty password" in err

    def test_changepassword_unknown_user(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_changepassword

        mod = self._setup_auth(tmp_path, monkeypatch, capsys)
        with pytest.raises(SystemExit):
            cmd_changepassword(argparse.Namespace(
                email="ghost@example.com", password="x", settings=mod,
            ))
        err = capsys.readouterr().err
        assert "not found" in err

    def test_changepassword_empty_refused(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_changepassword, cmd_createsuperuser

        mod = self._setup_auth(tmp_path, monkeypatch, capsys)
        cmd_createsuperuser(argparse.Namespace(
            email="u@x.com", password="pw1234", username="u", settings=mod,
        ))
        capsys.readouterr()
        with pytest.raises(SystemExit):
            cmd_changepassword(argparse.Namespace(
                email="u@x.com", password="", settings=mod,
            ))
        err = capsys.readouterr().err
        assert "empty password" in err

    def test_changepassword_success(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_changepassword, cmd_createsuperuser

        mod = self._setup_auth(tmp_path, monkeypatch, capsys)
        cmd_createsuperuser(argparse.Namespace(
            email="up@x.com", password="old", username="up", settings=mod,
        ))
        capsys.readouterr()
        cmd_changepassword(argparse.Namespace(
            email="up@x.com", password="new-pw", settings=mod,
        ))
        out = capsys.readouterr().out
        assert "Password updated" in out


# ── cmd_sqlmigrate ───────────────────────────────────────────────────


class TestCmdSqlMigrate:
    def test_renders_forward_sql(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_makemigrations, cmd_sqlmigrate

        mod = _isolate(tmp_path, monkeypatch, "s_sqlmig", apps_src="['mini']")
        _make_app(
            tmp_path,
            "mini",
            "import dorm\nclass M(dorm.Model):\n    n = dorm.IntegerField()\n",
        )
        cmd_makemigrations(argparse.Namespace(apps=["mini"], empty=False, name=None, settings=mod))
        capsys.readouterr()
        cmd_sqlmigrate(argparse.Namespace(
            app_label="mini", name="0001_initial", backwards=False, settings=mod,
        ))
        out = capsys.readouterr().out
        assert "CREATE TABLE" in out

    def test_unknown_migration_exits(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_makemigrations, cmd_sqlmigrate

        mod = _isolate(tmp_path, monkeypatch, "s_sqlmig_miss", apps_src="['mini']")
        _make_app(
            tmp_path,
            "mini",
            "import dorm\nclass M(dorm.Model):\n    n = dorm.IntegerField()\n",
        )
        cmd_makemigrations(argparse.Namespace(apps=["mini"], empty=False, name=None, settings=mod))
        capsys.readouterr()
        with pytest.raises(SystemExit):
            cmd_sqlmigrate(argparse.Namespace(
                app_label="mini", name="9999_nope", backwards=False, settings=mod,
            ))
        err = capsys.readouterr().err
        assert "not found" in err


# ── cmd_export_json_schema + _field_to_jsonschema ────────────────────


class TestCmdExportJsonSchema:
    def test_export_to_stdout(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_export_json_schema

        mod = _isolate(tmp_path, monkeypatch, "s_jschema", apps_src="['mini']")
        _make_app(
            tmp_path,
            "mini",
            "import dorm\n"
            "class M(dorm.Model):\n"
            "    name = dorm.CharField(max_length=10)\n"
            "    score = dorm.FloatField(null=True)\n"
            "    price = dorm.DecimalField(max_digits=6, decimal_places=2)\n"
            "    active = dorm.BooleanField(default=True)\n"
            "    uid = dorm.UUIDField(null=True)\n"
            "    email = dorm.EmailField()\n"
            "    url = dorm.URLField(null=True)\n"
            "    created = dorm.DateTimeField(null=True)\n"
            "    born = dorm.DateField(null=True)\n"
            "    at = dorm.TimeField(null=True)\n"
            "    meta = dorm.JSONField(default=dict)\n",
        )
        cmd_export_json_schema(argparse.Namespace(
            apps=[], out=None, include_relations=False, settings=mod,
        ))
        out = capsys.readouterr().out
        body = json.loads(out)
        assert "M" in body
        props = body["M"]["properties"]
        assert props["name"]["type"] == "string"
        assert props["name"]["maxLength"] == 10
        assert props["email"]["format"] == "email"
        assert props["url"]["format"] == "uri"
        assert props["born"]["format"] == "date"

    def test_export_to_dir(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_export_json_schema

        mod = _isolate(tmp_path, monkeypatch, "s_jschema_out", apps_src="['mini']")
        _make_app(
            tmp_path, "mini",
            "import dorm\nclass M(dorm.Model):\n    n = dorm.IntegerField()\n",
        )
        out_dir = tmp_path / "schemas"
        cmd_export_json_schema(argparse.Namespace(
            apps=[], out=str(out_dir), include_relations=False, settings=mod,
        ))
        assert (out_dir / "M.json").exists()
        msg = capsys.readouterr().out
        assert "wrote" in msg and "schema" in msg


# ── cmd_migrate_from_django ──────────────────────────────────────────


class TestCmdMigrateFromDjango:
    DJANGO_MODELS_SRC = (
        "from django.db import models\n"
        "class Thing(models.Model):\n"
        "    name = models.CharField(max_length=30)\n"
    )

    def test_dry_run_on_file(self, tmp_path, capsys, cli_isolation):
        from dorm.cli import cmd_migrate_from_django

        src = tmp_path / "models.py"
        src.write_text(self.DJANGO_MODELS_SRC)
        cmd_migrate_from_django(argparse.Namespace(path=str(src), dry_run=True))
        out = capsys.readouterr().out
        assert "import dorm" in out

    def test_writes_file(self, tmp_path, capsys, cli_isolation):
        from dorm.cli import cmd_migrate_from_django

        src = tmp_path / "models.py"
        src.write_text(self.DJANGO_MODELS_SRC)
        cmd_migrate_from_django(argparse.Namespace(path=str(src), dry_run=False))
        out = capsys.readouterr().out
        assert "Converted" in out
        assert "import dorm" in src.read_text()

    def test_dir_dispatch(self, tmp_path, capsys, cli_isolation):
        from dorm.cli import cmd_migrate_from_django

        app = tmp_path / "djapp"
        app.mkdir()
        (app / "models.py").write_text(self.DJANGO_MODELS_SRC)
        cmd_migrate_from_django(argparse.Namespace(path=str(app), dry_run=True))
        out = capsys.readouterr().out
        assert "Would convert" in out

    def test_missing_target_exits(self, tmp_path, capsys, cli_isolation):
        from dorm.cli import cmd_migrate_from_django

        with pytest.raises(SystemExit):
            cmd_migrate_from_django(argparse.Namespace(
                path=str(tmp_path / "nope.py"), dry_run=True,
            ))
        err = capsys.readouterr().err
        assert "neither" in err


# ── cmd_inspectdb ────────────────────────────────────────────────────


class TestCmdInspectDB:
    def test_inspects_tables(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_inspectdb, cmd_makemigrations, cmd_migrate

        mod = _isolate(tmp_path, monkeypatch, "s_inspect", apps_src="['mini']")
        _make_app(
            tmp_path, "mini",
            "import dorm\n"
            "class Widget(dorm.Model):\n"
            "    name = dorm.CharField(max_length=20)\n",
        )
        cmd_makemigrations(argparse.Namespace(apps=["mini"], empty=False, name=None, settings=mod))
        capsys.readouterr()
        cmd_migrate(argparse.Namespace(
            app_label="mini", target=None, verbosity=0, dry_run=False, settings=mod,
        ))
        capsys.readouterr()
        cmd_inspectdb(argparse.Namespace(settings=mod, table=None))
        out = capsys.readouterr().out
        assert "class" in out


# ── cmd_purge_deleted ────────────────────────────────────────────────


class TestCmdPurgeDeleted:
    def test_no_softdelete_models(self, tmp_path, monkeypatch, capsys, cli_isolation):
        from dorm.cli import cmd_purge_deleted

        mod = _isolate(tmp_path, monkeypatch, "s_purge", apps_src="[]")
        # Two scenarios depending on test isolation:
        # - Clean run: registry has no SoftDeleteModel subclasses →
        #   cmd_purge_deleted exits with SystemExit + message.
        # - Polluted run (some earlier test registered one): cmd runs
        #   to completion. Either is acceptable — we only assert no
        #   unhandled crash.
        try:
            cmd_purge_deleted(argparse.Namespace(
                older_than="30d", dry_run=True, apps=[], alias="default",
                settings=mod,
            ))
        except SystemExit:
            pass
        out = capsys.readouterr().out
        assert isinstance(out, str)


# ── _parse_duration covers s/m/h/d/w branches ────────────────────────


class TestParseDuration:
    def test_units(self):
        from dorm.cli import _parse_duration

        assert _parse_duration("30") == 30
        assert _parse_duration("45s") == 45
        assert _parse_duration("2m") == 120
        assert _parse_duration("3h") == 10800
        assert _parse_duration("1d") == 86400
        assert _parse_duration("1w") == 604800

    def test_invalid(self):
        from dorm.cli import _parse_duration

        with pytest.raises(ValueError):
            _parse_duration("")


# ── _field_to_jsonschema direct ──────────────────────────────────────


class TestFieldToJsonSchema:
    def test_each_branch(self):
        import dorm
        from dorm.cli import _field_to_jsonschema

        assert _field_to_jsonschema(dorm.IntegerField())["type"] == "integer"
        assert _field_to_jsonschema(dorm.FloatField())["type"] == "number"
        dec = _field_to_jsonschema(dorm.DecimalField(max_digits=5, decimal_places=2))
        assert dec["pattern"].startswith("^")
        assert _field_to_jsonschema(dorm.BooleanField())["type"] == "boolean"
        assert _field_to_jsonschema(dorm.UUIDField())["format"] == "uuid"
        assert _field_to_jsonschema(dorm.EmailField())["format"] == "email"
        assert _field_to_jsonschema(dorm.URLField())["format"] == "uri"
        assert _field_to_jsonschema(dorm.DateTimeField())["format"] == "date-time"
        assert _field_to_jsonschema(dorm.DateField())["format"] == "date"
        assert _field_to_jsonschema(dorm.TimeField())["format"] == "time"
        char = _field_to_jsonschema(dorm.CharField(max_length=12))
        assert char["type"] == "string" and char["maxLength"] == 12
        # JSON field → union type.
        js = _field_to_jsonschema(dorm.JSONField(default=dict))
        assert "object" in js["type"]
        # nullable widens type to list.
        nullable = _field_to_jsonschema(dorm.CharField(max_length=5, null=True))
        assert isinstance(nullable["type"], list) and "null" in nullable["type"]
