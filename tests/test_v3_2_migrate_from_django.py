"""Coverage for the v3.2 ``dorm migrate-from-django`` converter
(:mod:`dorm.contrib.migrate_from_django`).
"""

from __future__ import annotations

from pathlib import Path

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Source-string conversion
# ──────────────────────────────────────────────────────────────────────────────


def test_converts_basic_django_models_to_dorm():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db import models\n"
        "\n"
        "class Author(models.Model):\n"
        "    name = models.CharField(max_length=100)\n"
        "    email = models.EmailField(unique=True)\n"
        "    bio = models.TextField(null=True, blank=True)\n"
        "\n"
        "    class Meta:\n"
        "        db_table = 'authors'\n"
        "        ordering = ['name']\n"
    )
    out, todos = convert_models_source(src)
    assert "import dorm" in out
    assert "from django.db import models" not in out
    assert "dorm.Model" in out
    assert "dorm.CharField" in out
    assert "dorm.EmailField" in out
    assert "dorm.TextField" in out
    # Meta options stay untouched (1:1).
    assert "db_table = 'authors'" in out
    assert "ordering = ['name']" in out
    assert todos == []


def test_converts_foreign_key_with_on_delete_constant():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db import models\n"
        "\n"
        "class Book(models.Model):\n"
        "    author = models.ForeignKey(\n"
        "        'Author',\n"
        "        on_delete=models.CASCADE,\n"
        "        related_name='books',\n"
        "    )\n"
    )
    out, _ = convert_models_source(src)
    assert "dorm.ForeignKey" in out
    assert "on_delete=dorm.CASCADE" in out
    assert "related_name='books'" in out


def test_handles_every_supported_on_delete_constant():
    from dorm.contrib.migrate_from_django import convert_models_source

    for const in ("CASCADE", "SET_NULL", "SET_DEFAULT", "PROTECT", "DO_NOTHING", "RESTRICT"):
        src = (
            "from django.db import models\n"
            "class T(models.Model):\n"
            f"    fk = models.ForeignKey('X', on_delete=models.{const})\n"
        )
        out, _ = convert_models_source(src)
        assert f"dorm.{const}" in out


def test_meta_indexes_constraints_q_f_namespace():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db import models\n"
        "\n"
        "class T(models.Model):\n"
        "    qty = models.IntegerField()\n"
        "\n"
        "    class Meta:\n"
        "        indexes = [models.Index(fields=['qty'], name='ix_t_qty')]\n"
        "        constraints = [\n"
        "            models.UniqueConstraint(fields=['qty'], name='uq_t_qty'),\n"
        "            models.CheckConstraint(check=models.Q(qty__gte=0), name='chk_t'),\n"
        "        ]\n"
    )
    out, _ = convert_models_source(src)
    assert "dorm.Index" in out
    assert "dorm.UniqueConstraint" in out
    assert "dorm.CheckConstraint" in out
    assert "dorm.Q" in out


def test_redirects_django_db_models_imports():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db.models import Count, Sum, Avg\n"
        "from django.db import models\n"
        "\n"
        "class Author(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    out, _ = convert_models_source(src)
    assert "from dorm import Count, Sum, Avg" in out
    assert "import dorm" in out


def test_flags_custom_manager_assignments():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db import models\n"
        "\n"
        "class CustomManager(models.Manager):\n"
        "    pass\n"
        "\n"
        "class T(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
        "    objects = CustomManager()\n"
    )
    out, todos = convert_models_source(src)
    assert any("custom Manager" in t for t in todos)
    # TODO banner is prepended to the file.
    assert out.startswith("# TODO: dorm migrate-from-django")


def test_flags_django_contrib_auth_imports():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.contrib.auth import get_user_model\n"
        "from django.db import models\n"
        "\n"
        "class T(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    _out, todos = convert_models_source(src)
    assert any("dorm.contrib.auth" in t for t in todos)


def test_flags_django_signals_imports():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db.models.signals import post_save\n"
        "from django.db import models\n"
        "\n"
        "class T(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    _out, todos = convert_models_source(src)
    assert any("dorm.signals" in t for t in todos)


def test_flags_unrecognised_models_attribute():
    from dorm.contrib.migrate_from_django import convert_models_source

    src = (
        "from django.db import models\n"
        "\n"
        "class T(models.Model):\n"
        "    when = models.SomethingNew()\n"
    )
    _out, todos = convert_models_source(src)
    assert any("models.SomethingNew" in t for t in todos)


def test_raises_on_invalid_python_source():
    from dorm.contrib.migrate_from_django import convert_models_source

    with pytest.raises(ValueError, match="syntax error"):
        convert_models_source("class :::")


# ──────────────────────────────────────────────────────────────────────────────
# File / directory I/O
# ──────────────────────────────────────────────────────────────────────────────


def test_convert_models_file_round_trip(tmp_path: Path):
    from dorm.contrib.migrate_from_django import convert_models_file

    src = tmp_path / "models.py"
    src.write_text(
        "from django.db import models\n"
        "class T(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    rewritten, todos = convert_models_file(src)
    assert "import dorm" in rewritten
    assert todos == []


def test_convert_models_file_missing_path():
    from dorm.contrib.migrate_from_django import convert_models_file

    with pytest.raises(FileNotFoundError):
        convert_models_file(Path("/nonexistent/path/models.py"))


def test_convert_app_writes_files(tmp_path: Path):
    from dorm.contrib.migrate_from_django import convert_app

    app = tmp_path / "myapp"
    app.mkdir()
    (app / "models.py").write_text(
        "from django.db import models\n"
        "class T(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    results = convert_app(app)
    assert results
    rewritten = (app / "models.py").read_text()
    assert "import dorm" in rewritten
    assert "from django.db import models" not in rewritten


def test_convert_app_dry_run_does_not_modify(tmp_path: Path):
    from dorm.contrib.migrate_from_django import convert_app

    app = tmp_path / "myapp"
    app.mkdir()
    src = "from django.db import models\nclass T(models.Model): name = models.CharField(max_length=10)\n"
    (app / "models.py").write_text(src)
    convert_app(app, dry_run=True)
    assert (app / "models.py").read_text() == src


def test_convert_app_models_subpackage(tmp_path: Path):
    """Some Django apps use ``models/__init__.py`` + sub-modules
    instead of a single ``models.py``. The converter walks the
    sub-package."""
    from dorm.contrib.migrate_from_django import convert_app

    app = tmp_path / "myapp"
    pkg = app / "models"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("from .a import *\n")
    (pkg / "a.py").write_text(
        "from django.db import models\n"
        "class A(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    convert_app(app)
    assert "import dorm" in (pkg / "a.py").read_text()


def test_convert_app_missing_directory_raises():
    from dorm.contrib.migrate_from_django import convert_app

    with pytest.raises(FileNotFoundError):
        convert_app(Path("/no/such/dir"))


# ──────────────────────────────────────────────────────────────────────────────
# CLI surface
# ──────────────────────────────────────────────────────────────────────────────


def test_cli_migrate_from_django_runs(tmp_path: Path):
    """Smoke the subprocess invocation — the converter is the
    interesting bit and is unit-tested above; the CLI just wires
    argparse + emits TODOs to stderr."""
    import subprocess

    target = tmp_path / "models.py"
    target.write_text(
        "from django.db import models\n"
        "class T(models.Model):\n"
        "    name = models.CharField(max_length=10)\n"
    )
    out = subprocess.run(
        ["uv", "run", "dorm", "migrate-from-django", str(target), "--dry-run"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert out.returncode == 0
    assert "import dorm" in out.stdout


def test_cli_migrate_from_django_help_lists_command():
    import subprocess

    out = subprocess.run(
        ["uv", "run", "dorm", "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert "migrate-from-django" in out.stdout
