"""Coverage for v3.2 ``dorm makemigrations --merge``.

Two devs branch off ``0001_initial`` and each land their own
``0002_*`` against ``main``. After the merge commit the migration
graph forks: both ``0002_a`` and ``0002_b`` claim ``0001`` as their
parent and neither references the other, so the loader sees two
leaves.

``--merge`` writes a new empty migration whose ``dependencies = [...]``
references every leaf, collapsing the fork back to a linear graph.
The merge migration carries no operations — it just re-points the
graph's tip.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from dorm import cli
from dorm.conf import settings as dorm_settings
from dorm.db.connection import reset_connections


@pytest.fixture
def merge_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Mirror ``cli_env`` from test_cli_inprocess.py — minimal project
    on tmp_path, dorm config restored at teardown."""
    saved_databases = dict(dorm_settings.DATABASES)
    saved_apps = list(dorm_settings.INSTALLED_APPS)
    saved_configured = dorm_settings._configured
    saved_modules = set(sys.modules)
    saved_path = list(sys.path)

    reset_connections()
    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    yield tmp_path

    for name in list(sys.modules):
        if name in saved_modules or name in {"dorm", "dorm.cli"}:
            continue
        mod = sys.modules.get(name)
        file = getattr(mod, "__file__", None) if mod is not None else None
        if file and str(tmp_path) in str(file):
            del sys.modules[name]
            continue
        if name in {"settings", "shop", "shop.models", "shop.migrations"}:
            del sys.modules[name]

    sys.path[:] = saved_path
    reset_connections()
    dorm_settings.DATABASES = saved_databases
    dorm_settings.INSTALLED_APPS = saved_apps
    dorm_settings._configured = saved_configured


def _settings(tmp: Path, apps: list[str]) -> None:
    db_path = str(tmp / "db.sqlite3")
    (tmp / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": {db_path!r}}}}}\n'
        f"INSTALLED_APPS = {apps!r}\n"
    )


def _scaffold_app_with_initial(tmp: Path, app: str = "shop") -> Path:
    app_dir = tmp / app
    app_dir.mkdir()
    (app_dir / "__init__.py").touch()
    (app_dir / "models.py").write_text(
        "import dorm\n"
        "class Product(dorm.Model):\n"
        "    name = dorm.CharField(max_length=80)\n"
    )
    mig_dir = app_dir / "migrations"
    mig_dir.mkdir()
    (mig_dir / "__init__.py").touch()
    (mig_dir / "0001_initial.py").write_text(
        '"""Initial."""\n'
        "from dorm.migrations.operations import CreateModel\n"
        "from dorm.fields import CharField, BigAutoField\n"
        "dependencies = []\n"
        "operations = [\n"
        "    CreateModel(name='Product', fields=[\n"
        "        ('id', BigAutoField(primary_key=True)),\n"
        "        ('name', CharField(max_length=80)),\n"
        "    ], options={'db_table': 'shop_product'}),\n"
        "]\n"
    )
    return mig_dir


def _write_leaf(mig_dir: Path, num: int, name: str, parents: list[str]) -> None:
    """Write an empty stub migration with the given parents."""
    deps = repr([("shop", p) for p in parents])
    (mig_dir / f"{num:04d}_{name}.py").write_text(
        '"""Stub branch migration."""\n'
        f"dependencies = {deps}\n"
        "operations = []\n"
    )


def _ns(**overrides) -> argparse.Namespace:
    """Default Namespace matching cmd_makemigrations expectations."""
    base = dict(
        apps=["shop"],
        empty=False,
        name=None,
        settings="settings",
        merge=False,
        enable_pgvector=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# ─────────────────────────────────────────────────────────────────────────────
# No-op when the migration graph is already linear
# ─────────────────────────────────────────────────────────────────────────────


def test_merge_noop_when_only_one_leaf(capsys, merge_env: Path):
    _settings(merge_env, apps=["shop"])
    _scaffold_app_with_initial(merge_env)

    cli.cmd_makemigrations(_ns(merge=True))
    out = capsys.readouterr().out

    assert "No migration conflicts detected" in out
    names = {f.name for f in (merge_env / "shop" / "migrations").glob("*.py")}
    assert names == {"__init__.py", "0001_initial.py"}


def test_merge_noop_on_linear_chain(capsys, merge_env: Path):
    """0001 → 0002 → 0003: still one leaf, still nothing to do."""
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    _write_leaf(mig_dir, 2, "second", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "third", parents=["0002_second"])

    cli.cmd_makemigrations(_ns(merge=True))
    out = capsys.readouterr().out

    assert "No migration conflicts detected" in out


# ─────────────────────────────────────────────────────────────────────────────
# Two parallel leaves → merge migration written
# ─────────────────────────────────────────────────────────────────────────────


def test_merge_collapses_two_leaves(capsys, merge_env: Path):
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    # Both branches sit on top of 0001_initial, neither references the other.
    _write_leaf(mig_dir, 2, "branch_a", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "branch_b", parents=["0001_initial"])

    cli.cmd_makemigrations(_ns(merge=True))
    out = capsys.readouterr().out

    assert "Merged 2 leaves" in out
    merge_files = list(mig_dir.glob("0004_*.py"))
    assert len(merge_files) == 1
    body = merge_files[0].read_text()
    assert "branch_a" in body and "branch_b" in body


def test_merge_writes_dependencies_for_every_leaf(capsys, merge_env: Path):
    """Three leaves, one merge — all three referenced in dependencies."""
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    _write_leaf(mig_dir, 2, "branch_a", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "branch_b", parents=["0001_initial"])
    _write_leaf(mig_dir, 4, "branch_c", parents=["0001_initial"])

    cli.cmd_makemigrations(_ns(merge=True))
    capsys.readouterr()

    merge_files = list(mig_dir.glob("0005_*.py"))
    assert len(merge_files) == 1
    body = merge_files[0].read_text()
    for branch in ("branch_a", "branch_b", "branch_c"):
        assert branch in body


def test_merge_uses_custom_name_when_given(capsys, merge_env: Path):
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    _write_leaf(mig_dir, 2, "branch_a", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "branch_b", parents=["0001_initial"])

    cli.cmd_makemigrations(_ns(merge=True, name="reconcile"))
    capsys.readouterr()

    files = [f.name for f in mig_dir.glob("*.py")]
    assert any("reconcile" in n for n in files)


def test_merge_default_name_is_merge(capsys, merge_env: Path):
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    _write_leaf(mig_dir, 2, "branch_a", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "branch_b", parents=["0001_initial"])

    cli.cmd_makemigrations(_ns(merge=True))
    capsys.readouterr()

    merge_file = next(mig_dir.glob("0004_*.py"))
    assert merge_file.name == "0004_merge.py"


def test_merge_migration_carries_no_operations(capsys, merge_env: Path):
    """The merge file is a graph-only re-point; ``operations = []``."""
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    _write_leaf(mig_dir, 2, "branch_a", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "branch_b", parents=["0001_initial"])

    cli.cmd_makemigrations(_ns(merge=True))
    capsys.readouterr()

    merge_file = next(mig_dir.glob("0004_*.py"))
    ns: dict = {}
    exec(compile(merge_file.read_text(), str(merge_file), "exec"), ns)
    assert ns["operations"] == []
    deps = ns["dependencies"]
    assert ("shop", "0002_branch_a") in deps
    assert ("shop", "0003_branch_b") in deps


def test_merge_resolves_loader_after_write(capsys, merge_env: Path):
    """After ``--merge`` runs, the migration graph has exactly one leaf again."""
    _settings(merge_env, apps=["shop"])
    mig_dir = _scaffold_app_with_initial(merge_env)
    _write_leaf(mig_dir, 2, "branch_a", parents=["0001_initial"])
    _write_leaf(mig_dir, 3, "branch_b", parents=["0001_initial"])

    cli.cmd_makemigrations(_ns(merge=True))
    capsys.readouterr()

    from dorm.db.connection import get_connection
    from dorm.migrations.loader import MigrationLoader

    loader = MigrationLoader(get_connection())
    loader.load(mig_dir, "shop")
    entries = loader.migrations.get("shop", [])
    referenced: set[str] = set()
    for _num, _name, mod in entries:
        for dep in getattr(mod, "dependencies", []) or []:
            if isinstance(dep, tuple) and len(dep) == 2 and dep[0] == "shop":
                referenced.add(dep[1])
    leaves = [name for _n, name, _m in entries if name not in referenced]
    assert len(leaves) == 1
