"""Tests for the second-tier production-readiness features:

  - A1: transient retry helper
  - B4: pre_query / post_query signals
  - A3: nested Pydantic schemas via ``Meta.nested``
  - C9: ``dorm dbcheck`` CLI
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import dorm

REPO_ROOT = Path(__file__).resolve().parent.parent


# ── A1: transient retry ───────────────────────────────────────────────────────


def test_with_transient_retry_succeeds_first_try():
    from dorm.db.utils import with_transient_retry

    calls = {"n": 0}

    def op() -> int:
        calls["n"] += 1
        return 42

    assert with_transient_retry(op) == 42
    assert calls["n"] == 1


def test_with_transient_retry_recovers_from_transient():
    """Simulated transient error → succeeds on second attempt."""
    from dorm.db.utils import with_transient_retry

    try:
        import psycopg
    except ImportError:
        pytest.skip("psycopg not installed; transient retry detection requires it")

    calls = {"n": 0}

    def op() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return "ok"

    out = with_transient_retry(op, attempts=3, backoff=0.01)
    assert out == "ok"
    assert calls["n"] == 2


def test_with_transient_retry_propagates_non_transient():
    from dorm.db.utils import with_transient_retry

    class _NotTransient(Exception):
        pass

    def op() -> None:
        raise _NotTransient("real bug")

    with pytest.raises(_NotTransient):
        with_transient_retry(op, attempts=5, backoff=0.001)


def test_with_transient_retry_skips_inside_transaction():
    """Inside a transaction we MUST NOT retry — committed state would
    be re-applied. The helper just runs once and lets the error propagate."""
    from dorm.db.utils import with_transient_retry

    try:
        import psycopg
    except ImportError:
        pytest.skip("psycopg not installed")

    calls = {"n": 0}

    def op() -> None:
        calls["n"] += 1
        raise psycopg.OperationalError("connection lost mid-tx")

    with pytest.raises(psycopg.OperationalError):
        with_transient_retry(op, in_transaction=True, attempts=5, backoff=0.001)
    assert calls["n"] == 1


def test_with_transient_retry_gives_up_after_max_attempts():
    from dorm.db.utils import with_transient_retry

    try:
        import psycopg
    except ImportError:
        pytest.skip("psycopg not installed")

    def op() -> None:
        raise psycopg.OperationalError("still down")

    with pytest.raises(psycopg.OperationalError):
        with_transient_retry(op, attempts=2, backoff=0.001)


async def test_awith_transient_retry_recovers():
    from dorm.db.utils import awith_transient_retry

    try:
        import psycopg
    except ImportError:
        pytest.skip("psycopg not installed")

    calls = {"n": 0}

    async def op() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise psycopg.OperationalError("transient")
        return "ok"

    out = await awith_transient_retry(lambda: op(), attempts=3, backoff=0.01)
    assert out == "ok"
    assert calls["n"] == 2


def test_is_transient_classifies_correctly():
    """Transient = network/server-level. Programming errors are NOT."""
    from dorm.db.utils import _is_transient

    try:
        import psycopg
        assert _is_transient(psycopg.OperationalError("conn lost")) is True
        assert _is_transient(psycopg.InterfaceError("no socket")) is True
        assert _is_transient(psycopg.errors.UniqueViolation("dup")) is False
    except ImportError:
        pass

    import sqlite3
    assert _is_transient(sqlite3.OperationalError("database is locked")) is True
    assert _is_transient(sqlite3.OperationalError("no such table: x")) is False
    assert _is_transient(ValueError("nope")) is False


# ── B4: pre_query / post_query signals ────────────────────────────────────────


def test_pre_post_query_signals_fire_around_each_query():
    from tests.models import Author

    pre_calls: list = []
    post_calls: list = []

    def pre(sender, sql, params):
        pre_calls.append((sender, sql, params))

    def post(sender, sql, params, elapsed_ms, error):
        post_calls.append((sender, sql, params, elapsed_ms, error))

    dorm.pre_query.connect(pre, weak=False, dispatch_uid="t-pre")
    dorm.post_query.connect(post, weak=False, dispatch_uid="t-post")
    try:
        Author.objects.filter(name="__sentinel__").count()
    finally:
        dorm.pre_query.disconnect(dispatch_uid="t-pre")
        dorm.post_query.disconnect(dispatch_uid="t-post")

    assert pre_calls, "pre_query never fired"
    assert post_calls, "post_query never fired"
    # Last post call should have an error of None (query succeeded)
    last = post_calls[-1]
    assert last[4] is None
    # elapsed_ms is non-negative
    assert last[3] >= 0


def test_post_query_receives_error_on_failure():
    from dorm.db.connection import get_connection

    captured: list = []

    def post(sender, sql, params, elapsed_ms, error):
        captured.append(error)

    dorm.post_query.connect(post, weak=False, dispatch_uid="t-err")
    try:
        with pytest.raises(Exception):
            get_connection().execute("SELECT * FROM definitely_missing_x47")
    finally:
        dorm.post_query.disconnect(dispatch_uid="t-err")

    assert any(e is not None for e in captured), \
        "post_query should report the exception via 'error' kwarg"


# ── A3: nested Pydantic schemas ───────────────────────────────────────────────


def test_meta_nested_fk_serializes_subschema():
    from dorm.contrib.pydantic import DormSchema
    from tests.models import Author, Publisher

    class PublisherOut(DormSchema):
        class Meta:
            model = Publisher

    class AuthorOut(DormSchema):
        class Meta:
            model = Author
            nested = {"publisher": PublisherOut}

    fields = AuthorOut.model_fields
    # Author.publisher is nullable → annotation is PublisherOut | None.
    assert fields["publisher"].annotation == (PublisherOut | None)


def test_meta_nested_validates_from_dorm_with_related_object():
    from dorm.contrib.pydantic import DormSchema
    from tests.models import Author, Publisher

    class PublisherOut(DormSchema):
        class Meta:
            model = Publisher
            fields = ("id", "name")

    class AuthorOut(DormSchema):
        class Meta:
            model = Author
            fields = ("id", "name", "age", "publisher")
            nested = {"publisher": PublisherOut}

    p = Publisher.objects.create(name="ACME")
    a = Author.objects.create(name="Nested", age=33, email="n@x.com", publisher=p)
    try:
        out = AuthorOut.model_validate(a)
        assert out.name == "Nested"  # type: ignore
        # publisher came through as a populated PublisherOut instance.
        sub = out.publisher  # type: ignore
        assert sub is not None
        assert sub.name == "ACME"
        assert sub.id == p.pk
    finally:
        a.delete()
        p.delete()


def test_meta_nested_m2m_serializes_list_of_subschema():
    from dorm.contrib.pydantic import DormSchema
    from tests.models import Article, Tag

    class TagOut(DormSchema):
        class Meta:
            model = Tag

    class ArticleOut(DormSchema):
        class Meta:
            model = Article
            nested = {"tags": TagOut}

    fields = ArticleOut.model_fields
    assert fields["tags"].annotation == list[TagOut]


def test_meta_nested_unknown_field_raises():
    from dorm.contrib.pydantic import DormSchema
    from tests.models import Author

    with pytest.raises(TypeError, match="unknown field"):

        class _Bad(DormSchema):
            class Meta:
                model = Author
                nested = {"nonexistent": object}


# ── C9: dorm dbcheck CLI ──────────────────────────────────────────────────────


def _run_dbcheck(args: list[str], cwd: Path):
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + pp if pp else "")
    return subprocess.run(
        [sys.executable, "-m", "dorm", "dbcheck", *args],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_dbcheck_reports_in_sync_when_schema_matches(tmp_path: Path):
    """A fresh app whose tables we created via raw SQL matching the model
    columns reports OK."""
    db_path = tmp_path / "db.sqlite3"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["dbk_app"]\n'
    )
    app = tmp_path / "dbk_app"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class Widget(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    class Meta:\n"
        "        db_table = 'widgets'\n"
        "        app_label = 'dbk_app'\n"
    )
    # Pre-create the table matching the model.
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(50) NOT NULL)")
    conn.commit()
    conn.close()

    res = _run_dbcheck([], cwd=tmp_path)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "match" in res.stdout.lower()


def test_dbcheck_reports_missing_table(tmp_path: Path):
    db_path = tmp_path / "db.sqlite3"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["dbk_app2"]\n'
    )
    app = tmp_path / "dbk_app2"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class GhostTable(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    class Meta:\n"
        "        db_table = 'ghost_table'\n"
        "        app_label = 'dbk_app2'\n"
    )
    # Touch the DB but do NOT create the table.
    import sqlite3
    sqlite3.connect(str(db_path)).close()

    res = _run_dbcheck([], cwd=tmp_path)
    assert res.returncode == 1
    assert "missing" in res.stdout.lower()


def test_dbcheck_passes_after_fresh_migration_with_fk(tmp_path: Path):
    """Regression: when a migration is *re-loaded from disk*, the
    deserialized ``ForeignKey`` field hasn't gone through
    ``contribute_to_class`` so its ``column`` attribute is None. The
    migration writer used to fall back to the field name (``author``)
    instead of the FK column (``author_id``), which then made
    ``dbcheck`` report fake drift right after a successful migrate.

    Run a real makemigrations + migrate cycle that involves a FK and
    confirm dbcheck is clean."""
    db_path = tmp_path / "lib.db"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["dbk_fk"]\n'
    )
    app = tmp_path / "dbk_fk"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class Author(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    class Meta:\n"
        "        db_table = 'fk_authors'\n"
        "        app_label = 'dbk_fk'\n"
        "class Book(dorm.Model):\n"
        "    title = dorm.CharField(max_length=100)\n"
        "    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)\n"
        "    class Meta:\n"
        "        db_table = 'fk_books'\n"
        "        app_label = 'dbk_fk'\n"
    )

    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + pp if pp else "")

    def _run(args: list[str]):
        return subprocess.run(
            args, env=env, cwd=str(tmp_path),
            capture_output=True, text=True, timeout=30,
        )

    mk = _run([sys.executable, "-m", "dorm", "makemigrations"])
    assert mk.returncode == 0, mk.stdout + mk.stderr
    mg = _run([sys.executable, "-m", "dorm", "migrate"])
    assert mg.returncode == 0, mg.stdout + mg.stderr

    dc = _run_dbcheck([], cwd=tmp_path)
    assert dc.returncode == 0, (
        "dbcheck should be clean after fresh makemigrations + migrate — "
        f"output:\n{dc.stdout}\n{dc.stderr}"
    )
    assert "match" in dc.stdout.lower()


# ── health_check / ahealth_check ──────────────────────────────────────────────


def test_health_check_returns_ok_when_db_works():
    result = dorm.health_check()
    assert result["status"] == "ok"
    assert result["alias"] == "default"
    assert result["elapsed_ms"] >= 0


def test_health_check_returns_error_for_unknown_alias():
    """Wrong alias must return ``error`` status, not raise — health
    endpoints have to respond to the orchestrator regardless."""
    result = dorm.health_check(alias="does_not_exist")
    assert result["status"] == "error"
    assert "error" in result


async def test_ahealth_check_returns_ok():
    result = await dorm.ahealth_check()
    assert result["status"] == "ok"
    assert result["elapsed_ms"] >= 0


async def test_ahealth_check_handles_error_gracefully():
    result = await dorm.ahealth_check(alias="missing")
    assert result["status"] == "error"


# ── pool_stats() ──────────────────────────────────────────────────────────────


def test_pool_stats_shape():
    from dorm.db.connection import get_connection

    wrapper = get_connection()
    # Trigger pool open with a query.
    wrapper.execute("SELECT 1")
    stats = wrapper.pool_stats()
    assert "vendor" in stats
    assert stats["vendor"] in ("sqlite", "postgresql")
    if stats["vendor"] == "postgresql":
        # min_size / max_size always present for PG; pool was opened above.
        assert "min_size" in stats
        assert "max_size" in stats


def test_pool_stats_settings_max_idle_lifetime_pass_through():
    """Verify MAX_IDLE / MAX_LIFETIME from DATABASES reach the wrapper."""
    from dorm.db.backends.postgresql import PostgreSQLDatabaseWrapper

    w = PostgreSQLDatabaseWrapper({
        "NAME": "x",
        "USER": "u",
        "MAX_IDLE": 120.0,
        "MAX_LIFETIME": 1800.0,
    })
    assert w._max_idle == 120.0
    assert w._max_lifetime == 1800.0


# ── DATABASE_ROUTERS ──────────────────────────────────────────────────────────


def test_router_db_for_read_consults_routers(monkeypatch):
    from dorm.db.connection import router_db_for_read
    from dorm.conf import settings

    class _R:
        def db_for_read(self, model, **hints):
            return "replica"

    original = list(settings.DATABASE_ROUTERS)
    settings.DATABASE_ROUTERS = [_R()]
    try:
        from tests.models import Author
        assert router_db_for_read(Author) == "replica"
    finally:
        settings.DATABASE_ROUTERS = original


def test_router_falls_back_to_default_when_no_router_returns():
    from dorm.db.connection import router_db_for_read
    from tests.models import Author

    class _R:
        def db_for_read(self, model, **hints):
            return None  # opt out

    from dorm.conf import settings
    original = list(settings.DATABASE_ROUTERS)
    settings.DATABASE_ROUTERS = [_R()]
    try:
        assert router_db_for_read(Author) == "default"
    finally:
        settings.DATABASE_ROUTERS = original


def test_router_first_truthy_wins():
    """First router that returns a non-None alias wins; later ones don't run."""
    from dorm.db.connection import router_db_for_read
    from tests.models import Author

    calls = []

    class _A:
        def db_for_read(self, model, **hints):
            calls.append("A")
            return "first"

    class _B:
        def db_for_read(self, model, **hints):
            calls.append("B")
            return "second"

    from dorm.conf import settings
    original = list(settings.DATABASE_ROUTERS)
    settings.DATABASE_ROUTERS = [_A(), _B()]
    try:
        assert router_db_for_read(Author) == "first"
        assert calls == ["A"]
    finally:
        settings.DATABASE_ROUTERS = original


# ── execute_streaming / iterator(chunk_size=N) ────────────────────────────────


def test_iterator_with_chunk_size_streams():
    """``iterator(chunk_size=N)`` must yield rows lazily — verify by
    inserting a known volume and consuming the generator one row at a
    time."""
    from tests.models import Author

    Author.objects.filter(name__startswith="STR-").delete()
    Author.objects.bulk_create([
        Author(name=f"STR-{i:04d}", age=i, email=f"str{i}@x.com")
        for i in range(250)
    ])
    try:
        seen = 0
        for obj in (
            Author.objects.filter(name__startswith="STR-")
            .order_by("name")
            .iterator(chunk_size=50)
        ):
            assert obj.name.startswith("STR-")
            seen += 1
        assert seen == 250
    finally:
        Author.objects.filter(name__startswith="STR-").delete()


async def test_aiterator_with_chunk_size_streams():
    from tests.models import Author

    await Author.objects.filter(name__startswith="ASTR-").adelete()
    await Author.objects.abulk_create([
        Author(name=f"ASTR-{i:04d}", age=i, email=f"astr{i}@x.com")
        for i in range(150)
    ])
    try:
        seen = 0
        async for obj in (
            Author.objects.filter(name__startswith="ASTR-")
            .order_by("name")
            .aiterator(chunk_size=50)
        ):
            assert obj.name.startswith("ASTR-")
            seen += 1
        assert seen == 150
    finally:
        await Author.objects.filter(name__startswith="ASTR-").adelete()


def test_iterator_with_values_mode_and_chunk_size():
    """Streaming + values() — yields dicts, still streams."""
    from tests.models import Author

    Author.objects.filter(name__startswith="STV-").delete()
    Author.objects.bulk_create([
        Author(name=f"STV-{i}", age=i, email=f"stv{i}@x.com")
        for i in range(50)
    ])
    try:
        rows = list(
            Author.objects.filter(name__startswith="STV-")
            .order_by("name")
            .values("name", "age")
            .iterator(chunk_size=10)
        )
        assert len(rows) == 50
        assert all(isinstance(r, dict) for r in rows)
        assert {"name", "age"} <= set(rows[0].keys())
    finally:
        Author.objects.filter(name__startswith="STV-").delete()


def test_dbcheck_reports_column_drift(tmp_path: Path):
    db_path = tmp_path / "db.sqlite3"
    (tmp_path / "settings.py").write_text(
        f'DATABASES = {{"default": {{"ENGINE": "sqlite", "NAME": "{db_path}"}}}}\n'
        'INSTALLED_APPS = ["dbk_app3"]\n'
    )
    app = tmp_path / "dbk_app3"
    app.mkdir()
    (app / "__init__.py").touch()
    (app / "models.py").write_text(
        "import dorm\n"
        "class Drifted(dorm.Model):\n"
        "    name = dorm.CharField(max_length=50)\n"
        "    age = dorm.IntegerField()\n"  # model has it
        "    class Meta:\n"
        "        db_table = 'drifted'\n"
        "        app_label = 'dbk_app3'\n"
    )
    # Create the table WITHOUT the `age` column → drift.
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE drifted (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(50))")
    conn.commit()
    conn.close()

    res = _run_dbcheck([], cwd=tmp_path)
    assert res.returncode == 1
    assert "missing in db: age" in res.stdout.lower()
