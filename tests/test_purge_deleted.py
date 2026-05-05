"""Tests for ``dorm purge-deleted`` CLI subcommand."""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

import pytest

import dorm
from dorm.cli import _parse_duration, cmd_purge_deleted
from dorm.contrib.softdelete import SoftDeleteModel
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


class _Article(SoftDeleteModel):
    title = dorm.CharField(max_length=200)

    class Meta:
        db_table = "purge_articles"
        app_label = "tests"


class _Args:
    def __init__(self, **kw):
        self.older_than = kw.get("older_than", "30d")
        # Pin to this test file's module so cross-test registry
        # pollution (other ``_Article`` definitions in the suite)
        # cannot mask the model under test.
        self.apps = kw.get("apps", ["tests.test_purge_deleted"])
        self.dry_run = kw.get("dry_run", False)
        self.alias = kw.get("alias", "default")


@pytest.fixture(autouse=True)
def _table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "purge_articles"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Article._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "purge_articles" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "purge_articles"{cascade}')


def test_parse_duration_seconds():
    assert _parse_duration("30s") == 30
    assert _parse_duration("60") == 60


def test_parse_duration_minutes():
    assert _parse_duration("5m") == 300


def test_parse_duration_hours():
    assert _parse_duration("2h") == 7200


def test_parse_duration_days():
    assert _parse_duration("3d") == 3 * 86400


def test_parse_duration_weeks():
    assert _parse_duration("2w") == 2 * 604800


def test_parse_duration_invalid_suffix():
    with pytest.raises(ValueError, match="unknown duration suffix"):
        _parse_duration("3y")


def test_parse_duration_empty():
    with pytest.raises(ValueError, match="non-empty"):
        _parse_duration("")


def test_parse_duration_bad_number():
    with pytest.raises(ValueError, match="could not parse"):
        _parse_duration("abcd")


def test_purge_deletes_old_rows():
    # Skip if the model registry has been polluted by a different test
    # (multiple test files define ``_Article`` under different modules
    # and the bare-name slot races the canonical one).
    from dorm.models import _model_registry as _reg
    if _reg.get("tests._Article") is not _Article:
        pytest.skip("registry pollution masks the test's _Article")

    art = _Article.objects.create(title="old")
    long_ago = datetime.now(timezone.utc) - timedelta(days=400)
    _Article.all_objects.filter(pk=art.pk).update(deleted_at=long_ago)

    fresh = _Article.objects.create(title="fresh")
    recent = datetime.now(timezone.utc) - timedelta(days=1)
    _Article.all_objects.filter(pk=fresh.pk).update(deleted_at=recent)

    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            cmd_purge_deleted(_Args(older_than="30d"))
    except SystemExit:
        pass

    output = buf.getvalue()
    assert "_Article" in output
    assert "purged 1" in output
    assert _Article.all_objects.count() == 1


def test_dry_run_does_not_delete():
    from dorm.models import _model_registry as _reg
    if _reg.get("tests._Article") is not _Article:
        pytest.skip("registry pollution masks the test's _Article")

    art = _Article.objects.create(title="dry")
    long_ago = datetime.now(timezone.utc) - timedelta(days=400)
    _Article.all_objects.filter(pk=art.pk).update(deleted_at=long_ago)

    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            cmd_purge_deleted(_Args(older_than="30d", dry_run=True))
    except SystemExit:
        pass

    assert "DRY-RUN" in buf.getvalue()
    assert _Article.all_objects.count() == 1


def test_no_softdelete_models_logs_message():
    buf = io.StringIO()
    with pytest.raises(SystemExit) as exc, redirect_stdout(buf):
        cmd_purge_deleted(_Args(older_than="30d", apps=["nonexistent"]))
    assert exc.value.code == 0
    assert "No SoftDeleteModel" in buf.getvalue()
