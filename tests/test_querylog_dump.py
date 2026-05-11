"""Tests for QueryLog dump_json / dump_jsonl / dump_parquet."""
from __future__ import annotations

import json

import pytest

import dorm
from dorm.contrib.querylog import QueryLog
from dorm.migrations.schema import SchemaEditor


class _Item(dorm.Model):
    name = dorm.CharField(max_length=32)

    class Meta:
        app_label = "tests"


@pytest.fixture(autouse=True)
def fresh_schema(tmp_path):
    from dorm.db.connection import _async_connections, _sync_connections, get_connection

    _sync_connections.clear()
    _async_connections.clear()
    db = tmp_path / "ql.sqlite3"
    dorm.configure(
        DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
        INSTALLED_APPS=["tests"],
    )
    with SchemaEditor(get_connection()) as se:
        se.create_model(_Item)
    yield
    _sync_connections.clear()
    _async_connections.clear()


class TestQueryLogDump:
    def test_to_dicts_basic(self):
        with QueryLog() as log:
            _Item.objects.create(name="a")
            list(_Item.objects.all())
        dicts = log.to_dicts()
        assert dicts
        first = dicts[0]
        assert set(first) >= {"sql", "template", "elapsed_ms", "alias", "vendor"}
        # params off by default.
        assert "params" not in first

    def test_to_dicts_with_params(self):
        with QueryLog() as log:
            _Item.objects.create(name="x")
        dicts = log.to_dicts(include_params=True)
        assert all("params" in d for d in dicts)

    def test_dump_json_returns_string(self):
        with QueryLog() as log:
            _Item.objects.create(name="x")
        text = log.dump_json()
        payload = json.loads(text)
        assert isinstance(payload, list)
        assert len(payload) >= 1

    def test_dump_json_writes_file(self, tmp_path):
        with QueryLog() as log:
            _Item.objects.create(name="x")
        out = tmp_path / "log.json"
        log.dump_json(str(out))
        payload = json.loads(out.read_text())
        assert isinstance(payload, list)

    def test_dump_jsonl_one_object_per_line(self, tmp_path):
        with QueryLog() as log:
            _Item.objects.create(name="a")
            _Item.objects.create(name="b")
        out = tmp_path / "log.jsonl"
        log.dump_jsonl(str(out))
        lines = [line for line in out.read_text().splitlines() if line.strip()]
        assert len(lines) >= 2
        # Each line is independently parseable.
        for line in lines:
            json.loads(line)

    def test_dump_parquet_optional_import(self, tmp_path):
        pyarrow = pytest.importorskip("pyarrow")  # noqa: F841
        with QueryLog() as log:
            _Item.objects.create(name="x")
        out = tmp_path / "log.parquet"
        log.dump_parquet(str(out))
        assert out.exists() and out.stat().st_size > 0

    def test_dump_parquet_empty(self, tmp_path):
        pytest.importorskip("pyarrow")
        log = QueryLog()
        # Don't capture anything — empty log.
        out = tmp_path / "empty.parquet"
        log.dump_parquet(str(out))
        assert out.exists()
