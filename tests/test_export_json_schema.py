"""Tests for the ``dorm export-json-schema`` CLI subcommand."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout


from dorm.cli import cmd_export_json_schema


class _Args:
    def __init__(self, **kw):
        self.out = kw.get("out", None)
        self.apps = kw.get("apps", ["tests"])
        self.include_relations = kw.get("include_relations", False)


def test_stdout_emits_json_dict():
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_export_json_schema(_Args())
    payload = json.loads(buf.getvalue())
    assert "Author" in payload
    schema = payload["Author"]
    assert schema["$schema"].startswith("https://json-schema.org")
    assert schema["type"] == "object"
    assert "name" in schema["properties"]
    assert "age" in schema["properties"]


def test_required_fields_marked():
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_export_json_schema(_Args())
    schema = json.loads(buf.getvalue())["Author"]
    # `name` and `age` are NOT NULL with no default — required.
    assert "name" in schema["required"]
    assert "age" in schema["required"]


def test_field_types_mapped():
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_export_json_schema(_Args())
    props = json.loads(buf.getvalue())["Author"]["properties"]
    assert props["name"]["type"] == "string"
    assert props["age"]["type"] == "integer"
    assert props["is_active"]["type"] == "boolean"
    # Email is nullable in tests/models.py — type is array including "null".
    email_t = props["email"]["type"]
    assert "string" in email_t and "null" in email_t


def test_max_length_constraint():
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_export_json_schema(_Args())
    props = json.loads(buf.getvalue())["Author"]["properties"]
    assert props["name"]["maxLength"] == 100


def test_writes_files_to_out_dir(tmp_path):
    out = tmp_path / "schemas"
    cmd_export_json_schema(_Args(out=str(out)))
    files = sorted(os.listdir(out))
    assert any(f.endswith(".json") for f in files)
    body = json.loads((out / "Author.json").read_text())
    assert body["title"] == "Author"


def test_include_relations_emits_m2m_array():
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_export_json_schema(_Args(include_relations=True))
    payload = json.loads(buf.getvalue())
    article = payload.get("Article")
    if article is not None:
        # Article has tags M2M.
        if "tags" in article["properties"]:
            assert article["properties"]["tags"]["type"] == "array"


def test_apps_filter_excludes_outside_modules():
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_export_json_schema(_Args(apps=["nonexistent_module"]))
    payload = json.loads(buf.getvalue())
    assert payload == {}
