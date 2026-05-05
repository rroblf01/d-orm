"""Tests for ``dorm.contrib.streaming``.

Backend-agnostic — every helper works against SQLite and PostgreSQL
without changes. The async tests exercise the async iterator path;
the sync tests cover the sync iterator path.
"""

from __future__ import annotations

import csv
import io
import json

import pytest

from dorm.contrib.streaming import (
    astream_csv,
    astream_json,
    astream_jsonl,
    stream_csv,
    stream_json,
    stream_jsonl,
    stream_ndjson_pretty,
)
from tests.models import Author


def _join(chunks):
    return b"".join(chunks)


def test_stream_jsonl_yields_one_line_per_row():
    Author.objects.bulk_create([Author(name=f"a{i}", age=i) for i in range(3)])
    blob = _join(stream_jsonl(Author.objects.all()))
    lines = blob.decode().strip().split("\n")
    assert len(lines) == 3
    rows = [json.loads(line) for line in lines]
    names = sorted(r["name"] for r in rows)
    assert names == ["a0", "a1", "a2"]


def test_stream_json_yields_array():
    Author.objects.bulk_create([Author(name=f"a{i}", age=i) for i in range(2)])
    blob = _join(stream_json(Author.objects.all()))
    rows = json.loads(blob)
    assert len(rows) == 2


def test_stream_json_empty_queryset():
    blob = _join(stream_json(Author.objects.all()))
    assert blob == b"[]"


def test_stream_csv_includes_header_and_rows():
    Author.objects.bulk_create([Author(name=f"a{i}", age=i) for i in range(2)])
    blob = _join(stream_csv(Author.objects.all().values("name", "age")))
    reader = csv.reader(io.StringIO(blob.decode()))
    rows = list(reader)
    assert rows[0] == ["name", "age"]
    assert {tuple(r) for r in rows[1:]} == {("a0", "0"), ("a1", "1")}


def test_stream_csv_empty_with_columns():
    blob = _join(
        stream_csv(Author.objects.none(), columns=["name", "age"])
    )
    assert blob.decode().strip() == "name,age"


def test_stream_csv_empty_without_columns_emits_nothing():
    blob = _join(stream_csv(Author.objects.none()))
    assert blob == b""


def test_stream_jsonl_handles_special_types():
    """datetime / Decimal / UUID should serialise without crashing."""
    from datetime import datetime
    from decimal import Decimal
    from uuid import uuid4

    rows = [
        {"name": "x", "ts": datetime(2026, 1, 1, 12, 0), "p": Decimal("3.14"), "u": uuid4()},
    ]
    blob = _join(stream_jsonl(rows))
    parsed = json.loads(blob.decode().strip())
    assert parsed["name"] == "x"
    assert parsed["ts"].startswith("2026-01-01")
    assert parsed["p"] == "3.14"


def test_stream_ndjson_pretty_indents():
    rows = [{"a": 1, "b": 2}]
    blob = _join(stream_ndjson_pretty(rows))
    assert b"\n" in blob
    assert b"  " in blob  # indent


@pytest.mark.asyncio
async def test_astream_jsonl():
    await Author.objects.abulk_create(
        [Author(name=f"y{i}", age=i) for i in range(3)]
    )
    chunks = []
    async for c in astream_jsonl(Author.objects.all()):
        chunks.append(c)
    blob = _join(chunks)
    lines = blob.decode().strip().split("\n")
    assert len(lines) == 3


@pytest.mark.asyncio
async def test_astream_json():
    await Author.objects.abulk_create(
        [Author(name=f"z{i}", age=i) for i in range(2)]
    )
    chunks = []
    async for c in astream_json(Author.objects.all()):
        chunks.append(c)
    rows = json.loads(_join(chunks))
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_astream_csv():
    await Author.objects.abulk_create(
        [Author(name=f"w{i}", age=i) for i in range(2)]
    )
    chunks = []
    async for c in astream_csv(Author.objects.all().values("name", "age")):
        chunks.append(c)
    rows = list(csv.reader(io.StringIO(_join(chunks).decode())))
    assert rows[0] == ["name", "age"]
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_astream_handles_plain_async_iter():
    async def _gen():
        for i in range(3):
            yield {"i": i}

    chunks = []
    async for c in astream_jsonl(_gen()):
        chunks.append(c)
    assert _join(chunks).decode().count("\n") == 3
