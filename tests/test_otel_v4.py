"""Tests for the v4 OpenTelemetry instrumentation enrichments.

We exercise the helper functions directly (no live tracer required).
The instrumented signal flow is covered by the existing ``otel`` test
file in the repo's broader suite.
"""

from __future__ import annotations

import pytest

from dorm.contrib.otel import _classify_operation, _extract_table


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT * FROM authors", "SELECT"),
        ("  select 1", "SELECT"),
        ("/* hint */ SELECT id FROM x", "SELECT"),
        ("INSERT INTO authors (name) VALUES (%s)", "INSERT"),
        ("update authors set age=10", "UPDATE"),
        ("DELETE FROM authors WHERE 1=0", "DELETE"),
        ("COPY authors (name) FROM STDIN", "COPY"),
        ("CREATE TABLE x (id INT)", "CREATE"),
        ("ALTER TABLE x ADD COLUMN y INT", "ALTER"),
        ("DROP INDEX x", "DROP"),
        ("MERGE INTO target USING src ON ...", "MERGE"),
        ("", None),
        ("BEGIN", None),
    ],
)
def test_classify_operation(sql, expected):
    assert _classify_operation(sql) == expected


@pytest.mark.parametrize(
    "sql,op,expected",
    [
        ('SELECT * FROM "authors"', "SELECT", "authors"),
        ("SELECT a FROM authors WHERE 1=1", "SELECT", "authors"),
        ('INSERT INTO "books" (title) VALUES (%s)', "INSERT", "books"),
        ("UPDATE authors SET age=1", "UPDATE", "authors"),
        ('DELETE FROM "authors" WHERE id=1', "DELETE", "authors"),
        ('COPY "authors" (name) FROM STDIN', "COPY", "authors"),
        ("SELECT 1", "SELECT", None),
        ("nonsense", "SELECT", None),
    ],
)
def test_extract_table(sql, op, expected):
    assert _extract_table(sql, op) == expected


def test_extract_table_handles_no_op():
    assert _extract_table("SELECT 1", None) is None
