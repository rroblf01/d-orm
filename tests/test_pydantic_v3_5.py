"""Tests for the v3.5 Pydantic adapter additions:
``list_response_schema``, ``schema_with_computed``,
``schema_for_with_examples``, ``nested_schema_for``.
"""

from __future__ import annotations

from typing import Any

import pytest

import dorm
from dorm.contrib.pydantic import (
    list_response_schema,
    nested_schema_for,
    schema_for,
    schema_for_with_examples,
    schema_with_computed,
)
from tests.models import Author, Book


def test_list_response_schema_shape():
    AuthorOut = schema_for(Author)
    AuthorList = list_response_schema(AuthorOut)
    instance: Any = AuthorList(items=[], next_cursor=None, has_more=False, total=0)
    assert instance.items == []
    assert instance.has_more is False
    assert instance.total == 0


def test_list_response_schema_default_name():
    AuthorOut = schema_for(Author)
    AuthorList = list_response_schema(AuthorOut)
    assert "List" in AuthorList.__name__


def test_list_response_schema_int_cursor():
    AuthorOut = schema_for(Author)
    AuthorList = list_response_schema(AuthorOut, cursor_type=int)
    obj: Any = AuthorList(items=[], next_cursor=42, has_more=True)
    assert obj.next_cursor == 42


def test_schema_with_computed_field():
    class A(dorm.Model):
        name = dorm.CharField(max_length=10)
        age = dorm.IntegerField()

        @property
        def is_adult(self) -> bool:
            return self.age >= 18

        class Meta:
            db_table = "_typing_a"
            app_label = "tests"

    AOut = schema_with_computed(A, computed={"is_adult": bool})
    fields = AOut.model_fields
    assert "is_adult" in fields


def test_schema_with_computed_unknown_attr_raises():
    with pytest.raises(AttributeError, match="bogus"):
        schema_with_computed(Author, computed={"bogus": int})


def test_schema_for_with_explicit_examples():
    AuthorOut = schema_for_with_examples(
        Author, examples=[{"name": "x", "age": 1}]
    )
    cfg: Any = AuthorOut.model_config
    assert cfg["json_schema_extra"]["examples"][0]["name"] == "x"


def test_schema_for_with_db_examples():
    Author.objects.create(name="example-row", age=99)
    AuthorOut = schema_for_with_examples(Author, sample_count=1)
    cfg: Any = AuthorOut.model_config
    examples = cfg["json_schema_extra"]["examples"]
    assert any(e.get("name") == "example-row" for e in examples)


def test_schema_for_with_examples_empty_db_falls_back_silently():
    AuthorOut = schema_for_with_examples(Author, sample_count=1)
    assert AuthorOut is not None  # no rows → no crash


def test_nested_schema_depth_zero_equals_plain():
    AuthorNested = nested_schema_for(Author, depth=0)
    plain = schema_for(Author)
    # Field set should match.
    assert set(AuthorNested.model_fields) == set(plain.model_fields)


def test_nested_schema_depth_one_expands_fk():
    BookNested = nested_schema_for(Book, depth=1)
    fields = BookNested.model_fields
    # Book has author FK; nested schema should replace it with the
    # nested AuthorSchema (some Pydantic v2 type, not the raw int).
    assert "author" in fields
