"""Tests for v4 search additions: TrigramSimilarity,
TrigramWordSimilarity, search_index helper.
"""

from __future__ import annotations

import pytest

from dorm.exceptions import ImproperlyConfigured
from dorm.search import (
    SearchQuery,
    SearchVector,
    TrigramSimilarity,
    TrigramWordSimilarity,
    search_index,
)


def test_trigram_similarity_compiles():
    sql, params = TrigramSimilarity("name", "alice").as_sql()
    assert "similarity(" in sql
    assert "name" in sql
    assert params == ["alice"]


def test_trigram_similarity_validates_value_type():
    from typing import cast as _cast

    with pytest.raises(ImproperlyConfigured):
        TrigramSimilarity("name", _cast(str, 123))


def test_trigram_word_similarity_compiles():
    sql, params = TrigramWordSimilarity("alice", "name").as_sql()
    assert "word_similarity(" in sql
    assert params == ["alice"]


def test_search_index_emits_gin_create_index():
    sql = search_index("articles", "title", "body")
    assert "CREATE INDEX" in sql
    assert "USING GIN" in sql
    assert "to_tsvector" in sql
    assert "articles" in sql


def test_search_index_requires_fields():
    with pytest.raises(ImproperlyConfigured):
        search_index("articles")


def test_search_index_validates_table_name():
    with pytest.raises(ImproperlyConfigured):
        search_index("name; DROP TABLE x;--", "title")


def test_search_index_validates_field_names():
    with pytest.raises(ImproperlyConfigured):
        search_index("articles", "name; DROP TABLE x;--")


def test_search_index_custom_config():
    sql = search_index("articles", "body", config="spanish")
    assert "spanish" in sql


def test_search_index_custom_name():
    sql = search_index("articles", "title", name="my_search_idx")
    assert "my_search_idx" in sql


def test_search_vector_query_combine_for_match():
    # Smoke: SearchVector @@ SearchQuery compiles for a query.
    v = SearchVector("title")
    q = SearchQuery("postgres")
    v_sql, _ = v.as_sql()
    q_sql, _ = q.as_sql()
    assert "to_tsvector" in v_sql
    assert "tsquery" in q_sql
