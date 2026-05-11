"""Vector + full-text rerank — combine pgvector similarity with
PostgreSQL FTS rank in a single query.

Typical hybrid-search pattern: cast a wide net with FTS, then
re-order the top N by vector cosine distance. This module ships a
helper that emits the canonical SQL shape::

    rerank(
        Document,
        vector_field="embedding",
        text_field_tsv="search_vector",
        query_vector=embedding,
        query_text="machine learning",
        candidates=200,
        weight_vector=0.7,
    )

Returns a queryset annotated with ``rerank_score`` ordered descending.

PostgreSQL-only.
"""
from __future__ import annotations

from typing import Any


def rerank(
    model_cls: Any,
    *,
    vector_field: str,
    text_field_tsv: str,
    query_vector: list[float],
    query_text: str,
    candidates: int = 100,
    weight_vector: float = 0.5,
    weight_text: float | None = None,
    distance: str = "cosine",
    using: str = "default",
) -> list[Any]:
    """Run an FTS + vector hybrid query against *model_cls*.

    Strategy: pre-filter via ``websearch_to_tsquery`` on the
    pre-computed ``tsvector`` column (``text_field_tsv``), take the
    top *candidates* by ``ts_rank``, then re-order by a weighted
    combination of the FTS rank and the vector distance.

    ``weight_text`` defaults to ``1 - weight_vector``. Higher
    weights mean *more important* in the final score.
    """
    from ..pgvector.expressions import (
        CosineDistance,
        L2Distance,
        MaxInnerProduct,
    )

    if weight_text is None:
        weight_text = 1.0 - weight_vector

    dist_cls = {
        "cosine": CosineDistance,
        "l2": L2Distance,
        "ip": MaxInnerProduct,
    }.get(distance)
    if dist_cls is None:
        raise ValueError(
            f"rerank: distance must be 'cosine' / 'l2' / 'ip'; got {distance!r}"
        )

    from ..pgvector.expressions import _format_pgvector_literal

    vec_lit = _format_pgvector_literal(query_vector)
    table = model_cls._meta.db_table
    sql = (
        f'WITH cand AS ( '
        f'  SELECT *, ts_rank("{text_field_tsv}", websearch_to_tsquery(%s)) '
        f'  AS rank_text FROM "{table}" '
        f'  WHERE "{text_field_tsv}" @@ websearch_to_tsquery(%s) '
        f'  ORDER BY rank_text DESC LIMIT %s '
        f') '
        f'SELECT *, '
        f'(%s * rank_text) - (%s * ("{vector_field}" <-> %s::vector)) '
        f'AS rerank_score '
        f'FROM cand ORDER BY rerank_score DESC'
    )
    params = [
        query_text,
        query_text,
        candidates,
        weight_text,
        weight_vector,
        vec_lit,
    ]
    from ...db.connection import get_connection

    conn = get_connection(using)
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            "rerank() is PostgreSQL-only (uses tsvector + pgvector)."
        )
    return conn.execute(sql, params)


__all__ = ["rerank"]
