"""Index helpers for pgvector columns.

Two index methods, two helper classes::

    class Document(dorm.Model):
        embedding = VectorField(dimensions=1536)

        class Meta:
            indexes = [
                # HNSW: best recall, slower build, larger on disk.
                HnswIndex(
                    fields=["embedding"],
                    name="doc_emb_hnsw",
                    opclass="vector_l2_ops",
                    m=16,
                    ef_construction=64,
                ),
                # IVFFlat: faster build, lower memory, recall depends
                # on ``lists`` and ``probes`` (set per-session via
                # ``SET ivfflat.probes = N`` before the query).
                IvfflatIndex(
                    fields=["embedding"],
                    name="doc_emb_ivf",
                    opclass="vector_cosine_ops",
                    lists=100,
                ),
            ]

The ``opclass`` argument MUST match the distance you query with —
``vector_l2_ops`` for :class:`L2Distance`, ``vector_cosine_ops``
for :class:`CosineDistance`, ``vector_ip_ops`` for
:class:`MaxInnerProduct`. A mismatched opclass means the query
planner can't use the index and silently falls back to a sequential
scan.
"""

from __future__ import annotations

from typing import Any

from ...indexes import Index


_VECTOR_OPCLASSES = frozenset(
    {"vector_l2_ops", "vector_cosine_ops", "vector_ip_ops"}
)


class _VectorIndexBase(Index):
    """Common construction for HNSW / IVFFlat index helpers.

    Both methods declare a single ``embedding`` column with one
    of the three pgvector opclasses, and accept method-specific
    storage parameters that get serialised into ``WITH (k = v, …)``
    by :meth:`Index.create_sql`.
    """

    method: str = ""

    def __init__(
        self,
        *,
        fields: list[str],
        name: str | None = None,
        opclass: str = "vector_l2_ops",
        condition: Any = None,
        **storage: Any,
    ) -> None:
        if len(fields) != 1:
            raise ValueError(
                f"{type(self).__name__} indexes a single vector "
                f"column; got {len(fields)} fields."
            )
        if opclass not in _VECTOR_OPCLASSES:
            raise ValueError(
                f"{type(self).__name__}(opclass={opclass!r}) — must be "
                f"one of {sorted(_VECTOR_OPCLASSES)}. Pick the one that "
                "matches the distance you query with."
            )
        super().__init__(
            fields=fields,
            name=name,
            method=self.method,
            opclasses=[opclass],
            condition=condition,
        )
        self.with_options: dict[str, Any] = dict(storage)


class HnswIndex(_VectorIndexBase):
    """HNSW (Hierarchical Navigable Small World) index for pgvector.

    Tuning knobs you'll most often touch:

    * ``m=`` — graph fan-out. Default 16. Higher = better recall +
      bigger index. Range typically 4-64.
    * ``ef_construction=`` — build-time search depth. Default 64.
      Higher = better recall + slower build.
    * Query-time recall vs latency is controlled by
      ``SET hnsw.ef_search = N`` (default 40); not part of the
      index definition itself.

    Build time is roughly linear in row count + ``ef_construction``;
    expect minutes for a million rows even on fast hardware.
    """

    method = "hnsw"


class IvfflatIndex(_VectorIndexBase):
    """IVFFlat (Inverted File with Flat compression) index for
    pgvector.

    Required tuning knob:

    * ``lists=`` — number of cluster centroids. Rule of thumb:
      ``rows / 1000`` for under 1M rows, ``sqrt(rows)`` for larger
      tables. The index needs at least one row per list at build
      time, so populate the table before creating the index.

    Query-time recall is tuned via ``SET ivfflat.probes = N`` (1
    to ``lists``; default 1). Higher = better recall + slower.

    Build is faster and the on-disk footprint smaller than HNSW,
    but recall plateaus lower. Use HNSW unless build time is a
    real constraint.
    """

    method = "ivfflat"
