"""Vendor-aware distance expressions.

Each class compiles to either:

* pgvector operators on PostgreSQL — ``col <-> %s`` (L2),
  ``col <=> %s`` (cosine), ``col <#> %s`` (negated inner product).
* sqlite-vec functions on SQLite — ``vec_distance_L2(col, ?)``,
  ``vec_distance_cosine(col, ?)``. sqlite-vec doesn't ship a
  negated-inner-product function, so :class:`MaxInnerProduct`
  raises on SQLite — use :class:`CosineDistance` over normalised
  embeddings instead.

Use as ``annotate`` values, then ``order_by`` the alias for kNN::

    from dorm.contrib.pgvector import L2Distance

    qs = (
        Document.objects
        .annotate(score=L2Distance("embedding", query_vec))
        .order_by("score")[:10]
    )

The annotation pipeline passes ``connection=connection`` to
:meth:`as_sql` (see :meth:`SQLQuery.as_select`), so each call
already has the vendor available — no global config or threadlocal
needed.
"""

from __future__ import annotations

import struct
from typing import Any, Iterable

from ...query import _validate_identifier


def _format_pgvector_literal(vec: Iterable[float]) -> str:
    """Serialise *vec* to pgvector's ``[v1,v2,…]`` text form."""
    tolist = getattr(vec, "tolist", None)
    if callable(tolist):
        vec = tolist()
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


def _pack_sqlite_vec(vec: Iterable[float]) -> bytes:
    """Pack *vec* as little-endian float32 for sqlite-vec BLOB
    binding."""
    tolist = getattr(vec, "tolist", None)
    if callable(tolist):
        vec = tolist()
    seq = [float(x) for x in vec]
    return struct.pack(f"<{len(seq)}f", *seq)


class _VectorDistance:
    """Common machinery for the three vector-distance expressions.

    Subclasses provide a per-vendor SQL fragment:

    * :attr:`pg_operator` — pgvector binary operator.
    * :attr:`sqlite_function` — sqlite-vec scalar function name,
      or ``None`` to mark the distance as unsupported on SQLite.

    :meth:`as_sql` reads ``connection.vendor`` from the
    ``connection=`` kwarg the annotation pipeline supplies and
    routes accordingly.
    """

    pg_operator: str = ""
    sqlite_function: str | None = None
    # libsql ships native ``vector_distance_*`` functions over its
    # ``F32_BLOB(N)`` columns. Subclasses set the function name
    # (or ``None`` to mark the distance as unsupported there).
    libsql_function: str | None = None

    def __init__(self, column: str, vector: Iterable[float]) -> None:
        # Column names go through identifier validation so a
        # ``L2Distance("evil; DROP TABLE")`` can't reach the SQL.
        _validate_identifier(column, kind="vector column")
        self.column = column
        self.vector = vector

    def as_sql(
        self, table_alias: str | None = None, **kwargs: Any
    ) -> tuple[str, list]:
        connection = kwargs.get("connection")
        vendor = getattr(connection, "vendor", "postgresql")
        col = (
            f'"{table_alias}"."{self.column}"'
            if table_alias
            else f'"{self.column}"'
        )
        if vendor == "libsql":
            # libsql ships native ``vector_distance_l2`` /
            # ``vector_distance_cos`` SQL functions over
            # ``F32_BLOB(N)`` columns. The names are lowercase and
            # the cosine variant uses ``cos`` (not ``cosine``).
            # Inner-product is not yet exposed; fall back to
            # cosine over normalised vectors as documented.
            fn = self.libsql_function
            if fn is None:
                raise NotImplementedError(
                    f"{type(self).__name__} is not supported on "
                    "libsql. Use CosineDistance over L2-normalised "
                    "embeddings instead."
                )
            return f"{fn}({col}, vector32(?))", [
                _pack_sqlite_vec(self.vector)
            ]
        if vendor == "sqlite":
            if self.sqlite_function is None:
                raise NotImplementedError(
                    f"{type(self).__name__} is not supported on "
                    "sqlite-vec. Use CosineDistance over "
                    "L2-normalised embeddings instead."
                )
            return f"{self.sqlite_function}({col}, %s)", [
                _pack_sqlite_vec(self.vector)
            ]
        return f"{col} {self.pg_operator} %s::vector", [
            _format_pgvector_literal(self.vector)
        ]


class L2Distance(_VectorDistance):
    """Euclidean (L2) distance.

    * pgvector: ``col <-> %s``. Pair with ``vector_l2_ops``.
    * sqlite-vec: ``vec_distance_L2(col, %s)``.
    * libsql: ``vector_distance_l2(col, vector32(?))``.

    Smaller = more similar.
    """

    pg_operator = "<->"
    sqlite_function = "vec_distance_L2"
    libsql_function = "vector_distance_l2"


class CosineDistance(_VectorDistance):
    """Cosine distance (``1 - cosine_similarity``).

    * pgvector: ``col <=> %s``. Pair with ``vector_cosine_ops``.
    * sqlite-vec: ``vec_distance_cosine(col, %s)``.
    * libsql: ``vector_distance_cos(col, vector32(?))``.

    Smaller = more similar. On L2-normalised embeddings this is
    equivalent to :class:`MaxInnerProduct` and works on every
    backend.
    """

    pg_operator = "<=>"
    sqlite_function = "vec_distance_cosine"
    libsql_function = "vector_distance_cos"


class MaxInnerProduct(_VectorDistance):
    """Negated inner product.

    * pgvector: ``col <#> %s``. Pair with ``vector_ip_ops``.
    * sqlite-vec: **not supported** — sqlite-vec doesn't ship a
      negated-inner-product function.
    * libsql: **not supported** today — fall back to
      :class:`CosineDistance` over L2-normalised embeddings.

    pgvector returns ``-inner_product`` so that ``ORDER BY ASC``
    still puts the most-similar rows first.
    """

    pg_operator = "<#>"
    sqlite_function = None
    libsql_function = None
