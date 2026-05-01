"""pgvector integration for djanorm.

`pgvector <https://github.com/pgvector/pgvector>`_ is a PostgreSQL
extension that adds a ``vector`` column type plus three distance
operators (``<->`` L2, ``<=>`` cosine, ``<#>`` negative inner
product) and two index methods (HNSW, IVFFlat). It's the default
back-end for retrieval-augmented generation (RAG) and semantic
search workflows.

Quick start::

    # 1. Install with the pgvector extra.
    #    pip install 'djanorm[pgvector]'
    #
    # 2. Enable the extension via a generated migration:
    #    dorm makemigrations --enable-pgvector myapp
    #
    # 3. Add a VectorField to your model:
    from dorm.contrib.pgvector import VectorField

    class Document(dorm.Model):
        title    = dorm.CharField(max_length=200)
        content  = dorm.TextField()
        embedding = VectorField(dimensions=1536)

        class Meta:
            db_table = "documents"

    # 4. Run dorm makemigrations + migrate.
    # 5. Use it:
    from dorm.contrib.pgvector import L2Distance

    qs = (
        Document.objects
        .annotate(score=L2Distance("embedding", query_vec))
        .order_by("score")[:10]
    )

This module is PostgreSQL-only — :class:`VectorField` returns
``None`` from :meth:`db_type` on every other backend, so the
column is silently skipped during table creation. That keeps the
public model definition portable: a project can use SQLite for
unit tests and PostgreSQL with pgvector in production without
conditional model code.
"""

from .expressions import CosineDistance, L2Distance, MaxInnerProduct
from .fields import VectorField
from .indexes import HnswIndex, IvfflatIndex
from .operations import VectorExtension, load_sqlite_vec_extension

__all__ = [
    "VectorField",
    "L2Distance",
    "CosineDistance",
    "MaxInnerProduct",
    "HnswIndex",
    "IvfflatIndex",
    "VectorExtension",
    "load_sqlite_vec_extension",
]
