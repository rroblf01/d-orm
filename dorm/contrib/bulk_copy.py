"""High-level ``COPY FROM/TO`` helpers for PostgreSQL.

PostgreSQL's ``COPY`` protocol is the only practical way to load tens of
thousands of rows in one round-trip — ``bulk_create`` issues a single
multi-row ``INSERT`` that scales until the SQL parser runs out of
parameters (~32k). For ETL / seeding / replication, ``COPY`` is
typically 10-100× faster.

These helpers wrap the low-level ``copy_from`` / ``copy_to`` methods on
the PostgreSQL backend wrapper and add field-aware value preparation,
so callers can pass model instances or dicts without thinking about
``get_db_prep_value`` themselves.

Synchronous and asynchronous variants share the same shape::

    from dorm.contrib.bulk_copy import bulk_copy_from, abulk_copy_from

    bulk_copy_from(Author, [Author(name=…, email=…) for _ in range(N)])
    await abulk_copy_from(Author, generator())

Returns the number of rows written. Raises ``NotImplementedError`` on
non-PostgreSQL backends — the dialect-specific call has no portable
equivalent (MySQL's ``LOAD DATA`` and SQLite's ``.import`` work very
differently).
"""

from __future__ import annotations

from typing import Any, Iterable

from ..db.connection import get_async_connection, get_connection, router_db_for_write
from ..exceptions import ImproperlyConfigured


def _resolve_columns_and_fields(
    model: Any, columns: list[str] | None
) -> tuple[list[str], list[Any]]:
    """Return (column_names, field_objects) for the COPY frame.

    When *columns* is ``None`` the helper picks every concrete field
    that has a database column, dropping ``AutoField`` (the database
    fills it server-side). Caller-supplied *columns* are validated
    against the model so a typo raises early.
    """
    from ..fields import AutoField

    meta = model._meta
    if columns is None:
        return (
            [f.column for f in meta.fields if f.column and not isinstance(f, AutoField)],
            [f for f in meta.fields if f.column and not isinstance(f, AutoField)],
        )
    cols: list[str] = []
    fields: list[Any] = []
    for name in columns:
        try:
            f = meta.get_field(name)
        except Exception as exc:
            raise ValueError(
                f"bulk_copy_from(columns=…): unknown field {name!r} on {model.__name__}"
            ) from exc
        if not f.column:
            raise ValueError(
                f"bulk_copy_from(columns=…): field {name!r} has no DB column."
            )
        cols.append(f.column)
        fields.append(f)
    return cols, fields


def _row_from_obj(obj: Any, fields: list[Any]) -> tuple:
    """Extract a tuple of DB-prepared values from a model instance, dict,
    or already-aligned sequence."""
    if isinstance(obj, dict):
        return tuple(
            f.get_db_prep_value(obj.get(f.attname, obj.get(f.column, f.get_default())))
            for f in fields
        )
    if isinstance(obj, (tuple, list)):
        # Caller did the prep themselves — trust the values.
        return tuple(obj)
    return tuple(
        f.get_db_prep_value(obj.__dict__.get(f.attname, f.get_default()))
        for f in fields
    )


def _ensure_postgres(conn: Any) -> None:
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            "bulk_copy_from() is PostgreSQL-only — the COPY protocol has "
            "no portable equivalent on other backends. Use bulk_create() "
            "instead, or switch the alias to a PG database."
        )


def bulk_copy_from(
    model: Any,
    objs: Iterable[Any],
    *,
    columns: list[str] | None = None,
    using: str | None = None,
    binary: bool = False,
) -> int:
    """Stream *objs* into ``model``'s table via PostgreSQL ``COPY FROM STDIN``.

    Each item in *objs* may be a model instance, a dict keyed by field
    ``attname`` / column name, or a pre-prepared tuple aligned with
    *columns*. Returns the number of rows written.
    """
    if model is None or getattr(model, "_meta", None) is None:
        raise ImproperlyConfigured("bulk_copy_from(): a Model class is required.")
    alias = using or router_db_for_write(model, default="default")
    conn = get_connection(alias)
    _ensure_postgres(conn)

    cols, fields = _resolve_columns_and_fields(model, columns)
    table = model._meta.db_table

    def _row_iter():
        for obj in objs:
            yield _row_from_obj(obj, fields)

    return conn.copy_from(table, cols, _row_iter(), binary=binary)


async def abulk_copy_from(
    model: Any,
    objs: Any,
    *,
    columns: list[str] | None = None,
    using: str | None = None,
    binary: bool = False,
) -> int:
    """Async counterpart of :func:`bulk_copy_from`. Accepts both sync and
    async iterables of model instances / dicts / tuples."""
    if model is None or getattr(model, "_meta", None) is None:
        raise ImproperlyConfigured("abulk_copy_from(): a Model class is required.")
    alias = using or router_db_for_write(model, default="default")
    conn = get_async_connection(alias)
    _ensure_postgres(conn)

    cols, fields = _resolve_columns_and_fields(model, columns)
    table = model._meta.db_table

    if hasattr(objs, "__aiter__"):
        async def _aiter():
            async for obj in objs:
                yield _row_from_obj(obj, fields)

        return await conn.copy_from(table, cols, _aiter(), binary=binary)

    def _iter():
        for obj in objs:
            yield _row_from_obj(obj, fields)

    return await conn.copy_from(table, cols, _iter(), binary=binary)


def copy_to(
    sql: str,
    params: list[Any] | None = None,
    *,
    using: str = "default",
    binary: bool = False,
):
    """Yield rows from ``COPY (<sql>) TO STDOUT``. *sql* is a plain
    ``SELECT`` — the helper wraps it. Useful for full-table exports."""
    conn = get_connection(using)
    _ensure_postgres(conn)
    yield from conn.copy_to(sql, params, binary=binary)


async def acopy_to(
    sql: str,
    params: list[Any] | None = None,
    *,
    using: str = "default",
    binary: bool = False,
):
    """Async generator counterpart of :func:`copy_to`."""
    conn = get_async_connection(using)
    _ensure_postgres(conn)
    async for row in conn.copy_to(sql, params, binary=binary):
        yield row


__all__ = [
    "bulk_copy_from",
    "abulk_copy_from",
    "copy_to",
    "acopy_to",
]
