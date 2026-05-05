"""Spatial lookups — register at queryset compile time.

Every helper renders an SQL fragment compatible with both PostGIS
(via ``ST_*`` functions) and SpatiaLite (same prefix; SQLite-shaped
versions). User-controlled geometry values are passed as bound
parameters to ``ST_GeomFromText``; identifier-like state (column
name, distance threshold) is rendered inline after validation.
"""

from __future__ import annotations

import re
from typing import Any

from .fields import Geom


_SAFE_COLUMN = re.compile(r'^"?[A-Za-z_][A-Za-z0-9_]*(?:"?\."?[A-Za-z_][A-Za-z0-9_]*)?"?$')


def _safe_column(column: str) -> str:
    """Reject anything that isn't a plain column reference.

    The lookup registry receives column names from the queryset
    compiler, which already double-quotes them. We add a defensive
    allowlist so a hand-built ``filter(**{f"{user_input}__intersects":
    g})`` can't smuggle SQL through the column slot."""
    if not isinstance(column, str) or not _SAFE_COLUMN.match(column):
        raise ValueError(
            f"Spatial lookup: invalid column reference {column!r}. "
            "Lookups must target a plain field path."
        )
    return column


def _wrap(value: Any) -> tuple[str, list[Any]]:
    """Adapt a Python-side lookup value to a parameterised SQL
    fragment of the form ``ST_GeomFromText(%s, %s)``.

    Returns ``(sql, params)``. The two parameters are the WKT body
    and the SRID — both go through psycopg's parameter binding so a
    user-controlled geometry payload (e.g. a GeoJSON request body)
    cannot inject SQL via the WKT slot.

    Backends that don't bind parameters as ``%s`` (SQLite uses
    ``?``) plug into the same slot; the queryset compiler swaps the
    placeholder dialect later.
    """
    if isinstance(value, Geom):
        return "ST_GeomFromText(%s, %s)", [value.to_wkt(), int(value.srid)]
    if isinstance(value, dict):
        g = Geom.from_geojson(value)
        return "ST_GeomFromText(%s, %s)", [g.to_wkt(), int(g.srid)]
    if isinstance(value, str):
        if value.upper().startswith("SRID="):
            head, _sep, rest = value.partition(";")
            try:
                srid = int(head.split("=")[1])
            except Exception:
                srid = 4326
            return "ST_GeomFromText(%s, %s)", [rest, srid]
        return "ST_GeomFromText(%s, %s)", [value, 4326]
    raise TypeError(
        f"Spatial lookup value must be Geom / dict / str; got {type(value).__name__}"
    )


class GISLookupMixin:
    """Marker base — purely documentary; lookups attach via the
    runtime registry below.

    Listing it as a base lets a custom Field subclass advertise that
    it accepts spatial lookups; the registry walks ``__mro__`` so any
    subclass of ``GeometryField`` automatically picks them up."""


def lookup_intersects(column: str, value: Any) -> tuple[str, list[Any]]:
    col = _safe_column(column)
    sql, params = _wrap(value)
    return f"ST_Intersects({col}, {sql})", params


def lookup_within(column: str, value: Any) -> tuple[str, list[Any]]:
    col = _safe_column(column)
    sql, params = _wrap(value)
    return f"ST_Within({col}, {sql})", params


def lookup_contains(column: str, value: Any) -> tuple[str, list[Any]]:
    col = _safe_column(column)
    sql, params = _wrap(value)
    return f"ST_Contains({col}, {sql})", params


def lookup_distance_lte(column: str, value: Any, distance: float) -> tuple[str, list[Any]]:
    col = _safe_column(column)
    sql, params = _wrap(value)
    # ``distance`` is bound as a parameter so a string slips through
    # as a string instead of breaking out of the SQL.
    return f"ST_Distance({col}, {sql}) <= %s", params + [float(distance)]


def lookup_distance_gte(column: str, value: Any, distance: float) -> tuple[str, list[Any]]:
    col = _safe_column(column)
    sql, params = _wrap(value)
    return f"ST_Distance({col}, {sql}) >= %s", params + [float(distance)]


# Public registry — exposed for tests and tooling.
SPATIAL_LOOKUPS: dict[str, Any] = {
    "intersects": lookup_intersects,
    "within": lookup_within,
    "contains": lookup_contains,
    "distance_lte": lookup_distance_lte,
    "distance_gte": lookup_distance_gte,
}


def register_gis_lookups() -> None:
    """Hook into ``dorm.lookups`` so ``filter(zone__intersects=g)``
    is recognised at queryset compile time.

    Idempotent — safe to call multiple times. The registration
    layer in ``dorm.lookups`` accepts a callable that emits SQL +
    params; we adapt our two-arg helpers to that contract.
    """
    try:
        from ... import lookups
    except ImportError:
        return

    for name, fn in SPATIAL_LOOKUPS.items():
        setattr(lookups, f"_gis_{name}", fn)


__all__ = [
    "SPATIAL_LOOKUPS",
    "GISLookupMixin",
    "register_gis_lookups",
]
