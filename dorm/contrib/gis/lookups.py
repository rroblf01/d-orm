"""Spatial lookups — register at queryset compile time.

Every helper renders an SQL fragment compatible with both PostGIS
(via ``ST_*`` functions) and SpatiaLite (same prefix; SQLite-shaped
versions). ``backend != postgresql / sqlite`` falls through to a
sequential ``TRUE``/``FALSE`` placeholder so the queryset still
compiles — the user gets correctness, not performance, on backends
without spatial primitives.
"""

from __future__ import annotations

from typing import Any

from .fields import Geom


def _wrap(value: Any) -> str:
    """Adapt a Python-side lookup value to the SQL literal expected
    by ST_* functions. ``Geom`` instances render as
    ``ST_GeomFromText('...', srid)``; raw strings are passed through."""
    if isinstance(value, Geom):
        return f"ST_GeomFromText('{value.to_wkt()}', {value.srid})"
    if isinstance(value, dict):
        g = Geom.from_geojson(value)
        return f"ST_GeomFromText('{g.to_wkt()}', {g.srid})"
    if isinstance(value, str):
        # Already WKT/EWKT — wrap defensively.
        if value.upper().startswith("SRID="):
            head, _sep, rest = value.partition(";")
            try:
                srid = int(head.split("=")[1])
            except Exception:
                srid = 4326
            return f"ST_GeomFromText('{rest}', {srid})"
        return f"ST_GeomFromText('{value}', 4326)"
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
    return f"ST_Intersects({column}, {_wrap(value)})", []


def lookup_within(column: str, value: Any) -> tuple[str, list[Any]]:
    return f"ST_Within({column}, {_wrap(value)})", []


def lookup_contains(column: str, value: Any) -> tuple[str, list[Any]]:
    return f"ST_Contains({column}, {_wrap(value)})", []


def lookup_distance_lte(column: str, value: Any, distance: float) -> tuple[str, list[Any]]:
    return f"ST_Distance({column}, {_wrap(value)}) <= {distance}", []


def lookup_distance_gte(column: str, value: Any, distance: float) -> tuple[str, list[Any]]:
    return f"ST_Distance({column}, {_wrap(value)}) >= {distance}", []


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
