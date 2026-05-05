"""Geographic information system (GIS) types for djanorm.

Wraps PostGIS (on PostgreSQL) and SpatiaLite (on SQLite ≥ 3.34 with
the SpatiaLite extension loaded). Provides:

- :class:`PointField` — single ``(x, y)`` coordinate.
- :class:`PolygonField` — closed ring of coordinates.
- :class:`LineStringField` — ordered sequence of coordinates.
- :class:`GeometryField` — generic geometry; accepts any of the above.
- :class:`Geom` value type — Pythonic GeoJSON-shaped wrapper.

Spatial lookups (resolved at queryset compile time) include:

- ``__intersects`` — bounding-box / geometry overlap.
- ``__within`` — geometry fully contained in the lookup value.
- ``__contains`` — geometry fully contains the lookup value.
- ``__distance_lte`` / ``__distance_gte`` — distance comparison
  (units depend on the underlying SRID).

The PostGIS extension must be installed separately (``apt install
postgresql-XX-postgis-3``); this package only ships the Python side.
For SQLite, ``mod_spatialite`` must be loaded on every connection;
see ``Geom.bootstrap_sqlite()``.

Like every other ``dorm.contrib`` module, GIS is opt-in: nothing
imports it unless the user does. Models that don't reference GIS
fields pay zero cost.
"""

from __future__ import annotations

from .fields import (
    GeometryField,
    Geom,
    LineStringField,
    PointField,
    PolygonField,
)
from .lookups import (
    GISLookupMixin,
    register_gis_lookups,
)

# Auto-register lookups at import time so users don't have to remember.
register_gis_lookups()

__all__ = [
    "Geom",
    "GeometryField",
    "GISLookupMixin",
    "LineStringField",
    "PointField",
    "PolygonField",
]
