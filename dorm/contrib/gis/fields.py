"""Geometry value type and Field subclasses for the GIS contrib."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from ...fields import Field


@dataclass(frozen=True)
class Geom:
    """Pythonic geometry value.

    *kind* is the GeoJSON-style geometry type: ``"Point"``,
    ``"Polygon"``, ``"LineString"``, etc. *coordinates* follows the
    same shape rules as GeoJSON — ``[x, y]`` for Point, a list of
    rings for Polygon, etc. *srid* is the spatial reference system
    identifier (default 4326 = WGS84 long/lat).

    The runtime stores the value as Well-Known Text (WKT) when
    talking to the database — both PostGIS and SpatiaLite accept WKT
    via standard text adapters and avoid binary protocol drift.
    """

    kind: str
    coordinates: Any
    srid: int = 4326

    def to_wkt(self) -> str:
        """Render this geometry as Well-Known Text (without the SRID
        prefix). Use :meth:`to_ewkt` for ``SRID=4326;POINT(...)``
        encoding accepted by PostGIS' geometry type."""
        return _wkt(self.kind, self.coordinates)

    def to_ewkt(self) -> str:
        return f"SRID={self.srid};{self.to_wkt()}"

    def to_geojson(self) -> dict:
        return {"type": self.kind, "coordinates": self.coordinates}

    @classmethod
    def from_geojson(cls, blob: str | dict) -> "Geom":
        if isinstance(blob, str):
            blob = json.loads(blob)
        return cls(kind=blob["type"], coordinates=blob["coordinates"])

    @classmethod
    def point(cls, x: float, y: float, *, srid: int = 4326) -> "Geom":
        return cls(kind="Point", coordinates=[x, y], srid=srid)

    @classmethod
    def polygon(cls, rings: list[list[list[float]]], *, srid: int = 4326) -> "Geom":
        return cls(kind="Polygon", coordinates=rings, srid=srid)

    @classmethod
    def linestring(cls, points: list[list[float]], *, srid: int = 4326) -> "Geom":
        return cls(kind="LineString", coordinates=points, srid=srid)

    @staticmethod
    def bootstrap_sqlite(connection: Any) -> None:
        """Load ``mod_spatialite`` into a SQLite connection so its
        spatial functions become available. Idempotent — calling
        twice is a no-op.

        The extension path defaults to whatever the system provides
        as ``mod_spatialite``; on Debian / Ubuntu install
        ``libsqlite3-mod-spatialite``."""
        sqlite3 = getattr(connection, "_dbapi_connection", connection)
        try:
            sqlite3.enable_load_extension(True)
            sqlite3.load_extension("mod_spatialite")
            sqlite3.enable_load_extension(False)
        except Exception:
            # Best-effort — many SQLite builds disable extension
            # loading for security. The error is the user's problem
            # to handle (install the extension, rebuild SQLite, etc.).
            raise


def _wkt(kind: str, coords: Any) -> str:
    if kind == "Point":
        x, y = coords
        return f"POINT({x} {y})"
    if kind == "LineString":
        body = ",".join(f"{p[0]} {p[1]}" for p in coords)
        return f"LINESTRING({body})"
    if kind == "Polygon":
        rings = []
        for ring in coords:
            rings.append("(" + ",".join(f"{p[0]} {p[1]}" for p in ring) + ")")
        return f"POLYGON({','.join(rings)})"
    raise ValueError(f"Unsupported geometry kind {kind!r} for WKT encoding.")


# ── Field subclasses ─────────────────────────────────────────────────────────


class GeometryField(Field):
    """Generic geometry column — holds Point / Polygon / LineString.

    Backend mapping:

    - PostgreSQL: ``GEOMETRY(<kind>, <srid>)`` (PostGIS).
    - SQLite: ``BLOB`` with mod_spatialite-managed serialisation.
    - Other backends: ``TEXT`` (WKT). Functional but not optimised —
      every spatial query becomes a sequential scan.
    """

    _gis_kind = "GEOMETRY"

    def __init__(self, *args, srid: int = 4326, dim: int = 2, **kwargs):
        if dim not in (2, 3):
            raise ValueError("GeometryField.dim must be 2 or 3.")
        self.srid = srid
        self.dim = dim
        super().__init__(*args, **kwargs)

    def db_type(self, connection: Any) -> str | None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            return f"GEOMETRY({self._gis_kind}, {self.srid})"
        if vendor in ("sqlite", "libsql"):
            # Spatialite stores geometries as BLOB; the actual
            # ``AddGeometryColumn`` declaration is a follow-up SQL
            # call. We surface BLOB here so basic CRUD works even
            # without the spatial extension loaded.
            return "BLOB"
        return "TEXT"

    def get_db_prep_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Geom):
            return value.to_ewkt()
        if isinstance(value, str):
            return value  # already WKT/EWKT
        if isinstance(value, dict):
            return Geom.from_geojson(value).to_ewkt()
        raise TypeError(
            f"GeometryField cannot adapt value of type {type(value).__name__}; "
            "pass a Geom, dict (GeoJSON) or WKT string."
        )

    def from_db_value(self, value: Any) -> Any:
        if value is None or isinstance(value, Geom):
            return value
        if isinstance(value, str):
            return _parse_wkt(value)
        return value


class PointField(GeometryField):
    _gis_kind = "POINT"


class PolygonField(GeometryField):
    _gis_kind = "POLYGON"


class LineStringField(GeometryField):
    _gis_kind = "LINESTRING"


def _parse_wkt(s: str) -> Geom:
    """Tiny EWKT parser — only the kinds we declare above. Falls
    back to returning the raw string if the shape isn't recognised
    (some PostGIS responses come back as hex EWKB; the user can
    decode those via ``shapely`` if needed)."""
    body = s
    srid = 4326
    if body.upper().startswith("SRID="):
        head, _sep, rest = body.partition(";")
        try:
            srid = int(head.split("=")[1])
        except Exception:
            pass
        body = rest
    body = body.strip()
    upper = body.upper()
    if upper.startswith("POINT"):
        inside = body[body.index("(") + 1 : body.rindex(")")]
        x, y = (float(p) for p in inside.split())
        return Geom(kind="Point", coordinates=[x, y], srid=srid)
    if upper.startswith("LINESTRING"):
        inside = body[body.index("(") + 1 : body.rindex(")")]
        pts = [
            [float(c) for c in pair.strip().split()]
            for pair in inside.split(",")
        ]
        return Geom(kind="LineString", coordinates=pts, srid=srid)
    if upper.startswith("POLYGON"):
        # Strip "POLYGON" prefix; remaining shape is "((..),(..))".
        inside = body[body.index("(") + 1 : body.rindex(")")]
        rings_text = inside.strip()
        rings: list[list[list[float]]] = []
        # Split top-level rings on "),("
        depth = 0
        buf: list[str] = []
        for ch in rings_text:
            if ch == "(":
                depth += 1
                if depth == 1:
                    buf = []
                    continue
            if ch == ")":
                depth -= 1
                if depth == 0:
                    raw = "".join(buf)
                    rings.append(
                        [
                            [float(c) for c in pair.strip().split()]
                            for pair in raw.split(",")
                        ]
                    )
                    continue
            if depth >= 1:
                buf.append(ch)
        return Geom(kind="Polygon", coordinates=rings, srid=srid)
    return Geom(kind="Unknown", coordinates=body, srid=srid)


__all__ = [
    "Geom",
    "GeometryField",
    "PointField",
    "PolygonField",
    "LineStringField",
]
_ = field, dataclass  # silence unused-import linter (used in @dataclass)
