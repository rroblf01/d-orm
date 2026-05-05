"""Tests for ``dorm.contrib.gis``.

Geom value type and Field machinery are unit-tested without a real
spatial backend. End-to-end PostGIS / SpatiaLite integration tests
are out-of-scope here (require an extension preinstalled in the
container) but the WKT round-trip is covered.
"""

from __future__ import annotations

import json

import pytest

from dorm.contrib.gis import (
    Geom,
    GeometryField,
    LineStringField,
    PointField,
    PolygonField,
)
from dorm.contrib.gis.fields import _parse_wkt
from dorm.contrib.gis.lookups import SPATIAL_LOOKUPS


def test_point_to_wkt():
    p = Geom.point(1.5, 2.5)
    assert p.to_wkt() == "POINT(1.5 2.5)"


def test_point_to_ewkt_with_srid():
    p = Geom.point(0, 0, srid=3857)
    assert p.to_ewkt() == "SRID=3857;POINT(0 0)"


def test_polygon_to_wkt():
    poly = Geom.polygon([[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]])
    assert poly.to_wkt().startswith("POLYGON((0 0,")


def test_linestring_to_wkt():
    ls = Geom.linestring([[0, 0], [1, 1], [2, 2]])
    assert ls.to_wkt() == "LINESTRING(0 0,1 1,2 2)"


def test_geom_from_geojson():
    blob = {"type": "Point", "coordinates": [1, 2]}
    g = Geom.from_geojson(blob)
    assert g.kind == "Point"
    assert g.coordinates == [1, 2]


def test_geom_from_geojson_string():
    g = Geom.from_geojson(json.dumps({"type": "Point", "coordinates": [3, 4]}))
    assert g.coordinates == [3, 4]


def test_to_geojson_round_trip():
    p = Geom.point(7, 8)
    blob = p.to_geojson()
    assert blob == {"type": "Point", "coordinates": [7, 8]}
    re_parsed = Geom.from_geojson(blob)
    assert re_parsed == p


def test_parse_wkt_point():
    g = _parse_wkt("SRID=4326;POINT(1.0 2.0)")
    assert g.kind == "Point"
    assert g.coordinates == [1.0, 2.0]
    assert g.srid == 4326


def test_parse_wkt_polygon():
    g = _parse_wkt("POLYGON((0 0,1 0,1 1,0 1,0 0))")
    assert g.kind == "Polygon"
    assert g.coordinates == [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]


def test_parse_wkt_linestring():
    g = _parse_wkt("LINESTRING(0 0,1 1,2 2)")
    assert g.kind == "LineString"
    assert g.coordinates == [[0, 0], [1, 1], [2, 2]]


def test_field_db_type_pg():
    class FakePG:
        vendor = "postgresql"

    f = PointField(srid=4326)
    assert f.db_type(FakePG()) == "GEOMETRY(POINT, 4326)"


def test_field_db_type_sqlite():
    class FakeSQLite:
        vendor = "sqlite"

    f = PointField()
    assert f.db_type(FakeSQLite()) == "BLOB"


def test_field_get_db_prep_value_geom():
    f = PointField()
    out = f.get_db_prep_value(Geom.point(1, 2))
    assert out == "SRID=4326;POINT(1 2)"


def test_field_get_db_prep_value_dict():
    f = PolygonField()
    out = f.get_db_prep_value({"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]})
    assert "POLYGON" in out


def test_field_get_db_prep_value_passthrough_str():
    f = LineStringField()
    out = f.get_db_prep_value("SRID=4326;LINESTRING(0 0,1 1)")
    assert "LINESTRING" in out


def test_field_get_db_prep_value_rejects_garbage():
    f = PointField()
    with pytest.raises(TypeError):
        f.get_db_prep_value(object())


def test_field_dim_validation():
    with pytest.raises(ValueError):
        GeometryField(dim=4)


def test_field_from_db_value_parses_wkt():
    f = PointField()
    g = f.from_db_value("SRID=4326;POINT(5 6)")
    assert g.coordinates == [5.0, 6.0]


def test_field_from_db_value_passes_through_geom():
    f = PointField()
    g = Geom.point(0, 0)
    assert f.from_db_value(g) is g


def test_lookup_intersects_returns_sql():
    sql, params = SPATIAL_LOOKUPS["intersects"]("zone", Geom.point(1, 2))
    assert "ST_Intersects" in sql
    assert "POINT(1 2)" in sql
    assert params == []


def test_lookup_distance_lte_includes_threshold():
    sql, _ = SPATIAL_LOOKUPS["distance_lte"]("zone", Geom.point(0, 0), 100.0)
    assert "ST_Distance" in sql
    assert "100.0" in sql or "100" in sql


def test_lookup_within():
    sql, _ = SPATIAL_LOOKUPS["within"]("zone", Geom.polygon([[[0, 0], [1, 0], [1, 1], [0, 0]]]))
    assert "ST_Within" in sql
    assert "POLYGON" in sql
