# GIS (geometries and spatial lookups)

`dorm.contrib.gis` (4.0+) ships fields and lookups for
geographic data. Designed for PostGIS with a SpatiaLite fallback
(SQLite with the extension).

## Install

PostgreSQL — install the PostGIS extension:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

(Yes, this is DDL outside dorm migrations. Use
`RunSQL("CREATE EXTENSION IF NOT EXISTS postgis")` in a
migration, or run it manually when provisioning the DB.)

SQLite — install `mod_spatialite` and load the extension:

```python
from dorm.contrib.gis import Geom
from dorm.db.connection import get_connection

Geom.bootstrap_sqlite(get_connection())
```

## Fields

```python
from dorm.contrib.gis import (
    PointField, PolygonField, LineStringField, GeometryField,
)

class Store(dorm.Model):
    name = dorm.CharField(max_length=100)
    location = PointField(srid=4326)                      # WGS84 lat/long
    delivery_zone = PolygonField(srid=4326, null=True)
    route = LineStringField(srid=4326, null=True)
    any_shape = GeometryField(srid=4326, null=True)       # Point/Polygon/Line
```

`srid` is the spatial reference system. 4326 = WGS84 (GPS
standard). 3857 = Web Mercator (Google Maps / OSM).

## `Geom` value type

Pythonic GeoJSON-shaped wrapper:

```python
from dorm.contrib.gis import Geom

Store.objects.create(
    name="Madrid HQ",
    location=Geom.point(-3.7038, 40.4168),     # (lon, lat)
    delivery_zone=Geom.polygon([
        [
            [-3.71, 40.40],
            [-3.69, 40.40],
            [-3.69, 40.42],
            [-3.71, 40.42],
            [-3.71, 40.40],   # closes the ring
        ]
    ]),
)

# From GeoJSON:
g = Geom.from_geojson({"type": "Point", "coordinates": [1.5, 2.5]})

# To WKT / EWKT:
print(g.to_wkt())     # POINT(1.5 2.5)
print(g.to_ewkt())    # SRID=4326;POINT(1.5 2.5)
```

## Lookups

```python
from dorm.contrib.gis import Geom

# Stores inside a given polygon
zone = Geom.polygon([[[-3.8, 40.3], [-3.6, 40.3], [-3.6, 40.5], [-3.8, 40.5], [-3.8, 40.3]]])
Store.objects.filter(location__within=zone)

# Stores whose delivery_zone intersects a client point
client = Geom.point(-3.7, 40.41)
Store.objects.filter(delivery_zone__intersects=client)

# Stores within 1000m
me = Geom.point(-3.7, 40.41)
Store.objects.filter(location__distance_lte=(me, 1000))   # 1000m PostGIS
```

Available lookups: `__intersects`, `__within`, `__contains`,
`__distance_lte`, `__distance_gte`.

## Distance-based ranking

```python
me = Geom.point(-3.7, 40.41)
nearest = (
    Store.objects
    .filter(location__distance_lte=(me, 5000))   # cap at 5km
    .order_by("-rating")[:10]                    # extra ranking
)
```

(PostGIS doesn't expose "distance" as an annotate expression out
of the box; for exact distance ordering use `RawSQL` or `extra()`
with `ST_Distance(location, ST_GeomFromText('POINT(...)', 4326))`.)

## Caveats

- **Spatial index needed**: for spatial filters on large tables,
  without a GIST/GIN index the query becomes a seq-scan. Create
  with `Index(fields=["location"], method="GIST")` in
  `Meta.indexes` or a custom `RunSQL`.
- **Consistent SRID**: if you mix 4326 and 3857 in the same
  query, PostGIS has to reproject. Slow and error-prone — pin
  the SRID at the schema level and convert client-side if needed.
- **`Geom.bootstrap_sqlite()` requires SQLite with
  `enable_load_extension`**. Some SQLite builds (Apple's
  official, e.g.) disable it.

## More

- [API: gis](api/gis.md)
- [PostGIS reference](https://postgis.net/) — full spatial function
  reference
