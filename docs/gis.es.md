# GIS (geometrías y lookups espaciales)

`dorm.contrib.gis` (4.0+) ofrece campos y lookups para datos
geográficos. Diseñado pensando en PostGIS pero con fallback a
SpatiaLite (SQLite con extensión).

## Instalación

PostgreSQL — instala la extensión PostGIS:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

(Sí, esto es DDL fuera de las migraciones de dorm. Usa
`RunSQL("CREATE EXTENSION IF NOT EXISTS postgis")` en una
migración, o ejecútalo manualmente al provisionar la BD.)

SQLite — instala `mod_spatialite` y carga la extensión:

```python
from dorm.contrib.gis import Geom
from dorm.db.connection import get_connection

Geom.bootstrap_sqlite(get_connection())
```

## Campos

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

`srid` es el sistema de referencia espacial. 4326 = WGS84
(GPS estándar). 3857 = Web Mercator (Google Maps / OSM).

## Tipo `Geom`

Wrapper Pythonic GeoJSON-shaped:

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
            [-3.71, 40.40],   # cierra el ring
        ]
    ]),
)

# Desde GeoJSON:
g = Geom.from_geojson({"type": "Point", "coordinates": [1.5, 2.5]})

# A WKT / EWKT:
print(g.to_wkt())     # POINT(1.5 2.5)
print(g.to_ewkt())    # SRID=4326;POINT(1.5 2.5)
```

## Lookups

```python
from dorm.contrib.gis import Geom

# Tiendas dentro de un polígono dado
zone = Geom.polygon([[[-3.8, 40.3], [-3.6, 40.3], [-3.6, 40.5], [-3.8, 40.5], [-3.8, 40.3]]])
Store.objects.filter(location__within=zone)

# Tiendas cuyo delivery_zone se intersecte con un punto cliente
client = Geom.point(-3.7, 40.41)
Store.objects.filter(delivery_zone__intersects=client)

# Tiendas en radio 1000m
me = Geom.point(-3.7, 40.41)
Store.objects.filter(location__distance_lte=(me, 1000))   # 1000m PostGIS
```

Lookups disponibles: `__intersects`, `__within`, `__contains`,
`__distance_lte`, `__distance_gte`.

## Ranking por distancia

```python
from dorm.search import F           # F sirve para expresiones

# 10 tiendas más cercanas:
me = Geom.point(-3.7, 40.41)
nearest = (
    Store.objects
    .filter(location__distance_lte=(me, 5000))   # cap a 5km
    .order_by("-rating")[:10]                    # ranking adicional
)
```

(PostGIS no expone "distancia" como expresión annotate de fábrica;
para ordenar por distancia exacta, usa `RawSQL` o `extra()` con
`ST_Distance(location, ST_GeomFromText('POINT(...)', 4326))`.)

## Caveats

- **Index espacial necesario**: para queries con filtros
  espaciales sobre tablas grandes, sin GIST/GIN index la query
  es seq-scan. Crea con `Index(fields=["location"], method="GIST")`
  en `Meta.indexes` o RunSQL custom.
- **SRID consistente**: si mezclas 4326 y 3857 en la misma query,
  PostGIS necesita reproyectar. Lento y propenso a errores —
  fija el SRID al schema y conviértelo en el cliente si hace
  falta.
- **`Geom.bootstrap_sqlite()` requiere SQLite con
  `enable_load_extension`**. Algunos builds de SQLite (Apple
  oficial, e.g.) lo deshabilitan.

## Más

- [API: gis](api/gis.md)
- [PostGIS reference](https://postgis.net/) — funciones
  espaciales completas
