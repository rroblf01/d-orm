# Backend DuckDB

DuckDB es una base de datos analítica embebida — proceso único, sin
servidor, almacenamiento columnar, motor de ejecución vectorizado.
Mismo modelo mental que SQLite (un archivo, sin daemons) con SQL
estilo PostgreSQL y rendimiento OLAP.

Añadido en **4.0**.

## Cuándo usarlo

- **Dashboards**: agregaciones rápidas sobre tablas medianas
  (~10⁹ filas) sin levantar Postgres.
- **ETL / staging local**: ingesta CSV/Parquet, transforma,
  exporta. Más rápido que pandas para joins grandes.
- **ML feature stores**: entrenamiento iterativo con queries
  repetidas sobre el mismo dataset.
- **Tests CI**: como SQLite pero con SQL más cercano a Postgres
  (window functions, CTEs, lateral joins).

## Cuándo NO usarlo

- **OLTP serio**: DuckDB no está pensado para escrituras
  concurrentes. Sin SAVEPOINT, sin replicación, sin pool de
  conexiones; un escritor a la vez.
- **Multi-proceso**: cada proceso abre su propia conexión al
  mismo archivo, pero el aislamiento entre escritores
  concurrentes no es robusto.
- **APIs públicas**: para una request/response típica con
  muchas escrituras y reads pequeños, PostgreSQL gana.

## Instalación

```bash
pip install 'djanorm[duckdb]'
```

Trae el cliente Python `duckdb` (incluye el motor en el wheel).
Sin paquetes del sistema.

## Configuración

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "duckdb",
        "NAME": "analytics.duckdb",   # ":memory:" para in-process volátil
    }
}
INSTALLED_APPS = ["dashboards"]
```

`ENGINE` resuelve a
`dorm.db.backends.duckdb.DuckDBDatabaseWrapper`.

## Capacidades

- **CRUD completo**: `Model.objects.create`, `filter`, `bulk_create`,
  `delete`, etc.
- **Migraciones**: `dorm makemigrations` / `dorm migrate` funcionan
  igual que con SQLite.
- **Streaming**: `qs.iterator(chunk_size=N)` usa
  `cursor.fetchmany(N)` de DuckDB.
- **Atomic transactions**: `with transaction.atomic():` envuelve
  `BEGIN`/`COMMIT`. Caveat abajo.
- **Async wrapper**: `await Model.objects.acreate(...)` enruta a
  un thread executor (DuckDB es síncrono internamente).
- **`information_schema`**: `dorm diff` funciona sin cambios.
- **`__search`**: ejecuta `LIKE`/`ILIKE` (DuckDB no tiene
  `tsvector`); para full-text usa pattern matching o trigram
  alternativo desde otro backend.

## Limitaciones a conocer

### Sin `SAVEPOINT`

DuckDB no soporta savepoints. `atomic()` anidado degrada a
no-op boundary — outer rollback descarta todo:

```python
with transaction.atomic():           # BEGIN
    Author.objects.create(name="x")
    try:
        with transaction.atomic():   # nested → no-op
            Author.objects.create(name="bad")
            raise RuntimeError       # rollback all
    except RuntimeError:
        pass
# final state: Author count = 0 (everything rolled back)
```

Comportamiento equivalente a `try/except` mostrando el patrón
"todo o nada" puro. Si necesitas savepoints reales, cambia a
PostgreSQL/SQLite.

### `RETURNING` requiere alias correcto

DuckDB acepta `RETURNING <pk_col>` en INSERT, pero la columna
debe ser el PK declarado, no el alias `id` automático. dorm
maneja esto interno usando `pk_col` configurado.

### Async = thread executor

DuckDB no tiene API async nativa. `DuckDBAsyncDatabaseWrapper`
delega cada llamada a `asyncio.to_thread`. Funciona pero no es
"async-native" — para concurrency real (event-loop con miles de
conexiones simultáneas) usa libsql o PG.

Las conexiones DuckDB son **per-thread**: para BD persistente en
disco está OK (cada thread abre su propio handle al archivo);
para `:memory:` cada thread tendría su propia DB en memoria, así
que evita `:memory:` en código async.

## Receta: dashboard rápido

```python
import dorm

dorm.configure(
    DATABASES={"default": {"ENGINE": "duckdb", "NAME": "analytics.duckdb"}},
    INSTALLED_APPS=["dash"],
)

class PageView(dorm.Model):
    path = dorm.CharField(max_length=200)
    user_id = dorm.IntegerField()
    ts = dorm.DateTimeField()

# Top 10 paths por visitas únicas (PG-style SQL — DuckDB lo digiere)
from dorm import Count, F

top = (
    PageView.objects
    .values("path")
    .annotate(uniques=Count("user_id", distinct=True))
    .order_by("-uniques")[:10]
)
```

## Receta: leer Parquet directo

DuckDB puede leer Parquet/CSV sin importar — útil para staging
sin migrar a tablas dorm. Bypass del ORM con SQL crudo:

```python
from dorm.db.connection import get_connection

conn = get_connection()
rows = conn.execute(
    "SELECT region, COUNT(*) AS n "
    "FROM 'sales_2026.parquet' "
    "GROUP BY region ORDER BY n DESC"
)
for r in rows:
    print(r["region"], r["n"])
```

## Migrar SQLite → DuckDB

Mismas migraciones funcionan (DuckDB acepta sintaxis SQLite-like).
Cambia `ENGINE` y vuelve a aplicar:

```bash
# Settings con ENGINE=sqlite → cambia a ENGINE=duckdb
dorm migrate
```

Cuidado: tipos como `BOOLEAN` y `TIMESTAMP` se mapean
automáticamente; `BLOB`/`TEXT` también. Pero si tienes
`PRAGMA`-specific lógica (típica SQLite), revísala.

## Más

- [Novedades 4.0](v4_0.md#6-backend-duckdb) — overview release
- [When to use what](when-to-use-what.md) — DuckDB vs PostgreSQL vs SQLite
- [Migraciones](migrations.md) — todas las ops funcionan
