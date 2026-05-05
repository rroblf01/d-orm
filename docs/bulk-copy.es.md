# Bulk COPY (PostgreSQL)

`dorm.contrib.bulk_copy` (3.4+) usa el protocolo `COPY` de
PostgreSQL para ingestar decenas/centenas de miles de filas
**10-100× más rápido** que `bulk_create`. Crítico para ETL,
seeding, replicación, migraciones de datos masivas.

PostgreSQL-only. Otros backends levantan `NotImplementedError`
deliberadamente (ver "diseño" abajo).

## API rápida

```python
from dorm.contrib.bulk_copy import (
    bulk_copy_from, abulk_copy_from,
    copy_to, acopy_to,
)

# INGEST — instancias modelo
n = bulk_copy_from(
    Author,
    [Author(name=f"a-{i}", age=i) for i in range(50_000)],
)
print(f"insertadas {n} filas")

# INGEST — dicts
bulk_copy_from(
    Author,
    [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 40}],
    columns=["name", "age"],
)

# INGEST — async desde generator
async def feed():
    for line in open("data.csv"):
        name, age = line.strip().split(",")
        yield {"name": name, "age": int(age)}

await abulk_copy_from(Author, feed(), columns=["name", "age"])

# EXPORT — yield rows
for row in copy_to('SELECT id, name FROM "authors"'):
    process(row)

# EXPORT async
async for row in acopy_to('SELECT id, name FROM "authors"'):
    process(row)
```

## Cuándo usarlo

- ETL nightly que mueve millones de filas.
- Seeding de fixtures grandes para tests/dev.
- Replicación de datos entre instancias.
- Importación de CSV/Parquet a tablas dorm.

## Cuándo NO usarlo

- Inserts pequeños (<1000 filas) — el overhead del COPY no compensa
  versus `bulk_create`.
- Datos con dependencias FK que requieren validación pre-insert
  (COPY no dispara signals).
- Si necesitas que `pre_save`/`post_save` se disparen — COPY
  bypasea todas las señales por rendimiento.

## Modos: text vs binary

```python
bulk_copy_from(Author, objs)               # text (default)
bulk_copy_from(Author, objs, binary=True)  # binary
```

- **Text**: el cliente convierte cada valor a string,
  PostgreSQL parsea. Más tolerante con tipos Python (`None` →
  NULL, `datetime` → ISO 8601 string).
- **Binary**: el cliente envía bytes con el formato exacto que PG
  espera. ~2× más rápido pero **type-strict** — un `int` cuando
  PG espera `bigint` falla. Reserve para cuando ya tienes los
  tipos correctos garantizados.

## Diseño: por qué no fallback a `bulk_create`

Si un usuario llama `bulk_copy_from(Author, ...)` contra SQLite
levantamos:

```
NotImplementedError: bulk_copy_from() is PostgreSQL-only — the COPY
protocol has no portable equivalent on other backends.
```

Sin fallback silencioso a `bulk_create`. Razón: si llamaste a
`bulk_copy_from` es porque te importa el rendimiento. Caer a
`bulk_create` cuando cambias `ENGINE` te da un cuello de botella
oculto sin avisarte. Mejor falla rápido y claro.

## Pitfalls

- **No reusa la conexión del autocommit pool** para evitar
  bloqueos largos. Usa una checkout dedicada del pool.
- **Bypass de signals**: `pre_save`/`post_save` NO se disparan.
  Si tu lógica de auditoría depende de ellas, usa `bulk_create`.
- **Sin auto-PK fill**: COPY no devuelve los PKs autogenerados.
  Si necesitas los PKs, post-query con `SELECT pk FROM ... WHERE
  campo_unique IN (...)`.
- **Constraint violations**: cualquier fila que viole un constraint
  aborta el COPY entero. Pre-filtra o usa staging table +
  `INSERT ... ON CONFLICT DO NOTHING` después.

## Más

- [Avanzado](bulk-copy.md)
- API: `dorm.contrib.bulk_copy`
