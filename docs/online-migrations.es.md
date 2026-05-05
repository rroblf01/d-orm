# Migraciones online (zero-downtime)

Receta end-to-end para añadir una columna `NOT NULL` con default a
una tabla grande **sin reescribirla** y sin downtime.

Añadido en **4.0**.

## El problema

```python
operations = [
    AddField(
        "Order",
        "currency",
        dorm.CharField(max_length=3, null=False, default="USD"),
    ),
]
```

En PostgreSQL ≤ 10 esto reescribe **toda la tabla** (`ALTER TABLE
... NOT NULL DEFAULT 'USD'`). Una tabla de 50M filas tarda horas;
durante ese tiempo cualquier escritura espera al `ACCESS EXCLUSIVE`
lock. Down.

PG 11+ optimiza si el default es no-volátil — pero un default
calculado por Python o un `ALTER` que cambie tipo sí rewriteea.

## La receta

Tres operaciones, cada una en una migración separada (o tres pasos
de la misma migración):

```python
from dorm.migrations.operations import (
    AddFieldOnline, BackfillBatch, SetNotNullOnline,
)

operations = [
    # Paso 1 — column nullable, sin rewrite.
    AddFieldOnline(
        "Order",
        "currency",
        dorm.CharField(max_length=3, null=False, default="USD"),
    ),
    # Paso 2 — backfill por chunks de PK.
    BackfillBatch(
        table="orders",
        update_sql=(
            'UPDATE "orders" SET "currency" = \'USD\' '
            'WHERE "id" BETWEEN %s AND %s '
            'AND "currency" IS NULL'
        ),
        pk_column="id",
        batch_size=10_000,
        sleep_seconds=0.05,    # estrangula carga sobre primario
    ),
    # Paso 3 — promover a NOT NULL sin rewrite (PG ≥ 12).
    SetNotNullOnline("Order", "currency"),
]
```

## Qué hace cada paso

### `AddFieldOnline`

```sql
-- En PostgreSQL: solo metadata, instantáneo.
ALTER TABLE "orders" ADD COLUMN "currency" VARCHAR(3) NULL;
```

El field se declara `NOT NULL` en el modelo, pero la op fuerza
nullable temporalmente. Sin rewrite.

`set_not_null_now=True` opcional: si la tabla tiene <1000 filas y
el default es seguro, hace los pasos 2+3 inline. Para tablas
grandes deja por defecto (`False`).

### `BackfillBatch`

```sql
-- Loop por rangos de PK, cada rango en su tx propia:
UPDATE "orders" SET "currency" = 'USD'
WHERE "id" BETWEEN 1 AND 10000 AND "currency" IS NULL;
COMMIT;
-- ... siguiente batch ...
```

Cada batch:
- Toma `batch_size` filas (default 10k).
- Una transacción dedicada.
- Lock row-level por la duración del UPDATE.
- `sleep_seconds` entre batches para no saturar I/O.

Parámetros:
- `batch_size` — más bajo = locks más cortos pero más overhead de
  commit. 10k es buen punto inicial.
- `sleep_seconds` — pausa entre batches. 0.05s típico para
  primarios con tráfico. 0 si la tabla está congelada.
- `max_batches` — corta la migración tras N batches (testing /
  rollout incremental).

### `SetNotNullOnline`

En PG ≥ 12 el secreto es:

```sql
-- 1. CHECK NOT VALID — adopt instantáneo (no escanea filas).
ALTER TABLE "orders"
  ADD CONSTRAINT chk_orders_currency_notnull
  CHECK ("currency" IS NOT NULL) NOT VALID;

-- 2. VALIDATE — escanea con SHARE UPDATE EXCLUSIVE lock,
--    no bloquea readers/writers.
ALTER TABLE "orders" VALIDATE CONSTRAINT chk_orders_currency_notnull;

-- 3. SET NOT NULL — metadata-only ahora que el CHECK validado
--    confirma que ninguna fila viola el constraint.
ALTER TABLE "orders" ALTER COLUMN "currency" SET NOT NULL;

-- 4. Limpia el CHECK redundante.
ALTER TABLE "orders" DROP CONSTRAINT chk_orders_currency_notnull;
```

PG ≤ 11 no tiene la optimización del paso 3 — la op cae a un
`ALTER COLUMN SET NOT NULL` que sí reescribe. Si target es 11,
considera dejar la columna nullable.

## ¿Cuándo separar en 3 migraciones?

Si vas a desplegar el código de la app y la migración en releases
distintos, separa:

1. **Release N** — deploy `AddFieldOnline`. La columna existe
   nullable, código antiguo no la lee, código nuevo no se ha
   desplegado todavía.
2. **Backfill batch job** — corre `BackfillBatch` como migración o
   script independiente, fuera del release window. Puede tardar
   horas; el código sigue funcionando porque el campo es nullable.
3. **Release N+1** — deploy `SetNotNullOnline` + cualquier código
   que dependa del NOT NULL. Si el backfill no terminó antes de
   este release, falla la migración (visible y temprano).

## Caveat sobre `BackfillBatch.update_sql`

La SQL la escribes tú — usa los placeholders `%s` (PG) o `?`
(SQLite) según el backend, y siempre incluye:

- `BETWEEN %s AND %s` para el rango PK (los dos `%s` son inyectados
  por la op).
- `WHERE ... IS NULL` para idempotencia (re-correr el job no
  duplica trabajo).

```python
update_sql=(
    'UPDATE "orders" '
    'SET "currency" = "billing_country_currency" '   # algún cómputo
    'WHERE "id" BETWEEN %s AND %s '
    'AND "currency" IS NULL'                          # idempotente
),
```

## Caveats backends

- **PostgreSQL ≥ 12**: receta completa, sin rewrite.
- **PostgreSQL 10–11**: `SetNotNullOnline` cae a `ALTER COLUMN
  SET NOT NULL` que sí rewriteea. Considera dejar nullable.
- **SQLite**: `AddFieldOnline` siempre añade nullable (la única
  forma de `ADD COLUMN NOT NULL` requiere DEFAULT). Backfill
  funciona. `SetNotNullOnline` cae a un rewrite. Para tablas
  grandes, considera un `RunSQL` con la receta `CREATE TABLE
  ... AS SELECT ...; DROP; RENAME`.
- **MySQL**: DDL no es transaccional. Cada paso commit-or-die.
  Plan rollback manual.
- **DuckDB**: receta funciona, sin SAVEPOINT. La tabla se
  reescribe en `SET NOT NULL` por arquitectura columnar — pero
  reescribir es barato en DuckDB.

## Tests

Para tests unitarios usa el patrón mostrado en
`tests/test_online_migrations.py`. Cada op acepta un `_State`
mock, así no necesitas el migration runner completo.

## Más

- [Migraciones](migrations.md) — ops básicas + nuevos ops
- [`dorm diff`](cli.md#dorm-diff-40) — gate post-deploy CI
- [Avanzado](advanced.md) — features PG complementarias
