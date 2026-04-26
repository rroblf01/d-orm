# Resolución de problemas

Una colección de errores que puedes encontrarte y qué significan en
realidad. Formato: síntoma → causa → arreglo.

## `ImproperlyConfigured: DATABASES is not configured`

**Causa.** dorm no encontró un módulo de settings antes de la
primera query.

**Arreglo.** O bien:

- fija `DORM_SETTINGS_MODULE=miproyecto.settings`, o
- llama a `dorm.configure(DATABASES={...})` al arrancar la app
  (p.ej. en un lifespan de FastAPI), o
- ejecuta desde el directorio que contiene `settings.py`.

## `ImproperlyConfigured: Database alias 'replica' not found`

**Causa.** Un `using("replica")` o un router devolvió un alias que
no está en `DATABASES`.

**Arreglo.** Añade el alias a `DATABASES`, o arregla el router para
que solo devuelva alias existentes.

## `psycopg.errors.OperationalError: too many clients already`

**Causa.** El total de conexiones de todos tus procesos supera el
`max_connections` de PostgreSQL.

**Arreglo.** Baja `MAX_POOL_SIZE` o escala Postgres. Regla
estimativa: `MAX_POOL_SIZE × workers × pods ≤ max_connections / 2`.
Mete un PgBouncer delante si estás escalando workers horizontalmente.

## `PoolTimeout: pool timeout`

**Causa.** Cada conexión del pool está tomada y un nuevo checkout
esperó más de `POOL_TIMEOUT`.

**Arreglo.** Suele ser una conexión filtrada (mantenida sobre un
`await` fuera de su bloque) o una query que duró demasiado.
Comprueba `pool_stats()`, haz `EXPLAIN` de la query lenta, y
considera subir `MAX_POOL_SIZE` solo si realmente lo necesitas.

## `RuntimeError: this event loop is already running`

**Causa.** Llamar un método ORM sync desde una función async — los
métodos sync pueden levantar su propio loop, que colisiona con el
que ya está corriendo.

**Arreglo.** Usa la variante `a*`. `Author.objects.all()` está bien
construirlo, pero materialízalo con `async for` o
`await Author.objects.all()`.

## `MultipleObjectsReturned: get() returned more than one Author`

**Causa.** Tu filtro coincide con más de una fila.

**Arreglo.** O haces el lookup único (filtra por `pk` o una columna
unique), usas `.filter(...).first()`, o `.get_or_none(...)` si
esperas cero o uno.

## `dbcheck` reporta drift pero la migración se ve aplicada

**Causa.** O la migración se aplicó a un alias distinto del que
estás chequeando, o alguien editó la tabla a mano.

**Arreglo.** Compara
`dorm showmigrations <app> --settings=...` con el entorno
afectado. Ejecuta `dbcheck --settings=...` contra el alias exacto
para confirmar. Si se editó a mano, escribe una migración
`RunSQL` que codifique el diff.

## pytest se cuelga para siempre con `-n 4`

**Causa.** pytest-asyncio crea un event loop nuevo por test por
defecto; con xdist se acumulan pools dangling.

**Arreglo.** En `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

## `EmailField` acepta basura

**Causa.** Era un bug real pre-2.0. Si lo sigues viendo, estás en
una versión antigua.

**Arreglo.** Actualiza a djanorm ≥ 2.0. Desde 2.0 la validación
corre en `to_python` así que tanto `Author(email="x")` como
`obj.email = "x"` lanzan.

## Una migración con rollback deja una tabla huérfana

**Causa.** Un paso `RunPython` no tiene `reverse_code` así que dorm
no pudo revertirlo.

**Arreglo.** Pasa siempre `reverse_code=` a `RunPython`. Usa
`RunPython.noop` si genuinamente no hay nada que deshacer a nivel
de datos (la parte de esquema la revierten las operaciones de
esquema a ambos lados).

## Migraciones de una rama de larga vida no aplican

**Causa.** Colisión de numeración: ambas ramas añadieron `0017_*`.
El recorder aplica la primera que ve y rechaza el resto.

**Arreglo.** Renumera las migraciones de tu rama tras mergear main.
`dorm makemigrations --name <suffix>` regenera el archivo con el
siguiente número disponible.

## `select_related` ejecutó una query separada por fila

**Causa.** Lo llamaste sin argumentos. `select_related()` a secas
hace JOIN sobre cada FK del modelo, lo cual puede ser enorme o
incluso inválido si la FK target carece de campos.

**Arreglo.** Especifica siempre qué FKs seguir:
`Book.objects.select_related("author", "publisher")`.

## Tests async pasan en local y fallan en CI

**Causa.** Casi siempre un pool no drenado entre tests. Llama a
`await close_all_async()` en el teardown de un fixture
session-scoped y usa un event loop session-scoped.

**Arreglo.** Mira [Despliegue en producción / Compartir el event loop async](production.md#compartir-el-event-loop-async).

## `IntegrityError` en `bulk_create`

**Causa.** Un duplicado viola un constraint `UNIQUE`. Postgres
aborta toda la transacción; falla el batch entero.

**Arreglo.** Pre-filtra duplicados en Python (mira [Recetario](cookbook.md#bulk-insert-con-deduplicacion))
o empuja la dedup a la BD con `ON CONFLICT DO NOTHING` vía
`RunSQL` o `get_connection().execute(...)`.

## "La migración corre eternamente" en una tabla grande

**Causa.** `ALTER TABLE ADD COLUMN ... NOT NULL DEFAULT '...'` en
PostgreSQL ≤ 10 reescribe toda la tabla.

**Arreglo.** Pártelo en tres migraciones: añadir nullable, backfill
en chunks, fijar NOT NULL. En PG 11+, añadir una columna con un
default no-volátil es solo metadata — dorm lo usa cuando puede.

## Dónde pedir más ayuda

- Abre una issue en
  [GitHub](https://github.com/MrFrozen11/djanorm/issues) con el
  traceback completo, el bloque `DATABASES` (con secretos
  redacted), y la versión (`dorm --version`).
- Para problemas de migraciones, adjunta la salida de
  `dorm showmigrations` y `dorm dbcheck`.
