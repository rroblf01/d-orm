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

## Mi endpoint ejecuta N queries en vez de 1

**Causa.** Una lectura de descriptor dentro de un bucle pega a la BD
una vez por fila. Patrones típicos:

* `for author in Author.objects.all(): print(author.publisher.name)`
  — N selects sobre `publishers`. Arregla con
  `select_related("publisher")`.
* `for art in Article.objects.all(): list(art.tags.all())` — N
  selects sobre la tabla intermedia. Arregla con
  `prefetch_related("tags")`.

**Cómo confirmarlo.** Envuelve el bloque sospechoso en
`dorm.contrib.nplusone.NPlusOneDetector`:

```python
from dorm.contrib.nplusone import NPlusOneDetector

with NPlusOneDetector(threshold=5):
    handler()                 # lanza NPlusOneError si cualquier template SQL
                              # se ejecuta más de 5 veces
```

El mensaje del error incluye el template SQL (con parámetros
quitados) que cruzó el umbral, así puedes grepear el origen. Para
tests usa el helper `assert_no_nplusone()` — lanza un
`AssertionError` así pytest lo presenta como fallo normal.

Para auditoría tipo staging sin fail-fast, construye el detector con
`raise_on_detect=False` y lee `detector.findings` /
`detector.report()` después del bloque.

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

**Arreglo.** Usa la receta zero-downtime con `AddFieldOnline` +
`BackfillBatch` + `SetNotNullOnline` (ver
[Migraciones online](online-migrations.md)). En PG 11+, añadir una
columna con un default no-volátil es solo metadata — dorm lo usa
cuando puede.

## `BudgetExceeded: Query returned N rows, exceeds active budget`

**Causa.** Una query dentro de un bloque
`with dorm.budget(max_rows=…):` materializó más filas que el techo.

**Arreglo.** O subes `max_rows`, o estrechas el `filter()`/`limit()`
de la query. El error es por diseño — bloquea silenciosamente
querysets que han perdido selectividad en producción.

## `BudgetExceeded` / `OperationalError: canceling statement due to statement timeout`

**Causa.** Una query dentro de `dorm.budget(timeout_ms=…)` excedió
el reloj de pared. PG la abortó vía `statement_timeout`.

**Arreglo.** Diagnostica con `EXPLAIN ANALYZE` antes de subir el
budget — un `timeout_ms` alto enmascara queries verdaderamente
lentas. La feature existe precisamente para que la SLA del HTTP no
salte por culpa de una query mala.

## `NoActiveTenantError`

**Causa.** Una query contra un `TenantModel` corrió sin
`with current_tenant(<id>):` activo.

**Arreglo.** Envuelve el handler / job con
`with current_tenant(request.user.tenant_id):`. Si la query es
deliberadamente cross-tenant (admin / reporte), usa
`MyModel.unscoped.all()` para saltar el filtro explícitamente.
El error es por diseño — un fallback silencioso a "todas las
tenants" sería un leak de datos.

## `ReadOnlyModelError`

**Causa.** Una llamada a `save()` / `delete()` / `asave()` /
`adelete()` sobre un modelo con `Meta.read_only = True`.

**Arreglo.** Lee del modelo, escribe en el origen-de-verdad
(materialised view subyacente, tabla maestra, etc.). El flag está
ahí precisamente para bloquear mutaciones accidentales.

## `AsyncOnlyError: AsyncModel forbids sync access`

**Causa.** Llamaste un método sync (`MyModel.objects.create(...)`,
`obj.save()`) sobre un `AsyncModel`. Esos modelos rechazan la API
sync para forzar uso de `acreate` / `asave` en stacks async puros.

**Arreglo.** Usa la variante async (`acreate`, `aget`, `afilter`,
`asave`, `adelete`). Si necesitas paths sync, hereda de `dorm.Model`
en lugar de `AsyncModel`.

## `CircuitOpenError: Circuit '<name>' is OPEN`

**Causa.** El circuit breaker de ese alias acumuló tantas fallas
consecutivas que se abrió. Toda llamada `with cb:` ahora rebota
hasta que el cooldown pase a HALF_OPEN.

**Arreglo.** Espera al cooldown (default 30s) o
`circuit_breaker(name).reset()` manualmente. Si aparece a menudo,
hay un problema real downstream — log + alerta.

## `_duckdb.ParserException: syntax error at or near "SAVEPOINT"`

**Causa.** DuckDB **no soporta `SAVEPOINT`**. Si lo ves es porque
algo intenta savepoints contra DuckDB.

**Arreglo.** En `atomic()` de DuckDB, los nested blocks degradan a
no-op boundary — outer rollback descarta todo. Patrón típico que
falla: librerías terceras que asumen savepoints. Aísla con un
`try/except` o cambia a SQLite/PG si necesitas savepoints reales.

## "Mi fixture `transactional_db` no aparece"

**Causa.** El paquete `pytest-djanorm` no está instalado. Los
fixtures viven ahí, **no en el wheel principal**.

**Arreglo.**

```bash
pip install pytest-djanorm                       # SQLite-only
pip install 'pytest-djanorm[postgres]'           # + container PG
```

Después auto-discovery vía `pytest11` entry-point. Más detalle en
[paquetes hermanos](sibling-packages.md).

## "mypy no detecta `filter(naem=...)` como typo"

**Causa.** El plugin `djanorm-mypy` no está instalado o no está
configurado.

**Arreglo.** `pip install djanorm-mypy` y en `pyproject.toml`:

```toml
[tool.mypy]
plugins = ["djanorm_mypy"]
```

Sin el plugin, mypy ve `filter(**kwargs)` y no sabe qué validar.

## `ImproperlyConfigured: Unsupported database engine: 'duckdb'`

**Causa.** `ENGINE = "duckdb"` requiere el extra DuckDB.

**Arreglo.** `pip install 'djanorm[duckdb]'`. Si ya estaba
instalado, comprueba que el venv activo es el correcto (
`uv run python -c "import duckdb"`).

## "El backend de plugins de terceros no se carga"

**Causa.** El entry-point está mal escrito o el paquete con
`[project.entry-points."djanorm.backends"]` no está instalado.

**Arreglo.** `pip show <pkg>` para confirmar instalación; abrir un
shell Python y `from importlib.metadata import entry_points;
print(list(entry_points(group="djanorm.backends")))` para
verificar el registro. Si tras instalar nada cambia,
`reset_backend_cache()` desde Python o reinicia proceso.

## Dónde pedir más ayuda

- Abre una issue en
  [GitHub](https://github.com/rroblf01/d-orm/issues) con el
  traceback completo, el bloque `DATABASES` (con secretos
  redacted), y la versión (`dorm --version`).
- Para problemas de migraciones, adjunta la salida de
  `dorm showmigrations` y `dorm dbcheck`.
