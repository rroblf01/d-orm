# Benchmark comparativo

Comparativa side-by-side entre **djanorm**, **Django ORM**,
**SQLAlchemy 2.0** y **Tortoise ORM** sobre los mismos cinco
escenarios — todos contra SQLite en proceso para aislar el coste del
ORM del coste de red / disco.

## Reproducir

```bash
uv pip install django sqlalchemy tortoise-orm aiosqlite
uv run python -m bench.compare --runs 5 --ops 1000
```

ORMs no instalados se marcan `skipped:` y no rompen la corrida — la
salida muestra solo los disponibles.

Flags:

- `--runs N` — repeticiones por escenario (default 3); reporta
  mediana sobre las repeticiones.
- `--ops N` — operaciones por repetición (default 200).
- `--orms dorm django sqlalchemy tortoise` — subconjunto a medir.
- `--json` — salida JSON en lugar de tabla.

## Escenarios

| Escenario | Qué mide |
|-----------|----------|
| `insert_one` | `Model.objects.create(...)` repetido N veces (cada uno commit propio) |
| `bulk_insert` | `bulk_create([...N...])` en una llamada |
| `get_by_pk` | `Model.objects.get(pk=…)` punto por punto sobre N filas pre-existentes |
| `filter_count` | `Model.objects.filter(active=True).count()` repetido N veces |
| `list_first_n` | `list(Model.objects.all()[:N/10])` repetido 10 veces |

## Resultados

Entorno:

- Python 3.14.4, Linux x86_64, SQLite en proceso
- djanorm 4.0.0, Django 6.0.4, SQLAlchemy 2.0.49, Tortoise ORM 1.1.7
- 5 repeticiones × 1000 operaciones por escenario; valores reportados
  son la **mediana de microsegundos por operación**.

| Escenario | dorm | django | sqlalchemy | tortoise |
|---|---|---|---|---|
| `bulk_insert` | **1.5 µs/op** | 8.0 µs/op | 23.8 µs/op | 2.6 µs/op |
| `list_first_n` | **3.5 µs/op** | 4.4 µs/op | 5.5 µs/op | 6.9 µs/op |
| `filter_count` | **89.4 µs/op** | 243.6 µs/op | 202.0 µs/op | 204.2 µs/op |
| `get_by_pk` | **62.2 µs/op** | 182.4 µs/op | 172.4 µs/op | 157.4 µs/op |
| `insert_one` | 117.7 µs/op | 175.2 µs/op | 262.8 µs/op | **86.6 µs/op** |

dorm gana 4/5 escenarios. Tortoise gana en `insert_one` (commits
individuales) por un margen estrecho — el orden de magnitud es el
mismo. SQLAlchemy 2.0 es el más lento del set en escrituras
unitarias y bulk; Django queda en el medio.

### Lectura por categoría

- **Bulk inserts** (`bulk_insert`): dorm ~6× más rápido que Django,
  ~15× más rápido que SQLAlchemy. La diferencia viene de cómo cada
  ORM agrupa el `INSERT ... VALUES (…), (…), …`: dorm emite una
  sola query con todos los placeholders; SQLAlchemy `add_all + commit`
  envía N statements por defecto y solo agrupa con `executemany`
  cuando lo configuras explícitamente.
- **Lecturas indexadas** (`get_by_pk`, `filter_count`): dorm ~2-3×
  más rápido que el resto. La diferencia se concentra en el coste
  Python del compilador SQL — dorm cachea la forma compilada de
  consultas repetidas (`@functools.lru_cache` en `_to_pyformat`),
  algo que el plan de Django re-genera cada vez.
- **Inserts unitarios** (`insert_one`): Tortoise gana por agrupar
  los `INSERT` en un único cursor sin re-checkout de conexión por
  llamada. dorm pierde aquí ~30 % por su política conservadora de
  abrir/cerrar transacción autocommit por `create()`.
- **Iteración de querysets** (`list_first_n`): empate técnico —
  todos están dominados por el coste de fetching de SQLite, no por
  el ORM.

### Caveats

- La gráfica refleja **coste de framework Python**. En producción la
  latencia de red al servidor PG/MySQL eclipsa por 10-100× lo que se
  mide aquí. La métrica importa para hot loops que ejecutan miles
  de queries por segundo (cron jobs, pipelines ETL, dashboards).
- Cifras varían ±5-10 % entre corridas por jitter del scheduler.
  El comando reporta también `best_seconds_per_op` en `--json`,
  útil para descartar outliers.
- SQLAlchemy 2.0 expone una capa Core mucho más rápida que la capa
  Session/ORM medida aquí — la comparativa pone el escenario "ORM
  alto nivel" porque es el equivalente directo a `Model.objects`.
- Django mide al modo "modelo dinámico sin app" — INSTALLED_APPS
  reducido al mínimo. Una app real introduce coste de signal
  dispatch que no aparece aquí.

## Cómo extender

Añade un escenario nuevo en cada función `_run_<orm>` y agrégalo al
diccionario final que se devuelve. La firma estable es
`fn(ops: int) -> float` (segundos transcurridos). Usa `_measure(fn,
ops, runs)` para la agregación; ya devuelve mediana + best.

Para añadir un ORM nuevo, escribe `_run_<orm>(ops, runs) -> dict` y
registra el callable en `_RUNNERS` al final del módulo.
