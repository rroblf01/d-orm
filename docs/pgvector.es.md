# Búsqueda vectorial con djanorm

`dorm.contrib.pgvector` cubre búsqueda por similitud vectorial
sobre **ambos** backends: PostgreSQL (vía
[pgvector](https://github.com/pgvector/pgvector)) y SQLite (vía
[sqlite-vec](https://github.com/asg017/sqlite-vec)). El mismo
código de modelo + queryset corre contra cualquiera — el field
elige el formato wire según el vendor de la conexión activa:

| Backend       | Tipo columna  | Funciones de distancia                  |
|---------------|---------------|-----------------------------------------|
| PostgreSQL    | `vector(N)`   | operadores `<->` / `<=>` / `<#>`        |
| SQLite        | `BLOB`        | `vec_distance_L2` / `vec_distance_cosine` |

El módulo expone:

- **`VectorField(dimensions=N)`** — el tipo de columna.
- **`L2Distance` / `CosineDistance` / `MaxInnerProduct`** —
  expresiones de distancia que componen con `annotate()` y
  `order_by()`.
- **`HnswIndex` / `IvfflatIndex`** — helpers de índice (solo
  PostgreSQL — sqlite-vec usa otro modelo basado en virtual
  tables que aún no envolvemos).
- **`VectorExtension`** — la operación de migración que activa
  la extensión correcta en cada backend.

> **Nota sobre `MaxInnerProduct`** — pgvector la trae (operador
> `<#>`); sqlite-vec no. En SQLite usa `CosineDistance` sobre
> embeddings L2-normalizados (matemáticamente equivalente módulo
> una constante).

## Paso a paso (PostgreSQL)

### 1. Instalar pgvector en tu servidor PostgreSQL

pgvector se distribuye como extensión binaria. En Debian / Ubuntu
con PostgreSQL 16:

```bash
sudo apt install postgresql-16-pgvector
```

Para otras distribuciones / servicios gestionados ver el [README upstream](https://github.com/pgvector/pgvector#installation-notes).
En AWS RDS / Aurora la extensión viene preinstalada — solo hace
falta habilitarla (paso 3).

### 2. Instalar el extra de Python

```bash
pip install 'djanorm[postgresql,pgvector]'
```

El extra `[pgvector]` es **solo PostgreSQL** — instala el paquete
`pgvector`, que registra un adaptador psycopg para que
`list[float]` y `numpy.ndarray` se conviertan automáticamente.
Sin él el field sigue funcionando, solo pierdes la conveniencia
con numpy.

Si tu proyecto tiene como objetivo *ambos* PostgreSQL y SQLite
(CI corre SQLite, prod corre PG), instala el meta-extra de
conveniencia `[vector]` — incluye `[pgvector]` y `[sqlite-vec]`
en uno:

```bash
pip install 'djanorm[postgresql,sqlite,vector]'
```

### 3. Generar la migración de la extensión

```bash
dorm makemigrations --enable-pgvector myapp
```

Eso escribe `myapp/migrations/0001_enable_pgvector.py`:

```python
from dorm.contrib.pgvector import VectorExtension

dependencies = []
operations = [VectorExtension()]
```

`VectorExtension` ejecuta `CREATE EXTENSION IF NOT EXISTS "vector"`
al aplicar y `DROP EXTENSION IF EXISTS "vector"` al revertir.
En backends no-PostgreSQL la operación es un no-op, así que la
misma migración se aplica limpiamente bajo SQLite (tus tests
siguen pasando).

### 4. Añade un `VectorField` a tu modelo

```python
import dorm
from dorm.contrib.pgvector import VectorField


class Document(dorm.Model):
    title = dorm.CharField(max_length=200)
    content = dorm.TextField()
    embedding = VectorField(dimensions=1536)   # OpenAI text-embedding-3-small

    class Meta:
        db_table = "documents"
```

`dimensions=` es obligatorio y tiene que coincidir con tu modelo
de embeddings. La columna se declara `vector(1536)` y pgvector
rechaza inserts cuya longitud difiera — el field replica la
comprobación en Python para que el ValidationError aparezca con
tu stack frame, no dentro de libpq.

### 5. Ejecuta `makemigrations` + `migrate`

```bash
dorm makemigrations myapp
dorm migrate
```

El autodetector recoge la nueva columna y emite una operación
`AddField` contra la migración de la extensión existente.

### 6. Insertar y consultar

```python
import openai

resp = openai.embeddings.create(
    model="text-embedding-3-small",
    input="hola mundo",
)
emb = resp.data[0].embedding   # list[float] length 1536

doc = Document.objects.create(
    title="hola",
    content="hola mundo",
    embedding=emb,
)
```

Para recuperar los *k* vecinos más cercanos, anota con una
expresión de distancia y ordena por ella:

```python
from dorm.contrib.pgvector import L2Distance

query_emb = openai.embeddings.create(
    model="text-embedding-3-small",
    input="saludos",
).data[0].embedding

nearest = list(
    Document.objects
    .annotate(score=L2Distance("embedding", query_emb))
    .order_by("score")[:10]
)
for doc in nearest:
    print(doc.title, doc.score)   # type: ignore — atributo runtime
```

Las tres expresiones de distancia mapean uno a uno con los
operadores de pgvector:

| Clase             | Operador | Significado                                |
|-------------------|----------|--------------------------------------------|
| `L2Distance`      | `<->`    | Euclídea (L2). Menor = más similar.        |
| `CosineDistance`  | `<=>`    | `1 - cosine_similarity`. Menor = más cerca. |
| `MaxInnerProduct` | `<#>`    | Producto interno negado (menor = más cerca). |

### 7. Añadir un índice — *imprescindible* para kNN en producción

Sin índice cada query kNN es un seq scan. A partir de unos pocos
miles de filas eso son segundos por petición. Hay dos métodos:

```python
from dorm.contrib.pgvector import HnswIndex, IvfflatIndex


class Document(dorm.Model):
    embedding = VectorField(dimensions=1536)

    class Meta:
        db_table = "documents"
        indexes = [
            HnswIndex(
                fields=["embedding"],
                name="doc_emb_hnsw",
                opclass="vector_l2_ops",
                m=16,
                ef_construction=64,
            ),
        ]
```

Después de añadirlo, `dorm makemigrations` + `dorm migrate`
emiten el `CREATE INDEX … USING hnsw …`.

#### Elegir el método de índice

| Método     | Build      | Recall    | Memoria  | Cuándo usarlo                                |
|------------|-----------:|----------:|---------:|----------------------------------------------|
| HNSW       | minutos    | excelente | alta     | Por defecto. Mejor recall, paga disco + RAM. |
| IVFFlat    | segundos   | bueno     | baja     | Memoria justa, tablas grandes, build crítico. |

#### El `opclass` importa

Elige la clase de operador que coincida con la distancia que
consultas — si no, el planner no puede usar el índice y hace
silenciosamente seq scan:

| Distancia        | Opclass              |
|------------------|----------------------|
| `L2Distance`     | `vector_l2_ops`      |
| `CosineDistance` | `vector_cosine_ops`  |
| `MaxInnerProduct`| `vector_ip_ops`      |

#### Tuning en tiempo de query

Ambos métodos exponen knobs recall-vs-latencia que viven fuera de
la definición del índice (son GUCs por sesión):

```python
# HNSW: ef_search por defecto 40; sube para mejor recall.
get_connection().execute("SET hnsw.ef_search = 100")

# IVFFlat: probes por defecto 1; rango 1..lists.
get_connection().execute("SET ivfflat.probes = 10")
```

Configúralos en el entry-point de la request (dependency de
FastAPI, middleware Django) para que toda la request use el
mismo target.

## Paso a paso (SQLite)

### 1. Instalar sqlite-vec

sqlite-vec es una extensión cargable client-side — no requiere
instalación server-side. El paquete PyPI trae binarios compilados
para Linux / macOS / Windows:

```bash
pip install 'djanorm[sqlite,sqlite-vec]'
```

El extra `[sqlite-vec]` es **solo SQLite** — incluye únicamente
el paquete `sqlite-vec` sin tirar del adaptador `pgvector` de
psycopg. Usa `[pgvector]` para el lado PostgreSQL, o `[vector]`
para ambos a la vez si tu proyecto soporta los dos backends.

### 2. Verifica que tu Python soporta `enable_load_extension`

Casi todas las distros CPython traen `sqlite3` compilado contra
una SQLite que permite cargar extensiones externas. Algunas no —
notablemente Python de sistema en Ubuntu / Debian antes de 3.11.
Comprobación rápida:

```python
import sqlite3
conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)   # AttributeError → no soportado
```

Si lanza error, instala Python desde python.org / pyenv / uv.

### 3. Generar la migración de la extensión

Mismo comando que PostgreSQL:

```bash
dorm makemigrations --enable-pgvector myapp
```

La migración generada llama a `VectorExtension()`, que en SQLite:

- Carga sqlite-vec en la conexión de la migración.
- Marca el wrapper para que cada conexión *futura* (re-aperturas,
  hilos nuevos) auto-cargue la extensión.

La marca vive en la instancia del wrapper, no en la BD, así que
un restart de proceso necesita volver a tocar el código de la
migración — re-ejecuta la migración una vez al arranque o llama
a `load_sqlite_vec_extension(raw_sqlite3_conn)` desde el boot
de tu app.

### 4. Define el modelo igual

```python
import dorm
from dorm.contrib.pgvector import VectorField


class Document(dorm.Model):
    title = dorm.CharField(max_length=200)
    embedding = VectorField(dimensions=384)   # menor para SQLite

    class Meta:
        db_table = "documents"
```

En SQLite, `db_type()` devuelve `BLOB`. El field empaqueta los
valores como float32 little-endian — formato que sqlite-vec
almacena nativamente y que `vec_distance_L2(col, ?)` acepta
directamente.

### 5. Consulta igual

```python
from dorm.contrib.pgvector import L2Distance

nearest = list(
    Document.objects
    .annotate(score=L2Distance("embedding", query_emb))
    .order_by("score")[:10]
)
```

La expresión detecta el backend activo en tiempo de compilación
y emite `embedding <-> %s::vector` (PG) o
`vec_distance_L2(embedding, %s)` (SQLite).

### Soporte de índices (SQLite)

El modelo de índices de sqlite-vec se monta sobre virtual tables
(`vec0`), que no encaja con el flujo regular-table que djanorm
expone hoy. **Seq-scan con `vec_distance_L2` es razonable hasta
unos cientos de miles de vectores** en hardware estándar; si
necesitas ANN a escala SQLite, baja a `RunSQL` para crear una
virtual table `vec0` paralela a la columna. Posiblemente lo
envolvamos en un release futuro cuando la API de sqlite-vec
estabilice.

## Trampas comunes

* **Las dimensiones tienen que coincidir con el modelo que
  produjo el embedding.** OpenAI `text-embedding-3-small` es 1536,
  `…3-large` es 3072, `text-embedding-ada-002` también 1536. Un
  desajuste lanza `ValidationError` con el tamaño culpable.
* **pgvector limita `vector` a 16000 dimensiones.** Para vectores
  más grandes usa `halfvec` (floats de 16-bit, límite 32k) o
  `sparsevec` en pgvector ≥ 0.7. Esos tipos aún no están
  envueltos por djanorm.
* **El primer build HNSW sobre tabla grande es lento.** Construye
  el índice *después* del bulk-load, o acepta una ventana de
  migración larga. IVFFlat es más rápido pero techo de recall
  más bajo.
* **No mezcles opclasses en la misma columna.** Una opclass por
  índice y por columna.

## Referencia

- [`VectorField`](api/pgvector.md#vectorfield)
- [`L2Distance` / `CosineDistance` / `MaxInnerProduct`](api/pgvector.md#distance-expressions)
- [`HnswIndex` / `IvfflatIndex`](api/pgvector.md#index-helpers)
- [`VectorExtension`](api/pgvector.md#vectorextension)
- pgvector upstream: <https://github.com/pgvector/pgvector>
