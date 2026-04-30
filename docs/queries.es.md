# Consultas

El QuerySet de dorm es perezoso: construirlo no toca la BD. La SQL se
ejecuta solo cuando iteras, slices, o llamas a un método terminal
(`first()`, `count()`, `exists()`, ...).

## Filter, exclude, get

```python
# Igualdad
Author.objects.filter(name="Alice")

# Lookups: __gt, __gte, __lt, __lte, __contains, __icontains,
#          __startswith, __endswith, __in, __isnull, __range, __regex
Author.objects.filter(age__gte=18, name__icontains="al")
Author.objects.exclude(email__isnull=True)

# Un único objeto — lanza DoesNotExist / MultipleObjectsReturned
alice = Author.objects.get(email="alice@example.com")

# Misma idea pero devuelve None en lugar de lanzar
alice = Author.objects.get_or_none(email="missing@example.com")
```

### Lookups a través de relaciones

```python
# Libros cuyo autor empieza por "Al":
Book.objects.filter(author__name__startswith="Al")

# Relación inversa vía related_name
Author.objects.filter(books__published=True).distinct()
```

### Objetos Q — lógica booleana compleja

```python
from dorm import Q

Author.objects.filter(
    Q(age__gte=18) & (Q(name__startswith="A") | Q(email__contains="@hq."))
)
```

### Expresiones F — referenciar otras columnas

```python
from dorm import F

# Incremento atómico (sin race contra read-modify-write):
Post.objects.filter(pk=42).update(views=F("views") + 1)

# Comparar dos columnas
Post.objects.filter(updated_at__gt=F("created_at"))
```

## Slicing y ordenación

```python
# OFFSET / LIMIT — perezoso, no toca SQL hasta iterar
qs = Author.objects.order_by("name")[10:20]

# Invertir un queryset
Author.objects.order_by("-age")
```

## Conteos y existencia

```python
Author.objects.count()                       # SELECT COUNT(*)
Author.objects.filter(active=True).exists()  # SELECT 1 ... LIMIT 1
Author.objects.first()                       # SELECT ... LIMIT 1
Author.objects.last()
```

## Materializar el queryset completo

`all()` devuelve un `QuerySet` nuevo — no toca la BD hasta que
itere, slices, o llames a un método terminal.

```python
# Sync
authors = list(Author.objects.all())
for a in Author.objects.all():
    ...

# Async — tres formas equivalentes
authors = [a async for a in Author.objects.all()]
authors = await Author.objects.all()           # los QuerySets son awaitable
async for a in Author.objects.all():
    ...
```

Usa `iterator()` / `aiterator()` (ver [Streaming](#streaming-para-resultsets-enormes))
cuando no quieras cargar todas las filas en memoria.

## Values y value lists

```python
# Sync — list[dict[str, Any]] — encadenable (filter, order_by) antes de iterar
Author.objects.values("name", "age")

# Async — misma forma, awaitable
await Author.objects.avalues("name", "age")
# o bien, como los QuerySets son awaitable:
await Author.objects.values("name", "age")

# Sync — list[tuple]; flat=True con una sola columna devuelve list[value]
Author.objects.values_list("name", flat=True)

# Async — misma forma, awaitable
await Author.objects.avalues_list("name", flat=True)
await Author.objects.values_list("name", flat=True)
```

`avalues` / `avalues_list` materializan el queryset entero en un
único round-trip; para sets enormes prefiere streaming con
`aiterator()`.

## Agregaciones y anotaciones

```python
from dorm import Sum, Avg, Count, Max, Min

# Agregación de todo el queryset
Author.objects.aggregate(total=Sum("age"), avg=Avg("age"))
# → {"total": 137, "avg": 27.4}

# Anotación por fila (columna calculada)
Author.objects.annotate(post_count=Count("books"))
```

### `alias()` — annotate sin proyectar

`alias()` declara una expresión usable en `filter()` / `exclude()` /
`order_by()` pero **no** se proyecta en las filas resultado — te
ahorras el ancho de banda y la hidratación por fila cuando solo
necesitas el valor para construir un predicado o una clave de orden:

```python
authors = (
    Author.objects
    .alias(book_count=Count("books"))
    .filter(book_count__gte=5)        # usa el alias
    .order_by("name")
)
# SELECT solo las columnas normales de Author; el COUNT() participa
# en el WHERE pero no se devuelve.
```

Promueve un alias a proyección real volviéndolo a declarar con
`annotate(name=...)` más adelante en la cadena — paridad con Django.

## Funciones BD

```python
from dorm import Case, When, Coalesce, Lower, Upper, Length, Concat, Now, Cast, Abs

Author.objects.annotate(
    label=Case(
        When(age__lt=18, then="menor"),
        When(age__gte=65, then="senior"),
        default="adulto",
    ),
    full_name=Concat(Lower("first_name"), " ", Lower("last_name")),
)
```

## Operaciones de conjunto

```python
qs_a = Author.objects.filter(active=True)
qs_b = Author.objects.filter(books__published=True)

qs_a.union(qs_b)          # UNION (distinct)
qs_a.union(qs_b, all=True)
qs_a.intersection(qs_b)
qs_a.difference(qs_b)
```

## Update y delete

```python
# Update masivo — un único UPDATE, devuelve rowcount
n = Author.objects.filter(active=False).update(active=True)

# Delete masivo — gestiona cadenas on_delete CASCADE
n, by_model = Author.objects.filter(age__lt=10).delete()
```

Para updates masivos con valores *diferentes* por fila, usa
`bulk_update`:

```python
authors = list(Author.objects.all())
for a in authors:
    a.score = compute_score(a)
Author.objects.bulk_update(authors, fields=["score"], batch_size=500)
# 1 sentencia UPDATE por batch (CASE WHEN), no N sentencias.
```

## Insertar

```python
Author.objects.create(name="Alice", age=30)   # INSERT
Author.objects.bulk_create([
    Author(name=f"User{i}", age=i) for i in range(1_000)
], batch_size=500)
# 1 INSERT multi-row por batch.
```

### Upsert (`bulk_create` con manejo de conflicto)

`bulk_create` acepta dos flags de upsert que mapean a la semántica
`ON CONFLICT` de PostgreSQL / SQLite:

```python
# Saltar duplicados (ON CONFLICT DO NOTHING)
Tag.objects.bulk_create(
    [Tag(name="alpha"), Tag(name="beta")],
    ignore_conflicts=True,
)

# Actualizar al haber conflicto (ON CONFLICT (...) DO UPDATE SET ...)
Author.objects.bulk_create(
    [Author(email="x@y.com", name="Updated", age=42)],
    update_conflicts=True,
    update_fields=["name", "age"],     # qué refrescar al haber conflicto
    unique_fields=["email"],            # qué constraint identifica el conflicto
)
```

`unique_fields=` es **obligatorio** con `update_conflicts=True`.
`update_fields=` por defecto cubre todas las columnas no-PK / no-
unique cuando se omite — normalmente lo que quieres para una
sincronización idempotente desde una fuente externa. La contraparte
async, `abulk_create(...)`, expone los mismos flags.

Cuando puede haber filas saltadas por conflicto, las PKs devueltas
**no** se asignan a los objetos de entrada — la BD no reporta qué
filas escribieron de verdad. Re-fetch por `unique_fields` si
necesitas el set final de PKs.

## get_or_create / update_or_create

```python
obj, created = Author.objects.get_or_create(
    email="x@y.com",
    defaults={"name": "X", "age": 0},
)

obj, created = Author.objects.update_or_create(
    email="x@y.com",
    defaults={"name": "Actualizado", "age": 99},
)
```

Ambos corren dentro de una transacción para evitar dobles inserts en
escenarios concurrentes.

## Carga de relaciones

### `select_related` — JOIN

```python
# 1 query con JOIN — author precargado
for book in Book.objects.select_related("author"):
    print(book.author.name)         # sin query extra
```

### `prefetch_related` — query separada, en batch

```python
# 2 queries en total: posts + (1 IN-query con todos los authors)
for author in Author.objects.prefetch_related("books"):
    print(author.books.all())       # sin query extra
```

Para M2M, `prefetch_related` ejecuta un único JOIN contra la tabla
intermedia (sin el "fetch through y luego fetch targets" en dos pasos).

#### FKs polimórficas (`GenericForeignKey`)

`prefetch_related("target")` también funciona sobre un
`GenericForeignKey`. Sin él, cada lectura del descriptor hace su
propio `get(pk=…)` — N+1 cuando iteras una queryset de N tags
apuntando a K content types distintos. Con él, dorm agrupa las
instancias por `content_type_id`, recupera todos los `ContentType`
referenciados en un único SELECT, y luego emite un
`filter(pk__in=…)` por content type — total: **1 + 1 + K** queries.

```python
# 3 tags apuntando a 2 articles + 2 books
# = 1 (tags) + 1 (content_types) + 2 (uno por CT) = 4 queries
for tag in Tag.objects.prefetch_related("target"):
    print(tag.target)        # servido desde la caché, sin query extra
```

Dos notas de compatibilidad:

- Un `Prefetch("target", queryset=…)` personalizado **no está
  soportado** — una sola queryset no puede filtrar todos los
  targets de un GFK heterogéneo. Si necesitas filtrar, prefetcha
  cada relación concreta explícitamente con su propio `Prefetch`.
- `to_attr=…` tampoco está soportado en un GFK; dorm rellena el
  propio slot de caché del descriptor, así que `instance.target`
  devuelve el objeto resuelto sin una segunda query.

#### Relaciones genéricas inversas (`GenericRelation`)

Simétrico: `prefetch_related` sobre una `GenericRelation` inversa
(`Article.objects.prefetch_related("tags")`) agrupa cada instancia
target por PK, lanza **un** SELECT al modelo relacionado filtrando
por `content_type` + `object_id__in`, y rellena el slot de caché
del manager. Después `article.tags.all()` lee de memoria.

```python
# 3 artículos + 5 tags apuntando = 1 (artículos) + 1 (tags) = 2 queries
for article in Article.objects.prefetch_related("tags"):
    for tag in article.tags.all():     # servido desde caché
        ...
```

`Prefetch("tags", queryset=Tag.objects.filter(label="urgent"))` se
respeta — la queryset del usuario se AND-ea con el predicado
`content_type`.

## Carga parcial

```python
Author.objects.only("name", "email")     # SELECT name, email
Author.objects.defer("bio")              # SELECT todo menos bio
```

### Componiendo con `select_related`

`only()` / `defer()` aceptan rutas con puntos para restringir la
proyección de una relación cargada con `select_related`:

```python
# JOIN a publishers, pero solo trae publisher.name (más la PK para identidad).
Author.objects.select_related("publisher").only("name", "publisher__name")

# Mismo JOIN, pero excluye publisher.bio del SELECT — mantiene el resto.
Author.objects.select_related("publisher").defer("publisher__bio")
```

Nombres pelados restringen el modelo padre (comportamiento clásico);
nombres con puntos restringen la relación nombrada. La PK del modelo
relacionado se incluye siempre implícitamente para que la instancia
hidratada conserve su identidad. Los dos métodos escriben en buckets
distintos del estado, así que combinarlos funciona:

```python
Author.objects.select_related("publisher").only("name").defer("publisher__bio")
# padre: id, name. publisher: cada columna menos bio.
```

## Bloqueo de filas: `select_for_update`

Bloquea filas para la transacción que las envuelve. Tiene que
llamarse dentro de un bloque `atomic()` / `aatomic()` — si no,
PostgreSQL libera el lock de inmediato al hacer autocommit y la
llamada queda en no-op.

```python
from dorm import transaction

with transaction.atomic():
    a = Author.objects.select_for_update().get(pk=1)
    a.balance -= 100
    a.save()
```

Tres flags mapean a las variantes de lock por fila de PostgreSQL:

```python
# Patrón cola de tareas: cada worker se lleva la siguiente fila *no
# bloqueada*.
job = (
    Job.objects
    .filter(status="pending")
    .select_for_update(skip_locked=True)
    .first()
)

# Fallar rápido ante contención en lugar de esperar.
qs.select_for_update(no_wait=True)

# Bloquear solo tablas concretas en joins (evita bloquear padres en
# una cadena de select_related).
qs.select_related("publisher").select_for_update(of=("authors",))
```

`skip_locked` y `no_wait` son mutuamente exclusivos. Las tres son
PostgreSQL-only — pasarlas en SQLite lanza `NotImplementedError`
(SQLite serializa escritores con el lock de archivo, así que las
variantes a nivel de fila no traducen).

## Streaming para resultsets enormes

```python
# Por defecto: fetch de todas las filas, iterar en memoria (bien para miles).
for a in Author.objects.iterator():
    process(a)

# chunk_size → cursor server-side en PG, arraysize en SQLite.
# Para escaneos de millones de filas.
for a in Author.objects.order_by("id").iterator(chunk_size=5000):
    process(a)
```

## EXPLAIN

```python
slow_qs = Author.objects.filter(age__gte=18).select_related("publisher")
print(slow_qs.explain(analyze=True))
```

PG devuelve el plan completo; SQLite devuelve `EXPLAIN QUERY PLAN`.
Útil cuando una ruta va lenta en prod y necesitas saber qué eligió el
planner.

## SQL crudo (escape hatch)

```python
authors = Author.objects.raw(
    "SELECT * FROM authors WHERE age > %s ORDER BY name",
    [18],
)
for a in authors:
    print(a.name)
```

`raw()` devuelve un `RawQuerySet` que hidrata filas a instancias del
modelo. Para resultados que no mapean a un modelo, baja a
`get_connection().execute(...)`.

!!! danger "Usa placeholders, nunca f-strings"

    `raw()` envía `raw_sql` a la base de datos tal cual — los valores
    se ligan vía la lista `params`, **nunca** interpolados dentro de
    la propia cadena SQL:

    ```python
    # SEGURO — el valor pasa por el binding de psycopg / sqlite3
    Author.objects.raw("SELECT * FROM authors WHERE id = %s", [user_id])

    # INSEGURO — convierte input de usuario en SQL
    Author.objects.raw(f"SELECT * FROM authors WHERE id = {user_id}")
    ```

    Como red de seguridad, dorm cuenta los placeholders (`%s` y `$N`,
    saltando los que estén dentro de literales entrecomillados) y
    rechaza construir el `RawQuerySet` si el número no coincide con
    `len(params)`. Eso pilla el desliz más habitual — construir el
    SQL con `f""` y olvidar pasar los valores — en tiempo de
    construcción en vez de aparecer como un error confuso del
    motor.

    Para identificadores dinámicos (nombres de tabla o columna que no
    están fijos a coding time), valídalos contra una allowlist antes
    de interpolarlos — los placeholders ligan valores, no
    identificadores.

### `Cast(...)` acepta un conjunto fijo de tipos SQL

`Cast(expr, output_field=...)` interpola su segundo argumento dentro
del SQL (no existe binding para nombres de tipo), así que
`output_field` se valida contra una allowlist:

```python
from dorm import Cast, F

Author.objects.annotate(age_str=Cast(F("age"), output_field="TEXT"))
```

Los tipos base permitidos incluyen `INTEGER`, `BIGINT`, `SMALLINT`,
`REAL`, `DOUBLE PRECISION`, `FLOAT`, `NUMERIC`, `DECIMAL`, `TEXT`,
`VARCHAR`, `CHAR`, `BLOB`, `BYTEA`, `BOOLEAN`, `BOOL`, `DATE`,
`TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIME`, `JSON`, `JSONB`,
`UUID`. Se acepta una especificación opcional de longitud/precisión
(`VARCHAR(255)` o `NUMERIC(10, 2)`). Cualquier otro valor levanta
`ImproperlyConfigured` inmediatamente en construcción del queryset,
para que un typo o input no saneado nunca llegue al SQL.

## Consultas avanzadas

Bloques para las queries de reporting no triviales — lo que de
otra forma te obligaría a `RawQuerySet`:

- **`Subquery(qs)` / `Exists(qs)` / `OuterRef("col")`** —
  subconsultas correlacionadas que componen con `filter()` /
  `annotate()`.
- **`Window(expr, partition_by=, order_by=)`** más `RowNumber`,
  `Rank`, `DenseRank`, `NTile`, `Lag`, `Lead`, `FirstValue`,
  `LastValue` — ranking, totales acumulados, deltas sin bajar a
  SQL crudo.
- **`QuerySet.with_cte(name=qs)`** — CTEs no recursivos.
- **Funciones escalares**: `Greatest`, `Least`, `Round`, `Trunc`,
  `Extract`, `Substr`, `Replace`, `StrIndex`.
- **Búsqueda full-text (PostgreSQL)** vía
  `dorm.search.SearchVector` / `SearchQuery` / `SearchRank` y el
  lookup `__search`.
- **`QuerySet.cursor_paginate(...)` /
  `acursor_paginate(...)`** — paginación por cursor con ordenación
  estable y coste O(1) en páginas profundas.
