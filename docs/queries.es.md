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

## Carga parcial

```python
Author.objects.only("name", "email")     # SELECT name, email
Author.objects.defer("bio")              # SELECT todo menos bio
```

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
