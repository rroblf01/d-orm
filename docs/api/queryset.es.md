# QuerySet y Manager

Referencia de la superficie pública del query builder. Las firmas son
código Python — la prosa es traducción manual.

## `Manager` / `BaseManager`

`Author.objects` es un `Manager` (subclase de `BaseManager`). Cada
método terminal devuelve un valor; cada método chainable devuelve un
nuevo `QuerySet`.

```python
all() -> QuerySet
none() -> QuerySet
filter(*args, **kwargs) -> QuerySet
exclude(*args, **kwargs) -> QuerySet
get(*args, **kwargs) -> Model
get_or_none(*args, **kwargs) -> Model | None
get_or_create(**lookups, defaults=None) -> tuple[Model, bool]
update_or_create(**lookups, defaults=None) -> tuple[Model, bool]
order_by(*fields: str) -> QuerySet
distinct() -> QuerySet
select_related(*fields: str) -> QuerySet
prefetch_related(*fields: str) -> QuerySet
annotate(**exprs) -> QuerySet
values(*fields: str) -> QuerySet
values_list(*fields: str, flat: bool = False) -> ValuesListQuerySet
only(*fields: str) -> QuerySet
defer(*fields: str) -> QuerySet
create(**kwargs) -> Model
bulk_create(objs: list[Model], batch_size: int | None = None) -> list[Model]
bulk_update(objs: list[Model], fields: list[str], batch_size: int | None = None) -> int
in_bulk(id_list: list, field_name: str = "pk") -> dict
count() -> int
exists() -> bool
first() -> Model | None
last() -> Model | None
aggregate(**exprs) -> dict[str, Any]
update(**kwargs) -> int
delete() -> tuple[int, dict[str, int]]
raw(sql: str, params: list | None = None) -> RawQuerySet
using(alias: str) -> QuerySet
```

Cada método sync que toca BD tiene su contraparte `a*` async:
`acreate`, `aget`, `acount`, `afirst`, `aupdate`, `adelete`,
`abulk_create`, `abulk_update`, `aaggregate`, `avalues`,
`avalues_list`, `ain_bulk`, `araw`, etc.

`all()`, `none()`, `filter()`, etc. **no son async** — solo devuelven
QuerySets nuevos, no tocan BD. Mira [Patrones async](../async.md).

## `QuerySet`

Construido por chain a partir de un manager. Es **lazy**: no toca BD
hasta iterar, slicear, awaitar o llamar a un método terminal.

### Iteración

```python
qs = Author.objects.filter(active=True)

# Sync
for a in qs: ...
list(qs)
len(qs)

# Async
async for a in qs: ...
authors = await qs              # equivalente a [a async for a in qs]
```

### Lookups en filter / exclude / get

Soportados: `__exact`, `__iexact`, `__gt`, `__gte`, `__lt`, `__lte`,
`__contains`, `__icontains`, `__startswith`, `__istartswith`,
`__endswith`, `__iendswith`, `__in`, `__isnull`, `__range`, `__regex`,
`__iregex`. PG-only: `__array_contains` (`@>`), `__array_overlap`
(`&&`), `__json_has_key` (`?`), `__json_has_any` (`?|`),
`__json_has_all` (`?&`).

### Slicing

```python
Author.objects.order_by("name")[10:20]   # OFFSET 10 LIMIT 10, lazy
```

### `iterator(chunk_size=N)`

Streaming sin cargar todo en memoria. PG abre cursor server-side;
SQLite usa `arraysize=N` en el cursor.

```python
for a in Author.objects.order_by("id").iterator(chunk_size=5000):
    ...

async for a in Author.objects.aiterator(chunk_size=5000):
    ...
```

### `explain(analyze=False)` / `aexplain(analyze=False)`

Devuelve el plan de ejecución como string. PG: `EXPLAIN ANALYZE
BUFFERS`. SQLite: `EXPLAIN QUERY PLAN`.

```python
print(Author.objects.filter(age__gte=18).explain(analyze=True))
```

### `select_for_update()`

Añade `... FOR UPDATE` para tomar row-locks dentro de un `atomic()`
bloque (read-then-write seguro bajo contención).

## `CombinedQuerySet`

Resultado de `qs.union(...)`, `qs.intersection(...)`,
`qs.difference(...)`. Hereda de `QuerySet`; soporta el mismo iterar /
contar / first.

```python
a = Author.objects.filter(active=True)
b = Author.objects.filter(books__published=True)
combined = a.union(b)              # UNION DISTINCT
combined_all = a.union(b, all=True) # UNION ALL
```

## `RawQuerySet`

Devuelto por `Manager.raw(sql, params)`. Itera filas hidratadas a
instancias del modelo.

```python
authors = Author.objects.raw(
    "SELECT * FROM authors WHERE age > %s ORDER BY name",
    [18],
)
for a in authors:
    print(a.name)
```

Para filas que no mapean a un modelo, baja a
`get_connection().execute(sql, params)`.

---

> Para la versión auto-generada desde docstrings (en inglés), mira
> [QuerySet & Manager (English)](../../api/queryset/).
