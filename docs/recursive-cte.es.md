# Recursive CTEs (árboles, grafos)

`dorm.tree` (4.0+) genera CTEs recursivos para los dos patrones que
aparecen en 95% de los schemas con relaciones self-referenciales:

- **Descendientes** de un nodo (todos los hijos transitivos).
- **Ancestros** de un nodo (camino hacia la raíz).

PG, SQLite ≥ 3.8.3 y MySQL ≥ 8 soportan `WITH RECURSIVE`.

## El problema

Una tabla adyacente — cada fila tiene `parent_id` apuntando a otra
fila de la misma tabla:

```python
class Category(dorm.Model):
    name = dorm.CharField(max_length=100)
    parent_id = dorm.IntegerField(null=True, db_index=True)
```

Para listar todos los descendientes de `pk=42` haces falta SQL
recursivo. Sin helpers tendrías que escribir:

```sql
WITH RECURSIVE descendants AS (
    SELECT id, parent_id, name FROM categories WHERE parent_id = 42
    UNION ALL
    SELECT c.id, c.parent_id, c.name FROM categories c
    JOIN descendants d ON c.parent_id = d.id
)
SELECT * FROM descendants;
```

## Helper

```python
from dorm.tree import descendants

rows = descendants(Category, parent_field="parent_id", root_pk=42)
# [{"pk": 100, "parent_id": 42}, {"pk": 101, "parent_id": 42}, ...]
```

`descendants()` ejecuta el CTE y devuelve filas como dicts. Cada
dict tiene `pk` y `parent_id`. Para más columnas, construye el
CTE manualmente.

`ancestors()` es la inversa:

```python
from dorm.tree import ancestors

# Camino desde la categoría 999 hasta la raíz (sin incluir la raíz).
rows = ancestors(Category, parent_field="parent_id", leaf_pk=999)
```

## Construir el CTE manualmente

Para componer con un queryset normal:

```python
from dorm.tree import descendants_cte

cte = descendants_cte(
    Category,
    parent_field="parent_id",
    root_pk=42,
    fields=["id", "parent_id", "name"],   # columnas a proyectar
    cte_name="subtree",                    # nombre dentro del WITH
)

# Compone con with_cte() — carga las filas como Category instances:
qs = (
    Category.objects
    .with_cte(subtree=cte)
    .raw('SELECT * FROM "subtree" WHERE name LIKE %s', ["Books%"])
)
for cat in qs:
    print(cat.name)
```

## Detección de ciclos (PG)

Si tu grafo puede tener ciclos (raro en árboles, común en grafos
generales) usa `cycle_field`:

```python
cte = descendants_cte(
    Category,
    parent_field="parent_id",
    root_pk=42,
    cycle_field="path",
)
```

PG-only — usa `ARRAY[id]` para acumular el camino y un boolean
`is_cycle` que se vuelve `TRUE` cuando una fila ya se visitó. La
recursión se corta automáticamente.

SQLite no tiene literales array; si necesitas detección en SQLite,
emite el CTE manualmente con un contador de profundidad y un
`WHERE depth < N`.

## Caveats

- **Profundidad ilimitada por defecto**: si tu árbol es de
  millones de niveles encadenados (improbable), el CTE consume
  memoria server-side proporcional. Pon un `WHERE depth < N` en
  un CTE custom.
- **`UNION ALL` no detecta duplicados**: en grafos no-árbol, una
  misma fila puede aparecer N veces (cada camino que lleva a
  ella). Usa `cycle_field` para PG; en otros backends, agrega
  manualmente un campo `path` con concatenación de pks.
- **`fields=` debe ser válido**: la op valida los identificadores.
  Inyección no es posible vía esta API.

## Caso de uso: árbol de comentarios

```python
class Comment(dorm.Model):
    body = dorm.TextField()
    parent_id = dorm.IntegerField(null=True, db_index=True)
    article_id = dorm.IntegerField(db_index=True)

# Hilo completo bajo el comentario raíz 555:
ids = [r["pk"] for r in descendants(
    Comment, parent_field="parent_id", root_pk=555
)]
thread = list(Comment.objects.filter(pk__in=ids).order_by("created_at"))
```

## Caso de uso: ruta de breadcrumbs

```python
# De la categoría hoja hasta la raíz, ordenada de hoja a raíz:
breadcrumbs_ids = [r["pk"] for r in ancestors(
    Category, parent_field="parent_id", leaf_pk=current.pk
)]
# Recupera nombres en una sola query:
crumbs_by_id = {
    c.pk: c for c in Category.objects.filter(pk__in=breadcrumbs_ids)
}
crumbs = [crumbs_by_id[pk] for pk in breadcrumbs_ids]
```

## Más

- [Consultas](queries.md) — `with_cte()` y `CTE` literal
- [API: tree](api/tree.md)
