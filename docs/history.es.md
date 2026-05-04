# Auditoría (history)

`dorm.contrib.history` provee un audit trail opt-in: cada `save()`,
`asave()`, `delete()` y `adelete()` sobre un modelo trackeado
escribe una fila en una tabla paralela `<tabla>_history`. La tabla
history registra qué cambió, cuándo y (opcionalmente) quién.

Es un módulo contrib (no core) porque el tracking implica DDL extra
y write-amplification — lee los [caveats](#caveats) antes de
activarlo para cada modelo.

## Arranque rápido

```python
import dorm
from dorm.contrib.history import track_history


@track_history
class Article(dorm.Model):
    title: str = dorm.CharField(max_length=200)
    body: str = dorm.TextField()

    class Meta:
        db_table = "articles"
```

El decorador `@track_history` construye un modelo sibling
`ArticleHistorical` con los mismos campos más cuatro columnas de
auditoría, y lo registra en el registry de dorm — tu próximo
`dorm makemigrations` recoge la tabla nueva automáticamente.

```python
art: Article = Article.objects.create(title="hello", body="world")
art.title = "hi"
art.save()
art.delete()

# Tres filas: '+', '~', '-'
for row in Article.history.all().order_by("history_date"):
    print(row.history_type, row.title)
```

## Qué se trackea

El decorador construye un modelo sibling `<Name>Historical` con:

- Todas las columnas del modelo origen. Las PK pasan a columnas
  regulares indexadas (la tabla history tiene su propia PK
  surrogate porque la misma fila origen puede aparecer varias
  veces).
- `history_id: int` — `BigAutoField`, PK de la fila history.
- `history_date: datetime` — timestamp UTC del cambio.
- `history_type: str` — un solo carácter: `"+"` (insert),
  `"~"` (update), `"-"` (delete).
- `history_user_id: int | None` — entero opcional inyectado vía
  `set_history_user()` (ver [Atribución de usuario](#atribucion-de-usuario)).

El ordering por defecto del modelo history es `["-history_date"]`
así que `Article.history.all()` devuelve los cambios más recientes
primero.

## Atribución de usuario

Casi todo audit trail de producción quiere saber *quién* disparó el
cambio, no solo *qué*. `set_history_user()` planta el id del actor
en un `contextvars.ContextVar`; las filas history posteriores lo
recogen.

```python
from dorm.contrib.history import (
    set_history_user,
    reset_history_user,
    current_history_user,
)

# En un middleware FastAPI / Starlette:
async def history_user_middleware(request, call_next):
    token = set_history_user(request.user.id)
    try:
        return await call_next(request)
    finally:
        reset_history_user(token)
```

`current_history_user() -> int | None` lee el valor activo. El
default es `None`, así que las filas sin atribuir tienen
`history_user_id IS NULL`.

## Registro manual

Los hooks automáticos disparan en `save` / `delete` por instancia.
Las operaciones que esquivan la ruta por fila (`QuerySet.update()`,
`bulk_create`, `bulk_update`) **no** disparan `post_save` /
`post_delete`, así que no escriben filas history. Usa
`record_history_for(instance, kind)` (o `arecord_history_for(...)`)
para registrar una manualmente:

```python
from dorm.contrib.history import record_history_for, arecord_history_for


# Tras un update manual que esquiva save():
Article.objects.filter(pk=42).update(title="new")
art = Article.objects.get(pk=42)
record_history_for(art, "~", user_id=request.user.id)


# O versión async:
await arecord_history_for(art, "~", user_id=request.user.id)
```

`kind` debe ser `"+"`, `"~"` o `"-"`. `user_id` cae a
`current_history_user()` cuando se omite, así que los actores
puestos por middleware fluyen solos.

## Consultar history

`Model.history` expone un `Manager` sobre la tabla history — cada
método de queryset funciona igual que sobre el modelo origen.

```python
# Cada cambio sobre el article 42, del más antiguo al más nuevo:
changes = Article.history.filter(pk=42).order_by("history_date")

# Solo borrados registrados:
gone = Article.history.filter(history_type="-")

# ¿Quién borró la fila cuyo pk original era 42?
last_delete = (
    Article.history
    .filter(pk=42, history_type="-")
    .order_by("-history_date")
    .first()
)
print(last_delete.history_user_id, last_delete.history_date)
```

## Paridad async

`asave()` y `adelete()` van por la misma vía — cada modelo
trackeado registra un receiver async bajo `post_save.asend` /
`post_delete.asend`. El receiver sync detecta el event loop activo
y se sale, así que las escrituras async nunca duplican.

```python
import dorm
from dorm.contrib.history import track_history


@track_history
class Note(dorm.Model):
    body: str = dorm.TextField()

    class Meta:
        db_table = "notes"


async def write_note(body: str) -> None:
    note: Note = Note(body=body)
    await note.asave()
    # fila '+' async escrita vía aiosqlite / psycopg async.
```

## Caveats

- **Las rutas bulk esquivan los hooks.** `QuerySet.update`,
  `bulk_create`, `bulk_update` no emiten `post_save` /
  `post_delete`. Llama a `record_history_for` manualmente si la
  cobertura del audit importa ahí.
- **Drift de esquema.** La tabla history mirrorea las columnas del
  origen al decorar. Tras un add / rename de columna en el origen,
  re-ejecuta `dorm makemigrations` para que el autodetector
  re-sincronice la tabla history.
- **Write amplification.** Cada `save()` escribe una fila history.
  Para tablas de alto throughput, mete eso en tu presupuesto de
  IOPS / disco antes de activarlo.
- **No hay modo diff (todavía).** v3.2 graba un snapshot completo
  en cada cambio. Una versión futura puede añadir modo diff por
  columna para deployments sensibles a almacenamiento.
