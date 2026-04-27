# Integración con FastAPI

dorm trae un adaptador de Pydantic v2 que genera esquemas a partir
de tus modelos — así tienes una única fuente de verdad para tablas y
para los bodies de request / response de FastAPI.

## Instalación

Elige el extra de backend que se corresponda con tu base de datos —
`sqlite` (incluye `aiosqlite` para la ruta async) o `postgresql`
(incluye `psycopg` con su pool de conexiones). Añade `pydantic` para
el adaptador de esquemas de Pydantic v2 sobre el que se monta esta
guía:

```bash
# SQLite + esquemas Pydantic
uv pip install 'djanorm[sqlite,pydantic]'

# PostgreSQL + esquemas Pydantic
uv pip install 'djanorm[postgresql,pydantic]'
```

No existe un extra `async` separado — los drivers async
(`aiosqlite`, `psycopg`) viajan dentro de los mismos extras
`sqlite` / `postgresql` que sus equivalentes síncronos, así que con
una sola instalación cubres ambos modos.

## Lifespan de la app

Configura dorm en un lifespan de FastAPI y limpia al apagar:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
import dorm
from dorm.db.connection import close_all_async

@asynccontextmanager
async def lifespan(app: FastAPI):
    dorm.configure(
        DATABASES={
            "default": {
                "ENGINE": "postgresql",
                "NAME": "myapp",
                "USER": "myapp",
                "PASSWORD": "...",
                "HOST": "localhost",
                "PORT": 5432,
            }
        }
    )
    yield
    await close_all_async()

app = FastAPI(lifespan=lifespan)
```

`close_all_async()` drena cada pool async. Sin esto, el shutdown
graceful de FastAPI puede colgarse esperando conexiones residuales.

## Esquemas

Dos formas de derivar esquemas Pydantic desde un modelo dorm.

### `schema_for(...)` — one-liner rápido

```python
from dorm.contrib.pydantic import schema_for
from .models import Author

AuthorOut = schema_for(Author)
AuthorIn = schema_for(Author, exclude=("id",))
AuthorPatch = schema_for(Author, optional=("name", "age", "email"))
```

| Argumento | Efecto |
|---|---|
| `name=` | nombre de la clase (default `f"{Model.__name__}Schema"`) |
| `exclude=` | tupla de campos a omitir |
| `only=` | tupla de campos a conservar (mutuamente excluyente con `exclude`) |
| `optional=` | marca esos campos como `Optional[...] = None` (bodies de PATCH) |
| `base=` | `BaseModel` personalizado (p.ej. para config compartida) |

`from_attributes=True` se fija automáticamente, así que puedes pasar
una instancia dorm directamente a `Schema.model_validate(obj)` o
usarla como `response_model` de FastAPI.

### `DormSchema` — clase declarativa

```python
from dorm.contrib.pydantic import DormSchema
from .models import Author, Publisher

class PublisherOut(DormSchema):
    class Meta:
        model = Publisher
        fields = ("id", "name")

class AuthorOut(DormSchema):
    bio_url: str | None = None       # campo extra declarado explícitamente

    class Meta:
        model = Author
        exclude = ("internal_notes",)
        nested = {"publisher": PublisherOut}   # FK → sub-esquema
```

La metaclase recorre `Author._meta.fields` y añade anotaciones para
cada columna, *salvo* las que ya hayas declarado tú. Pasa
`fields=("a", "b")` para whitelist, `exclude=("c",)` para blacklist,
o `optional=("a",)` para esquemas estilo PATCH. `nested=` cambia
una FK o M2M por un sub-esquema (si no, las FKs se serializan como
su PK entera).

## Una ruta CRUD completa

```python
from fastapi import APIRouter, HTTPException
from dorm.contrib.pydantic import DormSchema
from .models import Author

class AuthorIn(DormSchema):
    class Meta:
        model = Author
        exclude = ("id",)

class AuthorOut(DormSchema):
    class Meta:
        model = Author

router = APIRouter(prefix="/authors", tags=["authors"])

@router.post("", response_model=AuthorOut, status_code=201)
async def create_author(payload: AuthorIn) -> Author:
    return await Author.objects.acreate(**payload.model_dump())

@router.get("/{author_id}", response_model=AuthorOut)
async def get_author(author_id: int) -> Author:
    author = await Author.objects.get_or_none(pk=author_id)
    if author is None:
        raise HTTPException(404, "Not found")
    return author

@router.get("", response_model=list[AuthorOut])
async def list_authors() -> list[Author]:
    return [a async for a in Author.objects.all()]

@router.patch("/{author_id}", response_model=AuthorOut)
async def patch_author(author_id: int, payload: AuthorIn) -> Author:
    fields = payload.model_dump(exclude_unset=True)
    n = await Author.objects.filter(pk=author_id).aupdate(**fields)
    if not n:
        raise HTTPException(404, "Not found")
    return await Author.objects.aget(pk=author_id)

@router.delete("/{author_id}", status_code=204)
async def delete_author(author_id: int) -> None:
    n, _ = await Author.objects.filter(pk=author_id).adelete()
    if not n:
        raise HTTPException(404, "Not found")
```

## Endpoint de health check

```python
@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

`ahealth_check()` devuelve
`{"status": "ok", "alias": ..., "latency_ms": ..., "pool": {...}}`,
o `status="error"` con el detalle de la excepción. Engánchalo a la
sonda de liveness/readiness de tu orquestador.

## Dependencia async para transacciones

```python
from fastapi import Depends
from dorm.transaction import aatomic

async def db_tx():
    async with aatomic():
        yield

@router.post("/transfer")
async def transfer(payload: TransferIn, _: None = Depends(db_tx)):
    ...
```

Todo lo que corra dentro de `transfer` queda envuelto en una sola
transacción; ante una excepción, el handler de excepciones de
FastAPI sigue disparándose *después* del rollback.

## Pitfalls

- **No reutilices la misma instancia dorm `Model` entre requests
  concurrentes** — las instancias son mutables y `save()` lee
  `__dict__`. Cada handler debería traerse la suya.
- **Bloquear con llamadas sync al ORM en rutas async** —
  `Author.objects.all()` dentro de un `async def` está bien para dev
  trivial, pero ata el event loop en cada query. En producción usa
  las variantes `a*`.
- **Coste de `response_model`** — Pydantic re-valida en la salida.
  Para endpoints de muy alto throughput, fija
  `response_model_exclude_unset=True` o salta `response_model` y
  devuelve `JSONResponse` directamente.
