# FastAPI integration

dorm ships a Pydantic v2 adapter that generates schemas from your
models — so you can use a single source of truth for both your tables
and your FastAPI request / response bodies.

## Installation

```bash
uv pip install 'djanorm[pydantic,async]'
```

The `pydantic` extra pulls Pydantic v2; `async` pulls
psycopg/aiosqlite. Both are needed for a typical FastAPI app.

## App lifespan

Configure dorm in a FastAPI lifespan and clean up on shutdown:

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

`close_all_async()` drains every async pool. Without it FastAPI's
graceful shutdown can hang on lingering connections.

## Schemas

Two ways to derive Pydantic schemas from a dorm model.

### `schema_for(...)` — quick one-liner

```python
from dorm.contrib.pydantic import schema_for
from .models import Author

AuthorOut = schema_for(Author)
AuthorIn = schema_for(Author, exclude=("id",))
AuthorPatch = schema_for(Author, optional=("name", "age", "email"))
```

| Argument | Effect |
|---|---|
| `name=` | class name (default `f"{Model.__name__}Schema"`) |
| `exclude=` | tuple of field names to drop |
| `only=` | tuple of field names to keep (mutually exclusive with `exclude`) |
| `optional=` | mark these fields as `Optional[...] = None` (PATCH bodies) |
| `base=` | custom `BaseModel` to inherit (e.g. for shared config) |

`from_attributes=True` is set automatically, so you can pass a dorm
instance straight to `Schema.model_validate(obj)` or use it as a
FastAPI `response_model`.

### `DormSchema` — declarative class

```python
from dorm.contrib.pydantic import DormSchema
from .models import Author, Publisher

class PublisherOut(DormSchema):
    class Meta:
        model = Publisher
        fields = ("id", "name")

class AuthorOut(DormSchema):
    bio_url: str | None = None       # extra field declared explicitly

    class Meta:
        model = Author
        exclude = ("internal_notes",)
        nested = {"publisher": PublisherOut}   # FK → nested schema
```

The metaclass walks `Author._meta.fields` and adds annotations for
every column, *unless* you've already declared one. Pass
`fields=("a", "b")` to whitelist, `exclude=("c",)` to blacklist, or
`optional=("a",)` for PATCH-style schemas. `nested=` swaps a FK or
M2M for a sub-schema (otherwise FKs serialize as their integer PK).

## A complete CRUD route

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

## Health check endpoint

```python
@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

`ahealth_check()` returns `{"status": "ok", "alias": ..., "latency_ms": ..., "pool": {...}}`,
or `status="error"` with the exception detail. Wire it to your
orchestrator's liveness/readiness probe.

## Async dependency for transactions

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

Anything that runs inside `transfer` is now wrapped in a single
transaction; on exception, FastAPI's exception handler still fires
*after* the rollback.

## Pitfalls

- **Don't reuse a single dorm `Model` instance across concurrent
  requests** — instances are mutable and `save()` reads `__dict__`.
  Each request handler should fetch its own.
- **Block on sync ORM calls in async routes** — `Author.objects.all()`
  in an `async def` is fine for tiny dev work but ties up the event
  loop on every query. Use the `a*` variants in production.
- **`response_model` validation cost** — Pydantic re-validates on
  output. For very high-throughput endpoints, set
  `response_model_exclude_unset=True` or skip `response_model` and
  return `JSONResponse` directly.
