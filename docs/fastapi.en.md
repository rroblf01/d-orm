# FastAPI integration

dorm ships a Pydantic v2 adapter that generates schemas from your
models — so you can use a single source of truth for both your tables
and your FastAPI request / response bodies.

## Installation

Pick the backend extra that matches your database — `sqlite` (pulls
`aiosqlite` for the async path) or `postgresql` (pulls `psycopg`
with the connection pool). Add `pydantic` for the Pydantic v2 schema
adapter that this guide builds on:

```bash
# SQLite + Pydantic schemas
uv pip install 'djanorm[sqlite,pydantic]'

# PostgreSQL + Pydantic schemas
uv pip install 'djanorm[postgresql,pydantic]'
```

There is no separate `async` extra — the async drivers (`aiosqlite`,
`psycopg`) ship under the same `sqlite` / `postgresql` extras as
their sync counterparts, so a single install covers both modes.

## App lifespan

This step is optional but recommended for production.
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
    author = await Author.objects.aget_or_none(pk=author_id)
    if author is None:
        raise HTTPException(404, "Not found")
    return author

@router.get("", response_model=list[AuthorOut])
async def list_authors() -> list[Author]:
    return Author.objects.all()

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

## File uploads

`FileField` integrates cleanly with FastAPI's `UploadFile`. The same
endpoint code works against `FileSystemStorage` (local disk) and
`S3Storage` (AWS / MinIO / R2) — only `settings.STORAGES` changes.

### Model + schema

```python
import dorm
from dorm.contrib.pydantic import DormSchema


class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)

    class Meta:
        db_table = "documents"


class DocumentOut(DormSchema):
    """The Pydantic interop's BeforeValidator unwraps the FieldFile
    descriptor to the storage name (a plain string) automatically —
    no custom serialiser needed."""

    url: str | None = None      # explicit override, populated in the route

    class Meta:
        model = Document
```

### Upload endpoint

```python
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

router = APIRouter(prefix="/documents")


@router.post("", response_model=DocumentOut)
async def upload_document(
    name: str = Form(...),
    file: UploadFile = File(...),
):
    """Accept a multipart upload, persist the bytes via the configured
    storage, and return the saved row + a downloadable URL.

    ``UploadFile`` exposes a SpooledTemporaryFile under ``.file``;
    wrapping it in :class:`dorm.File` lets dorm read the content
    chunked instead of loading the whole upload into RAM at once.
    """
    if not file.filename:
        raise HTTPException(400, "Missing filename")

    doc = Document(name=name)
    doc.attachment = dorm.File(file.file, name=file.filename)
    await doc.asave()

    out = DocumentOut.model_validate(doc)
    out.url = doc.attachment.url
    return out
```

A request like:

```bash
curl -F 'name=Q1 Report' -F 'file=@/tmp/q1.pdf' http://localhost:8000/documents
```

returns:

```json
{
  "id": 1,
  "name": "Q1 Report",
  "attachment": "docs/2026/04/q1.pdf",
  "url": "/media/docs/2026/04/q1.pdf"
}
```

— with `FileSystemStorage`. Swap `STORAGES` to `S3Storage` and `url`
becomes a presigned `https://bucket.s3.amazonaws.com/...?X-Amz-...`
link the browser can fetch directly. The endpoint code doesn't change.

### Listing + presigned URLs

```python
@router.get("", response_model=list[DocumentOut])
async def list_documents():
    docs = Document.objects.order_by("-id")
    return [
        DocumentOut.model_validate(d).model_copy(
            update={"url": d.attachment.url if d.attachment else None}
        )
        for d in docs
    ]
```

For S3, each `.url` is a fresh presigned URL — by default 1 hour TTL.
Adjust the expiry per call by re-instantiating the storage with a
different `querystring_expire`, or use `custom_domain=` for permanent
public-CDN links.

### Streaming download (when you don't want a public URL)

For private storage where you authenticate downloads in your app
(rather than handing out S3 presigned URLs), stream through FastAPI:

```python
from fastapi.responses import StreamingResponse


@router.get("/{doc_id}/download")
async def download_document(doc_id: int):
    doc = await Document.objects.aget(pk=doc_id)
    if not doc.attachment:
        raise HTTPException(404, "No file attached")

    handle = await doc.attachment.aopen("rb")
    return StreamingResponse(
        handle.chunks(),                         # 64 KiB chunks
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{doc.name}"',
            "Content-Length": str(doc.attachment.size),
        },
    )
```

`File.chunks()` is implemented for both backends, so the same handler
streams from local disk and from S3's `get_object` body.

### Serving a local `MEDIA_ROOT` in development

`FileSystemStorage` only writes the bytes — serving them is your
framework's job. For dev, mount the location at the URL prefix you
configured as `base_url`:

```python
from fastapi.staticfiles import StaticFiles

app.mount("/media", StaticFiles(directory="/var/app/media"), name="media")
```

In production, hand this off to nginx / CloudFront / the relevant
CDN — see [Production: file storage](production.md#file-storage).

### Deleting a file with the row

`FieldFile.delete()` removes the bytes from storage. Wire it in your
delete handler so a `DELETE /documents/{id}` doesn't leave orphans:

```python
@router.delete("/{doc_id}", status_code=204)
async def delete_document(doc_id: int):
    doc = await Document.objects.aget(pk=doc_id)
    if doc.attachment:
        await doc.attachment.adelete(save=False)   # delete file, don't re-save the row
    await doc.adelete()
```

`save=False` skips the redundant UPDATE that would otherwise persist
the cleared column right before the row is removed entirely.

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
