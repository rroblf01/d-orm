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

Este paso es opcional pero recomendado para producción.
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

### `create_schema_for(...)` / `update_schema_for(...)` — bodies de request

En endpoints CRUD típicos la forma del input diverge de la del
output. Los dos helpers ahorran boilerplate:

```python
from dorm.contrib.pydantic import (
    create_schema_for, update_schema_for, schema_for,
)

AuthorOut = schema_for(Author)              # response_model — fila completa
AuthorCreate = create_schema_for(Author)    # body POST — sin auto-PK / sin GeneratedField
AuthorUpdate = update_schema_for(Author)    # body PATCH — cada campo opcional con default None
```

* `create_schema_for` excluye automáticamente las PKs auto-incrementales
  y las columnas `GeneratedField` (las rellena el servidor). Los
  campos requeridos siguen requeridos. Los defaults se propagan —
  un campo con `default=False` es opcional en Pydantic con el default
  real.
* `update_schema_for` además convierte cada campo restante en
  `T | None` con default `None`, así el cliente puede omitir
  cualquier subset. En el handler usa
  `payload.model_dump(exclude_unset=True)` para iterar solo los
  campos que el cliente realmente envió.

```python
@app.patch("/authors/{pk}")
async def patch(pk: int, payload: AuthorUpdate):
    author = await Author.objects.aget(pk=pk)
    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(author, k, v)
    await author.asave()
    return AuthorOut.model_validate(author)
```

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

## Subida de archivos

`FileField` integra limpiamente con `UploadFile` de FastAPI. El mismo
código de endpoint funciona contra `FileSystemStorage` (disco local) y
`S3Storage` (AWS / MinIO / R2) — solo cambia `settings.STORAGES`.

### Modelo + esquema

```python
import dorm
from dorm.contrib.pydantic import DormSchema


class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/%Y/%m/", null=True, blank=True)

    class Meta:
        db_table = "documents"


class DocumentOut(DormSchema):
    """El BeforeValidator del adaptador Pydantic desenvuelve el
    descriptor FieldFile al storage name (un string plano)
    automáticamente — sin serializador a medida."""

    url: str | None = None      # override explícito, lo rellena la ruta

    class Meta:
        model = Document
```

### Endpoint de upload

```python
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

router = APIRouter(prefix="/documents")


@router.post("", response_model=DocumentOut)
async def upload_document(
    name: str = Form(...),
    file: UploadFile = File(...),
):
    """Acepta un upload multipart, persiste los bytes vía el storage
    configurado, y devuelve la fila guardada + una URL descargable.

    ``UploadFile`` expone un ``SpooledTemporaryFile`` bajo ``.file``;
    envolverlo en :class:`dorm.File` permite a dorm leer el contenido
    en chunks en lugar de cargar el upload entero en RAM.
    """
    if not file.filename:
        raise HTTPException(400, "Falta el nombre del archivo")

    doc = Document(name=name)
    doc.attachment = dorm.File(file.file, name=file.filename)
    await doc.asave()

    out = DocumentOut.model_validate(doc)
    out.url = doc.attachment.url
    return out
```

Una petición como:

```bash
curl -F 'name=Informe Q1' -F 'file=@/tmp/q1.pdf' http://localhost:8000/documents
```

devuelve:

```json
{
  "id": 1,
  "name": "Informe Q1",
  "attachment": "docs/2026/04/q1.pdf",
  "url": "/media/docs/2026/04/q1.pdf"
}
```

— con `FileSystemStorage`. Cambia `STORAGES` a `S3Storage` y `url`
pasa a ser un enlace presignado
`https://bucket.s3.amazonaws.com/...?X-Amz-...` que el navegador
puede descargar directamente. El código del endpoint no cambia.

### Listado + URLs presignadas

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

Para S3, cada `.url` es una URL presignada fresca — por defecto TTL
de 1 hora. Ajusta el TTL por llamada re-instanciando el storage con
otro `querystring_expire`, o usa `custom_domain=` para enlaces
permanentes vía CDN público.

### Descarga en streaming (cuando no quieres URL pública)

Para storage privado donde autenticas la descarga en tu app (en lugar
de repartir URLs presignadas de S3), haz streaming a través de
FastAPI:

```python
from fastapi.responses import StreamingResponse


@router.get("/{doc_id}/download")
async def download_document(doc_id: int):
    doc = await Document.objects.aget(pk=doc_id)
    if not doc.attachment:
        raise HTTPException(404, "Sin archivo adjunto")

    handle = await doc.attachment.aopen("rb")
    return StreamingResponse(
        handle.chunks(),                         # chunks de 64 KiB
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{doc.name}"',
            "Content-Length": str(doc.attachment.size),
        },
    )
```

`File.chunks()` está implementado en ambos backends, así que el mismo
handler hace streaming desde disco local y desde el body de
`get_object` de S3.

### Servir un `MEDIA_ROOT` local en desarrollo

`FileSystemStorage` solo escribe los bytes — servirlos es trabajo de
tu framework. Para dev, monta la ubicación bajo el prefijo URL que
configuraste como `base_url`:

```python
from fastapi.staticfiles import StaticFiles

app.mount("/media", StaticFiles(directory="/var/app/media"), name="media")
```

En producción, delega esto a nginx / CloudFront / la CDN
correspondiente — ver
[Producción: file storage](production.md#file-storage).

### Borrar un archivo con la fila

`FieldFile.delete()` elimina los bytes del storage. Cabléalo en tu
delete handler para que un `DELETE /documents/{id}` no deje
huérfanos:

```python
@router.delete("/{doc_id}", status_code=204)
async def delete_document(doc_id: int):
    doc = await Document.objects.aget(pk=doc_id)
    if doc.attachment:
        await doc.attachment.adelete(save=False)   # borra archivo, no re-guardes la fila
    await doc.adelete()
```

`save=False` evita el UPDATE redundante que persistiría la columna
limpia justo antes de que se borre la fila completa.

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
