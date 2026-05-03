"""End-to-end tests for ``dorm.FileField`` accessed through a real
FastAPI app.

The recipe in ``docs/fastapi.md`` ("File uploads") is shipped as
copy-paste documentation, but until now nothing exercised it: a typo
in the docs or a regression in the multipart bridge between
``UploadFile``, :class:`dorm.File` and the storage layer would slip
through CI silently.

Each test here mounts a live FastAPI app via ``httpx.AsyncClient`` +
``ASGITransport`` (real multipart parser, real Pydantic validation,
real status codes) and verifies that the full POST → save → DB → URL
→ DELETE cycle works against ``FileSystemStorage``. The same code
paths run unchanged against ``S3Storage`` — that switch is exercised
in ``test_s3_storage_minio.py``.

We avoid Starlette's sync ``TestClient`` on purpose: it spawns an
anyio portal thread per request, which under ``pytest -n N`` on
Python 3.14 + psycopg async pool was triggering one-off worker
SIGSEGVs (the portal thread's interpreter shutdown raced the
session-loop pool finalisation). ``AsyncClient`` keeps everything on
the test's own event loop, removing the threading interaction.

Skipped automatically when FastAPI / httpx aren't installed (the
recipe is opt-in, the test suite shouldn't refuse to run for users
who only need the ORM).
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

# Gate the whole module on the optional deps. ``importorskip`` runs at
# collection time so the file is silently skipped on environments
# that don't have FastAPI (matches the pattern used by the MinIO
# tests).
pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("multipart")  # python-multipart, used by UploadFile

from fastapi import FastAPI, File, Form, HTTPException, UploadFile  # noqa: E402
from fastapi.responses import StreamingResponse  # noqa: E402
import httpx  # noqa: E402
from httpx import ASGITransport  # noqa: E402

import dorm  # noqa: E402
from dorm.contrib.pydantic import DormSchema  # noqa: E402
from dorm.db.connection import get_connection  # noqa: E402
from dorm.storage import reset_storages  # noqa: E402


# ── Test model ───────────────────────────────────────────────────────────────


class FastApiDoc(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(
        upload_to="docs/", null=True, blank=True
    )

    class Meta:
        db_table = "fa_documents"


class DocOut(DormSchema):
    """Pydantic schema mirroring ``FastApiDoc``. The ``url`` field is
    declared explicitly because dorm's BeforeValidator unwraps the
    descriptor to a *path string* (the storage name) — the public URL
    has to be filled in by the route handler before serialisation."""

    url: str | None = None

    class Meta:
        model = FastApiDoc


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def media_root(tmp_path: Path):
    """Reset STORAGES to point at a per-test tmp directory.

    Restores the previous setting on exit so the rest of the suite
    (the autouse ``clean_db`` fixture etc.) keeps its config.
    """
    reset_storages()
    saved = getattr(dorm.settings, "STORAGES", {})
    dorm.configure(
        DATABASES=dorm.settings.DATABASES,
        INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
        STORAGES={
            "default": {
                "BACKEND": "dorm.storage.FileSystemStorage",
                "OPTIONS": {
                    "location": str(tmp_path / "media"),
                    "base_url": "/media/",
                },
            },
        },
    )
    yield tmp_path / "media"
    dorm.configure(
        DATABASES=dorm.settings.DATABASES,
        INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
        STORAGES=saved,
    )
    reset_storages()


@pytest.fixture
def documents_table(clean_db):
    """Create the ``fa_documents`` table for the integration tests."""
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "fa_documents"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in FastApiDoc._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "fa_documents" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "fa_documents"{cascade}')


def _build_app() -> FastAPI:
    """Construct a fresh FastAPI app with the upload / list / fetch /
    download / delete routes exactly as the docs recommend.

    Built per-test (cheap, FastAPI app construction is microseconds)
    so each test starts from a clean router state.
    """
    app = FastAPI()

    @app.post("/documents", response_model=DocOut)
    async def upload(
        name: str = Form(...),
        file: UploadFile = File(...),
    ):
        if not file.filename:
            raise HTTPException(400, "Missing filename")
        doc = FastApiDoc(name=name)
        # ``UploadFile.file`` is a ``SpooledTemporaryFile`` — wrapping
        # it in :class:`dorm.File` lets the storage backend read it
        # in chunks instead of materialising the whole upload in RAM.
        doc.attachment = dorm.File(file.file, name=file.filename)
        await doc.asave()
        out = DocOut.model_validate(doc)
        out.url = doc.attachment.url
        return out

    @app.get("/documents", response_model=list[DocOut])
    async def list_docs():
        docs = [d async for d in FastApiDoc.objects.order_by("id")]
        return [
            DocOut.model_validate(d).model_copy(
                update={"url": d.attachment.url if d.attachment else None}
            )
            for d in docs
        ]

    @app.get("/documents/{doc_id}", response_model=DocOut)
    async def get_doc(doc_id: int):
        try:
            doc = await FastApiDoc.objects.aget(pk=doc_id)
        except FastApiDoc.DoesNotExist:
            raise HTTPException(404)
        out = DocOut.model_validate(doc)
        if doc.attachment:
            out.url = doc.attachment.url
        return out

    @app.get("/documents/{doc_id}/download")
    async def download(doc_id: int):
        try:
            doc = await FastApiDoc.objects.aget(pk=doc_id)
        except FastApiDoc.DoesNotExist:
            raise HTTPException(404)
        if not doc.attachment:
            raise HTTPException(404, "No file attached")
        handle = await doc.attachment.aopen("rb")
        return StreamingResponse(
            handle.chunks(),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{doc.name}"',
                "Content-Length": str(doc.attachment.size),
            },
        )

    @app.delete("/documents/{doc_id}", status_code=204)
    async def delete_doc(doc_id: int):
        try:
            doc = await FastApiDoc.objects.aget(pk=doc_id)
        except FastApiDoc.DoesNotExist:
            raise HTTPException(404)
        if doc.attachment:
            # ``save=False`` skips the redundant UPDATE that would
            # otherwise persist the cleared column right before the
            # row is removed entirely.
            await doc.attachment.adelete(save=False)
        await doc.adelete()

    return app


@pytest.fixture
async def client(media_root, documents_table):
    """Async ``httpx.AsyncClient`` driving the FastAPI app via
    ``ASGITransport`` — keeps every request on the test's event loop
    so the psycopg async pool sees a single, coherent loop. The sync
    ``starlette.testclient.TestClient`` we used earlier spawned an
    anyio portal thread per request and was the source of one-off
    ``worker 'gw0' crashed`` failures under ``pytest -n N`` on
    Python 3.14.
    """
    app = _build_app()
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ── Upload pipeline ──────────────────────────────────────────────────────────


class TestUpload:
    async def test_upload_persists_file_and_returns_url(self, client, media_root):
        response = await client.post(
            "/documents",
            data={"name": "Q1 Report"},
            files={"file": ("q1.pdf", b"PDF body bytes", "application/pdf")},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["name"] == "Q1 Report"
        assert body["attachment"] == "docs/q1.pdf"
        assert body["url"] == "/media/docs/q1.pdf"

        # File is genuinely on disk under the configured location.
        on_disk = media_root / "docs" / "q1.pdf"
        assert on_disk.exists()
        assert on_disk.read_bytes() == b"PDF body bytes"

    async def test_upload_persists_via_dorm_orm(self, client, media_root):
        response = await client.post(
            "/documents",
            data={"name": "OrmCheck"},
            files={"file": ("x.bin", b"row-roundtrip", "application/octet-stream")},
        )
        assert response.status_code == 200
        pk = response.json()["id"]

        # Read back via the ORM (no HTTP) — the column is the
        # storage name, not the public URL.
        loaded = await FastApiDoc.objects.aget(pk=pk)
        assert loaded.attachment.name == "docs/x.bin"
        assert loaded.attachment.size == len(b"row-roundtrip")

    async def test_upload_streams_large_payload(self, client, media_root):
        # Pick a payload bigger than the default ``UploadFile`` spool
        # threshold (1 MB) so the bridge from ``SpooledTemporaryFile``
        # to dorm's ``File.chunks`` actually exercises disk IO, not
        # the in-memory fast path.
        big = b"a" * (3 * 1024 * 1024)  # 3 MiB
        response = await client.post(
            "/documents",
            data={"name": "Big"},
            files={"file": ("big.bin", big, "application/octet-stream")},
        )
        assert response.status_code == 200
        on_disk = media_root / "docs" / "big.bin"
        assert on_disk.read_bytes() == big

    async def test_upload_rejects_missing_filename(self, client):
        # An empty filename in the multipart envelope is rejected at
        # FastAPI's parser (422) before our handler's defensive 400
        # branch fires. Either way, the contract is "no row, no file
        # written" — assert on that behaviour instead of pinning the
        # exact status code, since whether the rejection happens at
        # the validator or the handler depends on the FastAPI version.
        response = await client.post(
            "/documents",
            data={"name": "no-name"},
            files={"file": ("", b"bytes", "application/octet-stream")},
        )
        assert response.status_code in (400, 422), response.text
        assert not await FastApiDoc.objects.filter(name="no-name").aexists()

    async def test_filename_collisions_get_unique_names(self, client, media_root):
        first = await client.post(
            "/documents",
            data={"name": "first"},
            files={"file": ("same.txt", b"first", "text/plain")},
        )
        second = await client.post(
            "/documents",
            data={"name": "second"},
            files={"file": ("same.txt", b"second", "text/plain")},
        )
        assert first.json()["attachment"] == "docs/same.txt"
        # The storage rename pattern inserts a token before the ext.
        second_name = second.json()["attachment"]
        assert second_name != "docs/same.txt"
        assert second_name.startswith("docs/same_") and second_name.endswith(".txt")

        # Both files exist on disk with their respective payloads.
        assert (media_root / "docs" / "same.txt").read_bytes() == b"first"
        assert (media_root / second_name).read_bytes() == b"second"


# ── Listing + retrieval ──────────────────────────────────────────────────────


class TestRetrieve:
    async def test_list_returns_url_for_each_doc(self, client, media_root):
        # Seed two uploads.
        await client.post(
            "/documents",
            data={"name": "A"},
            files={"file": ("a.txt", b"A", "text/plain")},
        )
        await client.post(
            "/documents",
            data={"name": "B"},
            files={"file": ("b.txt", b"B", "text/plain")},
        )

        listing = (await client.get("/documents")).json()
        assert {d["name"] for d in listing} == {"A", "B"}
        urls = sorted(d["url"] for d in listing)
        assert urls == ["/media/docs/a.txt", "/media/docs/b.txt"]

    async def test_get_individual_doc(self, client):
        upload = await client.post(
            "/documents",
            data={"name": "single"},
            files={"file": ("s.txt", b"single", "text/plain")},
        )
        pk = upload.json()["id"]

        got = (await client.get(f"/documents/{pk}")).json()
        assert got["name"] == "single"
        assert got["url"] == "/media/docs/s.txt"

    async def test_get_missing_doc_404s(self, client):
        assert (await client.get("/documents/999999")).status_code == 404


# ── Streaming download ───────────────────────────────────────────────────────


class TestDownload:
    async def test_download_returns_file_bytes(self, client):
        # Use a payload that doesn't fit in one default chunk so the
        # streaming response actually concatenates multiple frames.
        body = b"chunked-body" * 10_000   # ~120 KB
        upload = await client.post(
            "/documents",
            data={"name": "down"},
            files={"file": ("d.bin", body, "application/octet-stream")},
        )
        pk = upload.json()["id"]

        resp = await client.get(f"/documents/{pk}/download")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/octet-stream"
        assert "attachment" in resp.headers["content-disposition"]
        assert resp.headers["content-length"] == str(len(body))
        # Concatenate the streamed chunks back into the original payload.
        assert resp.content == body

    async def test_download_404_when_no_attachment(self, client):
        # Create a row without any attachment via the ORM directly,
        # then ask for its download endpoint.
        doc = await FastApiDoc.objects.acreate(name="empty")
        resp = await client.get(f"/documents/{doc.pk}/download")
        assert resp.status_code == 404

    async def test_download_404_on_missing_doc(self, client):
        assert (await client.get("/documents/999999/download")).status_code == 404


# ── Deletion ────────────────────────────────────────────────────────────────


class TestDelete:
    async def test_delete_removes_row_and_file(self, client, media_root):
        upload = await client.post(
            "/documents",
            data={"name": "del"},
            files={"file": ("d.txt", b"d", "text/plain")},
        )
        pk = upload.json()["id"]
        on_disk = media_root / "docs" / "d.txt"
        assert on_disk.exists()

        resp = await client.delete(f"/documents/{pk}")
        assert resp.status_code == 204
        # File gone, row gone.
        assert not on_disk.exists()
        assert not await FastApiDoc.objects.filter(pk=pk).aexists()


# ── Pydantic round-trip ─────────────────────────────────────────────────────


class TestPydanticSerialisation:
    """The Pydantic-side BeforeValidator was added in 2.2 specifically
    so ``DormSchema`` could read a ``FieldFile`` from a model instance
    via ``from_attributes=True``. This test tightens the contract:
    the serialised JSON must show the storage name as a plain string,
    not the FieldFile's ``__repr__`` or any other surprise."""

    async def test_response_model_serialises_field_file_as_string(self, client):
        upload = await client.post(
            "/documents",
            data={"name": "json-shape"},
            files={"file": ("j.txt", b"j", "text/plain")},
        )
        body = upload.json()
        # ``attachment`` is the storage name (not a FieldFile repr,
        # not a dict, not None).
        assert isinstance(body["attachment"], str)
        assert body["attachment"] == "docs/j.txt"

    async def test_response_model_handles_unset_attachment(self, client):
        # Create via the ORM with no attachment.
        doc = await FastApiDoc.objects.acreate(name="bare")
        body = (await client.get(f"/documents/{doc.pk}")).json()
        # An unset FileField surfaces as the empty string (the
        # FieldFile's ``.name`` when no file is attached). We don't
        # collapse to ``None`` because the Pydantic union shape
        # ``Annotated[str, BeforeValidator] | None`` would fail
        # validation — see the validator's docstring. Routes that
        # want JSON ``null`` should map ``""`` → ``None`` themselves.
        assert body["attachment"] in ("", None)
        # ``url`` was set to None by the route handler when no file
        # was attached.
        assert body["url"] is None


# ── Smoke: lifecycle on the async path ──────────────────────────────────────


class TestAsyncLifecycle:
    async def test_full_cycle_via_async_client(self, media_root, documents_table):
        """Drive the same routes through ``httpx.AsyncClient`` end-to-end.
        Other tests already use ``AsyncClient`` via the fixture, but
        this one stays self-contained on purpose: it builds its own
        app + transport so a regression in the shared fixture wiring
        can't mask a real lifecycle break."""
        app = _build_app()
        transport = ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            # Upload
            r = await ac.post(
                "/documents",
                data={"name": "async-life"},
                files={"file": ("a.txt", b"hello-async", "text/plain")},
            )
            assert r.status_code == 200
            pk = r.json()["id"]

            # Download
            r = await ac.get(f"/documents/{pk}/download")
            assert r.status_code == 200
            assert r.content == b"hello-async"

            # Delete
            r = await ac.delete(f"/documents/{pk}")
            assert r.status_code == 204

        # File and row are gone.
        assert not (media_root / "docs" / "a.txt").exists()
        assert not await FastApiDoc.objects.filter(pk=pk).aexists()


# ── ContentFile round-trip without going through HTTP ───────────────────────


class TestProgrammaticFromAttributes:
    """The Pydantic adapter has its own coverage in
    ``test_pydantic_new_fields.py``; this test focuses on the path
    FastAPI's ``response_model`` actually uses: ``model_validate(orm_instance)``
    with ``from_attributes=True``. Catches regressions where the
    coercer for ``FieldFile`` stops being applied during attribute
    reads."""

    def test_model_validate_unwraps_field_file_from_orm(
        self, media_root, documents_table
    ):
        doc = FastApiDoc(name="x")
        doc.attachment = dorm.ContentFile(b"x", name="from-orm.txt")
        doc.save()

        validated = DocOut.model_validate(doc)
        # ``attachment`` is a plain string (the storage name) — not
        # a FieldFile, not None — so JSON serialisation is trivial.
        dumped = validated.model_dump()
        assert isinstance(dumped["attachment"], str)
        assert dumped["attachment"] == "docs/from-orm.txt"

    def test_dorm_file_wrapper_round_trips_via_save(
        self, media_root, documents_table
    ):
        """Sanity guard: the docs example uses ``dorm.File(file.file, ...)``
        rather than ``ContentFile``. Both paths must produce the same
        on-disk result."""
        buf = io.BytesIO(b"buffered-bytes")
        doc = FastApiDoc(name="buffered")
        doc.attachment = dorm.File(buf, name="b.bin")
        doc.save()

        on_disk = media_root / "docs" / "b.bin"
        assert on_disk.read_bytes() == b"buffered-bytes"
