"""Tests for dorm.FileField and the storage backends.

Covers:

- ``dorm.storage`` core: File / ContentFile / FieldFile, the registry
  (``get_storage``, ``reset_storages``, ``default_storage``),
  ``FileSystemStorage`` round-trip on disk + path traversal guard.
- ``dorm.FileField`` end-to-end: assign a ContentFile, save the model,
  read back ``.url`` / ``.size`` / ``.open()``, delete via the
  descriptor.
- ``dorm.contrib.storage.s3.S3Storage`` with the boto3 client mocked
  (no network — ``moto``-style realism is overkill for what we wire).
"""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.exceptions import ImproperlyConfigured
from dorm.storage import (
    ContentFile,
    FieldFile,
    File,
    FileSystemStorage,
    Storage,
    get_storage,
    reset_storages,
)


# ── Test models ──────────────────────────────────────────────────────────────


class Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/", null=True, blank=True)
    avatar = dorm.FileField(upload_to="avatars/", null=True, blank=True)

    class Meta:
        db_table = "fs_documents"


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def media_root(tmp_path: Path):
    """Reset STORAGES to point at a per-test tmp directory so tests
    can read/write without colliding with each other or leaving
    artefacts on disk."""
    reset_storages()
    # Save and restore the STORAGES setting so the rest of the suite
    # (which might use FileFields elsewhere) keeps its config.
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
    """Create the ``fs_documents`` table for the FileField integration
    tests. Lives outside the autouse table list since FileField is the
    only test using it."""
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "fs_documents"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in Document._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "fs_documents" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "fs_documents"{cascade}')


# ── File / ContentFile ───────────────────────────────────────────────────────


class TestFileWrappers:
    def test_content_file_size_is_buffer_length(self):
        cf = ContentFile(b"hello world", name="x.txt")
        assert cf.size == 11
        assert cf.read() == b"hello world"
        assert cf.name == "x.txt"

    def test_content_file_chunks_yield_full_payload(self):
        cf = ContentFile(b"a" * 1000, name="big.bin")
        joined = b"".join(cf.chunks(chunk_size=300))
        assert joined == b"a" * 1000

    def test_string_content_file_round_trip(self):
        cf = ContentFile("hello", name="x.txt")
        assert cf.size == 5
        # ``read()`` returns a string here because the buffer is StringIO.
        assert cf.read() == "hello"

    def test_file_size_falls_back_to_seek(self):
        f = File(io.BytesIO(b"abcde"), name="x.bin")
        assert f.size == 5

    def test_close_propagates_to_underlying(self):
        buf = io.BytesIO(b"x")
        f = File(buf, name="x.bin")
        f.close()
        assert buf.closed


# ── FileSystemStorage ────────────────────────────────────────────────────────


class TestFileSystemStorage:
    def test_save_and_read_round_trip(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path), base_url="/m/")
        name = storage.save("hello.txt", ContentFile(b"hi"))
        assert name == "hello.txt"
        assert storage.exists("hello.txt")
        assert storage.size("hello.txt") == 2
        with storage.open("hello.txt", "rb") as f:
            assert f.read() == b"hi"

    def test_url_quotes_unsafe_chars(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path), base_url="/m/")
        url = storage.url("docs/My File.pdf")
        assert url == "/m/docs/My%20File.pdf"

    def test_delete_is_idempotent(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path))
        storage.save("a.txt", ContentFile(b"x"))
        storage.delete("a.txt")
        # second delete must not raise
        storage.delete("a.txt")
        assert not storage.exists("a.txt")

    def test_path_traversal_rejected_on_write(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path / "media"))
        # ``get_valid_name`` strips path components, so by the time the
        # storage write runs the basename is safe; but a caller using the
        # low-level ``_save`` directly (or ``exists`` / ``size``) should
        # still be rejected.
        with pytest.raises(ImproperlyConfigured):
            storage._save("../escape.txt", ContentFile(b"x"))

    def test_get_valid_name_strips_directory_components(self):
        assert FileSystemStorage.get_valid_name("../etc/passwd") == "passwd"
        assert FileSystemStorage.get_valid_name("foo bar baz.pdf") == "foo bar baz.pdf"
        assert FileSystemStorage.get_valid_name("héllo.pdf") == "h_llo.pdf"

    def test_collision_inserts_random_token(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path))
        first = storage.save("note.txt", ContentFile(b"1"))
        second = storage.save("note.txt", ContentFile(b"2"))
        assert first == "note.txt"
        assert second != first
        assert second.startswith("note_") and second.endswith(".txt")
        assert storage.size(first) == 1
        assert storage.size(second) == 1

    def test_creates_intermediate_directories(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path))
        storage.save("a/b/c/x.txt", ContentFile(b"deep"))
        assert (tmp_path / "a" / "b" / "c" / "x.txt").read_bytes() == b"deep"

    @pytest.mark.asyncio
    async def test_async_round_trip(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path))
        name = await storage.asave("async.txt", ContentFile(b"async"))
        assert await storage.aexists(name)
        assert await storage.asize(name) == 5
        opened = await storage.aopen(name, "rb")
        assert opened.read() == b"async"
        opened.close()
        await storage.adelete(name)
        assert not await storage.aexists(name)

    def test_path_returns_absolute_filesystem_path(self, tmp_path):
        storage = FileSystemStorage(location=str(tmp_path))
        name = storage.save("p.txt", ContentFile(b"x"))
        path = storage.path(name)
        assert os.path.isabs(path)
        assert os.path.dirname(path) == str(tmp_path)


# ── Registry / settings ──────────────────────────────────────────────────────


class TestStorageRegistry:
    def test_default_storage_resolves_to_filesystem(self, media_root):
        s = get_storage()
        assert isinstance(s, FileSystemStorage)
        assert os.path.normpath(s.location) == os.path.normpath(str(media_root))

    def test_unknown_alias_raises(self, media_root):
        with pytest.raises(ImproperlyConfigured) as exc_info:
            get_storage("nope")
        assert "not found" in str(exc_info.value)

    def test_invalid_backend_path_raises(self):
        reset_storages()
        saved = getattr(dorm.settings, "STORAGES", {})
        try:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES={
                    "default": {
                        "BACKEND": "no.such.module.Storage",
                        "OPTIONS": {},
                    }
                },
            )
            with pytest.raises(ImproperlyConfigured) as exc_info:
                get_storage()
            # Helpful hint for the canonical mistake — forgetting to
            # install the s3 extra — is part of the message.
            assert "Cannot import storage backend" in str(exc_info.value)
        finally:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES=saved,
            )
            reset_storages()

    def test_missing_default_alias_raises(self):
        reset_storages()
        saved = getattr(dorm.settings, "STORAGES", {})
        try:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES={
                    "uploads": {"BACKEND": "dorm.storage.FileSystemStorage"}
                },
            )
            with pytest.raises(ImproperlyConfigured) as exc_info:
                get_storage()
            assert "must contain a 'default' alias" in str(exc_info.value)
        finally:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES=saved,
            )
            reset_storages()

    def test_default_storage_proxy_re_resolves(self, tmp_path):
        reset_storages()
        saved = getattr(dorm.settings, "STORAGES", {})
        try:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES={
                    "default": {
                        "BACKEND": "dorm.storage.FileSystemStorage",
                        "OPTIONS": {"location": str(tmp_path / "first")},
                    }
                },
            )
            from dorm.storage import default_storage

            default_storage.save("foo.txt", ContentFile(b"x"))
            assert (tmp_path / "first" / "foo.txt").exists()

            # Reconfigure to a new root; the proxy should track it.
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES={
                    "default": {
                        "BACKEND": "dorm.storage.FileSystemStorage",
                        "OPTIONS": {"location": str(tmp_path / "second")},
                    }
                },
            )
            default_storage.save("bar.txt", ContentFile(b"y"))
            assert (tmp_path / "second" / "bar.txt").exists()
        finally:
            dorm.configure(
                DATABASES=dorm.settings.DATABASES,
                INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
                STORAGES=saved,
            )
            reset_storages()


# ── FileField end-to-end ─────────────────────────────────────────────────────


class TestFileField:
    def test_save_persists_file_and_column(self, media_root, documents_table):
        doc = Document(name="report")
        doc.attachment = ContentFile(b"PDF bytes", name="report.pdf")
        doc.save()

        # Column holds a string path; descriptor returns a FieldFile.
        on_disk = media_root / "docs" / "report.pdf"
        assert on_disk.exists()
        assert on_disk.read_bytes() == b"PDF bytes"

        loaded = Document.objects.get(pk=doc.pk)
        assert isinstance(loaded.attachment, FieldFile)
        assert loaded.attachment.name == "docs/report.pdf"
        assert loaded.attachment.size == 9
        assert loaded.attachment.url == "/media/docs/report.pdf"

    def test_open_reads_file_through_descriptor(self, media_root, documents_table):
        doc = Document(name="x")
        doc.attachment = ContentFile(b"hello-fieldfile", name="x.txt")
        doc.save()

        loaded = Document.objects.get(pk=doc.pk)
        with loaded.attachment.open("rb") as fh:
            assert fh.read() == b"hello-fieldfile"

    def test_delete_removes_file_and_clears_column(self, media_root, documents_table):
        doc = Document(name="d")
        doc.attachment = ContentFile(b"bye", name="d.txt")
        doc.save()

        on_disk = media_root / "docs" / "d.txt"
        assert on_disk.exists()

        doc.attachment.delete(save=True)

        assert not on_disk.exists()
        loaded = Document.objects.get(pk=doc.pk)
        assert not bool(loaded.attachment)

    def test_assign_string_keeps_existing_path(self, media_root, documents_table):
        # Useful when reusing an already-uploaded file.
        Path(media_root / "docs").mkdir(parents=True, exist_ok=True)
        (media_root / "docs" / "manual.pdf").write_bytes(b"manual")

        doc = Document(name="reuse")
        doc.attachment = "docs/manual.pdf"
        doc.save()

        loaded = Document.objects.get(pk=doc.pk)
        assert loaded.attachment.name == "docs/manual.pdf"
        with loaded.attachment.open("rb") as fh:
            assert fh.read() == b"manual"

    def test_assign_none_clears(self, media_root, documents_table):
        doc = Document(name="z")
        doc.attachment = ContentFile(b"x", name="z.txt")
        doc.save()
        assert (media_root / "docs" / "z.txt").exists()

        doc.attachment = None
        doc.save()

        loaded = Document.objects.get(pk=doc.pk)
        assert not bool(loaded.attachment)

    def test_filter_by_name(self, media_root, documents_table):
        doc = Document(name="search")
        doc.attachment = ContentFile(b"x", name="search.txt")
        doc.save()

        # ``_compile_leaf`` runs the value through ``get_db_prep_value``,
        # which on FileField returns the storage name string. Passing
        # the bare string directly should also work.
        found = Document.objects.filter(attachment="docs/search.txt").first()
        assert found is not None and found.pk == doc.pk

    def test_upload_to_strftime_template(self, media_root, documents_table):
        # Re-declare the model with a strftime upload_to to exercise
        # the rendering path without polluting the global Document.
        class TimestampedDoc(dorm.Model):
            name = dorm.CharField(max_length=50)
            file = dorm.FileField(upload_to="reports/%Y/", null=True, blank=True)

            class Meta:
                db_table = "fs_documents"  # piggyback the same table
                managed = False

        td = TimestampedDoc(name="ts")
        td.file = ContentFile(b"x", name="t.txt")
        # Just check the storage name was rendered correctly — don't
        # need to persist (the column is the same as Document.attachment).
        from datetime import datetime

        rendered = TimestampedDoc._meta.get_field("file")._render_target_name(td, "t.txt")
        assert rendered.startswith(f"reports/{datetime.now().year}/")
        assert rendered.endswith("/t.txt")

    def test_upload_to_callable(self, media_root):
        def location(instance, filename):
            return f"by-name/{instance.name}/{filename}"

        class CallableUploadDoc(dorm.Model):
            name = dorm.CharField(max_length=50)
            file = dorm.FileField(upload_to=location, null=True, blank=True)

            class Meta:
                db_table = "fs_documents"
                managed = False

        d = CallableUploadDoc(name="alice")
        rendered = CallableUploadDoc._meta.get_field("file")._render_target_name(d, "f.txt")
        assert rendered == "by-name/alice/f.txt"

    def test_invalid_assignment_raises(self, media_root, documents_table):
        doc = Document(name="bad")
        with pytest.raises(dorm.ValidationError):
            doc.attachment = 12345  # int — not File/str/None

    def test_field_file_truthiness(self, media_root, documents_table):
        empty = Document(name="empty")
        # Before save, descriptor returns a FieldFile with no name.
        assert not bool(empty.attachment)
        empty.attachment = ContentFile(b"x", name="x.txt")
        empty.save()
        loaded = Document.objects.get(pk=empty.pk)
        assert bool(loaded.attachment)


# ── Async FileField path ─────────────────────────────────────────────────────


class TestAsyncFileField:
    @pytest.mark.asyncio
    async def test_asave_persists_pending_upload(self, media_root, documents_table):
        doc = Document(name="async-doc")
        doc.attachment = ContentFile(b"async-bytes", name="a.bin")
        await doc.asave()

        assert (media_root / "docs" / "a.bin").read_bytes() == b"async-bytes"

        loaded = await Document.objects.aget(pk=doc.pk)
        assert loaded.attachment.name == "docs/a.bin"
        assert await loaded.attachment.storage.asize(loaded.attachment.name) == 11


# ── S3Storage (mocked) ───────────────────────────────────────────────────────


class _FakeS3Client:
    """Minimal stand-in for boto3's S3 client.

    Implements just enough of the surface ``S3Storage`` calls to make
    the round-trip tests meaningful without a network round-trip or a
    new test dep. ``moto`` is more realistic but heavier; this is
    enough for the wiring we want to verify.
    """

    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.acl: dict[tuple[str, str], str] = {}

    def upload_fileobj(self, body, bucket, key, ExtraArgs=None):
        body.seek(0)
        self.objects[(bucket, key)] = body.read()
        if ExtraArgs and "ACL" in ExtraArgs:
            self.acl[(bucket, key)] = ExtraArgs["ACL"]

    def get_object(self, Bucket, Key):
        data = self.objects[(Bucket, Key)]

        class _Body:
            def __init__(self, b: bytes) -> None:
                self._b = b

            def read(self) -> bytes:
                return self._b

        return {"Body": _Body(data), "ContentLength": len(data)}

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            class _NotFound(Exception):
                response = {"Error": {"Code": "404"}}
            raise _NotFound("Not Found")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def generate_presigned_url(self, op, Params, ExpiresIn):
        return (
            f"https://s3.example.test/{Params['Bucket']}/{Params['Key']}"
            f"?X-Amz-Expires={ExpiresIn}"
        )


class TestS3Storage:
    def _patched_storage(self, **kwargs: Any) -> tuple[Any, _FakeS3Client]:
        from dorm.contrib.storage.s3 import S3Storage

        fake = _FakeS3Client()
        storage = S3Storage(bucket_name="bucket-x", **kwargs)
        # Bypass boto3 lazy-init: shove the fake client straight in.
        storage._client = fake
        return storage, fake

    def test_save_uploads_to_bucket(self):
        storage, fake = self._patched_storage(location="prefix/")
        name = storage.save("hello.txt", ContentFile(b"hi-s3"))
        assert name == "hello.txt"
        # Key includes the configured prefix; storage-side name does not.
        assert ("bucket-x", "prefix/hello.txt") in fake.objects
        assert fake.objects[("bucket-x", "prefix/hello.txt")] == b"hi-s3"

    def test_save_applies_default_acl(self):
        storage, fake = self._patched_storage(default_acl="public-read")
        storage.save("x.txt", ContentFile(b"x"))
        assert fake.acl[("bucket-x", "x.txt")] == "public-read"

    def test_open_returns_streaming_body(self):
        storage, fake = self._patched_storage()
        storage.save("y.txt", ContentFile(b"hello"))
        with storage.open("y.txt", "rb") as f:
            assert f.read() == b"hello"

    def test_open_write_mode_rejected(self):
        storage, _ = self._patched_storage()
        with pytest.raises(NotImplementedError):
            storage.open("nope.txt", "wb")

    def test_exists_round_trip(self):
        storage, _ = self._patched_storage()
        assert storage.exists("missing") is False
        storage.save("there.txt", ContentFile(b"x"))
        assert storage.exists("there.txt") is True

    def test_size_round_trip(self):
        storage, _ = self._patched_storage()
        storage.save("z.txt", ContentFile(b"abcdef"))
        assert storage.size("z.txt") == 6

    def test_delete_is_idempotent(self):
        storage, _ = self._patched_storage()
        storage.save("d.txt", ContentFile(b"x"))
        storage.delete("d.txt")
        storage.delete("d.txt")  # second call: no error
        assert not storage.exists("d.txt")

    def test_url_uses_presigned_when_querystring_auth(self):
        storage, _ = self._patched_storage(querystring_expire=120)
        url = storage.url("file.pdf")
        assert "https://s3.example.test/bucket-x/file.pdf" in url
        assert "X-Amz-Expires=120" in url

    def test_url_with_custom_domain(self):
        storage, _ = self._patched_storage(custom_domain="cdn.example.com")
        url = storage.url("logo.png")
        assert url == "https://cdn.example.com/logo.png"

    def test_url_unsigned_when_disabled(self):
        storage, _ = self._patched_storage(querystring_auth=False)
        url = storage.url("public.txt")
        assert url == "https://s3.amazonaws.com/bucket-x/public.txt"

    def test_file_overwrite_disables_collision_dance(self):
        storage, fake = self._patched_storage(file_overwrite=True)
        storage.save("same.txt", ContentFile(b"first"))
        second = storage.save("same.txt", ContentFile(b"second"))
        assert second == "same.txt"
        assert fake.objects[("bucket-x", "same.txt")] == b"second"

    def test_file_overwrite_default_renames_on_collision(self):
        storage, _ = self._patched_storage(file_overwrite=False)
        first = storage.save("same.txt", ContentFile(b"a"))
        second = storage.save("same.txt", ContentFile(b"b"))
        assert first == "same.txt"
        assert second != first
        assert second.startswith("same_") and second.endswith(".txt")

    def test_missing_bucket_name_raises(self):
        from dorm.contrib.storage.s3 import S3Storage

        with pytest.raises(ImproperlyConfigured):
            S3Storage(bucket_name="")

    def test_missing_boto3_surfaces_helpful_error(self):
        from dorm.contrib.storage import s3

        # Force the lazy-init branch: drop any pre-cached client and
        # patch importlib so the next ``import boto3`` fails. The error
        # message must steer users at the optional dep.
        storage = s3.S3Storage(bucket_name="x")
        with mock.patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(ImproperlyConfigured) as exc_info:
                _ = storage.client
        assert "djanorm[s3]" in str(exc_info.value)


# ── Storage abstract base raises on unimplemented methods ────────────────────


class TestAbstractStorage:
    def test_unimplemented_save_raises(self):
        with pytest.raises(NotImplementedError):
            Storage()._save("x", ContentFile(b"x"))

    def test_unimplemented_url_raises(self):
        with pytest.raises(NotImplementedError):
            Storage().url("x")

    def test_path_raises_for_remote_storages(self):
        # The base class is treated as "remote-shaped": no local path.
        with pytest.raises(NotImplementedError):
            Storage().path("x")
