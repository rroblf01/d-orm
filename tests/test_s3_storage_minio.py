"""Live integration tests for ``dorm.contrib.storage.s3.S3Storage``
against a real S3-compatible service (MinIO running in Docker).

The mocked tests in ``test_storage.py`` cover the API wiring; this
file complements them by exercising the actual boto3 → S3 protocol
end-to-end. We catch:

- credential / signature-version drift (MinIO insists on s3v4),
- presigned-URL signing — the URL is fetched via plain HTTP to confirm
  the signature actually round-trips,
- ``upload_fileobj`` / ``head_object`` / ``get_object`` semantics on a
  real server (the ``_FakeS3Client`` in ``test_storage.py`` only
  models the happy paths we wired by hand),
- ``FileField`` integration with a non-default ``STORAGES`` config.

Skipped automatically when Docker / boto3 / testcontainers[minio]
aren't available — same gating as the Postgres backend tests.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.request import urlopen

import pytest

# Gate the whole module on the optional deps. ``importorskip`` runs at
# collection time; if any one of these isn't installed the file is
# skipped wholesale (clean, no per-test redundancy). The live
# ``minio_endpoint`` fixture handles the *Docker daemon* gate
# separately so dev environments with the deps but no Docker still
# skip cleanly.
pytest.importorskip("boto3")
pytest.importorskip("botocore.client")
pytest.importorskip("testcontainers.minio")

import boto3  # noqa: E402  — must come after importorskip
from botocore.client import Config  # noqa: E402

import dorm  # noqa: E402
from dorm.contrib.storage.s3 import S3Storage  # noqa: E402
from dorm.db.connection import get_connection  # noqa: E402
from dorm.storage import ContentFile, FieldFile, reset_storages  # noqa: E402


# ── Test models ──────────────────────────────────────────────────────────────


class S3Document(dorm.Model):
    name = dorm.CharField(max_length=100)
    attachment = dorm.FileField(upload_to="docs/", null=True, blank=True)

    class Meta:
        db_table = "s3_documents"


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _admin_client(endpoint: dict) -> Any:
    """Return a boto3 S3 client wired against the live MinIO endpoint.

    Used by fixtures (bucket lifecycle) — distinct from the
    :class:`S3Storage` instance the tests exercise, which uses the
    same coordinates but goes through dorm's wrapper.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint["endpoint_url"],
        aws_access_key_id=endpoint["access_key"],
        aws_secret_access_key=endpoint["secret_key"],
        region_name=endpoint["region_name"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


@pytest.fixture
def s3_bucket(minio_endpoint):
    """Create a unique bucket per test and tear it down at exit.

    Bucket names use only lowercase hex (MinIO inherits S3's bucket
    naming rules: 3–63 chars, ``[a-z0-9-]``). xdist parallelism is
    safe since each test rolls its own random suffix.
    """
    name = "t-" + os.urandom(6).hex()
    client = _admin_client(minio_endpoint)
    client.create_bucket(Bucket=name)
    yield name, client
    # Drain + drop on teardown. Failing here would mask the real test
    # failure, so swallow exceptions — the container is going away at
    # session end anyway.
    try:
        objs = client.list_objects_v2(Bucket=name).get("Contents", [])
        if objs:
            client.delete_objects(
                Bucket=name,
                Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
            )
        client.delete_bucket(Bucket=name)
    except Exception:
        pass


@pytest.fixture
def s3_storage(s3_bucket, minio_endpoint):
    """``S3Storage`` instance pointing at the per-test bucket."""
    bucket_name, _client = s3_bucket
    return S3Storage(
        bucket_name=bucket_name,
        endpoint_url=minio_endpoint["endpoint_url"],
        access_key=minio_endpoint["access_key"],
        secret_key=minio_endpoint["secret_key"],
        region_name=minio_endpoint["region_name"],
        # MinIO over IP needs path-style + s3v4. The S3Storage
        # constructor passes both straight through to botocore.
        signature_version="s3v4",
        addressing_style="path",
    )


@pytest.fixture
def s3_documents_table(clean_db, s3_bucket, minio_endpoint):
    """Create the ``s3_documents`` table and reconfigure dorm so the
    default storage points at the per-test MinIO bucket. Restores
    settings + storage cache on teardown so other tests aren't
    affected."""
    from dorm.migrations.operations import _field_to_column_sql

    bucket_name, _ = s3_bucket
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "s3_documents"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in S3Document._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "s3_documents" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )

    saved_storages = getattr(dorm.settings, "STORAGES", {})
    reset_storages()
    dorm.configure(
        DATABASES=dorm.settings.DATABASES,
        INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
        STORAGES={
            "default": {
                "BACKEND": "dorm.contrib.storage.s3.S3Storage",
                "OPTIONS": {
                    "bucket_name": bucket_name,
                    "endpoint_url": minio_endpoint["endpoint_url"],
                    "access_key": minio_endpoint["access_key"],
                    "secret_key": minio_endpoint["secret_key"],
                    "region_name": minio_endpoint["region_name"],
                    "signature_version": "s3v4",
                    "addressing_style": "path",
                },
            }
        },
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "s3_documents"{cascade}')
    dorm.configure(
        DATABASES=dorm.settings.DATABASES,
        INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
        STORAGES=saved_storages,
    )
    reset_storages()


# ── S3Storage smoke + round-trip ─────────────────────────────────────────────


class TestS3StorageRoundTrip:
    def test_save_persists_object_in_minio(self, s3_storage, s3_bucket):
        """``save`` lands the bytes at the right key — verified via the
        admin client, bypassing dorm's wrapper."""
        bucket_name, client = s3_bucket
        name = s3_storage.save("hello.txt", ContentFile(b"hi-minio"))

        # The user-facing name is what we passed in, not a server-mangled key.
        assert name == "hello.txt"

        # And the object is genuinely there at the expected key.
        head = client.head_object(Bucket=bucket_name, Key="hello.txt")
        assert head["ContentLength"] == 8

        body = client.get_object(Bucket=bucket_name, Key="hello.txt")["Body"].read()
        assert body == b"hi-minio"

    def test_save_with_location_prefix_writes_to_subkey(
        self, s3_bucket, minio_endpoint
    ):
        bucket_name, client = s3_bucket
        storage = S3Storage(
            bucket_name=bucket_name,
            endpoint_url=minio_endpoint["endpoint_url"],
            access_key=minio_endpoint["access_key"],
            secret_key=minio_endpoint["secret_key"],
            region_name=minio_endpoint["region_name"],
            signature_version="s3v4",
            addressing_style="path",
            location="prefix/sub/",
        )
        storage.save("file.txt", ContentFile(b"x"))
        listing = client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        keys = [o["Key"] for o in listing]
        assert keys == ["prefix/sub/file.txt"]

    def test_open_streams_back_full_payload(self, s3_storage):
        s3_storage.save("blob.bin", ContentFile(b"a" * 4096))
        with s3_storage.open("blob.bin") as f:
            assert f.read() == b"a" * 4096

    def test_size_matches_uploaded_payload(self, s3_storage):
        s3_storage.save("z.bin", ContentFile(b"abcdef"))
        assert s3_storage.size("z.bin") == 6

    def test_exists_round_trip(self, s3_storage):
        assert not s3_storage.exists("missing")
        s3_storage.save("there.txt", ContentFile(b"x"))
        assert s3_storage.exists("there.txt")

    def test_delete_idempotent_on_real_server(self, s3_storage):
        s3_storage.save("d.txt", ContentFile(b"x"))
        s3_storage.delete("d.txt")
        # Second delete must not raise — S3 (and MinIO) treat
        # ``DeleteObject`` as idempotent, but our wrapper has its own
        # error-handling path that we want to exercise here.
        s3_storage.delete("d.txt")
        assert not s3_storage.exists("d.txt")

    def test_collision_rename_preserves_both_payloads(self, s3_storage):
        first = s3_storage.save("same.txt", ContentFile(b"first"))
        second = s3_storage.save("same.txt", ContentFile(b"second"))
        assert first == "same.txt"
        assert second != first
        assert second.startswith("same_") and second.endswith(".txt")

        with s3_storage.open(first) as f:
            assert f.read() == b"first"
        with s3_storage.open(second) as f:
            assert f.read() == b"second"

    def test_file_overwrite_replaces_in_place(self, s3_bucket, minio_endpoint):
        bucket_name, _ = s3_bucket
        storage = S3Storage(
            bucket_name=bucket_name,
            endpoint_url=minio_endpoint["endpoint_url"],
            access_key=minio_endpoint["access_key"],
            secret_key=minio_endpoint["secret_key"],
            region_name=minio_endpoint["region_name"],
            signature_version="s3v4",
            addressing_style="path",
            file_overwrite=True,
        )
        storage.save("same.txt", ContentFile(b"first"))
        storage.save("same.txt", ContentFile(b"second"))
        with storage.open("same.txt") as f:
            assert f.read() == b"second"

    def test_default_acl_is_applied(self, s3_bucket, minio_endpoint):
        bucket_name, client = s3_bucket
        storage = S3Storage(
            bucket_name=bucket_name,
            endpoint_url=minio_endpoint["endpoint_url"],
            access_key=minio_endpoint["access_key"],
            secret_key=minio_endpoint["secret_key"],
            region_name=minio_endpoint["region_name"],
            signature_version="s3v4",
            addressing_style="path",
            default_acl="public-read",
        )
        storage.save("public.txt", ContentFile(b"hello"))
        # MinIO honours the ``ACL`` header on PutObject. We don't
        # assert the canned-acl name (its surface differs per MinIO
        # version) — the assertion is just "we got here without
        # PutObject rejecting our header".
        head = client.head_object(Bucket=bucket_name, Key="public.txt")
        assert head["ContentLength"] == 5


# ── Pre-signed URL really works ──────────────────────────────────────────────


class TestPresignedUrlAgainstMinIO:
    def test_presigned_url_fetches_payload_anonymously(self, s3_storage):
        """A presigned GET URL produced by ``S3Storage.url`` must be
        directly fetchable — that's the contract FastAPI relies on
        when handing out file links to unauthenticated clients."""
        s3_storage.save("pub.txt", ContentFile(b"hello-presigned"))
        url = s3_storage.url("pub.txt")
        assert url.startswith("http")
        # No auth header set — the signature in the URL is the only
        # credential the server sees.
        with urlopen(url, timeout=5) as resp:
            assert resp.status == 200
            assert resp.read() == b"hello-presigned"

    def test_presigned_url_expires(self, s3_bucket, minio_endpoint):
        """Configurable expiry passes through to ``generate_presigned_url``.

        We don't sleep past the expiry (would slow the suite); we just
        assert the URL carries the requested ``X-Amz-Expires`` value
        so the call site is wired correctly."""
        bucket_name, _ = s3_bucket
        storage = S3Storage(
            bucket_name=bucket_name,
            endpoint_url=minio_endpoint["endpoint_url"],
            access_key=minio_endpoint["access_key"],
            secret_key=minio_endpoint["secret_key"],
            region_name=minio_endpoint["region_name"],
            signature_version="s3v4",
            addressing_style="path",
            querystring_expire=120,
        )
        storage.save("p.txt", ContentFile(b"x"))
        url = storage.url("p.txt")
        assert "X-Amz-Expires=120" in url


# ── Async parity ─────────────────────────────────────────────────────────────


class TestS3StorageAsync:
    @pytest.mark.asyncio
    async def test_async_round_trip(self, s3_storage):
        name = await s3_storage.asave("a.txt", ContentFile(b"async-bytes"))
        assert await s3_storage.aexists(name)
        assert await s3_storage.asize(name) == 11
        opened = await s3_storage.aopen(name, "rb")
        assert opened.read() == b"async-bytes"
        opened.close()
        await s3_storage.adelete(name)
        assert not await s3_storage.aexists(name)


# ── FileField end-to-end against MinIO ───────────────────────────────────────


class TestFileFieldOverS3:
    def test_save_and_load_via_s3_default_storage(self, s3_documents_table):
        """``Document.save()`` puts the bytes on MinIO, the column
        stores the storage name, and reading the row back returns a
        ``FieldFile`` whose ``.url`` is a working presigned link."""
        doc = S3Document(name="report")
        doc.attachment = ContentFile(b"PDF body bytes", name="report.pdf")
        doc.save()

        loaded = S3Document.objects.get(pk=doc.pk)
        assert isinstance(loaded.attachment, FieldFile)
        assert loaded.attachment.name == "docs/report.pdf"

        # Direct read through the storage abstraction.
        assert loaded.attachment.size == len(b"PDF body bytes")
        with loaded.attachment.open("rb") as fh:
            assert fh.read() == b"PDF body bytes"

        # Anonymous fetch via the presigned URL — the FastAPI path.
        url = loaded.attachment.url
        with urlopen(url, timeout=5) as resp:
            assert resp.read() == b"PDF body bytes"

    def test_delete_removes_object_from_bucket(
        self, s3_documents_table, s3_bucket
    ):
        bucket_name, client = s3_bucket
        doc = S3Document(name="d")
        doc.attachment = ContentFile(b"to-delete", name="d.txt")
        doc.save()

        # Confirm the object landed.
        objs = client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        assert any(o["Key"] == "docs/d.txt" for o in objs)

        doc.attachment.delete(save=True)

        objs_after = client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        assert not any(o["Key"] == "docs/d.txt" for o in objs_after)

        loaded = S3Document.objects.get(pk=doc.pk)
        assert not bool(loaded.attachment)

    def test_collision_rename_when_two_docs_share_a_filename(
        self, s3_documents_table
    ):
        d1 = S3Document(name="one")
        d1.attachment = ContentFile(b"first", name="report.pdf")
        d1.save()

        d2 = S3Document(name="two")
        d2.attachment = ContentFile(b"second", name="report.pdf")
        d2.save()

        # Both live under ``docs/`` with the second renamed.
        assert d1.attachment.name == "docs/report.pdf"
        assert d2.attachment.name != d1.attachment.name
        assert d2.attachment.name.startswith("docs/report_")
        assert d2.attachment.name.endswith(".pdf")
