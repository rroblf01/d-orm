"""Comprehensive coverage for ``FileField(upload_to=callable)``.

The single existing test exercised the name-rendering helper in
isolation; this file goes through the full save / load / async path
with realistic dynamic-path scenarios — owner-scoped folders,
extension-aware buckets, content-addressed names, and a callable
that reads other fields off the instance.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.storage import ContentFile, FieldFile, reset_storages


# ── Module-level callables (Django-style — round-trips in migrations) ────────


def upload_owner_scoped(instance, filename):
    """Tenant- / owner-isolation pattern: each user's uploads live
    under their own prefix so a misconfigured ACL can't leak across
    accounts. Real apps cap the suffix at the basename to avoid
    storing user-controlled directory components verbatim."""
    return f"users/{instance.owner_id}/{filename}"


def upload_by_extension(instance, filename):
    """Common pattern: route uploads to per-mime buckets so a CDN's
    cache rules / lifecycle policies can target them differently."""
    _, ext = os.path.splitext(filename)
    bucket = {
        ".pdf": "documents",
        ".jpg": "images",
        ".jpeg": "images",
        ".png": "images",
    }.get(ext.lower(), "other")
    return f"{bucket}/{filename}"


def upload_content_addressed(instance, filename):
    """Content-addressed layout — the storage name is derived from
    a hash of the model's identity. Useful for dedup-friendly
    storage where the same logical file can live under one canonical
    key. Hashes the *instance attributes* the caller cares about,
    not the file body (we don't have the body here yet)."""
    digest = hashlib.sha256(
        f"{instance.owner_id}|{filename}".encode("utf-8")
    ).hexdigest()[:16]
    _, ext = os.path.splitext(filename)
    return f"cas/{digest}{ext}"


# ── Test model ───────────────────────────────────────────────────────────────


class _Doc(dorm.Model):
    name = dorm.CharField(max_length=100)
    owner_id = dorm.IntegerField()
    attachment = dorm.FileField(
        upload_to=upload_owner_scoped, null=True, blank=True
    )

    class Meta:
        db_table = "ft_callable_docs"


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def media_root(tmp_path: Path):
    """Reset STORAGES to a tmp directory for the test, restore on exit."""
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
def docs_table(clean_db):
    """Create the table for ``_Doc``. Lives outside the autouse
    table list since we declare a brand-new model here."""
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "ft_callable_docs"{cascade}')

    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Doc._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "ft_callable_docs" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "ft_callable_docs"{cascade}')


# ── Render-time behaviour ────────────────────────────────────────────────────


class TestRenderTargetName:
    """Direct calls to ``_render_target_name`` — fast, no I/O."""

    def test_callable_receives_instance_and_filename(self):
        captured = []

        def location(instance, filename):
            captured.append((type(instance).__name__, filename))
            return f"x/{filename}"

        field = dorm.FileField(upload_to=location)
        # The helper is on the field instance; we don't need a
        # ``contribute_to_class`` cycle to drive it.
        result = field._render_target_name(_Doc(name="x", owner_id=1), "f.txt")

        assert result == "x/f.txt"
        assert captured == [("_Doc", "f.txt")]

    def test_callable_can_use_fk_id(self, media_root):
        d = _Doc(name="alice", owner_id=42)
        field = _Doc._meta.get_field("attachment")
        rendered = field._render_target_name(d, "report.pdf")
        assert rendered == "users/42/report.pdf"

    def test_callable_can_route_by_extension(self):
        field = dorm.FileField(upload_to=upload_by_extension)
        d = _Doc(name="d", owner_id=1)
        assert field._render_target_name(d, "spec.pdf") == "documents/spec.pdf"
        assert field._render_target_name(d, "logo.PNG") == "images/logo.PNG"
        assert field._render_target_name(d, "weird.xyz") == "other/weird.xyz"

    def test_callable_can_be_lambda(self):
        field = dorm.FileField(upload_to=lambda inst, fn: f"L/{inst.name}/{fn}")
        d = _Doc(name="lam", owner_id=1)
        assert field._render_target_name(d, "f.txt") == "L/lam/f.txt"

    def test_callable_returning_subdirs_is_preserved(self):
        field = dorm.FileField(
            upload_to=lambda inst, fn: f"a/b/c/d/{fn}"
        )
        d = _Doc(name="x", owner_id=1)
        assert field._render_target_name(d, "f.txt") == "a/b/c/d/f.txt"

    def test_static_string_still_works(self):
        """Adding the callable form must not regress the string form."""
        field = dorm.FileField(upload_to="docs/")
        d = _Doc(name="x", owner_id=1)
        assert field._render_target_name(d, "report.pdf") == "docs/report.pdf"


# ── End-to-end save → file lands at the callable's path ──────────────────────


class TestCallableUploadEndToEnd:
    def test_save_writes_file_to_callable_path(self, media_root, docs_table):
        doc = _Doc(name="alice", owner_id=42)
        doc.attachment = ContentFile(b"PDF body", name="report.pdf")
        doc.save()

        # File lives under the path the callable produced.
        on_disk = media_root / "users" / "42" / "report.pdf"
        assert on_disk.exists()
        assert on_disk.read_bytes() == b"PDF body"

        loaded = _Doc.objects.get(pk=doc.pk)
        assert isinstance(loaded.attachment, FieldFile)
        assert loaded.attachment.name == "users/42/report.pdf"
        assert loaded.attachment.url == "/media/users/42/report.pdf"

    def test_callable_observes_per_instance_state(self, media_root, docs_table):
        """Two rows with different ``owner_id`` route to different
        directories — the same callable runs twice, each time bound
        to a fresh instance."""
        a = _Doc(name="a", owner_id=1)
        a.attachment = ContentFile(b"A", name="x.txt")
        a.save()
        b = _Doc(name="b", owner_id=2)
        b.attachment = ContentFile(b"B", name="x.txt")
        b.save()

        assert (media_root / "users" / "1" / "x.txt").read_bytes() == b"A"
        assert (media_root / "users" / "2" / "x.txt").read_bytes() == b"B"
        # Same input filename, different keys → no collision rename.
        assert _Doc.objects.get(pk=a.pk).attachment.name == "users/1/x.txt"
        assert _Doc.objects.get(pk=b.pk).attachment.name == "users/2/x.txt"

    def test_callable_can_use_content_addressed_layout(
        self, media_root, docs_table
    ):
        """Switch the field's ``upload_to`` to a content-addressed
        callable and verify the storage name follows."""
        from dorm.fields import FileField

        field: FileField = _Doc._meta.get_field("attachment")  # type: ignore[assignment]
        original = field.upload_to
        field.upload_to = upload_content_addressed
        try:
            doc = _Doc(name="cas", owner_id=99)
            doc.attachment = ContentFile(b"x", name="paper.pdf")
            doc.save()
        finally:
            field.upload_to = original

        loaded = _Doc.objects.get(pk=doc.pk)
        # Hash is deterministic given (owner_id, filename).
        expected_hash = hashlib.sha256(b"99|paper.pdf").hexdigest()[:16]
        assert loaded.attachment.name == f"cas/{expected_hash}.pdf"

    def test_filename_collisions_under_same_callable_path_get_renamed(
        self, media_root, docs_table
    ):
        """Two saves with the same ``owner_id`` AND same input filename
        end up in the same directory — the storage's collision dance
        kicks in, the second name gets a random token."""
        a = _Doc(name="a", owner_id=7)
        a.attachment = ContentFile(b"first", name="report.pdf")
        a.save()
        b = _Doc(name="b", owner_id=7)
        b.attachment = ContentFile(b"second", name="report.pdf")
        b.save()

        first = _Doc.objects.get(pk=a.pk).attachment.name
        second = _Doc.objects.get(pk=b.pk).attachment.name
        assert first == "users/7/report.pdf"
        assert second != first
        assert second.startswith("users/7/report_")
        assert second.endswith(".pdf")

        # Both files exist with their respective payloads.
        assert (media_root / first).read_bytes() == b"first"
        assert (media_root / second).read_bytes() == b"second"


# ── Async parity ─────────────────────────────────────────────────────────────


class TestCallableUploadAsync:
    @pytest.mark.asyncio
    async def test_asave_runs_callable_for_path(self, media_root, docs_table):
        doc = _Doc(name="async", owner_id=11)
        doc.attachment = ContentFile(b"async-bytes", name="a.bin")
        await doc.asave()

        # File ended up at the callable's path on the async path too.
        assert (media_root / "users" / "11" / "a.bin").read_bytes() == b"async-bytes"

        loaded = await _Doc.objects.aget(pk=doc.pk)
        assert loaded.attachment.name == "users/11/a.bin"


# ── Migration writer: callable round-trip ────────────────────────────────────


class TestMigrationWriterPreservesModuleLevelCallable:
    """A callable ``upload_to`` declared at module level is reachable
    via a normal ``from <module> import <fn>`` line. The writer
    should reconstruct that import; lambdas / nested functions still
    fall back to the FIXME comment."""

    def test_module_level_callable_round_trips(self, tmp_path):
        from dorm.migrations.operations import CreateModel
        from dorm.migrations.writer import write_migration

        ops = [
            CreateModel(
                name="X",
                fields=[
                    (
                        "att",
                        dorm.FileField(
                            upload_to=upload_owner_scoped, null=True, blank=True
                        ),
                    )
                ],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", tmp_path / "myapp" / "migrations", 1, ops)
        source = path.read_text()

        # The reference goes by the bare name, with the import added
        # as a separate line so the migration file is self-sufficient.
        assert "upload_to=upload_owner_scoped" in source
        assert "import upload_owner_scoped" in source
        # No FIXME — the writer was able to reconstruct the reference.
        assert "FIXME" not in source

    def test_lambda_still_falls_back_to_fixme(self, tmp_path):
        from dorm.migrations.operations import CreateModel
        from dorm.migrations.writer import write_migration

        ops = [
            CreateModel(
                name="X",
                fields=[
                    (
                        "att",
                        dorm.FileField(upload_to=lambda i, f: f"x/{f}"),
                    )
                ],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", tmp_path / "myapp" / "migrations", 1, ops)
        source = path.read_text()
        # Lambdas can't be imported back; the writer leaves a marker.
        assert "FIXME" in source

    def test_nested_function_falls_back_to_fixme(self, tmp_path):
        from dorm.migrations.operations import CreateModel
        from dorm.migrations.writer import write_migration

        def _local(instance, filename):
            return f"local/{filename}"

        ops = [
            CreateModel(
                name="X",
                fields=[
                    ("att", dorm.FileField(upload_to=_local)),
                ],
                options={"db_table": "x"},
            )
        ]
        path = write_migration("myapp", tmp_path / "myapp" / "migrations", 1, ops)
        source = path.read_text()
        # ``_local`` lives inside a function (``<locals>`` qualname);
        # the writer can't safely emit an import for it.
        assert "FIXME" in source
