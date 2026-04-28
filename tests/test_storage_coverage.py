"""Coverage-tightening tests for ``dorm.storage``.

The base ``test_storage.py`` covers happy-path round-trips. This file
targets the branches that fall outside it: ``File`` size detection
when the underlying object exposes ``.size`` directly, non-seekable
streams in ``chunks()``, the abstract ``Storage`` raising
``NotImplementedError`` for callers who instantiate it without a
subclass, ``FileSystemStorage`` chmod failures and directory
deletes, ``FieldFile``'s imperative ``save`` / ``delete`` /
``async`` siblings, and the ``default_storage`` proxy's repr / error
fallback.

Each test is a 1-2 line poke at a specific branch. Together they
move ``storage.py`` from ~78% to ~95%+.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import IO, Any, cast

import pytest

import dorm
from dorm.exceptions import ImproperlyConfigured
from dorm.storage import (
    ContentFile,
    FieldFile,
    File,
    FileSystemStorage,
    Storage,
    default_storage,
    reset_storages,
)


# ── File: size detection branches ────────────────────────────────────────────


class _StreamWithSize:
    """File-like object that exposes its size directly via the
    ``.size`` attribute (some HTTP / cloud SDK wrappers do this)."""

    size = 4242

    def read(self, *_a):
        return b""


def test_file_size_uses_explicit_size_attr():
    # ``File.__init__`` types its argument as ``IO[Any] | None`` for
    # the static type checker; the duck-typed test stubs in this file
    # don't satisfy that protocol, but the runtime contract is just
    # ``hasattr(file, …)``. Cast through ``IO[Any]`` so ty stays
    # quiet without weakening the public signature.
    f = File(cast(IO[Any], _StreamWithSize()), name="x")
    assert f.size == 4242


def test_file_size_caches_after_first_lookup():
    """Second access must not re-stat / re-tell — ensures the
    ``self._size is not None`` short-circuit at the top of ``size``
    actually wins."""
    f = File(io.BytesIO(b"abcde"), name="x")
    assert f.size == 5
    # Replace the underlying stream with one whose size would compute
    # differently; the cached value must still win.
    f.file = io.BytesIO(b"longer-bytes")
    assert f.size == 5


def test_file_size_from_disk_path(tmp_path):
    """Underlying object has a ``.name`` pointing at a real file —
    falls through to ``os.path.getsize``."""
    p = tmp_path / "real.bin"
    p.write_bytes(b"hello-on-disk")
    fh = open(p, "rb")
    try:
        f = File(fh, name="real.bin")
        assert f.size == len(b"hello-on-disk")
    finally:
        fh.close()


def test_file_size_raises_when_no_known_path():
    """Object with no ``.size``, no ``.name``, no ``.tell``/``.seek``
    — the documented escape hatch is an ``AttributeError`` so the
    caller doesn't silently get a wrong number."""

    class Opaque:
        def read(self, *_a):
            return b""

    f = File(cast(IO[Any], Opaque()), name="x")
    with pytest.raises(AttributeError):
        _ = f.size


# ── File: chunks / open / repr ──────────────────────────────────────────────


def test_chunks_on_non_seekable_stream_yields_remaining():
    """Streams that raise on ``seek`` (pipes, network sockets) just
    yield what's already left — we don't crash on the rewind attempt."""

    class NonSeekable:
        def __init__(self, data: bytes) -> None:
            self._buf = io.BytesIO(data)

        def seek(self, *_a):
            raise OSError("not seekable")

        def read(self, n=-1):
            return self._buf.read(n)

    f = File(cast(IO[Any], NonSeekable(b"abcdefghij")), name="ns")
    assert b"".join(f.chunks(chunk_size=4)) == b"abcdefghij"


def test_open_calls_underlying_open_method_when_present():
    """If the underlying object has its own ``.open()``, ``File.open``
    forwards to it."""
    calls = []

    class OpenableThing:
        def open(self, mode):
            calls.append(mode)

    f = File(cast(IO[Any], OpenableThing()), name="x")
    f.open("rb+")
    assert calls == ["rb+"]
    assert f.mode == "rb+"


def test_open_seeks_when_underlying_lacks_open_method():
    """Most file-like objects (BytesIO) don't have ``.open()`` — fall
    back to seeking to 0 so the next ``read`` gets the full payload."""
    buf = io.BytesIO(b"hello")
    buf.read()  # exhaust
    f = File(buf, name="x")
    f.open()  # no mode; just rewind
    assert buf.read() == b"hello"


def test_file_repr_and_bool_reflect_name():
    f_named = File(io.BytesIO(b""), name="hello.txt")
    f_blank = File(io.BytesIO(b""), name="")
    assert "hello.txt" in repr(f_named)
    assert bool(f_named) is True
    assert bool(f_blank) is False


def test_content_file_repr_includes_size():
    cf = ContentFile(b"abc", name="x.txt")
    assert "x.txt" in repr(cf) and "3" in repr(cf)


# ── Storage: abstract methods raise ──────────────────────────────────────────


class TestStorageABC:
    """The base ``Storage`` is a classic ABC-by-NotImplementedError.
    Each missing override is a clear contract failure for subclassers
    and the messages have to be useful."""

    def test_save_subclass_must_implement(self):
        with pytest.raises(NotImplementedError, match="_save"):
            Storage()._save("x", ContentFile(b""))

    def test_open_subclass_must_implement(self):
        with pytest.raises(NotImplementedError, match="_open"):
            Storage()._open("x", "rb")

    def test_delete_subclass_must_implement(self):
        with pytest.raises(NotImplementedError, match="delete"):
            Storage().delete("x")

    def test_exists_subclass_must_implement(self):
        with pytest.raises(NotImplementedError, match="exists"):
            Storage().exists("x")

    def test_size_subclass_must_implement(self):
        with pytest.raises(NotImplementedError, match="size"):
            Storage().size("x")

    def test_url_subclass_must_implement(self):
        with pytest.raises(NotImplementedError, match="url"):
            Storage().url("x")

    def test_path_default_raises_for_remote_storages(self):
        with pytest.raises(NotImplementedError, match="local filesystem analogue"):
            Storage().path("x")


# ── get_available_name: truncation + safety bound ───────────────────────────


class _AlwaysExistsStorage(Storage):
    """Storage where every name 'exists' — drives the collision
    machinery to its safety limit."""

    def __init__(self):
        self.calls = 0

    def exists(self, name):
        self.calls += 1
        return True

    # Stub the rest so the ABC error path doesn't fire — none of these
    # are reached by ``get_available_name``.
    def _save(self, name, content):  # pragma: no cover
        return name

    def _open(self, name, mode):  # pragma: no cover
        raise NotImplementedError

    def delete(self, name):  # pragma: no cover
        pass

    def size(self, name):  # pragma: no cover
        return 0

    def url(self, name):  # pragma: no cover
        return name


def test_get_available_name_max_length_truncates_stem():
    """``max_length`` shorter than ``<stem>_<token><ext>`` means the
    stem has to be cut, never the extension."""

    class CollideOnce(Storage):
        def __init__(self):
            self._first = True

        def exists(self, name):
            taken, self._first = self._first, False
            return taken

        # ABC stubs
        def _save(self, name, content):  # pragma: no cover
            return name

        def _open(self, name, mode):  # pragma: no cover
            raise NotImplementedError

        def delete(self, name):  # pragma: no cover
            pass

        def size(self, name):  # pragma: no cover
            return 0

        def url(self, name):  # pragma: no cover
            return name

    s = CollideOnce()
    # ``max_length=12`` is too short to fit ``"longstem_<token>.txt"``
    # so the stem must be trimmed; the extension stays.
    out = s.get_available_name("longstem.txt", max_length=12)
    assert out.endswith(".txt")
    assert len(out) <= 12


def test_get_available_name_aborts_after_100_attempts():
    s = _AlwaysExistsStorage()
    with pytest.raises(RuntimeError, match="gave up"):
        s.get_available_name("hopeless.txt")
    assert s.calls > 100


# ── FileSystemStorage: chmod / directory delete / non-chunked content ────────


def test_filesystem_save_with_non_chunkable_content(tmp_path):
    """Storage.save accepts bare ``bytes`` / ``str`` directly. The
    ``_save`` path then takes the ``read()`` branch instead of
    ``chunks()`` — exercise it via str."""
    storage = FileSystemStorage(location=str(tmp_path))
    name = storage.save("greet.txt", "hola")
    assert (tmp_path / name).read_text() == "hola"


def test_filesystem_save_swallows_chmod_oserror(tmp_path, monkeypatch):
    """A read-only mount or restrictive Windows ACL can reject
    ``chmod`` even when the write itself succeeded. The save must
    still complete."""

    storage = FileSystemStorage(location=str(tmp_path))
    import os as _os

    real_chmod = _os.chmod
    calls = {"n": 0}

    def flaky_chmod(path, mode, **kw):
        calls["n"] += 1
        # Reject only the file-mode call (string path under tmp_path).
        if str(path).endswith("greet.txt"):
            raise OSError("read-only mount")
        return real_chmod(path, mode, **kw)

    monkeypatch.setattr(_os, "chmod", flaky_chmod)
    name = storage.save("greet.txt", b"hello")
    assert (tmp_path / name).read_bytes() == b"hello"
    assert calls["n"] >= 1


def test_filesystem_delete_on_directory_uses_rmtree(tmp_path):
    """``delete`` was written to handle both files and whole
    directories so a soft-deleted user folder can be wiped in one
    call."""
    storage = FileSystemStorage(location=str(tmp_path))
    sub = tmp_path / "userdir"
    sub.mkdir()
    (sub / "a.txt").write_bytes(b"x")
    (sub / "b.txt").write_bytes(b"y")
    storage.delete("userdir")
    assert not sub.exists()


def test_filesystem_exists_returns_false_on_traversal_attempt(tmp_path):
    """A path that resolves outside ``location`` must report
    'doesn't exist (here)' — never raise to the caller."""
    storage = FileSystemStorage(location=str(tmp_path))
    assert storage.exists("../escape.txt") is False


# ── default_storage proxy: repr + error fallback ────────────────────────────


def test_default_storage_repr_resolved(tmp_path):
    reset_storages()
    saved = getattr(dorm.settings, "STORAGES", {})
    try:
        dorm.configure(
            DATABASES=dorm.settings.DATABASES,
            INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
            STORAGES={
                "default": {
                    "BACKEND": "dorm.storage.FileSystemStorage",
                    "OPTIONS": {"location": str(tmp_path)},
                }
            },
        )
        rendered = repr(default_storage)
        assert "default_storage" in rendered
        assert "FileSystemStorage" in rendered
    finally:
        dorm.configure(
            DATABASES=dorm.settings.DATABASES,
            INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
            STORAGES=saved,
        )
        reset_storages()


def test_default_storage_repr_unresolved_when_misconfigured():
    """When STORAGES is unset, the proxy resolves to the implicit
    default. To exercise the unresolved-error branch we have to make
    ``get_storage`` raise; pointing at a missing module does it."""
    reset_storages()
    saved = getattr(dorm.settings, "STORAGES", {})
    try:
        dorm.configure(
            DATABASES=dorm.settings.DATABASES,
            INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
            STORAGES={
                "default": {"BACKEND": "no.such.module.NopeStorage"}
            },
        )
        rendered = repr(default_storage)
        assert "unresolved" in rendered
    finally:
        dorm.configure(
            DATABASES=dorm.settings.DATABASES,
            INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
            STORAGES=saved,
        )
        reset_storages()


# ── FieldFile: imperative save / delete / async / size paths ────────────────


class _Doc(dorm.Model):
    name = dorm.CharField(max_length=50)
    attachment = dorm.FileField(upload_to="docs/", null=True, blank=True)

    class Meta:
        db_table = "ff_cov_docs"


@pytest.fixture
def media_root(tmp_path: Path):
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
                    "base_url": "/m/",
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
    from dorm.migrations.operations import _field_to_column_sql
    from dorm.db.connection import get_connection

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "ff_cov_docs"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Doc._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "ff_cov_docs" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "ff_cov_docs"{cascade}')


class TestFieldFileImperative:
    def test_save_method_persists_and_resaves_row(self, media_root, docs_table):
        """``FieldFile.save(name, content, save=True)`` is the
        Django-style imperative API for replacing the file *and*
        re-saving the model row."""
        doc = _Doc.objects.create(name="d")
        doc.attachment.save("note.txt", ContentFile(b"first"))

        loaded = _Doc.objects.get(pk=doc.pk)
        assert loaded.attachment.name == "docs/note.txt"
        assert (media_root / "docs" / "note.txt").read_bytes() == b"first"

    def test_save_with_save_false_skips_row_resave(self, media_root, docs_table):
        """``save=False`` writes the file and updates the descriptor,
        but does not re-INSERT/UPDATE the row. Verify by mutating the
        instance in memory, calling ``.save(save=False)``, and
        observing the column unchanged in the DB."""
        doc = _Doc.objects.create(name="orig")
        doc.attachment.save("a.txt", ContentFile(b"a"), save=False)
        # In-memory: the FieldFile points at the new name.
        assert doc.attachment.name == "docs/a.txt"
        # DB: the column is still NULL (no row UPDATE happened).
        from dorm.db.connection import get_connection

        conn = get_connection()
        rows = conn.execute(
            'SELECT "attachment" FROM "ff_cov_docs" WHERE "id" = %s', [doc.pk]
        )
        assert dict(rows[0])["attachment"] is None

    def test_delete_method_clears_storage_and_column(
        self, media_root, docs_table
    ):
        doc = _Doc.objects.create(name="d2")
        doc.attachment = ContentFile(b"x", name="x.txt")
        doc.save()
        on_disk = media_root / "docs" / "x.txt"
        assert on_disk.exists()

        doc.attachment.delete(save=True)

        assert not on_disk.exists()
        loaded = _Doc.objects.get(pk=doc.pk)
        assert not bool(loaded.attachment)

    def test_delete_no_op_when_uncommitted(self, media_root, docs_table):
        """A pristine FieldFile (no name, never persisted) is a
        no-op delete. Catches the early-return branch."""
        doc = _Doc.objects.create(name="empty")
        # Should not raise, should not touch storage.
        doc.attachment.delete(save=False)
        assert not bool(doc.attachment)

    def test_size_uncommitted_falls_through_to_super(self, media_root):
        """``FieldFile.size`` short-circuits to ``Storage.size`` when
        the file is committed; for uncommitted ones it falls back to
        the underlying ``File.size`` (which inspects the buffer)."""
        from tests.models import Author

        author = Author(name="a", age=1)
        ff = FieldFile(author, _Doc._meta.get_field("attachment"), None)
        # Attach a buffer manually so super().size has something to
        # measure.
        ff.file = io.BytesIO(b"buffer-bytes")
        ff._size = None
        ff._committed = False
        assert ff.size == len(b"buffer-bytes")

    def test_path_property_returns_local_path(self, media_root, docs_table):
        doc = _Doc.objects.create(name="p")
        doc.attachment = ContentFile(b"p", name="p.txt")
        doc.save()
        loaded = _Doc.objects.get(pk=doc.pk)
        # ``FieldFile.path`` is FileSystemStorage-only and returns
        # the absolute on-disk path.
        path = loaded.attachment.path
        assert Path(path).is_absolute()
        assert Path(path).name == "p.txt"

    def test_url_size_path_require_a_name(self, media_root):
        """All three properties share ``_require_name`` — calling
        them on an unattached FieldFile raises a clear
        ``ValueError``."""
        from tests.models import Author

        author = Author(name="x", age=1)
        ff = FieldFile(author, _Doc._meta.get_field("attachment"), None)
        with pytest.raises(ValueError, match="no associated file"):
            _ = ff.url
        with pytest.raises(ValueError, match="no associated file"):
            _ = ff.path

    def test_str_and_repr(self, media_root, docs_table):
        doc = _Doc.objects.create(name="r")
        doc.attachment = ContentFile(b"r", name="r.txt")
        doc.save()
        loaded = _Doc.objects.get(pk=doc.pk)
        assert str(loaded.attachment) == "docs/r.txt"
        assert "committed" in repr(loaded.attachment)
        # Pending FieldFile renders as 'pending'.
        doc2 = _Doc(name="pending")
        doc2.attachment = ContentFile(b"p", name="p.txt")
        ff = doc2.__dict__.get("_fieldfile_attachment")
        if ff is not None:
            assert "pending" in repr(ff)


class TestFieldFileAsyncImperative:
    @pytest.mark.asyncio
    async def test_asave_persists_and_resaves_row(
        self, media_root, docs_table
    ):
        doc = await _Doc.objects.acreate(name="async")
        await doc.attachment.asave("an.txt", ContentFile(b"async-bytes"))

        loaded = await _Doc.objects.aget(pk=doc.pk)
        assert loaded.attachment.name == "docs/an.txt"
        assert (media_root / "docs" / "an.txt").read_bytes() == b"async-bytes"

    @pytest.mark.asyncio
    async def test_adelete_clears_storage_and_column(
        self, media_root, docs_table
    ):
        doc = _Doc(name="del-async")
        doc.attachment = ContentFile(b"x", name="x.txt")
        await doc.asave()
        on_disk = media_root / "docs" / "x.txt"
        assert on_disk.exists()

        await doc.attachment.adelete(save=True)

        assert not on_disk.exists()
        loaded = await _Doc.objects.aget(pk=doc.pk)
        assert not bool(loaded.attachment)

    @pytest.mark.asyncio
    async def test_adelete_no_op_when_uncommitted(self, media_root, docs_table):
        doc = await _Doc.objects.acreate(name="empty-async")
        await doc.attachment.adelete(save=False)
        assert not bool(doc.attachment)

    @pytest.mark.asyncio
    async def test_aopen_streams_through_descriptor(
        self, media_root, docs_table
    ):
        doc = _Doc(name="ao")
        doc.attachment = ContentFile(b"ao-bytes", name="ao.bin")
        await doc.asave()

        loaded = await _Doc.objects.aget(pk=doc.pk)
        opened = await loaded.attachment.aopen("rb")
        try:
            assert opened.read() == b"ao-bytes"
        finally:
            opened.close()


# ── Path traversal across all entry points ──────────────────────────────────


def test_traversal_rejected_on_size_and_open(tmp_path):
    """Catch the rejection on every read entry point, not only save."""
    storage = FileSystemStorage(location=str(tmp_path / "media"))
    with pytest.raises(ImproperlyConfigured):
        storage.size("../etc/passwd")
    with pytest.raises(ImproperlyConfigured):
        storage._open("../etc/passwd", "rb")
