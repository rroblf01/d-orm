"""File storage abstraction for :class:`dorm.FileField`.

Two layers:

- :class:`Storage` — abstract interface mirrored by every backend
  (``save`` / ``open`` / ``delete`` / ``exists`` / ``size`` / ``url``,
  plus ``a*`` async counterparts). The built-in implementation,
  :class:`FileSystemStorage`, writes to local disk. The optional
  :class:`dorm.contrib.storage.s3.S3Storage` (gated behind the
  ``s3`` extra) writes to AWS S3.

- :class:`File` / :class:`ContentFile` / :class:`FieldFile` — Python
  wrappers callers manipulate. ``FieldFile`` is what
  :class:`FileField` returns from the descriptor; it knows the bound
  instance + field and delegates ``.url`` / ``.size`` / ``.open()`` to
  the underlying storage.

Configuration is via the ``STORAGES`` setting, mirroring ``DATABASES``::

    STORAGES = {
        "default": {
            "BACKEND": "dorm.storage.FileSystemStorage",
            "OPTIONS": {"location": "media", "base_url": "/media/"},
        },
        # Optional: add an alias for S3 uploads.
        # "uploads": {
        #     "BACKEND": "dorm.contrib.storage.s3.S3Storage",
        #     "OPTIONS": {"bucket_name": "my-uploads", "region_name": "us-east-1"},
        # },
    }

If ``STORAGES`` is not set, dorm falls back to a default
``FileSystemStorage`` rooted at ``./media`` so the simple "drop a
``FileField`` and run" path Just Works on a single machine.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import os
import secrets
import shutil
import string
import threading
from typing import IO, Any, Iterator
from urllib.parse import quote

from .exceptions import ImproperlyConfigured


# Filenames produced by user uploads can contain characters that don't
# round-trip safely through a filesystem (or an S3 key, or a URL). The
# allowlist below is conservative: ASCII letters, digits, and a few
# punctuation chars users expect to keep. Anything else gets replaced
# with ``_`` by :meth:`Storage.get_valid_name`. Path separators are
# rejected separately so a malicious upload can't escape ``location``.
_VALID_NAME_CHARS = set(string.ascii_letters + string.digits + "._-+ ")


class File:
    """Wraps a file-like object, exposing ``.name`` and ``.size``.

    Two important properties:

    - The file *content* is owned by ``file`` (a regular Python file
      object). ``File`` itself is just metadata + a thin wrapper.
    - ``size`` is computed lazily — for already-on-disk files it
      ``stat()``s; for in-memory content it reads the buffer length.
      Either way the result is cached on first access.
    """

    DEFAULT_CHUNK_SIZE = 64 * 1024

    def __init__(self, file: IO[Any] | None, name: str | None = None) -> None:
        self.file = file
        # ``name`` defaults to whatever the file object reports, falling
        # back to None for in-memory streams that have no path. Callers
        # typically pass ``name=`` explicitly when uploading because the
        # source filename and the target storage name are different
        # questions.
        if name is None and hasattr(file, "name"):
            name = os.path.basename(getattr(file, "name", "") or "")
        self.name = name or ""
        self.mode = getattr(file, "mode", "rb")
        self._size: int | None = None

    # ── Size ─────────────────────────────────────────────────────────────────

    @property
    def size(self) -> int:
        if self._size is not None:
            return self._size
        if hasattr(self.file, "size"):
            self._size = int(getattr(self.file, "size"))
            return self._size
        if hasattr(self.file, "name") and os.path.exists(self.file.name):
            self._size = os.path.getsize(self.file.name)
            return self._size
        # In-memory or duck-typed streams: peek via tell()/seek().
        if hasattr(self.file, "tell") and hasattr(self.file, "seek"):
            pos = self.file.tell()
            self.file.seek(0, os.SEEK_END)
            end = self.file.tell()
            self.file.seek(pos)
            self._size = end
            return self._size
        raise AttributeError(
            "Unable to determine size of File: underlying object has no "
            "'size' / '.name' / 'tell+seek'."
        )

    # ── I/O passthrough ──────────────────────────────────────────────────────

    def read(self, num_bytes: int | None = None) -> Any:
        assert self.file is not None, "File has no underlying stream."
        if num_bytes is None:
            return self.file.read()
        return self.file.read(num_bytes)

    def chunks(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """Yield successive ``chunk_size``-sized blocks of file content.

        Used by storage backends that prefer streaming (S3 multipart,
        large filesystem copies) over loading the entire payload into
        memory. Defaults to 64 KiB blocks.
        """
        size = chunk_size or self.DEFAULT_CHUNK_SIZE
        assert self.file is not None
        # Rewind so chunks() can be called more than once on a fresh file.
        if hasattr(self.file, "seek"):
            try:
                self.file.seek(0)
            except (OSError, ValueError):
                # Streams that aren't seekable just yield what's left.
                pass
        while True:
            data = self.file.read(size)
            if not data:
                break
            yield data

    def open(self, mode: str | None = None) -> "File":
        """Re-open the underlying file in *mode*. No-op for in-memory streams."""
        underlying = self.file
        if underlying is not None:
            opener = getattr(underlying, "open", None)
            if callable(opener):
                opener(mode)
            elif hasattr(underlying, "seek"):
                underlying.seek(0)
        if mode:
            self.mode = mode
        return self

    def close(self) -> None:
        if self.file is not None and hasattr(self.file, "close"):
            self.file.close()

    def __enter__(self) -> "File":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def __bool__(self) -> bool:
        return bool(self.name)

    def __repr__(self) -> str:
        return f"<File: {self.name!r}>"


class ContentFile(File):
    """In-memory file backed by ``bytes`` or ``str``.

    Convenience for tests and for code that produces content
    programmatically (rendered PDFs, JSON exports, …) without a
    temp file detour.

    Example::

        File(open("/tmp/foo.pdf", "rb"), name="foo.pdf")
        ContentFile(b"hello", name="hello.txt")
    """

    def __init__(self, content: bytes | str, name: str | None = None) -> None:
        if isinstance(content, str):
            stream: IO[Any] = io.StringIO(content)
        else:
            stream = io.BytesIO(content)
        super().__init__(stream, name=name or "")
        self._size = len(content)

    def __repr__(self) -> str:
        return f"<ContentFile: {self.name!r} ({self._size} bytes)>"


# ── Storage abstract base ────────────────────────────────────────────────────


class Storage:
    """Common interface every storage backend implements.

    Subclasses *must* override ``_save``, ``_open``, ``delete``,
    ``exists``, ``size`` and ``url``. The generic ``save`` /
    ``get_available_name`` / ``get_valid_name`` helpers handle the
    non-vendor-specific parts (filename normalisation, collision
    avoidance) so concrete backends only deal with bytes-on-the-wire.

    Async methods default to ``asyncio.to_thread`` wrappers around
    their sync counterparts. Backends that have a native async client
    (e.g. ``aiobotocore`` for S3) should override the ``a*`` methods
    directly.
    """

    # ── Naming helpers ───────────────────────────────────────────────────────

    @staticmethod
    def get_valid_name(name: str) -> str:
        """Strip path components and normalise unsafe characters.

        Returns a basename — never a full path — so a malicious caller
        passing ``../etc/passwd`` lands on ``etc_passwd`` instead of
        traversing out of the storage root. Concrete backends that
        accept folder structure should split *upload_to* prefixes
        separately and call this on the basename only.
        """
        base = os.path.basename(name.replace("\\", "/"))
        cleaned = []
        for ch in base:
            if ch in _VALID_NAME_CHARS:
                cleaned.append(ch)
            else:
                cleaned.append("_")
        result = "".join(cleaned).strip(" .")
        return result or "file"

    def get_available_name(self, name: str, max_length: int | None = None) -> str:
        """Return a name that doesn't yet exist in storage.

        On collision, insert a random 7-char token before the file
        extension (``report.pdf`` → ``report_a7B9c0d.pdf``). This
        avoids predictable suffixes that would let an attacker
        enumerate uploads, while staying short enough that
        ``max_length`` rarely needs to truncate.
        """
        directory, basename = os.path.split(name)
        stem, ext = os.path.splitext(basename)
        attempts = 0
        candidate = name
        while self.exists(candidate):
            token = secrets.token_urlsafe(6)[:7]
            new_basename = f"{stem}_{token}{ext}"
            if max_length is not None and len(new_basename) > max_length:
                # Trim the stem (not the extension) to fit.
                cut = len(new_basename) - max_length
                new_basename = f"{stem[:-cut]}_{token}{ext}"
            candidate = os.path.join(directory, new_basename) if directory else new_basename
            attempts += 1
            if attempts > 100:
                # Defensive: something is broken (storage stuck on
                # ``exists==True`` for every name we propose). Give up
                # cleanly rather than spinning forever.
                raise RuntimeError(
                    f"Storage.get_available_name gave up after {attempts} "
                    f"attempts for {name!r}."
                )
        return candidate

    def generate_filename(self, filename: str) -> str:
        """Hook for backends to rewrite a filename before save.

        Default is a no-op; subclasses can override to enforce a
        specific layout (e.g. content-addressed S3 keys).
        """
        return filename

    # ── Save ─────────────────────────────────────────────────────────────────

    def save(
        self,
        name: str,
        content: File | bytes | str,
        max_length: int | None = None,
    ) -> str:
        """Persist *content* under a name derived from *name*.

        Returns the final name actually used (which may differ from the
        input if the input collided). Subclasses customise behaviour by
        overriding :meth:`_save` (the bytes-on-the-wire step), not this
        method — naming + collision logic is shared so every backend
        handles it consistently.
        """
        if isinstance(content, (bytes, str)):
            content = ContentFile(content, name=name)
        directory, basename = os.path.split(name)
        cleaned = self.get_valid_name(basename)
        target = os.path.join(directory, cleaned) if directory else cleaned
        target = self.generate_filename(target)
        target = self.get_available_name(target, max_length=max_length)
        return self._save(target, content)

    async def asave(
        self,
        name: str,
        content: File | bytes | str,
        max_length: int | None = None,
    ) -> str:
        return await asyncio.to_thread(self.save, name, content, max_length)

    def _save(self, name: str, content: File) -> str:
        raise NotImplementedError("Storage subclasses must implement _save().")

    # ── Mandatory subclass hooks ─────────────────────────────────────────────

    def open(self, name: str, mode: str = "rb") -> File:
        return self._open(name, mode)

    async def aopen(self, name: str, mode: str = "rb") -> File:
        return await asyncio.to_thread(self.open, name, mode)

    def _open(self, name: str, mode: str) -> File:
        raise NotImplementedError("Storage subclasses must implement _open().")

    def delete(self, name: str) -> None:
        raise NotImplementedError("Storage subclasses must implement delete().")

    async def adelete(self, name: str) -> None:
        await asyncio.to_thread(self.delete, name)

    def exists(self, name: str) -> bool:
        raise NotImplementedError("Storage subclasses must implement exists().")

    async def aexists(self, name: str) -> bool:
        return await asyncio.to_thread(self.exists, name)

    def size(self, name: str) -> int:
        raise NotImplementedError("Storage subclasses must implement size().")

    async def asize(self, name: str) -> int:
        return await asyncio.to_thread(self.size, name)

    def url(self, name: str) -> str:
        raise NotImplementedError("Storage subclasses must implement url().")

    def path(self, name: str) -> str:
        """Return the local filesystem path for *name*.

        Only meaningful for backends that store files locally
        (:class:`FileSystemStorage`). Remote backends should raise
        :class:`NotImplementedError` so callers fall back to ``open``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} stores files remotely; .path() has no "
            "local filesystem analogue. Use .url() or .open() instead."
        )


# ── Local filesystem ─────────────────────────────────────────────────────────


class FileSystemStorage(Storage):
    """Local-disk storage. The default backend.

    *location* is the root directory under which all files live. It is
    created on first save if missing. *base_url* is the URL prefix
    used by :meth:`url` to build public links — set it to whatever
    your web server (or FastAPI ``StaticFiles``, or nginx ``alias``)
    serves *location* from.

    Path-traversal is rejected at write/read/delete time:
    :meth:`_resolve_path` joins *name* onto the absolute *location*
    and verifies the result is still under it. So a name like
    ``../../etc/passwd`` cannot escape the storage root even if it
    survives :meth:`get_valid_name`.
    """

    def __init__(
        self,
        location: str | None = None,
        *,
        base_url: str | None = None,
        directory_permissions_mode: int | None = 0o755,
        file_permissions_mode: int | None = 0o644,
    ) -> None:
        self.location = os.path.abspath(location or os.path.join(os.getcwd(), "media"))
        self.base_url = base_url if base_url is not None else "/media/"
        if not self.base_url.endswith("/"):
            self.base_url = self.base_url + "/"
        self.directory_permissions_mode = directory_permissions_mode
        self.file_permissions_mode = file_permissions_mode

    # ── Internals ────────────────────────────────────────────────────────────

    def _resolve_path(self, name: str) -> str:
        """Map a storage *name* to an absolute filesystem path under *location*.

        Rejects any *name* that, after resolution, would escape *location* —
        the canonical defence against path-traversal uploads.
        """
        # ``os.path.normpath`` collapses ``..`` segments. We then require
        # the absolute path to start with ``location + os.sep`` so any
        # escape attempt (``../foo`` or absolute paths spliced through
        # ``os.path.join``) lands outside and is rejected.
        target = os.path.abspath(os.path.join(self.location, name))
        if target != self.location and not target.startswith(self.location + os.sep):
            raise ImproperlyConfigured(
                f"Refusing to access {name!r}: resolves outside the storage "
                f"root {self.location!r}."
            )
        return target

    # ── Sync API ─────────────────────────────────────────────────────────────

    def _save(self, name: str, content: File) -> str:
        full_path = self._resolve_path(name)
        directory = os.path.dirname(full_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
            if self.directory_permissions_mode is not None:
                # ``os.makedirs(mode=)`` is ignored when intermediate dirs
                # already exist, so apply the mode explicitly to avoid
                # surprises across umask values.
                try:
                    os.chmod(directory, self.directory_permissions_mode)
                except OSError:
                    # Non-fatal: filesystems like Windows or restricted
                    # mounts may reject chmod. The save still succeeded.
                    pass

        # Stream the content in chunks so a multi-GiB upload doesn't
        # have to be materialised into RAM. ``content.chunks()`` will
        # call ``seek(0)`` first if possible.
        with open(full_path, "wb") as fh:
            if hasattr(content, "chunks"):
                for chunk in content.chunks():
                    fh.write(chunk if isinstance(chunk, bytes) else chunk.encode("utf-8"))
            else:
                data = content.read()  # type: ignore[union-attr]
                fh.write(data if isinstance(data, bytes) else data.encode("utf-8"))

        if self.file_permissions_mode is not None:
            try:
                os.chmod(full_path, self.file_permissions_mode)
            except OSError:
                pass
        # The on-disk path is the user-supplied name normalised to
        # forward slashes (URL semantics) for storage-side use.
        return name.replace("\\", "/")

    def _open(self, name: str, mode: str) -> File:
        full_path = self._resolve_path(name)
        # ``buffering=-1`` lets the OS pick a sensible buffer size —
        # which for sequential reads is what callers want.
        fh = open(full_path, mode)
        return File(fh, name=name)

    def delete(self, name: str) -> None:
        full_path = self._resolve_path(name)
        try:
            if os.path.isdir(full_path):
                shutil.rmtree(full_path)
            else:
                os.remove(full_path)
        except FileNotFoundError:
            # Idempotent delete: a missing file isn't an error. Mirrors
            # the behaviour S3's DeleteObject has when the key is gone.
            return

    def exists(self, name: str) -> bool:
        try:
            return os.path.exists(self._resolve_path(name))
        except ImproperlyConfigured:
            # A traversal attempt → "no, that file doesn't exist (here)".
            return False

    def size(self, name: str) -> int:
        return os.path.getsize(self._resolve_path(name))

    def url(self, name: str) -> str:
        # ``quote(safe="/")`` keeps the path-segment slashes but escapes
        # spaces and any non-URL-safe character a filename might carry.
        return self.base_url + quote(name.replace("\\", "/"), safe="/")

    def path(self, name: str) -> str:
        return self._resolve_path(name)


# ── Registry / settings glue ─────────────────────────────────────────────────


_DEFAULT_STORAGES: dict[str, dict[str, Any]] = {
    "default": {
        "BACKEND": "dorm.storage.FileSystemStorage",
        "OPTIONS": {},
    },
}

_storage_instances: dict[str, Storage] = {}
_storage_lock = threading.Lock()


def _resolve_storages_config() -> dict[str, dict[str, Any]]:
    """Read ``STORAGES`` from settings, or fall back to the local default.

    Looking it up lazily keeps the import graph simple — ``dorm.storage``
    can be imported before ``dorm.configure(...)`` runs (e.g. during
    fields module-level evaluation) without forcing every project to
    declare ``STORAGES`` upfront.
    """
    from .conf import settings

    configured = getattr(settings, "STORAGES", None) or {}
    if not configured:
        return dict(_DEFAULT_STORAGES)
    if "default" not in configured:
        raise ImproperlyConfigured(
            "settings.STORAGES must contain a 'default' alias. "
            "Add e.g. STORAGES = {'default': {'BACKEND': "
            "'dorm.storage.FileSystemStorage', 'OPTIONS': {}}}."
        )
    return configured


def _instantiate(alias: str, spec: dict[str, Any]) -> Storage:
    backend = spec.get("BACKEND")
    if not backend:
        raise ImproperlyConfigured(
            f"STORAGES[{alias!r}] is missing the required 'BACKEND' key."
        )
    if not isinstance(backend, str):
        raise ImproperlyConfigured(
            f"STORAGES[{alias!r}]['BACKEND'] must be a dotted import path "
            f"(string), got {type(backend).__name__}."
        )
    module_path, _, class_name = backend.rpartition(".")
    if not module_path:
        raise ImproperlyConfigured(
            f"STORAGES[{alias!r}]['BACKEND']={backend!r} is not a dotted path."
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ImproperlyConfigured(
            f"Cannot import storage backend {backend!r} for STORAGES[{alias!r}]: "
            f"{exc}. If this is the S3 backend, install the optional dep: "
            "`pip install 'djanorm[s3]'`."
        ) from exc
    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise ImproperlyConfigured(
            f"Module {module_path!r} has no attribute {class_name!r}; "
            f"check STORAGES[{alias!r}]['BACKEND']."
        ) from exc
    options = spec.get("OPTIONS") or {}
    if not isinstance(options, dict):
        raise ImproperlyConfigured(
            f"STORAGES[{alias!r}]['OPTIONS'] must be a dict, got "
            f"{type(options).__name__}."
        )
    return cls(**options)


def get_storage(alias: str = "default") -> Storage:
    """Return the configured :class:`Storage` for *alias*, instantiating
    it on first call. Backends are cached per-process; call
    :func:`reset_storages` to drop the cache (typically only needed in
    tests that switch ``STORAGES`` mid-suite).
    """
    cached = _storage_instances.get(alias)
    if cached is not None:
        return cached
    with _storage_lock:
        cached = _storage_instances.get(alias)
        if cached is not None:
            return cached
        cfg = _resolve_storages_config()
        if alias not in cfg:
            raise ImproperlyConfigured(
                f"Storage alias {alias!r} not found in settings.STORAGES; "
                f"known aliases: {sorted(cfg)}."
            )
        instance = _instantiate(alias, cfg[alias])
        _storage_instances[alias] = instance
        return instance


def reset_storages() -> None:
    """Drop every cached :class:`Storage` instance. Forces the next
    :func:`get_storage` to re-read ``settings.STORAGES``."""
    with _storage_lock:
        _storage_instances.clear()


class _DefaultStorageProxy:
    """Module-level proxy that resolves to ``get_storage("default")``
    on every call.

    Re-resolving each time means ``dorm.configure(STORAGES=...)`` after
    import time still takes effect, which matches how
    ``dorm.connection`` already behaves for databases.
    """

    def __getattr__(self, item: str) -> Any:
        return getattr(get_storage("default"), item)

    def __repr__(self) -> str:
        try:
            return f"<default_storage → {get_storage('default')!r}>"
        except Exception as exc:
            return f"<default_storage (unresolved: {exc})>"


default_storage = _DefaultStorageProxy()


# ── FieldFile (the descriptor's return value) ────────────────────────────────


class FieldFile(File):
    """The Python value a :class:`FileField` returns from its descriptor.

    A ``FieldFile`` carries:

    - a back-reference to the model *instance* (so ``.delete()`` /
      ``.save()`` can update the column in place);
    - a reference to its declaring *field* (for ``.storage``,
      ``.upload_to``, etc.);
    - the storage *name* (the path/key actually persisted).

    A *committed* ``FieldFile`` corresponds to a stored file and
    delegates everything to ``field.storage``. An *uncommitted* one
    holds a Python ``File`` waiting to be saved on the next
    ``Model.save()``.
    """

    def __init__(self, instance: Any, field: Any, name: str | None) -> None:
        super().__init__(file=None, name=name or "")
        self.instance = instance
        self.field = field
        # ``_committed`` is True for files already on the storage. New
        # uploads (assigned via ``obj.attachment = File(...)`` or
        # ``ContentFile(...)``) start as uncommitted; the field's
        # ``pre_save`` hook flips this when it persists them.
        self._committed = bool(name)

    # ── Storage delegation ───────────────────────────────────────────────────

    @property
    def storage(self) -> Storage:
        return self.field.storage

    @property
    def url(self) -> str:  # type: ignore[override]
        self._require_name()
        return self.storage.url(self.name)

    @property
    def size(self) -> int:  # type: ignore[override]
        if self._committed:
            self._require_name()
            return self.storage.size(self.name)
        return super().size

    @property
    def path(self) -> str:
        self._require_name()
        return self.storage.path(self.name)

    def open(self, mode: str | None = None) -> "FieldFile":
        self._require_name()
        actual_mode = mode or "rb"
        opened = self.storage.open(self.name, actual_mode)
        # ``opened`` is a fresh File pointing at the underlying stream;
        # we mutate ``self.file`` so ``.read`` / ``.chunks`` / ``with``
        # work on the FieldFile itself, the way callers expect.
        self.file = opened.file
        self.mode = actual_mode
        return self

    async def aopen(self, mode: str = "rb") -> "FieldFile":
        self._require_name()
        opened = await self.storage.aopen(self.name, mode)
        self.file = opened.file
        self.mode = mode
        return self

    # ── Imperative save / delete ─────────────────────────────────────────────

    def save(self, name: str, content: File | bytes | str, save: bool = True) -> None:
        """Persist *content* under *name* (subject to upload_to + naming
        rules) and update the model column. If *save=True* (default),
        the model row is also re-saved so the new column value lands
        in the DB.
        """
        rendered = self.field._render_target_name(self.instance, name)
        saved_name = self.storage.save(
            rendered, content, max_length=self.field.max_length
        )
        self.name = saved_name
        self._committed = True
        # Reflect the change on the instance dict so subsequent reads
        # of the descriptor see the new name without re-instantiating.
        self.instance.__dict__[self.field.attname] = saved_name
        if save:
            self.instance.save()

    async def asave(self, name: str, content: File | bytes | str, save: bool = True) -> None:
        rendered = self.field._render_target_name(self.instance, name)
        saved_name = await self.storage.asave(
            rendered, content, max_length=self.field.max_length
        )
        self.name = saved_name
        self._committed = True
        self.instance.__dict__[self.field.attname] = saved_name
        if save:
            await self.instance.asave()

    def delete(self, save: bool = True) -> None:
        if not self._committed or not self.name:
            return
        self.storage.delete(self.name)
        self.name = ""
        self._committed = False
        self.instance.__dict__[self.field.attname] = None
        if save:
            self.instance.save()

    async def adelete(self, save: bool = True) -> None:
        if not self._committed or not self.name:
            return
        await self.storage.adelete(self.name)
        self.name = ""
        self._committed = False
        self.instance.__dict__[self.field.attname] = None
        if save:
            await self.instance.asave()

    # ── Internals ────────────────────────────────────────────────────────────

    def _require_name(self) -> None:
        if not self.name:
            raise ValueError(
                f"FieldFile for {self.field.name!r} has no associated file; "
                "assign one before reading .url / .size / .path."
            )

    def __bool__(self) -> bool:
        return bool(self.name)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        state = "committed" if self._committed else "pending"
        return f"<FieldFile {self.name!r} ({state})>"
