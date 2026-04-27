"""AWS S3 storage backend.

Optional dependency — install via ``pip install 'djanorm[s3]'``.
Uses :mod:`boto3` synchronously; the ``a*`` async methods inherited
from :class:`Storage` wrap each call in :func:`asyncio.to_thread`.
That's the right trade-off for most apps: ``aioboto3`` would buy
true async at the cost of a much heavier dependency, and most file
operations are short-lived enough that the threadpool detour is
invisible next to network latency.

Configuration (in ``settings.STORAGES``)::

    STORAGES = {
        "default": {
            "BACKEND": "dorm.contrib.storage.s3.S3Storage",
            "OPTIONS": {
                "bucket_name": "my-app-uploads",
                "region_name": "eu-west-1",
                # Optional, generally not needed if the runtime has IAM
                # creds — boto3 picks them up automatically.
                # "access_key": "...", "secret_key": "...",
                # "endpoint_url": "https://s3.example.com",  # MinIO etc.
                "default_acl": "private",
                "location": "uploads/",        # key prefix
                "querystring_auth": True,      # generate presigned URLs
                "querystring_expire": 3600,    # seconds
                "file_overwrite": False,       # use storage-side dedupe
            },
        },
    }

If your S3-compatible service speaks a slightly different dialect
(MinIO, Cloudflare R2, Backblaze B2), set ``endpoint_url``. Pre-signed
URLs work against any endpoint that implements the S3 sigv4 surface.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ...exceptions import ImproperlyConfigured
from ...storage import File, Storage


class S3Storage(Storage):
    """Storage backend that puts files on AWS S3 (or any S3-compatible
    object store).

    The ``boto3`` client is created lazily on first use so importing
    this module without the optional dep installed only fails if you
    actually try to use it. That keeps ``ImportError`` surprises
    confined to the call site rather than blowing up on settings load.
    """

    def __init__(
        self,
        *,
        bucket_name: str,
        region_name: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        endpoint_url: str | None = None,
        default_acl: str | None = None,
        location: str = "",
        querystring_auth: bool = True,
        querystring_expire: int = 3600,
        file_overwrite: bool = False,
        signature_version: str | None = None,
        addressing_style: str | None = None,
        custom_domain: str | None = None,
    ) -> None:
        if not bucket_name:
            raise ImproperlyConfigured(
                "S3Storage requires 'bucket_name' in OPTIONS."
            )
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.endpoint_url = endpoint_url
        self.default_acl = default_acl
        # Normalise prefix so it always looks like ``"folder/"`` (no
        # leading slash, exactly one trailing slash if non-empty).
        self.location = location.strip("/") + "/" if location.strip("/") else ""
        self.querystring_auth = querystring_auth
        self.querystring_expire = int(querystring_expire)
        self.file_overwrite = file_overwrite
        self.signature_version = signature_version
        self.addressing_style = addressing_style
        self.custom_domain = custom_domain
        self._client: Any = None

    # ── boto3 lazy init ─────────────────────────────────────────────────────

    @property
    def client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:  # pragma: no cover — exercised only without boto3
            raise ImproperlyConfigured(
                "S3Storage needs the 'boto3' package. Install the optional "
                "dependency: pip install 'djanorm[s3]'."
            ) from exc

        cfg_kwargs: dict[str, Any] = {}
        if self.signature_version:
            cfg_kwargs["signature_version"] = self.signature_version
        if self.addressing_style:
            cfg_kwargs["s3"] = {"addressing_style": self.addressing_style}
        config = Config(**cfg_kwargs) if cfg_kwargs else None

        self._client = boto3.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=self.session_token,
            endpoint_url=self.endpoint_url,
            config=config,
        )
        return self._client

    # ── Naming ──────────────────────────────────────────────────────────────

    def _key(self, name: str) -> str:
        # ``location`` is the user-configured prefix; ``name`` is what
        # the FileField (or caller) supplied. We normalise both ends so
        # the key never has a doubled slash and never an absolute root.
        cleaned = name.lstrip("/")
        return f"{self.location}{cleaned}" if self.location else cleaned

    def get_available_name(
        self, name: str, max_length: int | None = None
    ) -> str:
        """Override the default collision dance when overwrite is enabled.

        With ``file_overwrite=True`` callers explicitly want
        ``put_object`` to clobber whatever's at the key — typical for
        content-addressed storage. The default behaviour (rename
        until unique) still wins when ``file_overwrite=False``.
        """
        if self.file_overwrite:
            return name
        return super().get_available_name(name, max_length=max_length)

    # ── Read/write ──────────────────────────────────────────────────────────

    def _save(self, name: str, content: File) -> str:
        key = self._key(name)
        extra: dict[str, Any] = {}
        if self.default_acl:
            extra["ACL"] = self.default_acl

        # ``upload_fileobj`` accepts any file-like object with ``read``
        # and handles multipart for large payloads automatically. We
        # rewind first so re-saves of the same File work.
        body = content.file
        if body is None:
            raise ValueError("Cannot save a File with no underlying stream.")
        if hasattr(body, "seek"):
            try:
                body.seek(0)
            except (OSError, ValueError):
                pass

        self.client.upload_fileobj(
            body, self.bucket_name, key,
            ExtraArgs=extra or None,
        )
        # We return the user-facing *name*, not the bucket-prefixed
        # *key*. Storage abstraction: callers persist the same name
        # they passed in; the prefix is an implementation detail.
        return name.replace("\\", "/")

    def _open(self, name: str, mode: str) -> File:
        # S3 has no notion of write-mode ``open()``; for a write you
        # build a ``File`` and call ``save()`` instead. Read mode opens
        # via ``get_object`` and wraps the streaming body in a File.
        if "w" in mode or "a" in mode:
            raise NotImplementedError(
                "S3Storage.open() supports read-mode only; use save() to "
                "write new content."
            )
        import io

        resp = self.client.get_object(Bucket=self.bucket_name, Key=self._key(name))
        body = resp["Body"].read()  # streamed; small/medium files OK
        return File(io.BytesIO(body), name=name)

    def delete(self, name: str) -> None:
        # ``delete_object`` is idempotent on S3 — a missing key is not
        # an error. Mirrors :class:`FileSystemStorage`.
        self.client.delete_object(Bucket=self.bucket_name, Key=self._key(name))

    def exists(self, name: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=self._key(name))
            return True
        except Exception as exc:
            # ``head_object`` returns ClientError(404) for "not found".
            # Anything else (auth failures, networking) shouldn't be
            # silenced — reraise so misconfigurations surface.
            code = getattr(getattr(exc, "response", {}).get("Error", {}), "get", lambda *_: None)("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            # Robust fallback: parse the underlying response dict.
            resp_err = getattr(exc, "response", {}).get("Error", {})
            if resp_err.get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def size(self, name: str) -> int:
        head = self.client.head_object(Bucket=self.bucket_name, Key=self._key(name))
        return int(head.get("ContentLength", 0))

    def url(self, name: str) -> str:
        if self.custom_domain:
            # CDN / vanity domain — return a plain URL, no signing.
            return f"https://{self.custom_domain}/{quote(self._key(name), safe='/')}"
        if self.querystring_auth:
            return self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": self._key(name)},
                ExpiresIn=self.querystring_expire,
            )
        # Public-read bucket: build the canonical virtual-hosted URL.
        # Doesn't include the region for the legacy us-east-1 endpoint
        # so behaviour matches what boto3 hands back from
        # ``client.meta.endpoint_url``.
        endpoint = (self.endpoint_url or "https://s3.amazonaws.com").rstrip("/")
        return f"{endpoint}/{self.bucket_name}/{quote(self._key(name), safe='/')}"
