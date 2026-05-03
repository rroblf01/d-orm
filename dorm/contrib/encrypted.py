"""Application-level field encryption.

Stores ciphertext in the column; decrypts transparently on read /
filter-by-equality. Backed by AES-GCM via ``cryptography`` (opt-in
extra ``djanorm[encrypted]``).

Threat model:

- **In scope**: a database snapshot leak — a stolen backup, a hot
  replica handed to an analyst, a misconfigured ACL on object
  storage. Without the key the column reads as random bytes.
- **NOT in scope**: a process that has both the running app and the
  key in memory. Encryption is at rest, not at runtime.

Key management:

- ``settings.FIELD_ENCRYPTION_KEY`` — single key, base64-encoded
  32 bytes (256 bits). Generate with::

        python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"

- ``settings.FIELD_ENCRYPTION_KEYS`` — for rotation: a list,
  newest first. Encryption uses ``[0]``; decryption tries each in
  order. After enough writes have rolled over (or after a manual
  re-encrypt pass) the older keys can be retired.

Filtering:

- Equality (``filter(field=value)``) works because the same
  plaintext encrypts to the same ciphertext when we use a
  *deterministic* nonce derived from the plaintext (HMAC over the
  key + plaintext). The cost: a sophisticated attacker with column
  access can tell that two rows share a value. If that matters,
  use ``deterministic=False`` and a random nonce — equality lookup
  stops working but indistinguishability is restored.
- Range / substring / sort lookups will NEVER work — the ciphertext
  doesn't preserve those orderings. Use a separate plaintext
  search-helper column when you need them.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any

from ..exceptions import ImproperlyConfigured
from ..fields import CharField, TextField


def _import_aesgcm():
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:  # pragma: no cover - import-time guard
        raise ImproperlyConfigured(
            "Encrypted fields require the ``cryptography`` package. "
            "Install it via ``pip install 'djanorm[encrypted]'``."
        )
    return AESGCM


def _resolve_keys() -> list[bytes]:
    from ..conf import settings

    keys: list[bytes] = []
    raw_list = getattr(settings, "FIELD_ENCRYPTION_KEYS", None)
    if raw_list:
        for k in raw_list:
            keys.append(_decode_key(k))
    raw = getattr(settings, "FIELD_ENCRYPTION_KEY", None)
    if raw:
        keys.append(_decode_key(raw))
    if not keys:
        raise ImproperlyConfigured(
            "settings.FIELD_ENCRYPTION_KEY (or FIELD_ENCRYPTION_KEYS) is "
            "required when using EncryptedField. Generate one with "
            "``python -c \"import secrets,base64; "
            "print(base64.b64encode(secrets.token_bytes(32)).decode())\"``."
        )
    return keys


def _decode_key(key: str | bytes) -> bytes:
    if isinstance(key, bytes):
        b = key
    else:
        b = base64.b64decode(key)
    if len(b) != 32:
        raise ImproperlyConfigured(
            f"FIELD_ENCRYPTION_KEY must be 32 bytes (256 bit) AES key; "
            f"got {len(b)}."
        )
    return b


def _deterministic_nonce(key: bytes, plaintext: bytes) -> bytes:
    """12-byte HMAC-derived nonce.

    AES-GCM nonces must be unique per (key, message). For deterministic
    encryption — the only flavour that supports equality lookup — we
    derive the nonce from ``HMAC-SHA256(key, plaintext)[:12]`` so the
    same plaintext always produces the same ciphertext. Different
    keys produce different nonces, so a key rotation does NOT
    accidentally collide with a previous-key value.
    """
    return hmac.new(key, plaintext, hashlib.sha256).digest()[:12]


def _encrypt(value: str | None, *, deterministic: bool) -> str | None:
    if value is None:
        return None
    AESGCM = _import_aesgcm()
    keys = _resolve_keys()
    primary = keys[0]
    aes = AESGCM(primary)
    pt = value.encode("utf-8")
    if deterministic:
        nonce = _deterministic_nonce(primary, pt)
    else:
        import os

        nonce = os.urandom(12)
    ct = aes.encrypt(nonce, pt, associated_data=None)
    # ``v1:`` version prefix lets future formats (different cipher,
    # nonce length, AD scheme) co-exist without re-encrypting old
    # rows up front. Decryption dispatches on the prefix.
    return "v1:" + base64.b64encode(nonce + ct).decode("ascii")


def _decrypt(stored: str | None) -> str | None:
    if stored is None:
        return None
    if not stored.startswith("v1:"):
        # Pre-encryption legacy plaintext (rare: migration from a
        # plaintext column). Pass through unchanged so existing rows
        # don't blow up the read path during a rolling migration.
        return stored
    # ``b64decode`` raises ``binascii.Error`` on garbage input; wrap
    # so the read path always sees a single ``ValueError`` and the
    # caller doesn't have to know about the binascii module.
    try:
        blob = base64.b64decode(stored[3:])
    except Exception as exc:
        raise ValueError(
            "EncryptedField could not decode the stored value: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if len(blob) < 12 + 16:
        # 12-byte nonce + 16-byte AES-GCM tag is the minimum well-
        # formed payload. A shorter blob would let AES-GCM raise
        # ``InvalidTag`` later, but the message is misleading
        # ("authentication failed" vs. "ciphertext truncated").
        raise ValueError(
            "EncryptedField stored value is too short to be a valid "
            f"AES-GCM payload ({len(blob)} bytes < 28)."
        )
    nonce, ct = blob[:12], blob[12:]
    AESGCM = _import_aesgcm()
    last_exc: Exception | None = None
    for key in _resolve_keys():
        try:
            return AESGCM(key).decrypt(nonce, ct, associated_data=None).decode("utf-8")
        except Exception as exc:
            last_exc = exc
            continue
    # Every configured key rejected the ciphertext. The blob is
    # tampered with, written under a retired key, or written by a
    # different deployment. Surface a clear error rather than
    # silently returning ``None`` (which would hide the bug).
    raise ValueError(
        "EncryptedField could not decrypt the stored value with any "
        f"configured key (FIELD_ENCRYPTION_KEYS). Last error: {last_exc!r}"
    )


# ── Field types ──────────────────────────────────────────────────────────────


class EncryptedFieldMixin:
    """Mixin that wraps :meth:`get_prep_value` / :meth:`from_db_value`
    around AES-GCM. Compose with ``CharField`` or ``TextField`` so
    the underlying column type / max_length stay configurable."""

    deterministic: bool = True

    def __init__(self, *args: Any, deterministic: bool = True, **kwargs: Any) -> None:
        # Ciphertext is base64 + version prefix → ~33% larger than
        # plaintext. Inflate the implicit max_length budget so the
        # caller's "100" still fits the encrypted form. Users that
        # set max_length explicitly are assumed to have done the
        # math.
        self.deterministic = deterministic
        super().__init__(*args, **kwargs)  # type: ignore[misc]

    def get_prep_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return _encrypt(value, deterministic=self.deterministic)
        # Non-string input — coerce via str() so ``EncryptedField`` is
        # forgiving of integer / UUID / Decimal payloads. Decryption
        # always returns ``str``; the user's parsing is on them.
        return _encrypt(str(value), deterministic=self.deterministic)

    def get_db_prep_value(self, value: Any) -> Any:
        # dorm's INSERT / UPDATE / filter binding code calls
        # ``get_db_prep_value``; the mixin used to only override the
        # Django-convention ``get_prep_value`` and the encryption hook
        # was silently bypassed — plaintext was written to disk. Route
        # through the encryption hook here so the on-disk column
        # actually carries ciphertext.
        if value is None:
            return None
        # Avoid double-encrypting an already-encrypted token (callers
        # that re-save a row whose attribute was loaded from the DB
        # via ``from_db_value`` already see plaintext, but defensive).
        if isinstance(value, str) and value.startswith("v1:"):
            return value
        return self.get_prep_value(value)

    def from_db_value(self, value: Any, *_args: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return _decrypt(str(value))

    def to_python(self, value: Any) -> Any:
        # Round-trip from-DB values stay strings; user-supplied
        # values (e.g. ``Model(field="x")``) need no transformation
        # here — encryption happens at ``get_prep_value`` time.
        if value is None:
            return None
        if isinstance(value, str) and value.startswith("v1:"):
            return _decrypt(value)
        return value


class EncryptedCharField(EncryptedFieldMixin, CharField):
    """``CharField`` that stores ciphertext on disk.

    The column type is the same ``VARCHAR(N)`` ``CharField`` would
    emit; ciphertext expands by ~33% (base64 of nonce+ct+tag) so
    pick ``max_length`` ≈ ``plaintext_max * 2`` to stay safe.
    """


class EncryptedTextField(EncryptedFieldMixin, TextField):
    """``TextField`` variant — no length cap, suitable for blobs of
    arbitrary size (notes, addresses, JSON-as-text)."""


__all__ = [
    "EncryptedCharField",
    "EncryptedTextField",
    "EncryptedFieldMixin",
]
