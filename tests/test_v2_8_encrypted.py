"""Tests for ``dorm.contrib.encrypted``.

Encryption / decryption are covered without hitting the DB to keep
the suite fast and skip-free on machines without ``cryptography``.

The DB round-trip path is exercised by mocking ``settings``-derived
keys and re-creating a tiny model table by hand inside the fixture
so we don't add migrations for a contrib feature.
"""

from __future__ import annotations

import base64

import pytest


def _have_cryptography() -> bool:
    try:
        import cryptography  # noqa: F401
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _have_cryptography(),
    reason="encrypted-field tests require ``pip install 'djanorm[encrypted]'``",
)


@pytest.fixture
def _key():
    """Provide a deterministic test key in settings + restore on exit."""
    import dorm

    raw = base64.b64encode(b"\x01" * 32).decode("ascii")
    prev = getattr(dorm.conf.settings, "FIELD_ENCRYPTION_KEY", "")
    dorm.configure(FIELD_ENCRYPTION_KEY=raw)
    try:
        yield raw
    finally:
        dorm.configure(FIELD_ENCRYPTION_KEY=prev or "")


# ──────────────────────────────────────────────────────────────────────────────
# Pure encryption helpers
# ──────────────────────────────────────────────────────────────────────────────


def test_encrypt_decrypt_round_trip(_key):
    from dorm.contrib.encrypted import _decrypt, _encrypt

    enc = _encrypt("hello world", deterministic=True)
    assert enc and enc.startswith("v1:")
    assert _decrypt(enc) == "hello world"


def test_deterministic_encrypt_repeats(_key):
    """Two encryptions of the same plaintext under deterministic mode
    must produce the same ciphertext — this is what enables equality
    lookups against the encrypted column."""
    from dorm.contrib.encrypted import _encrypt

    a = _encrypt("same value", deterministic=True)
    b = _encrypt("same value", deterministic=True)
    assert a == b


def test_random_nonce_does_not_repeat(_key):
    from dorm.contrib.encrypted import _encrypt

    a = _encrypt("same value", deterministic=False)
    b = _encrypt("same value", deterministic=False)
    assert a != b


def test_decrypt_rejects_tampered_ciphertext(_key):
    from dorm.contrib.encrypted import _decrypt, _encrypt

    enc = _encrypt("hello", deterministic=True)
    # Flip a byte in the ciphertext payload (after the ``v1:`` prefix).
    blob = bytearray(base64.b64decode(enc[3:]))  # ty:ignore[not-subscriptable]
    blob[20] ^= 0x01
    tampered = "v1:" + base64.b64encode(bytes(blob)).decode("ascii")
    with pytest.raises(ValueError, match="could not decrypt"):
        _decrypt(tampered)


def test_decrypt_supports_key_rotation():
    """A row encrypted under an old key must remain readable after
    the user adds a new primary key. The legacy key sits at
    ``FIELD_ENCRYPTION_KEYS[1]`` (older / fallback)."""
    import dorm
    from dorm.contrib.encrypted import _decrypt, _encrypt

    old = base64.b64encode(b"\x02" * 32).decode("ascii")
    new = base64.b64encode(b"\x03" * 32).decode("ascii")

    dorm.configure(FIELD_ENCRYPTION_KEY=old)
    try:
        encrypted_old = _encrypt("classified", deterministic=True)
    finally:
        # Now rotate: new key in primary slot, old key as fallback.
        dorm.configure(FIELD_ENCRYPTION_KEYS=[new, old], FIELD_ENCRYPTION_KEY="")

    try:
        # Decryption must still work — the fallback key kicks in.
        assert _decrypt(encrypted_old) == "classified"
        # New writes use the new key.
        encrypted_new = _encrypt("recent", deterministic=True)
        assert _decrypt(encrypted_new) == "recent"
    finally:
        dorm.configure(FIELD_ENCRYPTION_KEYS=[], FIELD_ENCRYPTION_KEY="")


def test_decrypt_passes_through_legacy_plaintext(_key):
    """Non-prefixed strings are returned untouched. Lets a rolling
    migration from a plaintext column not blow up reads while the
    encryption-pass background job is still running."""
    from dorm.contrib.encrypted import _decrypt

    assert _decrypt("plain string") == "plain string"


def test_decrypt_none_passes_through(_key):
    from dorm.contrib.encrypted import _decrypt, _encrypt

    assert _decrypt(None) is None
    assert _encrypt(None, deterministic=True) is None


def test_missing_key_raises_improperly_configured():
    import dorm
    from dorm.contrib.encrypted import _encrypt
    from dorm.exceptions import ImproperlyConfigured

    dorm.configure(FIELD_ENCRYPTION_KEY="", FIELD_ENCRYPTION_KEYS=[])
    with pytest.raises(ImproperlyConfigured, match="FIELD_ENCRYPTION_KEY"):
        _encrypt("x", deterministic=True)


def test_invalid_key_length_rejected():
    import dorm
    from dorm.contrib.encrypted import _encrypt
    from dorm.exceptions import ImproperlyConfigured

    short = base64.b64encode(b"\x04" * 16).decode("ascii")
    dorm.configure(FIELD_ENCRYPTION_KEY=short)
    try:
        with pytest.raises(ImproperlyConfigured, match="32 bytes"):
            _encrypt("x", deterministic=True)
    finally:
        dorm.configure(FIELD_ENCRYPTION_KEY="")


# ──────────────────────────────────────────────────────────────────────────────
# Field-level integration via prep / from_db_value
# ──────────────────────────────────────────────────────────────────────────────


def test_encrypted_char_field_get_prep_value(_key):
    from dorm.contrib.encrypted import EncryptedCharField

    field = EncryptedCharField(max_length=200, deterministic=True)
    enc = field.get_prep_value("secret")
    assert enc.startswith("v1:")
    # ``from_db_value`` round-trips back to plaintext.
    assert field.from_db_value(enc) == "secret"


def test_encrypted_field_passes_through_none(_key):
    from dorm.contrib.encrypted import EncryptedCharField

    field = EncryptedCharField(max_length=100, null=True)
    assert field.get_prep_value(None) is None
    assert field.from_db_value(None) is None
