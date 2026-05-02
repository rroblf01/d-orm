"""Stdlib-only password hashing — no passlib / bcrypt / argon2 dep.

Format mirrors Django's ``pbkdf2_sha256``::

    pbkdf2_sha256$<iterations>$<salt>$<base64 hash>

The leading algorithm tag lets future migrations layer in argon2 /
scrypt without breaking already-stored hashes — dispatch on the tag.
``check_password`` uses ``hmac.compare_digest`` for constant-time
comparison so timing side-channels can't leak the hash.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

# OWASP 2024 recommendation for PBKDF2-HMAC-SHA256. Bump in step
# with computer power; the algorithm tag in the encoded hash carries
# the actual iteration count so old passwords keep verifying after a
# bump.
PBKDF2_DEFAULT_ITERATIONS = 600_000

_UNUSABLE_PREFIX = "!"


def _pbkdf2(password: str, salt: str, iterations: int) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    return base64.b64encode(digest).decode("ascii").strip()


def make_password(
    password: str | None,
    *,
    salt: str | None = None,
    iterations: int | None = None,
) -> str:
    """Encode *password* into the storable format.

    ``None`` produces an unusable hash (starts with ``!``) — the user
    can't log in via password until ``set_password`` is called. Use
    this for SSO-only accounts or invitation flows.
    """
    if password is None:
        return _UNUSABLE_PREFIX + secrets.token_urlsafe(32)
    if salt is None:
        # 16 bytes of entropy is the OWASP minimum for password salts.
        salt = secrets.token_urlsafe(16)
    if iterations is None:
        iterations = PBKDF2_DEFAULT_ITERATIONS
    hashed = _pbkdf2(password, salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt}${hashed}"


def check_password(password: str, encoded: str) -> bool:
    """Verify *password* against the *encoded* stored value.

    Constant-time comparison via :func:`hmac.compare_digest` so a
    timing-side-channel can't leak the stored hash bytes.
    Unusable hashes (``!``-prefix) always return False. Non-string
    inputs (``None``, ``int`` from a malformed JSON payload, …) are
    rejected as a failed verification rather than crashing — this
    is a security boundary, callers should never reach a
    ``TypeError`` here.
    """
    if not isinstance(password, str) or not isinstance(encoded, str):
        return False
    if not encoded or encoded.startswith(_UNUSABLE_PREFIX):
        return False
    parts = encoded.split("$", 3)
    if len(parts) != 4:
        return False
    algorithm, iterations_s, salt, expected = parts
    if algorithm != "pbkdf2_sha256":
        # Future: dispatch table by algorithm name. For now we ship
        # only one algorithm; an unknown tag means the hash was
        # written by a future / different deployment.
        return False
    try:
        iterations = int(iterations_s)
    except ValueError:
        return False
    candidate = _pbkdf2(password, salt, iterations)
    return hmac.compare_digest(candidate, expected)


def is_password_usable(encoded: str | None) -> bool:
    """``True`` for hashes produced by :func:`make_password` with a
    real password; ``False`` for the unusable sentinel ``set_password
    (None)`` produces."""
    if not encoded:
        return False
    return not encoded.startswith(_UNUSABLE_PREFIX)


__all__ = [
    "make_password",
    "check_password",
    "is_password_usable",
    "PBKDF2_DEFAULT_ITERATIONS",
]
