"""HMAC-signed reset tokens for password-reset / email-verification flows.

Stateless design: the token encodes ``(user_pk, timestamp)`` plus a
HMAC signature derived from the user's ``last_login`` /
``password`` / ``email`` (so changing any of them invalidates every
outstanding token — the hash drops on use). No database table to
maintain; no cleanup job for expired tokens.

Mirrors Django's ``PasswordResetTokenGenerator`` shape but is
framework-agnostic: returns / consumes plain strings, no view
glue. Plug it into your framework of choice (FastAPI, Litestar,
Flask) by:

1. Calling :meth:`make_token` to mint a token, embedding it in a
   reset-email URL.
2. Calling :meth:`check_token` on the inbound request to verify
   the token still binds to the user's current state.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time
from typing import Any

from ...exceptions import ImproperlyConfigured

# Default token lifetime (seconds). Override per-instance with
# ``PasswordResetTokenGenerator(timeout=N)``. 24h matches Django's
# ``PASSWORD_RESET_TIMEOUT_DAYS=1`` default.
DEFAULT_TIMEOUT_SECONDS = 60 * 60 * 24


def _signing_key() -> bytes:
    """Resolve the HMAC key from settings.

    Order: ``CACHE_SIGNING_KEY`` (kept consistent with the queryset
    cache layer so users only configure one secret) → ``SECRET_KEY``.
    Refuses to fall back to a per-process random key — token-reset
    URLs must remain valid across restarts and across worker
    processes, so an ephemeral key would silently invalidate every
    outstanding email link.
    """
    try:
        from ...conf import settings

        for name in ("CACHE_SIGNING_KEY", "SECRET_KEY"):
            value = getattr(settings, name, None)
            if value:
                if isinstance(value, str):
                    return value.encode("utf-8")
                if isinstance(value, bytes):
                    return value
    except Exception:
        pass
    raise ImproperlyConfigured(
        "Token generator requires settings.SECRET_KEY (or "
        "CACHE_SIGNING_KEY). A per-process random key is refused — "
        "outstanding reset links would invalidate on every restart."
    )


def _user_state_hash(user: Any) -> bytes:
    """Build a salt that depends on the user's mutable state.

    Any change in ``last_login``, ``password`` or ``email``
    invalidates the salt → every outstanding token signed with the
    previous salt fails verification. This is the mechanism that
    lets a single use of the token "consume" it: after the user
    sets a new password, the salt rolls and the same token URL
    can't be reused.
    """
    parts = [
        str(getattr(user, "pk", "")),
        str(getattr(user, "password", "")),
        str(getattr(user, "last_login", "")),
        str(getattr(user, "email", "")),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).digest()


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    pad = -len(data) % 4
    return base64.urlsafe_b64decode(data + "=" * pad)


class PasswordResetTokenGenerator:
    """Stateless reset / verification tokens.

    Construct one instance per use case and reuse it: thread-safe,
    no state of its own. Different ``timeout`` values give different
    expiry policies (15-min email-verification vs. 24h
    password-reset vs. 7d "remember-this-device") without sharing
    the secret across multiple namespaces.
    """

    __slots__ = ("timeout", "salt_namespace")

    def __init__(
        self,
        *,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        salt_namespace: str = "password-reset",
    ) -> None:
        self.timeout = int(timeout)
        # Domain-separation salt: two generators with the same
        # secret key but different namespaces produce
        # non-interchangeable tokens, so an email-verification
        # token can't be replayed against the password-reset
        # endpoint.
        self.salt_namespace = salt_namespace

    def _signature(self, user: Any, timestamp: int) -> bytes:
        msg = (
            self.salt_namespace.encode("utf-8")
            + b":"
            + str(getattr(user, "pk", "")).encode("utf-8")
            + b":"
            + str(timestamp).encode("utf-8")
            + b":"
            + _user_state_hash(user)
        )
        return hmac.new(_signing_key(), msg, hashlib.sha256).digest()[:16]

    def make_token(self, user: Any) -> str:
        """Mint a reset token for *user*. Embed verbatim in a URL —
        no further escaping needed (the value is already
        URL-safe-base64).
        """
        timestamp = int(time.time())
        sig = self._signature(user, timestamp)
        return f"{timestamp}-{_b64url_encode(sig)}"

    def check_token(self, user: Any, token: str | None) -> bool:
        """Verify *token* binds to *user*'s current state and is
        within the configured ``timeout``. Constant-time
        comparison; never raises on malformed input — returns
        ``False``."""
        if not isinstance(token, str) or "-" not in token:
            return False
        ts_str, _, sig_str = token.partition("-")
        if not ts_str or not sig_str:
            return False
        try:
            timestamp = int(ts_str)
            sig = _b64url_decode(sig_str)
        except Exception:
            return False
        if timestamp <= 0:
            return False
        # Reject future-dated tokens — tolerate a small clock skew
        # (a few seconds) but anything beyond that is a bug or a
        # tampered timestamp.
        now = int(time.time())
        if timestamp > now + 60:
            return False
        if now - timestamp > self.timeout:
            return False
        try:
            expected = self._signature(user, timestamp)
        except ImproperlyConfigured:
            # Surface configuration errors loud — checking a token
            # without a signing key is a deployment bug, not a
            # silent token rejection.
            raise
        return hmac.compare_digest(sig, expected)


# Convenience pre-built generator with the standard 24-hour timeout
# — most apps just need one of these. Still safe to construct your
# own for narrower windows (email verification: 15 min; etc.).
default_token_generator = PasswordResetTokenGenerator()


def generate_short_lived_token(*, prefix: str = "tok_") -> str:
    """Stateful equivalent: a random URL-safe token that the caller
    stores in a one-shot table (e.g. an email-verification table
    keyed by hashed token + expiry). Use when stateless HMAC isn't
    a fit (e.g. when you need a one-row revoke list)."""
    return prefix + secrets.token_urlsafe(32)


__all__ = [
    "PasswordResetTokenGenerator",
    "default_token_generator",
    "generate_short_lived_token",
    "DEFAULT_TIMEOUT_SECONDS",
]
