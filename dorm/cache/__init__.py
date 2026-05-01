"""Pluggable result-cache layer for djanorm querysets.

Security note: cached payloads are deserialised via :mod:`pickle`,
which executes ``__reduce__`` on whatever bytes come back from the
backend. A Redis instance writeable by an attacker (multi-tenant
cluster, leaky ACL, no-auth deployment) would let that attacker
inject a malicious blob and trigger arbitrary code execution at
queryset materialisation time.

The cache layer therefore authenticates every payload with an
HMAC-SHA256 signature derived from ``settings.SECRET_KEY`` (or the
explicit ``settings.CACHE_SIGNING_KEY``). Payloads without a
matching signature are dropped on read and treated as a cache
miss — the queryset falls through to the database. Set
``settings.CACHE_INSECURE_PICKLE = True`` to opt out of
verification for legacy caches you can't migrate (don't).

Two opt-in entry points:

- :func:`get_cache` returns the configured cache backend for an
  alias (``"default"`` unless overridden). Reads from
  ``settings.CACHES`` and instantiates the BACKEND class lazily so
  installing dorm without :mod:`redis` is safe.
- :meth:`dorm.QuerySet.cache` chain method opts a single queryset
  into result caching for ``timeout`` seconds.

Settings shape::

    CACHES = {
        "default": {
            "BACKEND": "dorm.cache.redis.RedisCache",
            "LOCATION": "redis://localhost:6379/0",
            "OPTIONS": {"socket_timeout": 1.0},
            # default TTL (seconds) when ``qs.cache()`` is called
            # without an explicit ``timeout``.
            "TTL": 300,
        },
    }

Configurations without ``CACHES`` (the default) leave the cache
inert: ``get_cache()`` raises :class:`ImproperlyConfigured` and
``qs.cache()`` falls back to a no-op so existing code paths stay
zero-cost.
"""

from __future__ import annotations

import threading
from typing import Any

from ..exceptions import ImproperlyConfigured

_caches: dict[str, "BaseCache"] = {}


class BaseCache:
    """Minimal cache contract every backend implements.

    All methods accept string keys and serialised bytes values; the
    queryset layer takes care of (de)serialising rows so backends
    don't have to know about model classes.
    """

    def get(self, key: str) -> bytes | None:  # pragma: no cover - interface
        raise NotImplementedError

    def set(self, key: str, value: bytes, timeout: int | None = None) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def delete(self, key: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def delete_pattern(self, pattern: str) -> int:  # pragma: no cover - interface
        """Bulk-evict keys matching a glob ``pattern`` (e.g.
        ``"qs:books:*"``). Returns the number of keys removed.
        Used by signal-driven invalidation to drop every cached
        queryset for a model in one call.
        """
        raise NotImplementedError

    async def aget(self, key: str) -> bytes | None:  # pragma: no cover - interface
        raise NotImplementedError

    async def aset(
        self, key: str, value: bytes, timeout: int | None = None
    ) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def adelete(self, key: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    async def adelete_pattern(self, pattern: str) -> int:  # pragma: no cover - interface
        raise NotImplementedError

    @property
    def default_timeout(self) -> int:
        """Fallback TTL used by ``qs.cache()`` callers that don't
        pass an explicit ``timeout``. Backends override by setting
        ``self._default_timeout`` from the ``TTL`` settings key."""
        return getattr(self, "_default_timeout", 300)


def _import_class(dotted: str) -> Any:
    module_path, _, attr = dotted.rpartition(".")
    if not module_path:
        raise ImproperlyConfigured(
            f"CACHES.BACKEND must be a dotted path; got {dotted!r}."
        )
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, attr)


def get_cache(alias: str = "default") -> BaseCache:
    """Return (constructing on first use) the cache backend for *alias*.

    Reads ``settings.CACHES`` and instantiates the BACKEND class
    with the alias's configuration. Result is memoised in this
    module so subsequent ``get_cache(alias)`` calls reuse the same
    client (Redis connection pool, in-memory dict, etc.).
    """
    if alias in _caches:
        return _caches[alias]
    from ..conf import settings

    caches = getattr(settings, "CACHES", {}) or {}
    if alias not in caches:
        raise ImproperlyConfigured(
            f"Cache alias {alias!r} is not configured. Add it to "
            "settings.CACHES — e.g. CACHES = {'default': {'BACKEND': "
            "'dorm.cache.redis.RedisCache', 'LOCATION': "
            "'redis://localhost:6379/0'}}"
        )
    cfg = caches[alias]
    backend_path = cfg.get("BACKEND")
    if not backend_path:
        raise ImproperlyConfigured(
            f"Cache {alias!r} is missing a BACKEND key."
        )
    backend_cls = _import_class(backend_path)
    cache = backend_cls(cfg)
    _caches[alias] = cache
    return cache


def reset_caches() -> None:
    """Drop every memoised backend instance.

    Called by :func:`dorm.configure` when the ``CACHES`` setting
    changes; tests can also call it directly to force a re-read.
    """
    for cache in list(_caches.values()):
        close = getattr(cache, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    _caches.clear()


def model_cache_namespace(model: Any) -> str:
    """Build the cache-key prefix shared by every queryset that
    targets ``model``. Signal-driven invalidation calls
    ``delete_pattern(f"{namespace}:*")`` after a save / delete so a
    stale row can't survive a write."""
    meta = getattr(model, "_meta", None)
    label = getattr(meta, "app_label", "") if meta else ""
    name = getattr(model, "__name__", "model")
    return f"dormqs:{label}.{name}"


# ── Per-model invalidation versions ──────────────────────────────────────
#
# The naïve ``read → DB fetch → store`` flow has a stale-read race:
#
#   T0  reader R compiles SQL, runs it (DB returns rows v1).
#   T1  writer W persists v2 + ``delete_pattern`` clears the key.
#   T2  R writes the v1 rows under the same key — entry survives
#       until TTL expires, every later reader sees v1.
#
# We close the window with a per-model in-memory version counter:
# every save / delete bumps it, ``_cache_key`` includes it, and
# ``_cache_store_*`` re-reads the version *after* the DB fetch and
# stores under the (possibly new) key. A racing write between
# fetch and store now lands the rows under a key nobody will read.
#
# The counter is process-local — that's intentional: distributed
# caches use ``delete_pattern`` for cross-process coherence, and a
# *local* version bump is enough to defeat the *local* race
# between a single reader and a single writer in the same process.
_model_versions: dict[str, int] = {}
_versions_lock = threading.Lock()


def model_cache_version(model: Any) -> int:
    """Return the current invalidation version for *model*.

    Used by :class:`QuerySet`'s cache layer to namespace keys per
    write epoch. Bumped via :func:`bump_model_cache_version` on
    every ``post_save`` / ``post_delete``.
    """
    return _model_versions.get(model_cache_namespace(model), 0)


def bump_model_cache_version(model: Any) -> int:
    """Increment the model's cache version. Returns the new value.

    Called by the auto-invalidation signal handler immediately
    *before* it issues ``delete_pattern`` so any racing
    ``_cache_store_*`` call lands its bytes under a key that no
    subsequent read will ask for.
    """
    namespace = model_cache_namespace(model)
    with _versions_lock:
        _model_versions[namespace] = _model_versions.get(namespace, 0) + 1
        return _model_versions[namespace]


# ── Payload signing ──────────────────────────────────────────────────────
#
# pickle.loads on attacker-controlled bytes is RCE — see the
# module docstring. We sign every payload we hand to the backend
# with HMAC-SHA256 and reject anything that doesn't verify on
# load. The signing key comes from (in priority order):
#
#   1. ``settings.CACHE_SIGNING_KEY`` — explicit, recommended.
#   2. ``settings.SECRET_KEY`` — Django convention, reused.
#   3. A per-process random key — entries don't survive a
#      process restart, but the cache stays unforgeable. Useful
#      for one-off scripts that didn't bother configuring a
#      signing key. Logged once at warning level so the user
#      knows.
#
# Layout:
#
#   ``b"dormsig1:" + hex_digest(64) + b":" + pickle_payload``
#
# A 64-char hex digest of HMAC-SHA256 output. The leading
# ``dormsig1:`` prefix versions the format so future migrations
# (e.g. dropping pickle altogether) can co-exist.

_SIGN_PREFIX = b"dormsig1:"
_DIGEST_HEX_LEN = 64
_HEADER_LEN = len(_SIGN_PREFIX) + _DIGEST_HEX_LEN + 1  # ":" separator
_signing_key_cache: bytes | None = None


def _ephemeral_signing_key() -> bytes:
    """Build a per-process random signing key and warn once.

    Used only when neither ``CACHE_SIGNING_KEY`` nor ``SECRET_KEY``
    is configured — the cache is still unforgeable across the
    current process, but entries become useless after a restart
    (which is fine: signed-with-old-key hits get rejected and
    fall through to the database). Logged at warning level so
    the operator knows the cache isn't shared across workers.
    """
    import logging
    import secrets

    global _ephemeral_warned
    key = secrets.token_bytes(32)
    try:
        if not _ephemeral_warned:
            logging.getLogger("dorm.cache").warning(
                "Cache signing key not configured — using a per-process "
                "random key. Set settings.CACHE_SIGNING_KEY or "
                "settings.SECRET_KEY for cross-process cache sharing."
            )
            _ephemeral_warned = True
    except Exception:
        pass
    return key


_ephemeral_warned: bool = False


def _resolve_signing_key() -> bytes:
    """Read the configured signing key once and memoise it.

    Memoisation lets unit tests force a refresh by calling
    :func:`reset_signing_key` — production callers never need to.

    Multi-worker deployments (gunicorn, uvicorn ``--workers >1``,
    multi-process ASGI servers) MUST set ``CACHE_SIGNING_KEY`` or
    ``SECRET_KEY``. Without one, each worker falls back to a
    per-process random key — payloads written by one worker
    can't be verified by another and the cache is effectively
    per-worker (silent hit-rate collapse).

    Set ``CACHE_REQUIRE_SIGNING_KEY = True`` to refuse this
    fallback and raise :class:`ImproperlyConfigured` on first
    cache use. Recommended for any production-shaped multi-
    worker setup.
    """
    global _signing_key_cache
    if _signing_key_cache is not None:
        return _signing_key_cache
    try:
        from ..conf import settings

        for attr in ("CACHE_SIGNING_KEY", "SECRET_KEY"):
            try:
                value = getattr(settings, attr)
            except Exception:
                value = None
            if value:
                if isinstance(value, str):
                    value = value.encode("utf-8")
                _signing_key_cache = bytes(value)
                return _signing_key_cache
        # Honour the strict-mode opt-in BEFORE falling back to
        # an ephemeral random key — that fallback silently
        # breaks multi-worker deployments.
        try:
            require = bool(getattr(settings, "CACHE_REQUIRE_SIGNING_KEY", False))
        except Exception:
            require = False
        if require:
            raise ImproperlyConfigured(
                "settings.CACHE_REQUIRE_SIGNING_KEY is True but no "
                "CACHE_SIGNING_KEY (or SECRET_KEY) is configured. "
                "Set one before using qs.cache(...) — without a "
                "shared key, multi-worker deployments end up with "
                "per-worker caches because each worker generates "
                "its own random key."
            )
    except ImproperlyConfigured:
        raise
    except Exception:
        pass
    _signing_key_cache = _ephemeral_signing_key()
    return _signing_key_cache


def reset_signing_key() -> None:
    """Drop the memoised signing key. Called by
    :func:`dorm.configure` when ``CACHE_SIGNING_KEY`` /
    ``SECRET_KEY`` change so the next sign / verify reads the
    new value."""
    global _signing_key_cache
    _signing_key_cache = None


def _signing_disabled() -> bool:
    """Honour the ``CACHE_INSECURE_PICKLE`` opt-out for users
    migrating an unsigned cache. Default False — sign everything.
    """
    try:
        from ..conf import settings

        return bool(getattr(settings, "CACHE_INSECURE_PICKLE", False))
    except Exception:
        return False


def sign_payload(payload: bytes) -> bytes:
    """Wrap *payload* with an HMAC-SHA256 signature header.

    The signed envelope looks like:

        ``b"dormsig1:<hex64>:<payload>"``

    Verification on load checks the prefix + digest before
    handing *payload* to :func:`pickle.loads`.
    """
    if _signing_disabled():
        return payload
    import hmac
    import hashlib

    key = _resolve_signing_key()
    digest = hmac.new(key, payload, hashlib.sha256).hexdigest().encode("ascii")
    return _SIGN_PREFIX + digest + b":" + payload


def verify_payload(blob: bytes) -> bytes | None:
    """Strip + verify the signature header from *blob*.

    Returns the inner payload bytes when the signature matches,
    ``None`` otherwise (the caller treats that as a cache miss
    and falls through to the database). Invalid / unsigned blobs
    are rejected by default; set
    ``settings.CACHE_INSECURE_PICKLE = True`` to disable
    verification for legacy caches.
    """
    if _signing_disabled():
        return blob
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        return None
    blob = bytes(blob)
    if len(blob) < _HEADER_LEN:
        return None
    if not blob.startswith(_SIGN_PREFIX):
        return None
    sep_idx = len(_SIGN_PREFIX) + _DIGEST_HEX_LEN
    if blob[sep_idx : sep_idx + 1] != b":":
        return None
    digest_hex = blob[len(_SIGN_PREFIX) : sep_idx]
    payload = blob[sep_idx + 1 :]
    import hmac
    import hashlib

    key = _resolve_signing_key()
    expected = hmac.new(key, payload, hashlib.sha256).hexdigest().encode("ascii")
    if not hmac.compare_digest(digest_hex, expected):
        return None
    return payload
