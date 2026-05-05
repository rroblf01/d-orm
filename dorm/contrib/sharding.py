"""Horizontal hash-based sharding helpers.

Tenants (``dorm.contrib.tenants``) handle *vertical* isolation by
swapping the PG ``search_path`` on a single database. Sharding solves
the orthogonal problem: a single dataset has outgrown one database and
must be split across N physical databases keyed by a *shard key*
(typically a tenant id, user id, or org id).

This module provides:

- :func:`shard_for` — pure routing function: ``(shard_key, num_shards)
  → alias``.
- :class:`HashShardRouter` — Django-style ``DATABASE_ROUTERS`` entry
  that uses :func:`shard_for` plus a ``shard_models=`` allow-list to
  route reads/writes for sharded models.
- :func:`with_shard_key` — context manager that pins the shard key for
  the surrounding block; the router consults the contextvar instead of
  reaching for an ambient request object.
- :func:`for_each_shard` — fan-out helper that runs a callable against
  every configured shard alias and aggregates the results.

Hash function is :func:`hashlib.blake2b` truncated to 8 bytes — fast,
keyed (so each deployment can pick a salt to defeat targeted
key-collision attacks) and platform-stable. Avoid Python's built-in
``hash()`` for sharding: it is randomised per process by default and
would put the same row on different shards in different workers.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
from typing import Any, Callable, Iterable

_DEFAULT_SALT = b"dorm-shard"
_SHARD_KEY: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "dorm_shard_key", default=None
)


def shard_for(
    key: Any,
    num_shards: int,
    *,
    aliases: list[str] | None = None,
    salt: bytes = _DEFAULT_SALT,
) -> str:
    """Return the database alias for *key* across *num_shards* shards.

    Default alias names are ``shard_0`` … ``shard_<N-1>``; pass
    *aliases* to override (length must equal *num_shards*). The hash
    is deterministic across processes / Python versions because it
    uses a keyed BLAKE2b digest, not Python's built-in ``hash()``.
    """
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    if aliases is not None and len(aliases) != num_shards:
        raise ValueError(
            f"aliases must have exactly {num_shards} entries; got {len(aliases)}"
        )
    if key is None:
        raise ValueError(
            "shard_for(): key must not be None — every sharded row needs a "
            "deterministic key (typically tenant/user/org id)."
        )
    raw = str(key).encode("utf-8")
    digest = hashlib.blake2b(raw, key=salt, digest_size=8).digest()
    bucket = int.from_bytes(digest, "big") % num_shards
    if aliases is not None:
        return aliases[bucket]
    return f"shard_{bucket}"


@contextlib.contextmanager
def with_shard_key(key: Any):
    """Pin *key* as the active shard key for the enclosing block.

    Any sharded query issued inside the block (sync or async) is
    routed to ``shard_for(key, …)``. The pin is per-task (asyncio) /
    per-thread context — it does not bleed between requests.
    """
    token = _SHARD_KEY.set(key)
    try:
        yield key
    finally:
        _SHARD_KEY.reset(token)


def get_shard_key() -> Any | None:
    """Return the currently-pinned shard key, or ``None``."""
    return _SHARD_KEY.get()


class HashShardRouter:
    """Routing entry for ``settings.DATABASE_ROUTERS``.

    Configuration::

        from dorm.contrib.sharding import HashShardRouter
        from myapp.models import Order, Customer

        DATABASES = {
            "default": {...},
            "shard_0": {...},
            "shard_1": {...},
            "shard_2": {...},
            "shard_3": {...},
        }
        DATABASE_ROUTERS = [
            HashShardRouter(num_shards=4, shard_models={Order, Customer}),
        ]

    Inside the request handler::

        from dorm.contrib.sharding import with_shard_key

        with with_shard_key(request.user.tenant_id):
            order = Order.objects.create(...)

    Sharded models without a pinned shard key raise ``RuntimeError`` —
    silently routing to ``default`` would scatter rows across shards
    inconsistently. Non-sharded models are passed through to the
    next router by returning ``None``.
    """

    def __init__(
        self,
        *,
        num_shards: int,
        shard_models: Iterable[type] | None = None,
        aliases: list[str] | None = None,
        salt: bytes = _DEFAULT_SALT,
    ) -> None:
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1")
        self.num_shards = num_shards
        self.aliases = aliases
        self.salt = salt
        self._sharded: set[type] = set(shard_models or ())

    def _is_sharded(self, model: type) -> bool:
        return model in self._sharded or any(
            issubclass(model, m) for m in self._sharded if m is not model
        )

    def _route(self, model: type) -> str | None:
        if not self._is_sharded(model):
            return None
        key = _SHARD_KEY.get()
        if key is None:
            raise RuntimeError(
                f"HashShardRouter: no active shard key for sharded model "
                f"{model.__name__!r}. Wrap the call in "
                f"with_shard_key(<key>) so the router can pick a shard."
            )
        return shard_for(
            key, self.num_shards, aliases=self.aliases, salt=self.salt
        )

    # Django/dorm router protocol.
    def db_for_read(self, model, **hints):
        return self._route(model)

    def db_for_write(self, model, **hints):
        return self._route(model)

    def allow_relation(self, obj1, obj2, **hints):
        # Cross-shard relations don't work — return False so callers
        # surface the constraint at app-level instead of stumbling on
        # a foreign key that points to a different DB at runtime.
        if obj1._meta.model in self._sharded and obj2._meta.model in self._sharded:
            return obj1._state.db == obj2._state.db
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        # Sharded models migrate on every shard alias; non-sharded
        # models migrate only on default. The default shard alias
        # naming is ``shard_<i>`` — match either the explicit alias
        # list or the default prefix.
        if self.aliases is not None:
            shard_aliases = set(self.aliases)
        else:
            shard_aliases = {f"shard_{i}" for i in range(self.num_shards)}
        # ``model_name`` is a string; we can't easily dereference back
        # to the class here. Fall through to ``None`` (= no opinion)
        # so the caller's other routers / default policy decides.
        if db in shard_aliases:
            return None
        return None


def for_each_shard(
    func: Callable[[str], Any],
    *,
    num_shards: int,
    aliases: list[str] | None = None,
) -> dict[str, Any]:
    """Run ``func(alias)`` against every shard alias in turn and
    return ``{alias: result}``. Use for fan-out queries
    (``count()`` of a global table that lives on every shard, etc.).
    Sequential — wrap the body in threads/asyncio yourself if
    parallelism is needed."""
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    if aliases is None:
        aliases = [f"shard_{i}" for i in range(num_shards)]
    out: dict[str, Any] = {}
    for alias in aliases:
        out[alias] = func(alias)
    return out


__all__ = [
    "HashShardRouter",
    "for_each_shard",
    "get_shard_key",
    "shard_for",
    "with_shard_key",
]
