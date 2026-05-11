"""Synchronous dataloader — sibling of :class:`dorm.contrib.dataloader.DataLoader`.

The async variant batches keys requested in the same event-loop tick.
The sync variant can't auto-batch because there's no shared
scheduling point, so it exposes an explicit ``flush()`` boundary
instead. The pattern that works in practice:

- Collect the keys for the current request (resolver-style code that
  walks a payload).
- Call :meth:`SyncDataLoader.flush` once before iterating the
  collected work — the buffered keys resolve in **one** batch call.
- ``load()`` after ``flush()`` re-buffers the next round.

Usage::

    loader = SyncDataLoader(lambda ids: Author.objects.in_bulk(ids))

    for book in books:
        loader.load(book.author_id)
    loader.flush()

    for book in books:
        author = loader.load(book.author_id)
        ...
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Generic, Hashable, TypeVar

_K = TypeVar("_K", bound=Hashable)
_V = TypeVar("_V")
_MISSING = object()
_log = logging.getLogger("dorm.contrib.sync_dataloader")


class SyncDataLoader(Generic[_K, _V]):
    """Synchronous batch loader. Coalesces explicit :meth:`load`
    calls until :meth:`flush` resolves the batch.

    Args:
        batch_fn: same shapes as the async variant: dict / iterable /
            (key, value) tuples.
        max_batch_size: optional ceiling per batch invocation.
        cache: when True (default), repeated ``load(k)`` calls reuse
            the cached value across requests.
        missing: value returned for keys absent from the batch
            result.
        key_attr: attribute used to map model-instance results back
            to the input key.
    """

    def __init__(
        self,
        batch_fn: Callable[[list[_K]], Any],
        *,
        max_batch_size: int | None = None,
        cache: bool = True,
        missing: Any = None,
        key_attr: str = "pk",
    ) -> None:
        if max_batch_size is not None and max_batch_size < 1:
            raise ValueError("max_batch_size must be >= 1")
        self._batch_fn = batch_fn
        self._max_batch_size = max_batch_size
        self._use_cache = cache
        self._missing = missing
        self._key_attr = key_attr
        self._cache: dict[_K, _V] = {}
        self._pending: list[_K] = []

    # ── Public API ──────────────────────────────────────────────────────────

    def load(self, key: _K) -> _V | None:
        """Return the value for *key* if already resolved, else
        enqueue and return ``None``. Call :meth:`flush` then
        :meth:`get` (or :meth:`load` again) to retrieve the value
        once the batch ran."""
        if self._use_cache and key in self._cache:
            return self._cache[key]
        if key not in self._pending:
            self._pending.append(key)
        return None

    def get(self, key: _K) -> _V | None:
        """Read a previously-resolved value. Returns ``None`` (or the
        configured *missing* sentinel) when the key isn't cached."""
        return self._cache.get(key, self._missing)

    def flush(self) -> dict[_K, _V]:
        """Resolve every pending key via the batch function. Returns
        the mapping of newly-resolved values (additive to the cache)."""
        if not self._pending:
            return {}
        keys = list(self._pending)
        self._pending = []
        chunks: list[list[_K]] = []
        if self._max_batch_size is None:
            chunks.append(keys)
        else:
            for i in range(0, len(keys), self._max_batch_size):
                chunks.append(keys[i : i + self._max_batch_size])

        merged: dict[_K, _V] = {}
        for chunk in chunks:
            merged.update(self._normalise(self._batch_fn(chunk)))
        if self._use_cache:
            self._cache.update(merged)
        return merged

    def prime(self, key: _K, value: _V) -> None:
        """Seed the cache directly. No-op when caching is disabled
        (warning logged)."""
        if not self._use_cache:
            _log.warning(
                "SyncDataLoader.prime() called on cache=False loader; "
                "the value will not be served."
            )
            return
        self._cache[key] = value

    def clear(self, key: _K | None = None) -> None:
        if key is None:
            self._cache.clear()
        else:
            self._cache.pop(key, None)

    def clear_all(self) -> None:
        self._cache.clear()

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _normalise(self, result: Any) -> dict[_K, _V]:
        if isinstance(result, dict):
            return result
        out: dict[_K, _V] = {}
        try:
            iterator = iter(result)
        except TypeError as e:
            raise TypeError(
                f"SyncDataLoader.batch_fn must return dict / iterable; "
                f"got {type(result).__name__}"
            ) from e
        for item in iterator:
            if isinstance(item, tuple) and len(item) == 2:
                out[item[0]] = item[1]
                continue
            key = getattr(item, self._key_attr, None)
            if key is None and self._key_attr == "pk":
                key = getattr(item, "id", None)
            if key is not None:
                out[key] = item
        return out


__all__ = ["SyncDataLoader"]
