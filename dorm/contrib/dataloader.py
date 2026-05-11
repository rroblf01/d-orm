"""Async batch loader (dataloader) for coalescing per-key lookups.

The pattern: instead of N concurrent ``await Model.objects.aget(pk=k)``
calls — each costing a round-trip — buffer the keys for a short
window and resolve them in **one** ``filter(pk__in=keys)`` query.
Borrowed from Facebook's
`DataLoader <https://github.com/graphql/dataloader>`_ — useful in
GraphQL resolvers, fan-out RPC handlers, or any async code path where
several tasks ask for related rows in parallel.

Example::

    from dorm.contrib.dataloader import DataLoader

    loader = DataLoader(lambda pks: Author.objects.filter(pk__in=pks))

    async def resolve_author(book):
        return await loader.load(book.author_id)

    # Many concurrent calls share one round-trip:
    results = await asyncio.gather(
        resolve_author(b1), resolve_author(b2), resolve_author(b3)
    )

The loader auto-batches every key requested in the same event-loop
tick (``asyncio.sleep(0)`` boundary). Misses (key absent from the
batch result) resolve to ``None`` unless the caller passes a custom
``missing`` sentinel.
"""
from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Generic, Hashable, TypeVar

_K = TypeVar("_K", bound=Hashable)
_V = TypeVar("_V")


_MISSING = object()


class DataLoader(Generic[_K, _V]):
    """Coalesce N per-key requests into one batched fetch.

    Args:
        batch_fn: callable that accepts a ``list[K]`` and returns
            **any** of the following shapes (auto-detected):

            - a plain ``dict[K, V]``;
            - an awaitable that resolves to the same dict;
            - a synchronous iterable of ``(K, V)`` tuples;
            - a synchronous iterable of model instances (each with a
              ``key_attr`` attribute the loader uses to map back to
              the input key);
            - an **async iterable** of model instances or tuples
              (any object that exposes ``__aiter__``) — useful for
              the dorm async queryset, which is itself an async
              iterator.

            ``key_attr`` (default ``"pk"``) selects the attribute used
            to recover the input key from a model-instance result.
        max_batch_size: optional ceiling on the number of keys in one
            ``batch_fn`` invocation. When exceeded, the loader splits
            the batch into multiple concurrent calls.
        cache: when True (default), repeated ``load(k)`` calls return
            the cached value without re-issuing a batch. Set False
            for write-through patterns where the underlying row may
            change between loads in the same loop.
        missing: value returned for keys absent from the batch
            result. Default ``None``.
        key_attr: attribute on each returned item that maps back to
            the key. Defaults to ``"pk"`` — useful when batch_fn
            returns model instances rather than a dict.
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
        self._pending: dict[_K, asyncio.Future[_V]] = {}
        self._scheduled = False
        self._lock = asyncio.Lock()

    async def load(self, key: _K) -> _V:
        """Schedule *key* for the next batch and await the result."""
        if self._use_cache and key in self._cache:
            return self._cache[key]
        fut = self._pending.get(key)
        if fut is None:
            fut = asyncio.get_event_loop().create_future()
            self._pending[key] = fut
            if not self._scheduled:
                self._scheduled = True
                asyncio.get_event_loop().call_soon(self._kick)
        return await fut

    async def load_many(self, keys: list[_K]) -> list[_V]:
        """Convenience: gather many ``load`` calls into a single
        await. Preserves the input order."""
        return await asyncio.gather(*(self.load(k) for k in keys))

    def clear(self, key: _K | None = None) -> None:
        """Drop one cached entry (when *key* given) or every cached
        entry (when omitted). Pending requests are untouched.

        Equivalent to :meth:`clear_all` when *key* is None — the
        named alias is provided for readability at call sites.
        """
        if key is None:
            self._cache.clear()
        else:
            self._cache.pop(key, None)

    def clear_all(self) -> None:
        """Drop every cached entry. Same as ``clear(None)``."""
        self._cache.clear()

    def prime(self, key: _K, value: _V) -> None:
        """Pre-load (*key*, *value*) into the cache so the next
        ``load(key)`` skips the batch round-trip.

        Useful after a fan-out write — feeding the just-inserted row
        back into the loader avoids a redundant SELECT when the next
        resolver asks for it. ``cache=False`` makes ``prime`` a
        no-op; emit a warning so the caller can tell.
        """
        if not self._use_cache:
            import logging

            logging.getLogger("dorm.contrib.dataloader").warning(
                "DataLoader.prime() called on a cache=False loader; "
                "the value will not be served on the next load()."
            )
            return
        self._cache[key] = value

    # ── Internals ───────────────────────────────────────────────────────────

    def _kick(self) -> None:
        # Resolution happens in a fresh task so we don't run inside
        # ``call_soon``'s synchronous path (which would defeat the
        # purpose of awaiting `load`).
        self._scheduled = False
        asyncio.create_task(self._resolve_batch())

    async def _resolve_batch(self) -> None:
        async with self._lock:
            if not self._pending:
                return
            pending = self._pending
            self._pending = {}

        keys = list(pending.keys())

        async def _dispatch(chunk: list[_K]) -> dict[_K, _V]:
            result = self._batch_fn(chunk)
            if inspect.isawaitable(result):
                result = await result
            return await self._normalise(result, chunk)

        chunks: list[list[_K]] = []
        if self._max_batch_size is None:
            chunks.append(keys)
        else:
            for i in range(0, len(keys), self._max_batch_size):
                chunks.append(keys[i : i + self._max_batch_size])

        merged: dict[_K, _V] = {}
        try:
            results = await asyncio.gather(*(_dispatch(c) for c in chunks))
            for part in results:
                merged.update(part)
        except Exception as exc:
            for fut in pending.values():
                if not fut.done():
                    fut.set_exception(exc)
            return

        for key, fut in pending.items():
            if fut.done():
                continue
            value: Any = merged.get(key, _MISSING)
            if value is _MISSING:
                value = self._missing
            if self._use_cache and value is not self._missing:
                self._cache[key] = value
            fut.set_result(value)

    async def _normalise(
        self, result: Any, keys: list[_K]
    ) -> dict[_K, _V]:
        """Coerce *result* into a ``{key: value}`` dict.

        Accepts: dict, list / iterable of model instances, list of
        ``(key, value)`` tuples, async iterable of model instances,
        or a queryset-like object (anything with ``__iter__`` /
        ``__aiter__``).
        """
        if isinstance(result, dict):
            return result
        out: dict[_K, _V] = {}
        if hasattr(result, "__aiter__"):
            async for item in result:
                self._record(out, item)
            return out
        try:
            iterator = iter(result)
        except TypeError:
            raise TypeError(
                f"batch_fn must return dict / iterable / awaitable; "
                f"got {type(result).__name__}"
            ) from None
        for item in iterator:
            self._record(out, item)
        return out

    def _record(self, out: dict[_K, _V], item: Any) -> None:
        if isinstance(item, tuple) and len(item) == 2:
            out[item[0]] = item[1]
            return
        # Model-like: read ``key_attr``.
        key = getattr(item, self._key_attr, None)
        if key is None and self._key_attr == "pk":
            # Fall back to ``id`` since some legacy models miss the
            # `pk` descriptor in async paths.
            key = getattr(item, "id", None)
        if key is not None:
            out[key] = item


__all__ = ["DataLoader"]
