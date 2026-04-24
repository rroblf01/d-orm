from __future__ import annotations

import weakref
from typing import Any, Callable


class Signal:
    """Minimal signal/event dispatcher (pre/post save/delete)."""

    def __init__(self) -> None:
        self._receivers: list[tuple[Any, Any, type | None, bool]] = []

    # ── Registration ─────────────────────────────────────────────────────────

    def connect(
        self,
        receiver: Callable,
        sender: type | None = None,
        weak: bool = True,
        dispatch_uid: str | None = None,
    ) -> None:
        uid: Any = dispatch_uid if dispatch_uid is not None else id(receiver)
        self._receivers = [r for r in self._receivers if r[0] != uid]
        if weak:
            try:
                ref: Any = weakref.WeakMethod(receiver)  # type: ignore[arg-type]
            except TypeError:
                ref = weakref.ref(receiver)
        else:
            ref = receiver
        self._receivers.append((uid, ref, sender, weak))

    def disconnect(
        self,
        receiver: Callable | None = None,
        sender: type | None = None,
        dispatch_uid: str | None = None,
    ) -> bool:
        uid: Any = dispatch_uid if dispatch_uid is not None else (id(receiver) if receiver is not None else None)
        before = len(self._receivers)
        if uid is not None:
            self._receivers = [r for r in self._receivers if r[0] != uid]
        elif sender is not None:
            self._receivers = [r for r in self._receivers if r[2] is not sender]
        return len(self._receivers) < before

    # ── Dispatch ─────────────────────────────────────────────────────────────

    def send(self, sender: type, **kwargs: Any) -> list[tuple[Callable, Any]]:
        responses: list[tuple[Callable, Any]] = []
        live: list[tuple[Any, Any, type | None, bool]] = []
        for uid, ref, filt_sender, is_weak in self._receivers:
            if filt_sender is not None and filt_sender is not sender:
                live.append((uid, ref, filt_sender, is_weak))
                continue
            if is_weak:
                fn = ref()
                if fn is None:
                    continue  # garbage-collected
            else:
                fn = ref
            live.append((uid, ref, filt_sender, is_weak))
            try:
                responses.append((fn, fn(sender=sender, **kwargs)))
            except Exception:
                pass
        self._receivers = live
        return responses

    def __repr__(self) -> str:
        return f"<Signal receivers={len(self._receivers)}>"


pre_save = Signal()
post_save = Signal()
pre_delete = Signal()
post_delete = Signal()
