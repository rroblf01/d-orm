"""Declarative permissions DSL.

Small DSL for guarding callables behind permission checks. Wraps
the existing :mod:`dorm.contrib.auth.permissions` building blocks
in a decorator-friendly API::

    from dorm.contrib.permissions import requires

    @requires("article.edit")
    def edit_article(user, article_id, payload):
        ...

    @requires("article.edit", scope="article")
    def edit_article(user, article, payload):
        ...

Resolution rules:

- The decorated function's first positional argument is treated as
  the *user*.
- ``scope=`` names another positional argument used for object-level
  checks (passed to ``user.has_perm(perm, obj=...)``).
- Missing user attribute or missing permission raises
  :class:`PermissionDenied`.

Composable: chain decorators for ``all-of`` semantics, or use
:func:`requires_any` for ``or-of`` semantics.
"""
from __future__ import annotations

import functools
import inspect
from typing import Any, Callable


class PermissionDenied(Exception):
    """Raised when a guarded call lacks the required permission."""


def _user_from_args(func: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
    sig = inspect.signature(func)
    params = list(sig.parameters)
    fn_name = getattr(func, "__name__", repr(func))
    if not params:
        raise PermissionDenied(
            f"{fn_name} has no positional argument to extract user from"
        )
    name = params[0]
    if args:
        return args[0]
    if name in kwargs:
        return kwargs[name]
    raise PermissionDenied(
        f"{fn_name} called without user argument {name!r}"
    )


def _scope_from_args(
    func: Callable[..., Any], scope: str | None, args: tuple, kwargs: dict
) -> Any:
    if scope is None:
        return None
    sig = inspect.signature(func)
    params = list(sig.parameters)
    if scope in kwargs:
        return kwargs[scope]
    if scope in params:
        idx = params.index(scope)
        if idx < len(args):
            return args[idx]
    return None


def _check(user: Any, perm: str, obj: Any = None) -> bool:
    """Defer to ``user.has_perm`` when available, else look up
    ``user.permissions`` (a set / iterable). ``None`` user fails."""
    if user is None:
        return False
    has_perm = getattr(user, "has_perm", None)
    if callable(has_perm):
        try:
            return bool(has_perm(perm, obj=obj))
        except TypeError:
            return bool(has_perm(perm))
    perms = getattr(user, "permissions", None) or set()
    return perm in perms


def requires(
    *perms: str, scope: str | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Require every permission in *perms* on the wrapped function's
    user. *scope* names the kwarg / positional carrying the object
    for object-level checks.

    Async functions are auto-detected — the wrapper produces an
    awaitable matching the original."""
    if not perms:
        raise ValueError("requires() needs at least one permission")

    def _decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def _awrap(*args: Any, **kwargs: Any) -> Any:
                user = _user_from_args(func, args, kwargs)
                obj = _scope_from_args(func, scope, args, kwargs)
                for p in perms:
                    if not _check(user, p, obj):
                        raise PermissionDenied(
                            f"user lacks permission {p!r}"
                            + (f" on {obj!r}" if obj is not None else "")
                        )
                return await func(*args, **kwargs)

            return _awrap

        @functools.wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            user = _user_from_args(func, args, kwargs)
            obj = _scope_from_args(func, scope, args, kwargs)
            for p in perms:
                if not _check(user, p, obj):
                    raise PermissionDenied(
                        f"user lacks permission {p!r}"
                        + (f" on {obj!r}" if obj is not None else "")
                    )
            return func(*args, **kwargs)

        return _wrap

    return _decorate


def requires_any(
    *perms: str, scope: str | None = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Allow when **any** permission in *perms* is satisfied."""
    if not perms:
        raise ValueError("requires_any() needs at least one permission")

    def _decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            user = _user_from_args(func, args, kwargs)
            obj = _scope_from_args(func, scope, args, kwargs)
            for p in perms:
                if _check(user, p, obj):
                    return func(*args, **kwargs)
            raise PermissionDenied(
                f"user lacks any of {list(perms)!r}"
                + (f" on {obj!r}" if obj is not None else "")
            )

        return _wrap

    return _decorate


__all__ = ["requires", "requires_any", "PermissionDenied"]
