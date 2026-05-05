"""djanorm mypy plugin entry-point.

To enable, add to your ``pyproject.toml``::

    [tool.mypy]
    plugins = ["djanorm_mypy"]

Or to ``mypy.ini``::

    [mypy]
    plugins = djanorm_mypy

What the plugin does:

1. **Field descriptor narrowing**. ``Author.name`` (class-level access)
   types as ``CharField`` (the descriptor itself); ``author.name``
   (instance-level access) narrows to ``str``. The runtime ``Field[_T]``
   class already exposes the right overloads, but the plugin reinforces
   the narrowing across third-party Field subclasses that don't carry
   the overloads themselves.

2. **Lookup kwarg validation**. ``Author.objects.filter(naem="x")``
   reports an error: ``naem`` is not a field on ``Author``. Lookups
   (``name__icontains``, ``age__gte``) are also recognised and the
   suffix is checked against a whitelist of known lookup names.

3. **QuerySet generic preservation**. Manager-level methods
   (``.filter``, ``.exclude``, ``.order_by`` …) carry the model
   parameter through so iteration yields ``_T`` rather than ``Any``.
   The runtime API already returns ``QuerySet[_T]``; this entry point
   keeps subclassed Managers in line.
"""

from __future__ import annotations

from .plugin import DjanormPlugin


def plugin(version: str):
    """mypy plugin entry-point.

    *version* is the running mypy's API version. We accept any 1.x
    — the plugin only relies on the stable public hook surface.
    """
    return DjanormPlugin


__all__ = ["plugin", "DjanormPlugin"]
