"""pytest-djanorm — pytest plugin for djanorm.

Auto-loaded via the ``pytest11`` entry-point. The fixtures registered
below are available in any test file without an explicit import.

The plugin keeps a hard line on framework agnosticism: it never
imports FastAPI / Django / SQLAlchemy / Tortoise. Only ``dorm``,
``pytest``, and (optional, for the container fixtures) ``testcontainers``.
"""

from .plugin import (  # noqa: F401 — re-exports for direct ``from pytest_djanorm import ...``
    djanorm_settings,
    pg_container,
    transactional_db,
    atransactional_db,
    nplusone_guard,
)

__all__ = [
    "djanorm_settings",
    "pg_container",
    "transactional_db",
    "atransactional_db",
    "nplusone_guard",
]
