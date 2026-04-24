from __future__ import annotations

import re

_HINT = (
    "It looks like you forgot to create or apply your migrations.\n\n"
    "  Run the following commands:\n"
    "    dorm makemigrations\n"
    "    dorm migrate\n\n"
    "  Or, if you use a custom settings module:\n"
    "    dorm makemigrations --settings=<your_settings_module>\n"
    "    dorm migrate        --settings=<your_settings_module>\n"
)


def raise_migration_hint(exc: Exception) -> None:
    """Re-raise a missing-table error with a friendly migration hint."""
    from dorm.exceptions import OperationalError

    msg = str(exc)
    match = re.search(r"no such table: (\S+)", msg, re.IGNORECASE) or re.search(
        r'relation "([^"]+)" does not exist', msg, re.IGNORECASE
    )
    if match:
        raise OperationalError(
            f'Table "{match.group(1)}" does not exist.\n\n{_HINT}'
        ) from exc
