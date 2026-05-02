"""Optional auth models — User, Group, Permission.

Framework-agnostic: provides only the data model + password hashing
helpers. Session management, login views, permission decorators,
middleware, and CSRF are NOT included — that's the framework's
job (FastAPI dependency, Flask-Login, Litestar guards, …).

Quick start::

    INSTALLED_APPS = [
        "dorm.contrib.auth",
        "myapp",
    ]

    from dorm.contrib.auth.models import User

    user = User.objects.create_user(email="a@b.com", password="hunter2")
    user.check_password("hunter2")  # True
    user.set_password("new-password")
    user.save()

Password hashing uses ``hashlib.pbkdf2_hmac`` (stdlib) by default —
no external dependency required. The format
``pbkdf2_sha256$<iterations>$<salt>$<hash>`` matches Django's
default so passwords migrate cleanly between the two ORMs.
"""

from .password import (
    check_password,
    make_password,
    is_password_usable,
    PBKDF2_DEFAULT_ITERATIONS,
)

__all__ = [
    "check_password",
    "make_password",
    "is_password_usable",
    "PBKDF2_DEFAULT_ITERATIONS",
]
