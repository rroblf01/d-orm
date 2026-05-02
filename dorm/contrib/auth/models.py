"""User / Group / Permission models.

Minimal, framework-agnostic. The user model uses email as the natural
primary login identifier (most modern apps don't surface a separate
username) but a unique ``username`` is also available for legacy
migrations.

Permissions are intentionally simple: a ``Permission`` row carries a
``codename`` (e.g. ``"articles.publish"``) and a ``name``
(human-readable). Authorization checks (``user.has_perm``) walk the
direct ``user.user_permissions`` plus any granted via the user's
``groups``. Whether a given codename means "may publish article 42"
or "may publish any article" is up to your code — the model layer
does NOT impose object-level checks.
"""

from __future__ import annotations

from typing import Any

import dorm

from .password import (
    check_password,
    is_password_usable,
    make_password,
)


class Permission(dorm.Model):
    """A single named permission (e.g. ``"articles.publish"``)."""

    name = dorm.CharField(max_length=255)
    codename = dorm.CharField(max_length=100, unique=True)

    class Meta:
        app_label = "auth"
        db_table = "auth_permission"
        ordering = ["codename"]

    def __str__(self) -> str:
        return self.codename


class Group(dorm.Model):
    """A named bundle of permissions. Users grant permissions
    transitively by joining a group."""

    name = dorm.CharField(max_length=150, unique=True)
    permissions = dorm.ManyToManyField(
        Permission, related_name="groups", blank=True
    )

    class Meta:
        app_label = "auth"
        db_table = "auth_group"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class UserManager(dorm.Manager):
    """Helpers for creating users with a hashed password.

    The plain ``Manager.create(...)`` writes whatever you pass — use
    :meth:`create_user` instead so the password lands hashed.
    """

    def create_user(
        self,
        *,
        email: str,
        password: str | None = None,
        username: str | None = None,
        **extra: Any,
    ) -> "User":
        if not email:
            raise ValueError("create_user requires an email.")
        # ``self.model`` is set via ``contribute_to_class``; ty sees
        # the type as ``type[Any] | None`` because the attribute
        # carries that union at the BaseManager level. Use a real
        # runtime check (not ``assert`` — strippable with
        # ``python -O``) so a misconfigured Manager fails loudly
        # instead of crashing on the next line.
        model_cls = self.model
        if model_cls is None:
            raise RuntimeError(
                "UserManager.model is unset — was the manager attached "
                "to a model via ``contribute_to_class``?"
            )
        user: "User" = model_cls(
            email=email.lower().strip(),
            username=username or email.lower().strip(),
            **extra,
        )
        user.set_password(password)
        user.save()
        return user

    def create_superuser(
        self,
        *,
        email: str,
        password: str,
        username: str | None = None,
        **extra: Any,
    ) -> "User":
        extra.setdefault("is_staff", True)
        extra.setdefault("is_superuser", True)
        extra.setdefault("is_active", True)
        if not extra["is_staff"] or not extra["is_superuser"]:
            raise ValueError("Superusers must have is_staff and is_superuser True.")
        return self.create_user(
            email=email, password=password, username=username, **extra
        )


class User(dorm.Model):
    """Application user.

    The password column stores a salted PBKDF2 hash via
    :func:`dorm.contrib.auth.make_password`. Never write the plain
    string into ``user.password`` directly — use
    :meth:`set_password` so the value gets hashed.

    Fields mirror Django's ``AbstractUser`` shape so the
    ``migrate-from-django`` tooling can map them 1:1.
    """

    username = dorm.CharField(max_length=150, unique=True)
    email = dorm.EmailField(unique=True)
    password = dorm.CharField(max_length=128)

    is_active = dorm.BooleanField(default=True)
    is_staff = dorm.BooleanField(default=False)
    is_superuser = dorm.BooleanField(default=False)

    date_joined = dorm.DateTimeField(auto_now_add=True)
    last_login = dorm.DateTimeField(null=True, blank=True)

    groups = dorm.ManyToManyField(
        Group, related_name="users", blank=True
    )
    user_permissions = dorm.ManyToManyField(
        Permission, related_name="users", blank=True
    )

    objects = UserManager()

    class Meta:
        app_label = "auth"
        db_table = "auth_user"
        ordering = ["email"]

    def __str__(self) -> str:
        return self.email

    # ── Password helpers ────────────────────────────────────────────────────

    def set_password(self, raw_password: str | None) -> None:
        """Hash *raw_password* and store it. Pass ``None`` to mark the
        account password-unusable (SSO-only / invitation flows)."""
        self.password = make_password(raw_password)

    def check_password(self, raw_password: str) -> bool:
        return check_password(raw_password, self.password)

    def has_usable_password(self) -> bool:
        return is_password_usable(self.password)

    def set_unusable_password(self) -> None:
        self.set_password(None)

    # ── Permission helpers ──────────────────────────────────────────────────

    def has_perm(self, codename: str) -> bool:
        """``True`` if the user has the named permission either directly
        or via a group. Superusers short-circuit to ``True``.

        ``has_perm`` only checks **model-level** permissions; object-
        level rules are your application's responsibility (e.g. "may
        publish article 42 because they wrote it"). Use this for the
        broad "may publish any article" gate."""
        if self.is_superuser and self.is_active:
            return True
        if not self.is_active:
            return False
        if self.user_permissions.filter(codename=codename).exists():
            return True
        # Walk groups → group.permissions via forward M2M only — the
        # reverse-M2M traversal ``groups__users`` lands on the junction
        # column name which the queryset compiler can't always resolve
        # cleanly across vendors.
        for group in self.groups.all():
            if group.permissions.filter(codename=codename).exists():
                return True
        return False

    async def ahas_perm(self, codename: str) -> bool:
        """Async counterpart of :meth:`has_perm`."""
        if self.is_superuser and self.is_active:
            return True
        if not self.is_active:
            return False
        if await self.user_permissions.filter(codename=codename).aexists():
            return True
        async for group in self.groups.all().aiterator():
            if await group.permissions.filter(codename=codename).aexists():
                return True
        return False


__all__ = ["User", "Group", "Permission", "UserManager"]
