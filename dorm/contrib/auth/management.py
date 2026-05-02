"""Auth management hooks — currently the post-migrate permissions sync.

When ``dorm.contrib.auth`` is in ``INSTALLED_APPS``, after every
``dorm migrate`` run the user can call :func:`sync_permissions` to
materialise the ``Meta.permissions`` declarations of every model
into rows in the ``auth_permission`` table. Default permissions
(``add_x``, ``change_x``, ``delete_x``, ``view_x``) get created
automatically per concrete model — Django parity.

Why explicit instead of an automatic post-migrate hook: dorm's
migration executor doesn't expose a generic post-migrate signal
(yet), and an automatic hook that imports the auth model registry
would create circular bootstrap problems on apps that don't use
contrib.auth. A plain helper keeps the surface small.
"""

from __future__ import annotations

_DEFAULT_PERMISSION_VERBS = ("add", "change", "delete", "view")


def _default_permissions(model_name: str, app_label: str) -> list[tuple[str, str]]:
    """Build the default ``add_x`` / ``change_x`` / ``delete_x`` /
    ``view_x`` codename + human-readable name pairs."""
    pretty = model_name.replace("_", " ")
    out: list[tuple[str, str]] = []
    for verb in _DEFAULT_PERMISSION_VERBS:
        codename = f"{verb}_{model_name}"
        # Namespace by app_label too so two apps that ship a model
        # named "User" get distinct codenames in the auth table.
        out.append((f"{app_label}.{codename}", f"Can {verb} {pretty}"))
    return out


def sync_permissions(*, registry: dict | None = None) -> int:
    """Walk every model in *registry* (defaults to the global model
    registry) and ensure a ``Permission`` row exists for every
    default verb plus every entry in ``Meta.permissions``.

    Returns the number of new ``Permission`` rows created.

    Idempotent: existing rows are left untouched. Stale permissions
    (codename no longer declared) are NOT removed — that's a
    separate cleanup step the operator runs manually, since
    revoking a permission may break user assignments.
    """
    from ...models import _model_registry
    from .models import Permission

    if registry is None:
        registry = _model_registry

    seen_codenames: set[str] = set()
    desired: list[tuple[str, str]] = []
    for key, model_cls in registry.items():
        if "." in key:
            # Aliased entries duplicate concrete-model entries.
            continue
        meta = getattr(model_cls, "_meta", None)
        if meta is None:
            continue
        if getattr(meta, "abstract", False) or getattr(meta, "proxy", False):
            continue
        # Skip the auth tables themselves to avoid bootstrapping
        # ``add_permission`` against a model whose row inserts the
        # Permission rows. The auth app provides the schema; user
        # apps get the auto-generated entries.
        if meta.app_label == "auth":
            continue
        for code, name in _default_permissions(meta.model_name, meta.app_label):
            desired.append((code, name))
        for code, name in getattr(meta, "permissions", []) or []:
            # Custom codenames are stored verbatim — the user is
            # responsible for namespacing if they care about cross-app
            # collisions.
            desired.append((code, name))

    created = 0
    for code, name in desired:
        if code in seen_codenames:
            continue
        seen_codenames.add(code)
        # ``get_or_create`` is the obvious choice but contrib.auth's
        # Permission only carries ``codename`` + ``name``; we treat
        # codename as the lookup key.
        _, was_created = Permission.objects.get_or_create(
            codename=code, defaults={"name": name}
        )
        if was_created:
            created += 1
    return created


__all__ = ["sync_permissions"]
