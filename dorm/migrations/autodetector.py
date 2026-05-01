from __future__ import annotations

from .operations import (
    AddConstraint,
    AddField,
    AddIndex,
    AlterField,
    CreateModel,
    DeleteModel,
    RemoveConstraint,
    RemoveField,
    RemoveIndex,
    RenameField,
    RenameModel,
)
from .state import ProjectState


class MigrationAutodetector:
    """Compares two ProjectState objects and generates migration operations.

    rename_hints format::

        {
            "models": {"app_label": {"OldName": "NewName"}},
            "fields": {"app_label.ModelName": {"old_field": "new_field"}},
        }

    When a model or field rename hint is provided the autodetector emits a
    RenameModel / RenameField operation instead of a DeleteModel+CreateModel or
    RemoveField+AddField pair.  Without hints the behaviour is unchanged.

    A second heuristic (opt-in via ``detect_renames=True``) automatically
    matches a deleted model to a newly-created one when all their field names
    and types are identical – i.e. only the model name changed.  The same
    applies to fields within the same model when exactly one field is removed
    and one is added and both share the same db_type.
    """

    def __init__(
        self,
        from_state: ProjectState,
        to_state: ProjectState,
        rename_hints: dict | None = None,
        detect_renames: bool = False,
    ):
        self.from_state = from_state
        self.to_state = to_state
        self.rename_hints = rename_hints or {}
        self.detect_renames = detect_renames

    @staticmethod
    def _al(key: str) -> str:
        return key.rsplit(".", 1)[0]

    @staticmethod
    def _field_signature(fields: dict) -> frozenset:
        """Deterministic representation of a model's field set for rename detection."""
        class _DummyConn:
            vendor = "sqlite"
        dc = _DummyConn()
        sig = set()
        for name, field in fields.items():
            try:
                t = field.db_type(dc) or ""
            except Exception:
                t = type(field).__name__
            sig.add((name, t))
        return frozenset(sig)

    def changes(self, app_label: str | None = None) -> dict[str, list]:
        operations: dict[str, list] = {}

        from_models = {
            k: v for k, v in self.from_state.models.items()
            if app_label is None or self._al(k) == app_label
        }
        to_models = {
            k: v for k, v in self.to_state.models.items()
            if app_label is None or self._al(k) == app_label
        }

        from_keys = set(from_models)
        to_keys = set(to_models)

        # ── Collect explicit model rename hints ──────────────────────────────
        model_renames: dict[str, tuple[str, str]] = {}  # old_key → (old_name, new_name)
        for al, name_map in self.rename_hints.get("models", {}).items():
            for old_name, new_name in name_map.items():
                old_key = f"{al}.{old_name.lower()}"
                new_key = f"{al}.{new_name.lower()}"
                if old_key in from_keys and new_key in to_keys:
                    model_renames[old_key] = (old_name, new_name)

        # ── Auto-detect model renames (heuristic) ────────────────────────────
        if self.detect_renames:
            deleted = from_keys - to_keys - set(model_renames)
            created = to_keys - from_keys
            for del_key in list(deleted):
                del_sig = self._field_signature(from_models[del_key]["fields"])
                matches = [
                    new_key for new_key in created
                    if self._field_signature(to_models[new_key]["fields"]) == del_sig
                ]
                if len(matches) == 1:
                    new_key = matches[0]
                    al = self._al(del_key)
                    old_name = from_models[del_key]["name"]
                    new_name = to_models[new_key]["name"]
                    model_renames[del_key] = (old_name, new_name)

        # ── Emit model-level operations ──────────────────────────────────────
        renamed_old_keys = set(model_renames)
        renamed_new_keys = {
            f"{self._al(k)}.{v[1].lower()}" for k, v in model_renames.items()
        }

        # RenameModel
        for old_key, (old_name, new_name) in model_renames.items():
            al = self._al(old_key)
            operations.setdefault(al, []).append(RenameModel(old_name=old_name, new_name=new_name))

        # DeleteModel (skip renamed ones)
        for key in from_keys - to_keys - renamed_old_keys:
            al = self._al(key)
            operations.setdefault(al, []).append(DeleteModel(name=from_models[key]["name"]))

        # CreateModel (skip renamed ones)
        for key in to_keys - from_keys - renamed_new_keys:
            al = self._al(key)
            model_state = to_models[key]
            operations.setdefault(al, []).append(CreateModel(
                name=model_state["name"],
                fields=list(model_state["fields"].items()),
                options=model_state.get("options", {}),
            ))

        # ── Field-level operations for models that exist in both states ───────
        class _DummyConn:
            vendor = "sqlite"
        dc = _DummyConn()

        # Build a map: new_key → from_key (accounts for renamed models)
        key_map: dict[str, str] = {}
        for old_key, (_, new_name) in model_renames.items():
            new_key = f"{self._al(old_key)}.{new_name.lower()}"
            key_map[new_key] = old_key
        for key in from_keys & to_keys:
            key_map[key] = key

        for new_key, from_key in key_map.items():
            if new_key not in to_models or from_key not in from_models:
                continue
            al = self._al(new_key)
            from_m = from_models[from_key]
            to_m = to_models[new_key]
            ops = operations.setdefault(al, [])

            from_fields = from_m["fields"]
            to_fields = to_m["fields"]

            # ── Explicit field rename hints ──────────────────────────────────
            field_hint_key = f"{al}.{to_m['name']}"
            field_renames: dict[str, str] = dict(
                self.rename_hints.get("fields", {}).get(field_hint_key, {})
            )

            # ── Auto-detect field renames ─────────────────────────────────────
            if self.detect_renames:
                removed_names = set(from_fields) - set(to_fields) - set(field_renames)
                added_names = set(to_fields) - set(from_fields) - set(field_renames.values())
                if len(removed_names) == 1 and len(added_names) == 1:
                    old_fname = next(iter(removed_names))
                    new_fname = next(iter(added_names))
                    # Compare BOTH ``db_type`` AND the writer's
                    # serialised output. Comparing only ``db_type``
                    # collapses VectorField(384) and
                    # VectorField(1536) to the same SQLite ``BLOB``,
                    # so a rename + dimension change would be
                    # registered as a pure rename, silently
                    # dropping the type change.
                    from .writer import _serialize_field as _sf

                    try:
                        old_t = from_fields[old_fname].db_type(dc) or ""
                        new_t = to_fields[new_fname].db_type(dc) or ""
                    except Exception:
                        old_t = new_t = ""
                    try:
                        old_s = _sf(from_fields[old_fname])
                        new_s = _sf(to_fields[new_fname])
                    except Exception:
                        old_s = new_s = None
                    if old_t == new_t and (
                        old_s is None or new_s is None or old_s == new_s
                    ):
                        field_renames[old_fname] = new_fname

            renamed_old_fields = set(field_renames)
            renamed_new_fields = set(field_renames.values())

            # RenameField
            for old_fname, new_fname in field_renames.items():
                if old_fname in from_fields and new_fname in to_fields:
                    ops.append(RenameField(
                        model_name=to_m["name"],
                        old_name=old_fname,
                        new_name=new_fname,
                    ))

            # RemoveField (skip renamed)
            for fname in set(from_fields) - set(to_fields) - renamed_old_fields:
                ops.append(RemoveField(model_name=to_m["name"], name=fname))

            # AddField (skip renamed)
            for fname in set(to_fields) - set(from_fields) - renamed_new_fields:
                ops.append(AddField(model_name=to_m["name"], name=fname, field=to_fields[fname]))

            # AlterField — detect any meaningful change to the
            # field, not just a SQL-type-string change. Comparing
            # only ``db_type`` misses:
            #   * VectorField on SQLite — every dimension maps to
            #     ``BLOB`` so a 384→1536 swap looked identical.
            #   * Nullability / default / max_length tweaks on
            #     fields whose db_type is dimension-agnostic.
            # Fall back to the *serialised* field source: if the
            # migration writer would emit different Python for the
            # two fields, that's a real change and a migration is
            # warranted.
            from .writer import _serialize_field

            # Parenthesise the ``& - `` expression — Python parses
            # ``a & b - c`` as ``a & (b - c)``, so without the
            # explicit grouping ``renamed_old_fields`` is
            # subtracted from ``to_fields`` (a no-op since
            # renamed-from names aren't present in the new state)
            # and never excluded from the iteration target.
            for fname in (set(from_fields) & set(to_fields)) - renamed_old_fields:
                old_f = from_fields[fname]
                new_f = to_fields[fname]
                try:
                    old_t = old_f.db_type(dc)
                    new_t = new_f.db_type(dc)
                except Exception:
                    old_t = new_t = None
                # Track each serialise call separately so a
                # writer failure on one field doesn't poison the
                # comparison for both. Previously a single
                # ``except`` set ``old_s = new_s = None`` and the
                # equality check then evaluated to ``None != None``
                # (False) — silencing every change for fields the
                # writer can't currently emit (e.g. nested
                # ``EnumField``).
                try:
                    old_s = _serialize_field(old_f)
                except Exception:
                    old_s = None
                try:
                    new_s = _serialize_field(new_f)
                except Exception:
                    new_s = None
                # Real change if EITHER the SQL type changed OR
                # the writer would emit different Python. If
                # serialisation failed for one side but not the
                # other we treat it as changed (better a spurious
                # AlterField than a silently-missed one).
                serialise_says_changed = (
                    old_s is not None and new_s is not None and old_s != new_s
                ) or (
                    (old_s is None) ^ (new_s is None)
                )
                changed = (
                    (old_t is not None and old_t != new_t) or serialise_says_changed
                )
                if changed:
                    ops.append(AlterField(model_name=to_m["name"], name=fname, field=new_f))

        # ── Index changes ─────────────────────────────────────────────────────
        for new_key, from_key in key_map.items():
            if new_key not in to_models or from_key not in from_models:
                continue
            al = self._al(new_key)
            from_m = from_models[from_key]
            to_m = to_models[new_key]
            ops = operations.setdefault(al, [])

            from_indexes = from_m.get("options", {}).get("indexes", [])
            to_indexes = to_m.get("options", {}).get("indexes", [])

            def _idx_key(idx, model_name: str) -> str:
                return idx.get_name(model_name)

            from_idx_map = {_idx_key(i, from_m["name"]): i for i in from_indexes}
            to_idx_map = {_idx_key(i, to_m["name"]): i for i in to_indexes}

            for name, idx in to_idx_map.items():
                if name not in from_idx_map:
                    ops.append(AddIndex(model_name=to_m["name"], index=idx))

            for name, idx in from_idx_map.items():
                if name not in to_idx_map:
                    ops.append(RemoveIndex(model_name=from_m["name"], index=idx))

        # ── Constraint changes ────────────────────────────────────────────────
        for new_key, from_key in key_map.items():
            if new_key not in to_models or from_key not in from_models:
                continue
            al = self._al(new_key)
            from_m = from_models[from_key]
            to_m = to_models[new_key]
            ops = operations.setdefault(al, [])

            from_constraints = from_m.get("options", {}).get("constraints", []) or []
            to_constraints = to_m.get("options", {}).get("constraints", []) or []

            from_c_map = {getattr(c, "name", repr(c)): c for c in from_constraints}
            to_c_map = {getattr(c, "name", repr(c)): c for c in to_constraints}

            for name, c in to_c_map.items():
                if name not in from_c_map:
                    ops.append(AddConstraint(model_name=to_m["name"], constraint=c))

            for name, c in from_c_map.items():
                if name not in to_c_map:
                    ops.append(
                        RemoveConstraint(model_name=from_m["name"], constraint=c)
                    )

        return {k: v for k, v in operations.items() if v}
