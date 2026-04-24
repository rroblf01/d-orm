from __future__ import annotations

from .operations import (
    AddField,
    AlterField,
    CreateModel,
    DeleteModel,
    RemoveField,
    RenameField,
)
from .state import ProjectState


class MigrationAutodetector:
    """Compares two ProjectState objects and generates migration operations."""

    def __init__(self, from_state: ProjectState, to_state: ProjectState):
        self.from_state = from_state
        self.to_state = to_state

    @staticmethod
    def _al(key: str) -> str:
        """Extract app_label from a state key like 'example.sales.customer'."""
        return key.rsplit(".", 1)[0]

    def changes(self, app_label: str | None = None) -> dict[str, list]:
        """Return {app_label: [operations]} dict."""
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

        # Deleted models
        for key in from_keys - to_keys:
            al = self._al(key)
            model_state = from_models[key]
            ops = operations.setdefault(al, [])
            ops.append(DeleteModel(name=model_state["name"]))

        # New models
        for key in to_keys - from_keys:
            al = self._al(key)
            model_state = to_models[key]
            fields = list(model_state["fields"].items())
            ops = operations.setdefault(al, [])
            ops.append(CreateModel(
                name=model_state["name"],
                fields=fields,
                options=model_state.get("options", {}),
            ))

        # Existing models — check for field changes
        for key in from_keys & to_keys:
            al = self._al(key)
            from_m = from_models[key]
            to_m = to_models[key]
            ops = operations.setdefault(al, [])

            from_fields = from_m["fields"]
            to_fields = to_m["fields"]

            # Removed fields
            for fname in set(from_fields) - set(to_fields):
                ops.append(RemoveField(
                    model_name=to_m["name"],
                    name=fname,
                ))

            # Added fields
            for fname in set(to_fields) - set(from_fields):
                ops.append(AddField(
                    model_name=to_m["name"],
                    name=fname,
                    field=to_fields[fname],
                ))

            # Changed fields (simplified: compare db_type string)
            for fname in set(from_fields) & set(to_fields):
                old_f = from_fields[fname]
                new_f = to_fields[fname]
                # Compare field type via a dummy connection placeholder
                class _DummyConn:
                    vendor = "sqlite"
                dc = _DummyConn()
                try:
                    old_t = old_f.db_type(dc)
                    new_t = new_f.db_type(dc)
                    if old_t != new_t:
                        ops.append(AlterField(
                            model_name=to_m["name"],
                            name=fname,
                            field=new_f,
                        ))
                except Exception:
                    pass

        # Remove empty app entries
        return {k: v for k, v in operations.items() if v}
