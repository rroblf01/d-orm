from __future__ import annotations

import copy


class ProjectState:
    """Tracks the current state of models as known from migrations."""

    def __init__(self):
        # key: "app_label.model_name_lower" → model dict
        self.models: dict[str, dict] = {}

    def clone(self) -> "ProjectState":
        state = ProjectState()
        state.models = copy.deepcopy(self.models)
        return state

    def add_model(self, app_label: str, model_name: str, fields: dict, options: dict | None = None):
        key = f"{app_label}.{model_name.lower()}"
        self.models[key] = {
            "name": model_name,
            "fields": fields,
            "options": options or {},
        }

    def remove_model(self, app_label: str, model_name: str):
        self.models.pop(f"{app_label}.{model_name.lower()}", None)

    def get_model(self, app_label: str, model_name: str) -> dict | None:
        return self.models.get(f"{app_label}.{model_name.lower()}")

    @classmethod
    def from_apps(cls, app_label: str | None = None) -> "ProjectState":
        """Build state from currently loaded models."""
        from ..models import _model_registry

        state = cls()
        for key, model_cls in _model_registry.items():
            if "." in key:
                continue  # skip aliased entries
            meta = model_cls._meta  # type: ignore
            if meta.abstract:
                continue
            target_app = meta.app_label
            if app_label and target_app != app_label:
                continue

            fields = {}
            for field in meta.fields:
                fields[field.name] = field

            options = {
                "db_table": meta.db_table,
                "ordering": meta.ordering,
                "unique_together": meta.unique_together,
            }
            state.add_model(target_app, model_cls.__name__, fields, options)
        return state
