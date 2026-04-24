from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path

from .state import ProjectState


class MigrationLoader:
    """Loads migration files and reconstructs the project state."""

    def __init__(self, connection):
        self.connection = connection
        self.migrations: dict[str, list] = {}  # app_label → list of (number, name, module)
        self.applied: set[tuple[str, str]] = set()

    def load(self, migrations_dir: str | Path, app_label: str):
        migrations_dir = Path(migrations_dir)
        if not migrations_dir.exists():
            return

        files = sorted(migrations_dir.glob("*.py"))
        for path in files:
            if path.name.startswith("_") or not path.name[0].isdigit():
                continue
            stem = path.stem
            # e.g. "0001_initial"
            parts = stem.split("_", 1)
            try:
                number = int(parts[0])
            except ValueError:
                continue

            spec = importlib.util.spec_from_file_location(
                f"{app_label}.migrations.{stem}", path
            )
            module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            spec.loader.exec_module(module)  # type: ignore[union-attr]

            if app_label not in self.migrations:
                self.migrations[app_label] = []
            self.migrations[app_label].append((number, stem, module))

        if app_label in self.migrations:
            self.migrations[app_label].sort(key=lambda x: x[0])

    def load_applied(self, recorder):
        self.applied = recorder.applied_migrations()

    def get_migration_state(self, app_label: str) -> ProjectState:
        """Replay all applied migrations to get the current DB state."""
        state = ProjectState()
        for number, name, module in self.migrations.get(app_label, []):
            if (app_label, name) in self.applied:
                operations = getattr(module, "operations", [])
                for op in operations:
                    op.state_forwards(app_label, state)
        return state

    def unapplied_migrations(self, app_label: str) -> list:
        result = []
        for number, name, module in self.migrations.get(app_label, []):
            if (app_label, name) not in self.applied:
                result.append((number, name, module))
        return result
