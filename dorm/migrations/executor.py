from __future__ import annotations

from pathlib import Path

from .loader import MigrationLoader
from .recorder import MigrationRecorder
from .state import ProjectState


class MigrationExecutor:
    """Applies pending migrations to the database."""

    def __init__(self, connection, verbosity: int = 1):
        self.connection = connection
        self.verbosity = verbosity
        self.recorder = MigrationRecorder(connection)
        self.loader = MigrationLoader(connection)

    def migrate(self, app_label: str, migrations_dir: str | Path, target: str | None = None):
        migrations_dir = Path(migrations_dir)
        self.loader.load(migrations_dir, app_label)
        self.loader.load_applied(self.recorder)

        unapplied = self.loader.unapplied_migrations(app_label)
        if not unapplied:
            if self.verbosity:
                print(f"  No migrations to apply for '{app_label}'.")
            return

        # Build from_state by replaying applied migrations
        from_state = self.loader.get_migration_state(app_label)

        for number, name, module in unapplied:
            if self.verbosity:
                print(f"  Applying {app_label}.{name}...", end=" ")

            operations = getattr(module, "operations", [])
            to_state = from_state.clone()

            for op in operations:
                op.state_forwards(app_label, to_state)
                op.database_forwards(app_label, self.connection, from_state, to_state)
                from_state = to_state.clone()

            self.recorder.record_applied(app_label, name)
            if self.verbosity:
                print("OK")

    def rollback(self, app_label: str, migrations_dir: str | Path, target: str):
        """Roll back to the given migration name."""
        migrations_dir = Path(migrations_dir)
        self.loader.load(migrations_dir, app_label)
        self.loader.load_applied(self.recorder)

        applied = sorted(
            [(num, name, mod) for num, name, mod in self.loader.migrations.get(app_label, [])
             if (app_label, name) in self.loader.applied],
            key=lambda x: x[0],
            reverse=True,
        )

        for number, name, module in applied:
            if name == target:
                break
            if self.verbosity:
                print(f"  Unapplying {app_label}.{name}...", end=" ")

            operations = list(reversed(getattr(module, "operations", [])))
            from_state = ProjectState()
            to_state = ProjectState()

            for op in operations:
                op.database_backwards(app_label, self.connection, from_state, to_state)

            self.recorder.record_unapplied(app_label, name)
            if self.verbosity:
                print("OK")

    def show_migrations(self, app_label: str, migrations_dir: str | Path):
        migrations_dir = Path(migrations_dir)
        self.loader.load(migrations_dir, app_label)
        self.loader.load_applied(self.recorder)

        print(f"{app_label}")
        for number, name, _ in self.loader.migrations.get(app_label, []):
            mark = "X" if (app_label, name) in self.loader.applied else " "
            print(f"  [{mark}] {name}")
