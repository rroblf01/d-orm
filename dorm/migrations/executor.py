from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path

from .loader import MigrationLoader
from .recorder import MigrationRecorder
from .state import ProjectState


# Constant cross-process lock ID for dorm migrations. The integer is small
# enough to fit in PostgreSQL's bigint advisory-lock space.
_DORM_MIGRATION_LOCK_ID = 0x646F726D  # "dorm" as ASCII hex

_log = logging.getLogger("dorm.migrations")


@contextmanager
def _migration_lock(connection):
    """Acquire a cross-process lock for the duration of a migration run.

    PostgreSQL: ``pg_advisory_lock`` is **session-scoped**, so it must be
    acquired and released on the same connection. We pin a single pool
    connection for the duration via ``connection.atomic()``; otherwise the
    pool can hand out a different connection for the unlock call (no-op on
    that session) and leak the lock until the original connection's PG
    session ends — blocking subsequent ``dorm migrate`` invocations forever.

    SQLite: SQLite already serializes writers at the file-lock level, so
    a second concurrent ``dorm migrate`` will block at the first write
    transaction. No explicit lock needed.
    """
    vendor = getattr(connection, "vendor", "sqlite")
    if vendor == "postgresql":
        _log.debug("Acquiring PG advisory lock %s", _DORM_MIGRATION_LOCK_ID)
        # atomic() pins a single pool connection for the whole block, so
        # both lock and unlock land on the same PG session.
        with connection.atomic():
            connection.execute(
                f"SELECT pg_advisory_lock({_DORM_MIGRATION_LOCK_ID})"
            )
            try:
                yield
            finally:
                try:
                    connection.execute(
                        f"SELECT pg_advisory_unlock({_DORM_MIGRATION_LOCK_ID})"
                    )
                # Lock release is best-effort; if the connection died, the
                # server drops the lock automatically.
                except Exception:
                    _log.warning(
                        "Could not release PG advisory lock %s; "
                        "it will be released when the connection drops.",
                        _DORM_MIGRATION_LOCK_ID,
                    )
    else:
        yield


class MigrationExecutor:
    """Applies and rolls back migrations."""

    def __init__(self, connection, verbosity: int = 1):
        self.connection = connection
        self.verbosity = verbosity
        self.recorder = MigrationRecorder(connection)
        self.loader = MigrationLoader(connection)

    # ── Public API ────────────────────────────────────────────────────────────

    def migrate(self, app_label: str, migrations_dir: str | Path) -> None:
        """Apply all pending migrations for *app_label*."""
        migrations_dir = Path(migrations_dir)
        with _migration_lock(self.connection):
            self.loader.load(migrations_dir, app_label)
            self.loader.load_applied(self.recorder)

            all_migs = self._sorted(app_label)
            # Auto-mark squashed migrations as applied when all their replaces are done
            self._sync_squashed(app_label, all_migs)
            applied = self._applied_names(app_label)
            self._apply_forward(app_label, all_migs, applied)

    def rollback(self, app_label: str, migrations_dir: str | Path, target: str) -> None:
        """Roll back applied migrations until *target* is the latest applied.

        *target* can be a full migration name (``"0002_add_email"``), a numeric
        prefix (``"0002"`` or ``2``), or ``"zero"`` to undo every migration.
        """
        migrations_dir = Path(migrations_dir)
        with _migration_lock(self.connection):
            self.loader.load(migrations_dir, app_label)
            self.loader.load_applied(self.recorder)

            all_migs = self._sorted(app_label)
            applied = self._applied_names(app_label)
            target_num = self._resolve_target_num(target, all_migs, app_label)
            self._rollback_to(app_label, all_migs, applied, target_num)

    def migrate_to(self, app_label: str, migrations_dir: str | Path, target: str) -> None:
        """Go to *target*, applying or rolling back migrations as needed.

        Determines direction automatically:

        * If *target* is at or after the current state → apply forward.
        * If *target* is before the current state → roll back.

        *target* accepts the same forms as :meth:`rollback`.
        """
        migrations_dir = Path(migrations_dir)
        with _migration_lock(self.connection):
            self.loader.load(migrations_dir, app_label)
            self.loader.load_applied(self.recorder)

            all_migs = self._sorted(app_label)
            applied = self._applied_names(app_label)
            target_num = self._resolve_target_num(target, all_migs, app_label)
            latest_applied = max(
                (num for num, name, _ in all_migs if name in applied), default=-1
            )

            if target_num >= latest_applied:
                self._apply_forward(app_label, all_migs, applied, target_num)
            else:
                self._rollback_to(app_label, all_migs, applied, target_num)

    def show_migrations(self, app_label: str, migrations_dir: str | Path) -> None:
        migrations_dir = Path(migrations_dir)
        self.loader.load(migrations_dir, app_label)
        self.loader.load_applied(self.recorder)

        print(f"{app_label}")
        for _number, name, _ in self.loader.migrations.get(app_label, []):
            mark = "X" if (app_label, name) in self.loader.applied else " "
            print(f"  [{mark}] {name}")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _sorted(self, app_label: str) -> list:
        return sorted(
            self.loader.migrations.get(app_label, []), key=lambda x: x[0]
        )

    def _applied_names(self, app_label: str) -> set[str]:
        return {name for app, name in self.loader.applied if app == app_label}

    def _sync_squashed(self, app_label: str, all_migs: list) -> None:
        """Mark a squashed migration as applied if all its replaces are applied."""
        applied = self.loader.applied
        for _num, name, module in all_migs:
            replaces = getattr(module, "replaces", [])
            if not replaces:
                continue
            if (app_label, name) in applied:
                continue
            if all((rep_app, rep_name) in applied for rep_app, rep_name in replaces):
                self.recorder.record_applied(app_label, name)
                self.loader.applied.add((app_label, name))

    @staticmethod
    def _resolve_target_num(target: str, all_migs: list, app_label: str) -> int:
        """Return the migration number for *target*, or -1 for ``"zero"``."""
        if target == "zero":
            return -1
        # Accept plain numbers: "1", "0001", etc.
        normalized = target.zfill(4) if target.isdigit() else target
        for num, name, _ in all_migs:
            if name == target or name.split("_")[0] == normalized:
                return num
        available = [name for _, name, _ in all_migs]
        raise ValueError(
            f"Migration '{target}' not found for app '{app_label}'. "
            f"Available migrations: {available}"
        )

    def _apply_forward(
        self,
        app_label: str,
        all_migs: list,
        applied: set[str],
        target_num: int | None = None,
    ) -> None:
        unapplied = [
            (num, name, mod) for num, name, mod in all_migs
            if name not in applied and (target_num is None or num <= target_num)
        ]
        if not unapplied:
            if self.verbosity:
                print(f"  No migrations to apply for '{app_label}'.")
            return

        from_state = self.loader.get_migration_state(app_label)
        for number, name, module in unapplied:
            if self.verbosity:
                print(f"  Applying {app_label}.{name}...", end=" ")

            to_state = from_state.clone()
            for op in getattr(module, "operations", []):
                op.state_forwards(app_label, to_state)
                op.database_forwards(app_label, self.connection, from_state, to_state)
                from_state = to_state.clone()

            self.recorder.record_applied(app_label, name)
            for rep_app, rep_name in getattr(module, "replaces", []):
                self.recorder.record_applied(rep_app, rep_name)
            if self.verbosity:
                print("OK")

    def _rollback_to(
        self,
        app_label: str,
        all_migs: list,
        applied: set[str],
        target_num: int,
    ) -> None:
        to_rollback = sorted(
            [
                (num, name, mod) for num, name, mod in all_migs
                if name in applied and num > target_num
            ],
            key=lambda x: x[0],
            reverse=True,  # newest first
        )
        if not to_rollback:
            if self.verbosity:
                print(f"  Nothing to rollback for '{app_label}'.")
            return

        for number, name, module in to_rollback:
            if self.verbosity:
                print(f"  Unapplying {app_label}.{name}...", end=" ")

            # to_state = state BEFORE this migration (where we return to)
            to_state = ProjectState()
            for n, _nm, mod in all_migs:
                if n >= number:
                    break
                for op in getattr(mod, "operations", []):
                    op.state_forwards(app_label, to_state)

            # from_state = state AFTER this migration (current DB state)
            from_state = to_state.clone()
            for op in getattr(module, "operations", []):
                op.state_forwards(app_label, from_state)

            for op in reversed(getattr(module, "operations", [])):
                op.database_backwards(app_label, self.connection, from_state, to_state)

            self.recorder.record_unapplied(app_label, name)
            if self.verbosity:
                print("OK")
