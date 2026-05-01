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


class _DryRunConnection:
    """Drop-in replacement for the real connection that *captures* SQL
    instead of executing it. Used by ``dorm migrate --dry-run`` so SREs
    can review the exact statements before unleashing them on prod."""

    def __init__(self, real_conn):
        self._real = real_conn
        self.captured: list[tuple[str, list]] = []
        # Mirror these attributes so migration operations that introspect
        # the wrapper continue to work.
        self.vendor = getattr(real_conn, "vendor", "sqlite")
        self.settings = getattr(real_conn, "settings", {})

    def _record(self, sql: str, params=None) -> None:
        self.captured.append((sql, list(params or [])))

    # ── Reads pass through to the real connection (we still need real
    #    table_exists / column lookups for autodetectors etc.) ──────────────
    def execute(self, sql, params=None):
        # Treat reads as pass-through; mutating SQL goes through the
        # capturing methods below.
        sql_upper = sql.lstrip().upper()
        if sql_upper.startswith(("SELECT", "WITH", "PRAGMA", "EXPLAIN")):
            return self._real.execute(sql, params)
        self._record(sql, params)
        return []

    def execute_script(self, sql):
        self._record(sql)

    def execute_write(self, sql, params=None) -> int:
        self._record(sql, params)
        return 0

    def execute_insert(self, sql, params=None, pk_col: str = "id"):
        self._record(sql, params)
        return None

    def execute_bulk_insert(self, sql, params=None, pk_col: str = "id", count: int = 1) -> list[int]:
        self._record(sql, params)
        return []

    def table_exists(self, name: str) -> bool:
        return self._real.table_exists(name)

    def get_table_columns(self, name: str) -> list[dict]:
        return self._real.get_table_columns(name)

    def atomic(self):
        return self._real.atomic()

    def __getattr__(self, name):
        # Anything we didn't override (e.g. `_atomic_conn`, `_get_pool`) goes
        # straight to the real wrapper.
        return getattr(self._real, name)


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

    def migrate(
        self,
        app_label: str,
        migrations_dir: str | Path,
        dry_run: bool = False,
    ) -> list[tuple[str, list]] | None:
        """Apply all pending migrations for *app_label*.

        When ``dry_run=True``, no SQL hits the database — instead the
        wrapper captures every statement an operation would have
        executed and returns the list. The migration recorder is **not**
        updated, so subsequent runs see the same set of pending
        migrations. Useful as a pre-deploy review step.
        """
        migrations_dir = Path(migrations_dir)
        with _migration_lock(self.connection):
            self.loader.load(migrations_dir, app_label)
            self.loader.load_applied(self.recorder)

            all_migs = self._sorted(app_label)
            # Auto-mark squashed migrations as applied when all their
            # replaces are done. Skip on dry-run — the docstring
            # promises the recorder is not touched, but
            # ``_sync_squashed`` historically wrote through the real
            # connection (the dry-run capture proxy is only swapped
            # in inside ``_apply_forward``), so it leaked recorder
            # writes when squashed migrations were in scope.
            if not dry_run:
                self._sync_squashed(app_label, all_migs)
            applied = self._applied_names(app_label)
            return self._apply_forward(
                app_label, all_migs, applied, dry_run=dry_run
            )

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
        dry_run: bool = False,
    ) -> list[tuple[str, list]] | None:
        unapplied = [
            (num, name, mod) for num, name, mod in all_migs
            if name not in applied and (target_num is None or num <= target_num)
        ]
        if not unapplied:
            if self.verbosity:
                print(f"  No migrations to apply for '{app_label}'.")
            return [] if dry_run else None

        from_state = self.loader.get_migration_state(app_label)
        # In dry-run mode, swap the executor's connection for a capturing
        # proxy. Pass-through reads still hit the real DB; writes are
        # recorded.
        original_conn = self.connection
        capture_conn = _DryRunConnection(self.connection) if dry_run else None
        if capture_conn is not None:
            self.connection = capture_conn

        try:
            for number, name, module in unapplied:
                if self.verbosity:
                    label = "Would apply" if dry_run else "Applying"
                    print(f"  {label} {app_label}.{name}...", end=" ")

                # Each migration runs inside a single transaction so that
                # a failure in op N rolls back ops 1..N-1. Without this,
                # a partial failure leaves the schema in a half-applied
                # state and the recorder out of sync — the next run would
                # try to re-apply ops 1..N-1 and crash on duplicate-table
                # / duplicate-column. Atomic-DDL is a hard requirement for
                # safe migrations.
                #
                # ``atomic_for_migration`` is a no-op on backends that
                # don't support transactional DDL (none right now — both
                # SQLite and PG do — but we keep the indirection so a
                # future backend can opt out). On dry-run we use the
                # capture proxy's pass-through atomic().
                with self.connection.atomic():
                    to_state = from_state.clone()
                    for op in getattr(module, "operations", []):
                        op.state_forwards(app_label, to_state)
                        op.database_forwards(app_label, self.connection, from_state, to_state)
                        from_state = to_state.clone()

                    if not dry_run:
                        self.recorder.record_applied(app_label, name)
                        for rep_app, rep_name in getattr(module, "replaces", []):
                            self.recorder.record_applied(rep_app, rep_name)
                if self.verbosity:
                    print("OK")
        finally:
            if capture_conn is not None:
                self.connection = original_conn

        if dry_run and capture_conn is not None:
            return capture_conn.captured
        return None

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

            # Same atomicity guarantee as forward apply: rollbacks must
            # not be partially committed. If op M of N fails in
            # ``database_backwards``, the surrounding transaction is rolled
            # back so the schema state matches the recorder.
            with self.connection.atomic():
                for op in reversed(getattr(module, "operations", [])):
                    op.database_backwards(app_label, self.connection, from_state, to_state)
                self.recorder.record_unapplied(app_label, name)
            if self.verbosity:
                print("OK")
