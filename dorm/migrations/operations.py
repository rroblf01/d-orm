from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class Operation:
    reversible = True

    def state_forwards(self, app_label: str, state):
        raise NotImplementedError

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        raise NotImplementedError

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        raise NotImplementedError

    def describe(self) -> str:
        return repr(self)


class CreateModel(Operation):
    def __init__(self, name: str, fields: list[tuple], options: dict | None = None):
        self.name = name
        self.fields = fields  # list of (field_name, field_instance)
        self.options = options or {}

    def state_forwards(self, app_label: str, state):
        state.models[f"{app_label}.{self.name.lower()}"] = {
            "name": self.name,
            "fields": dict(self.fields),
            "options": self.options,
        }

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        table = self.options.get("db_table") or f"{app_label}_{self.name.lower()}"
        col_defs = []
        for fname, field in self.fields:
            col_defs.append(_field_to_column_sql(fname, field, connection))
        sql = f'CREATE TABLE IF NOT EXISTS "{table}" (\n  {",  ".join(col_defs)}\n)'
        connection.execute_script(sql)

        # Declared indexes: emit ``CREATE INDEX`` per entry.
        for idx in self.options.get("indexes", []) or []:
            AddIndex(self.name, idx).database_forwards(
                app_label, connection, from_state, to_state
            )

        # Declared constraints (CheckConstraint / UniqueConstraint): emit
        # the appropriate DDL. The base column DDL already reflects
        # ``unique=True`` / single-field UNIQUE; this loop covers
        # composite and conditional constraints.
        for c in self.options.get("constraints", []) or []:
            connection.execute_script(c.constraint_sql(table, connection))

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        table = self.options.get("db_table") or f"{app_label}_{self.name.lower()}"
        connection.execute_script(f'DROP TABLE IF EXISTS "{table}"')

    def describe(self) -> str:
        return f"Create model {self.name}"

    def __repr__(self):
        return f"CreateModel(name={self.name!r}, fields={[n for n, _ in self.fields]!r})"


class DeleteModel(Operation):
    def __init__(self, name: str):
        self.name = name

    def state_forwards(self, app_label: str, state):
        state.models.pop(f"{app_label}.{self.name.lower()}", None)

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        from_model = from_state.models.get(f"{app_label}.{self.name.lower()}", {})
        table = from_model.get("options", {}).get("db_table") or f"{app_label}_{self.name.lower()}"
        connection.execute_script(f'DROP TABLE IF EXISTS "{table}"')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        to_model = to_state.models.get(f"{app_label}.{self.name.lower()}", {})
        if to_model:
            op = CreateModel(self.name, list(to_model["fields"].items()), to_model.get("options", {}))
            op.database_forwards(app_label, connection, from_state, to_state)

    def describe(self) -> str:
        return f"Delete model {self.name}"

    def __repr__(self):
        return f"DeleteModel(name={self.name!r})"


class AddField(Operation):
    def __init__(self, model_name: str, name: str, field, preserve_default: bool = True):
        self.model_name = model_name
        self.name = name
        self.field = field
        self.preserve_default = preserve_default

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            state.models[key]["fields"][self.name] = self.field

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        model_state = to_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
        col_sql = _field_to_column_sql(self.name, self.field, connection)
        connection.execute_script(f'ALTER TABLE "{table}" ADD COLUMN {col_sql}')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
        field = getattr(self.field, "column", None) or self.name
        connection.execute_script(f'ALTER TABLE "{table}" DROP COLUMN "{field}"')

    def describe(self) -> str:
        return f"Add field {self.name} to {self.model_name}"

    def __repr__(self):
        return f"AddField(model_name={self.model_name!r}, name={self.name!r})"


class RemoveField(Operation):
    def __init__(self, model_name: str, name: str):
        self.model_name = model_name
        self.name = name

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            state.models[key]["fields"].pop(self.name, None)

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
        connection.execute_script(f'ALTER TABLE "{table}" DROP COLUMN "{self.name}"')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        model_state = to_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
        field = model_state.get("fields", {}).get(self.name)
        if field:
            col_sql = _field_to_column_sql(self.name, field, connection)
            connection.execute_script(f'ALTER TABLE "{table}" ADD COLUMN {col_sql}')

    def describe(self) -> str:
        return f"Remove field {self.name} from {self.model_name}"

    def __repr__(self):
        return f"RemoveField(model_name={self.model_name!r}, name={self.name!r})"


class AlterField(Operation):
    def __init__(self, model_name: str, name: str, field):
        self.model_name = model_name
        self.name = name
        self.field = field

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            state.models[key]["fields"][self.name] = self.field

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        # SQLite doesn't support ALTER COLUMN; PostgreSQL does
        if getattr(connection, "vendor", "sqlite") == "postgresql":
            model_state = to_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
            table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
            col = getattr(self.field, "column", self.name)
            db_t = self.field.db_type(connection)
            connection.execute_script(
                f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE {db_t}'
            )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        pass

    def describe(self) -> str:
        return f"Alter field {self.name} on {self.model_name}"

    def __repr__(self):
        return f"AlterField(model_name={self.model_name!r}, name={self.name!r})"


class RenameField(Operation):
    def __init__(self, model_name: str, old_name: str, new_name: str):
        self.model_name = model_name
        self.old_name = old_name
        self.new_name = new_name

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            fields = state.models[key]["fields"]
            if self.old_name in fields:
                fields[self.new_name] = fields.pop(self.old_name)

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
        connection.execute_script(
            f'ALTER TABLE "{table}" RENAME COLUMN "{self.old_name}" TO "{self.new_name}"'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = model_state.get("options", {}).get("db_table") or f"{app_label}_{self.model_name.lower()}"
        connection.execute_script(
            f'ALTER TABLE "{table}" RENAME COLUMN "{self.new_name}" TO "{self.old_name}"'
        )

    def describe(self) -> str:
        return f"Rename field {self.old_name} to {self.new_name} on {self.model_name}"

    def __repr__(self):
        return f"RenameField(model_name={self.model_name!r}, old_name={self.old_name!r}, new_name={self.new_name!r})"


class RenameModel(Operation):
    def __init__(self, old_name: str, new_name: str):
        self.old_name = old_name
        self.new_name = new_name

    def state_forwards(self, app_label: str, state):
        old_key = f"{app_label}.{self.old_name.lower()}"
        new_key = f"{app_label}.{self.new_name.lower()}"
        if old_key in state.models:
            state.models[new_key] = state.models.pop(old_key)
            state.models[new_key]["name"] = self.new_name

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        old_table = f"{app_label}_{self.old_name.lower()}"
        new_table = f"{app_label}_{self.new_name.lower()}"
        connection.execute_script(f'ALTER TABLE "{old_table}" RENAME TO "{new_table}"')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        old_table = f"{app_label}_{self.old_name.lower()}"
        new_table = f"{app_label}_{self.new_name.lower()}"
        connection.execute_script(f'ALTER TABLE "{new_table}" RENAME TO "{old_table}"')

    def describe(self) -> str:
        return f"Rename model {self.old_name} to {self.new_name}"

    def __repr__(self):
        return f"RenameModel(old_name={self.old_name!r}, new_name={self.new_name!r})"


class AddIndex(Operation):
    """Create an index. Pass ``concurrently=True`` for online,
    non-blocking index creation on PostgreSQL (``CREATE INDEX
    CONCURRENTLY``) — the canonical zero-downtime pattern.

    ``CONCURRENTLY`` cannot run inside a transaction, so the executor
    refuses to apply the migration when the surrounding ``atomic()``
    block would wrap it. The migration must be the only operation in
    its file (the executor enforces this) so the per-migration atomic
    can be skipped without affecting others. SQLite ignores the flag.
    """

    def __init__(self, model_name: str, index, *, concurrently: bool = False) -> None:
        self.model_name = model_name
        self.index = index
        self.concurrently = bool(concurrently)

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            state.models[key].setdefault("options", {}).setdefault("indexes", []).append(self.index)

    def _create_sql(self, table: str, connection) -> str:
        vendor = getattr(connection, "vendor", "sqlite")
        if hasattr(self.index, "create_sql"):
            forward, _ = self.index.create_sql(table, vendor=vendor)
        else:
            # Legacy index objects without create_sql — best-effort.
            unique = "UNIQUE " if getattr(self.index, "unique", False) else ""
            cols = ", ".join(f'"{f}"' for f in self.index.fields)
            idx_name = self.index.get_name(self.model_name)
            forward = (
                f'CREATE {unique}INDEX IF NOT EXISTS "{idx_name}" '
                f'ON "{table}" ({cols})'
            )
        if self.concurrently and vendor == "postgresql":
            forward = forward.replace("CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1)
            forward = forward.replace(
                "CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX CONCURRENTLY", 1
            )
            # ``CONCURRENTLY`` is incompatible with ``IF NOT EXISTS`` on
            # older PG; on PG 9.5+ ``IF NOT EXISTS`` is fine. Keep both;
            # if a target install rejects, the migration error names the
            # offending statement and the user can drop the IF NOT EXISTS
            # by hand. (Defensive choice: silent strip would mask a
            # genuine "already exists" case.)
        return forward

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        model_state = to_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )
        connection.execute_script(self._create_sql(table, connection))

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        idx_name = self.index.get_name(self.model_name)
        # DROP INDEX CONCURRENTLY mirrors the forward path on PG.
        vendor = getattr(connection, "vendor", "sqlite")
        if self.concurrently and vendor == "postgresql":
            connection.execute_script(
                f'DROP INDEX CONCURRENTLY IF EXISTS "{idx_name}"'
            )
        else:
            connection.execute_script(f'DROP INDEX IF EXISTS "{idx_name}"')

    def describe(self) -> str:
        c = " CONCURRENTLY" if self.concurrently else ""
        return f"Add index{c} {self.index!r} to {self.model_name}"

    def __repr__(self) -> str:
        c = ", concurrently=True" if self.concurrently else ""
        return f"AddIndex(model_name={self.model_name!r}, index={self.index!r}{c})"


class RemoveIndex(Operation):
    def __init__(self, model_name: str, index, *, concurrently: bool = False) -> None:
        self.model_name = model_name
        self.index = index
        self.concurrently = bool(concurrently)

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            indexes = state.models[key].get("options", {}).get("indexes", [])
            idx_name = self.index.get_name(self.model_name)
            state.models[key]["options"]["indexes"] = [
                i for i in indexes if i.get_name(self.model_name) != idx_name
            ]

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        idx_name = self.index.get_name(self.model_name)
        vendor = getattr(connection, "vendor", "sqlite")
        if self.concurrently and vendor == "postgresql":
            connection.execute_script(
                f'DROP INDEX CONCURRENTLY IF EXISTS "{idx_name}"'
            )
        else:
            connection.execute_script(f'DROP INDEX IF EXISTS "{idx_name}"')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(
            f"{app_label}.{self.model_name.lower()}", {}
        )
        table = (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )
        if hasattr(self.index, "create_sql"):
            forward, _ = self.index.create_sql(
                table, vendor=getattr(connection, "vendor", "sqlite")
            )
        else:
            unique = "UNIQUE " if getattr(self.index, "unique", False) else ""
            cols = ", ".join(f'"{f}"' for f in self.index.fields)
            idx_name = self.index.get_name(self.model_name)
            forward = (
                f'CREATE {unique}INDEX IF NOT EXISTS "{idx_name}" '
                f'ON "{table}" ({cols})'
            )
        connection.execute_script(forward)

    def describe(self) -> str:
        return f"Remove index {self.index!r} from {self.model_name}"

    def __repr__(self) -> str:
        c = ", concurrently=True" if self.concurrently else ""
        return (
            f"RemoveIndex(model_name={self.model_name!r}, "
            f"index={self.index!r}{c})"
        )


class AddConstraint(Operation):
    """Add a :class:`~dorm.constraints.BaseConstraint` to a model.

    Emitted by the autodetector when a new entry appears in
    ``Meta.constraints``. The constraint's :meth:`constraint_sql`
    decides the exact DDL (``ALTER TABLE ... ADD CONSTRAINT`` for plain
    UNIQUE / CHECK; ``CREATE UNIQUE INDEX`` for partial unique
    constraints and for SQLite's UNIQUE).
    """

    def __init__(self, model_name: str, constraint) -> None:
        self.model_name = model_name
        self.constraint = constraint

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            constraints = state.models[key].setdefault("options", {}).setdefault(
                "constraints", []
            )
            constraints.append(self.constraint)

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        model_state = to_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )
        connection.execute_script(self.constraint.constraint_sql(table, connection))

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )
        connection.execute_script(self.constraint.remove_sql(table, connection))

    def describe(self) -> str:
        return f"Add constraint {self.constraint.describe()} to {self.model_name}"

    def __repr__(self) -> str:
        return (
            f"AddConstraint(model_name={self.model_name!r}, "
            f"constraint={self.constraint!r})"
        )


class RemoveConstraint(Operation):
    """Inverse of :class:`AddConstraint` — drops a named constraint."""

    def __init__(self, model_name: str, constraint) -> None:
        self.model_name = model_name
        self.constraint = constraint

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            constraints = state.models[key].get("options", {}).get("constraints", [])
            state.models[key].setdefault("options", {})["constraints"] = [
                c for c in constraints if getattr(c, "name", None) != self.constraint.name
            ]

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        model_state = from_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )
        connection.execute_script(self.constraint.remove_sql(table, connection))

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        model_state = to_state.models.get(f"{app_label}.{self.model_name.lower()}", {})
        table = (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )
        connection.execute_script(self.constraint.constraint_sql(table, connection))

    def describe(self) -> str:
        return f"Remove constraint {self.constraint.describe()} from {self.model_name}"

    def __repr__(self) -> str:
        return (
            f"RemoveConstraint(model_name={self.model_name!r}, "
            f"constraint={self.constraint!r})"
        )


class SetLockTimeout(Operation):
    """Set ``lock_timeout`` for the duration of the migration, then
    restore the previous value on the way out.

    PostgreSQL only. ``ms`` is the maximum time (milliseconds) any DDL
    in this migration will wait to acquire its lock before bailing out
    with ``LockNotAvailable``. Pair with ``RunSQL("ALTER TABLE ...")``
    when you need to add a NOT NULL or a FK on a hot table without
    risking an indefinite wait — the migration fails fast and you can
    retry off-peak.

    On SQLite this is a no-op (SQLite serialises writers via the
    file-level lock; there is no per-statement lock timeout).
    """

    reversible = True

    def __init__(self, ms: int) -> None:
        if not isinstance(ms, int) or ms < 0:
            raise ValueError("SetLockTimeout(ms=...) must be a non-negative integer.")
        self.ms = ms

    def state_forwards(self, app_label: str, state):
        # Schema is unchanged; the lock_timeout is purely runtime.
        return None

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            connection.execute_script(f"SET lock_timeout = '{int(self.ms)}ms'")

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            connection.execute_script("RESET lock_timeout")

    def describe(self) -> str:
        return f"Set lock_timeout = {self.ms}ms"

    def __repr__(self) -> str:
        return f"SetLockTimeout(ms={self.ms!r})"


class ValidateConstraint(Operation):
    """Run ``ALTER TABLE ... VALIDATE CONSTRAINT`` on PostgreSQL.

    Combine with ``RunSQL("ALTER TABLE ... ADD CONSTRAINT ... NOT VALID")``
    to add foreign keys / CHECK constraints to a billion-row table
    without an ``AccessExclusiveLock`` for the validation pass:

    .. code-block:: python

        operations = [
            RunSQL(
                "ALTER TABLE orders ADD CONSTRAINT fk_orders_user "
                "FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID"
            ),
            ValidateConstraint(table="orders", name="fk_orders_user"),
        ]

    The first statement takes a short ``ShareRowExclusive`` lock; the
    ``VALIDATE`` step takes only a ``ShareUpdateExclusive`` lock and
    runs concurrently with reads and writes. Total downtime: zero.

    SQLite has no separate validation step — this raises
    ``NotImplementedError`` so the migration can't be applied silently
    against the wrong backend.
    """

    reversible = False

    def __init__(self, *, table: str, name: str) -> None:
        from ..conf import _validate_identifier

        _validate_identifier(table, kind="table")
        _validate_identifier(name, kind="constraint name")
        self.table = table
        self.name = name

    def state_forwards(self, app_label: str, state):
        return None

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                "ValidateConstraint is PostgreSQL-only. SQLite validates "
                "constraints at insert / update time and has no equivalent."
            )
        connection.execute_script(
            f'ALTER TABLE "{self.table}" VALIDATE CONSTRAINT "{self.name}"'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        # Validation is one-way — there's no "unvalidate".
        return None

    def describe(self) -> str:
        return f"Validate constraint {self.name} on {self.table}"

    def __repr__(self) -> str:
        return f"ValidateConstraint(table={self.table!r}, name={self.name!r})"


class RunSQL(Operation):
    def __init__(self, sql: str, reverse_sql: str = "", params=None):
        self.sql = sql
        self.reverse_sql = reverse_sql
        self.params = params

    def state_forwards(self, app_label: str, state):
        pass

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        connection.execute_script(self.sql)

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        if self.reverse_sql:
            connection.execute_script(self.reverse_sql)

    def describe(self) -> str:
        return f"Run SQL: {self.sql[:60]}..."

    def __repr__(self):
        return f"RunSQL(sql={self.sql!r})"


class RunPython(Operation):
    """Run an arbitrary Python callable as a migration step.

    Both *code* and *reverse_code* are called with
    ``(app_label: str, registry: dict[str, type[Model]])``. The
    registry is the live model registry, keyed by class name (and by
    ``"app_label.ClassName"``) — use it to fetch the model classes
    rather than importing them, so the migration keeps working after
    a future model rename / move.

    Pass :attr:`RunPython.noop` as ``reverse_code`` when the forward
    step has no meaningful inverse (e.g. a one-shot data backfill that
    tolerates being undone by simply leaving the rows in place).
    """

    @staticmethod
    def noop(app_label: str, registry: dict) -> None:
        """A reusable no-op callable safe to pass as ``code=`` or
        ``reverse_code=``. Mirrors Django's ``migrations.RunPython.noop``
        so users porting code don't need to redefine it. The signature
        matches the contract :class:`RunPython` expects, so swapping it
        in for an undo-step won't TypeError at apply time.
        """
        return None

    def __init__(self, code, reverse_code=None, hints=None):
        self.code = code
        self.reverse_code = reverse_code
        self.hints = hints or {}

    def state_forwards(self, app_label: str, state):
        pass

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        from ..models import _model_registry  # noqa: PLC0415
        self.code(app_label, _model_registry)

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        if self.reverse_code:
            from ..models import _model_registry  # noqa: PLC0415
            self.reverse_code(app_label, _model_registry)

    def describe(self) -> str:
        return f"Run Python: {self.code.__name__ if callable(self.code) else 'function'}"

    def __repr__(self):
        return f"RunPython(code={self.code!r})"


# ── Helpers ────────────────────────────────────────────────────────────────────


def _field_to_column_sql(fname: str, field, connection) -> str:
    from ..fields import AutoField, BigAutoField, ForeignKey, OneToOneField, SmallAutoField

    # Field instances re-created from a migration file haven't gone through
    # ``contribute_to_class``, so ``field.column`` is None. Reproduce the
    # naming rules here:
    #   - FK / O2O: ``<name>_id`` (or db_column override)
    #   - everything else: ``<name>`` (or db_column override)
    col = getattr(field, "column", None)
    if not col:
        db_column = getattr(field, "db_column", None)
        if db_column:
            col = db_column
        elif isinstance(field, (ForeignKey, OneToOneField)):
            col = f"{fname}_id"
        else:
            col = fname

    db_t = field.db_type(connection)
    if db_t is None:
        return ""  # M2M field, skip

    parts = [f'"{col}" {db_t}']

    if field.primary_key:
        if isinstance(field, (AutoField, BigAutoField, SmallAutoField)):
            vendor = getattr(connection, "vendor", "sqlite")
            if vendor == "sqlite":
                parts = [f'"{col}" INTEGER PRIMARY KEY AUTOINCREMENT']
                return parts[0]
        parts.append("PRIMARY KEY")

    if not field.null and not field.primary_key:
        parts.append("NOT NULL")

    if field.unique and not field.primary_key:
        parts.append("UNIQUE")

    if field.has_default() and field.default is not None:
        from ..fields import NOT_PROVIDED
        if field.default is not NOT_PROVIDED and not callable(field.default):
            vendor = getattr(connection, "vendor", "sqlite")
            default_val = field.get_db_prep_value(field.default)
            if isinstance(field.default, bool):
                if vendor == "sqlite":
                    parts.append(f"DEFAULT {int(field.default)}")
                else:
                    parts.append("DEFAULT TRUE" if field.default else "DEFAULT FALSE")
            elif isinstance(default_val, str):
                parts.append(f"DEFAULT '{default_val}'")
            elif default_val is not None:
                parts.append(f"DEFAULT {default_val}")

    # FK reference
    from ..fields import ForeignKey, OneToOneField, PROTECT
    if isinstance(field, (ForeignKey, OneToOneField)):
        rel = field._resolve_related_model()
        ref_table = rel._meta.db_table
        ref_col = rel._meta.pk.column
        on_delete = getattr(field, "on_delete", "CASCADE")
        # PROTECT is Python-only; use RESTRICT at the DB level
        db_on_delete = "RESTRICT" if on_delete == PROTECT else on_delete
        parts.append(f'REFERENCES "{ref_table}"("{ref_col}") ON DELETE {db_on_delete}')

    return " ".join(parts)
