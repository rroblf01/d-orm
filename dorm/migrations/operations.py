from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
        composite_pk_cols: list[str] | None = None
        for fname, field in self.fields:
            # Composite PK declarations carry no column of their own —
            # they emit a separate ``PRIMARY KEY (col1, col2)``
            # constraint at the end of the column list. Capture them
            # here and skip the per-field DDL.
            from ..fields import CompositePrimaryKey

            if isinstance(field, CompositePrimaryKey):
                # Resolve component field names → columns by walking
                # the rest of the field list (the component fields
                # were already emitted before us).
                lookup = dict(self.fields)
                composite_pk_cols = []
                for component in field.field_names:
                    comp_field = lookup.get(component)
                    if comp_field is None:
                        raise ValueError(
                            f"CompositePrimaryKey references unknown field "
                            f"{component!r} on {self.name}."
                        )
                    composite_pk_cols.append(
                        getattr(comp_field, "column", None) or component
                    )
                continue
            sql_def = _field_to_column_sql(fname, field, connection)
            if sql_def:
                col_defs.append(sql_def)
        # Strip ``PRIMARY KEY`` from any single-column DDL when a
        # composite PK is declared — the table can't have two PKs.
        if composite_pk_cols:
            col_defs = [
                c.replace(" PRIMARY KEY", "")
                .replace(" AUTOINCREMENT", "")
                for c in col_defs
            ]
            constraint_cols = ", ".join(f'"{c}"' for c in composite_pk_cols)
            col_defs.append(f"PRIMARY KEY ({constraint_cols})")

        # SQLite has no ``ALTER TABLE ADD CONSTRAINT`` — CHECK clauses
        # must live inside CREATE TABLE. Split constraints into inline
        # (embedded below) and deferred (post-CREATE ALTER TABLE).
        vendor = getattr(connection, "vendor", "sqlite")
        deferred_constraints = []
        for c in self.options.get("constraints", []) or []:
            inline = getattr(c, "create_sql_inline", None)
            if vendor == "sqlite" and inline is not None:
                col_defs.append(inline(connection))
            else:
                deferred_constraints.append(c)

        sql = f'CREATE TABLE IF NOT EXISTS "{table}" (\n  {",  ".join(col_defs)}\n)'
        connection.execute_script(sql)

        # ``db_comment`` (3.2+) — column-level comment. PostgreSQL
        # uses ``COMMENT ON COLUMN``; MySQL has no separate
        # statement (the comment lands in the column DDL via
        # ``COMMENT '...'`` inline) — emit the PG form and skip
        # MySQL silently here, the inline form is added in
        # ``_field_to_column_sql``.
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            for fname, field in self.fields:
                comment = getattr(field, "db_comment", None)
                if not comment:
                    continue
                col = getattr(field, "column", None) or fname
                escaped = str(comment).replace("'", "''")
                connection.execute_script(
                    f'COMMENT ON COLUMN "{table}"."{col}" IS \'{escaped}\''
                )
            # ``Meta.db_table_comment`` (3.2+) — table-level
            # comment, same shape.
            table_comment = self.options.get("db_table_comment")
            if table_comment:
                escaped = str(table_comment).replace("'", "''")
                connection.execute_script(
                    f'COMMENT ON TABLE "{table}" IS \'{escaped}\''
                )

        # Declared indexes: emit ``CREATE INDEX`` per entry.
        for idx in self.options.get("indexes", []) or []:
            AddIndex(self.name, idx).database_forwards(
                app_label, connection, from_state, to_state
            )

        # Declared constraints not embedded above (UniqueConstraint on
        # PostgreSQL → ALTER TABLE ADD CONSTRAINT; partial unique →
        # CREATE UNIQUE INDEX, which works on SQLite too).
        for c in deferred_constraints:
            connection.execute_script(c.constraint_sql(table, connection))

        # Auto-emit junction tables for ``ManyToManyField`` declarations
        # that don't carry an explicit ``through`` model. Field instances
        # rebuilt from a migration file haven't gone through
        # ``contribute_to_class``, so ``field.model`` is unset — we
        # synthesise the junction here using the source model's table
        # plus the field name (mirroring ``ManyToManyField._get_through_table``
        # at runtime). Without this the M2M descriptor can be queried
        # but every read/write hits a missing junction table at runtime.
        for fname, field in self.fields:
            from ..fields import ManyToManyField
            if not isinstance(field, ManyToManyField) or field.through is not None:
                continue
            self._emit_m2m_junction(table, fname, field, connection)

    def _emit_m2m_junction(self, src_table: str, fname, field, connection) -> None:
        from ..models import _model_registry
        target = field.remote_field_to
        target_model = (
            _model_registry.get(target) if isinstance(target, str) else target
        )
        if target_model is None:
            # Pending forward reference — the autodetector orders
            # CreateModel ops by dependency so this should be rare,
            # but if the target hasn't been registered yet we can't
            # generate a referencing junction. Skip rather than crash;
            # ``dorm dbcheck`` will flag the missing junction.
            return
        target_table = target_model._meta.db_table
        target_pk_col = target_model._meta.pk.column

        junction = f"{src_table}_{fname}"
        src_col = f"{self.name.lower()}_id"
        tgt_col = f"{target_model.__name__.lower()}_id"

        vendor = getattr(connection, "vendor", "sqlite")
        pk_decl = (
            '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
            if vendor == "sqlite"
            else (
                '"id" BIGSERIAL PRIMARY KEY' if vendor == "postgresql"
                else '"id" BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY'
            )
        )
        sql = (
            f'CREATE TABLE IF NOT EXISTS "{junction}" (\n'
            f"  {pk_decl},\n"
            f'  "{src_col}" BIGINT NOT NULL '
            f'REFERENCES "{src_table}"("id") ON DELETE CASCADE,\n'
            f'  "{tgt_col}" BIGINT NOT NULL '
            f'REFERENCES "{target_table}"("{target_pk_col}") ON DELETE CASCADE,\n'
            f'  UNIQUE ("{src_col}", "{tgt_col}")\n'
            f")"
        )
        connection.execute_script(sql)

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        table = self.options.get("db_table") or f"{app_label}_{self.name.lower()}"
        # Drop M2M junctions before the parent table — FKs reference us.
        for fname, field in self.fields:
            from ..fields import ManyToManyField
            if not isinstance(field, ManyToManyField) or field.through is not None:
                continue
            junction = f"{table}_{fname}"
            connection.execute_script(f'DROP TABLE IF EXISTS "{junction}"')
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
        vendor = getattr(connection, "vendor", "sqlite")
        model_state = to_state.models.get(
            f"{app_label}.{self.model_name.lower()}", {}
        )
        table = model_state.get("options", {}).get("db_table") or (
            f"{app_label}_{self.model_name.lower()}"
        )
        col = getattr(self.field, "column", self.name) or self.name

        if vendor == "postgresql":
            # PG handles ALTER COLUMN natively. We re-emit type AND
            # nullability — single AlterField may flip either or
            # both, and the user expects both to land.
            db_t = self.field.db_type(connection)
            connection.execute_script(
                f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE {db_t}'
            )
            null_clause = "DROP NOT NULL" if self.field.null else "SET NOT NULL"
            connection.execute_script(
                f'ALTER TABLE "{table}" ALTER COLUMN "{col}" {null_clause}'
            )
            return

        # SQLite path — no real ALTER COLUMN support. Rebuild the
        # table per the canonical SQLite recipe:
        # https://www.sqlite.org/lang_altertable.html#otheralter
        #
        # The migration executor wraps every operation in
        # ``atomic()``. ``PRAGMA foreign_keys`` is silently a
        # no-op inside an open transaction, so we use
        # ``PRAGMA defer_foreign_keys=ON`` instead — it DOES work
        # inside a txn and tells SQLite to defer FK validation
        # until COMMIT. That gives us a transaction-safe window
        # in which to drop+rename the parent without invalidating
        # child references.
        #
        # 1. ``PRAGMA defer_foreign_keys=ON`` (per-txn).
        # 2. Create a new table with the up-to-date schema.
        # 3. Copy rows over (column list shared between old and
        #    new is the intersection of the two field sets).
        # 4. Drop the old table and rename the new one in place.
        # 5. Recreate every index AND every Meta.constraints
        #    (CheckConstraint / UniqueConstraint) that lived on
        #    the old table — both are stored in ``sqlite_schema``
        #    and would be lost otherwise.
        # 6. ``PRAGMA foreign_key_check`` to surface any
        #    references the rebuild left dangling. Raise so the
        #    surrounding atomic() rolls back rather than commits
        #    a corrupted schema.
        if vendor != "sqlite":
            return
        new_fields = model_state.get("fields", {})
        if not new_fields:
            return
        old_model_state = from_state.models.get(
            f"{app_label}.{self.model_name.lower()}", {}
        )
        old_fields = old_model_state.get("fields", {}) or new_fields

        # ``defer_foreign_keys`` is per-transaction and
        # auto-clears at COMMIT, so no cleanup needed.
        connection.execute_script("PRAGMA defer_foreign_keys=ON")

        tmp = f"_dorm_alter_{table}"
        # Drop any leftover from a previously-failed rebuild before
        # we start: ``CREATE TABLE`` would otherwise complain.
        connection.execute_script(f'DROP TABLE IF EXISTS "{tmp}"')

        # Build the CREATE TABLE for the new shape.
        cols_sql = []
        for fname, f in new_fields.items():
            sql = _field_to_column_sql(fname, f, connection)
            if sql:
                cols_sql.append(sql)
        # Embed CHECK clauses inline — SQLite ALTER TABLE ADD CONSTRAINT
        # is unsupported, so we cannot re-attach them afterwards.
        deferred_rebuild = []
        for c in model_state.get("options", {}).get("constraints", []) or []:
            inline = getattr(c, "create_sql_inline", None)
            if inline is not None:
                cols_sql.append(inline(connection))
            else:
                deferred_rebuild.append(c)
        connection.execute_script(
            f'CREATE TABLE "{tmp}" (\n  ' + ",\n  ".join(cols_sql) + "\n)"
        )

        # Copy intersection of column names. A new NOT NULL
        # column without a default would crash here; the migration
        # writer warns elsewhere on that pattern.
        common = []
        for fname, f in new_fields.items():
            if fname in old_fields and f.db_type(connection) is not None:
                col_name = (
                    getattr(f, "column", None)
                    or getattr(f, "db_column", None)
                    or fname
                )
                common.append(f'"{col_name}"')
        if common:
            col_list = ", ".join(common)
            connection.execute_script(
                f'INSERT INTO "{tmp}" ({col_list}) '
                f'SELECT {col_list} FROM "{table}"'
            )

        connection.execute_script(f'DROP TABLE "{table}"')
        connection.execute_script(
            f'ALTER TABLE "{tmp}" RENAME TO "{table}"'
        )

        # Re-create indexes declared on the model (Meta.indexes).
        for idx in model_state.get("options", {}).get("indexes", []) or []:
            forward, _ = idx.create_sql(table, vendor=vendor)
            connection.execute_script(forward)

        # Re-create constraints declared on the model
        # (Meta.constraints). CHECK clauses were embedded inline in
        # the tmp CREATE TABLE above — only deferred ones (e.g.
        # UniqueConstraint → CREATE UNIQUE INDEX) need re-emitting.
        for c in deferred_rebuild:
            try:
                forward = c.constraint_sql(table, connection)
            except Exception:
                forward = None
            if forward:
                connection.execute_script(forward)

        # Surface any references the rebuild left dangling.
        # ``PRAGMA foreign_key_check`` returns one row per
        # violation; raise if non-empty so the surrounding
        # ``atomic()`` block rolls back instead of committing a
        # broken schema.
        violations = list(connection.execute("PRAGMA foreign_key_check", []))
        if violations:
            raise RuntimeError(
                f"AlterField rebuild on '{table}' produced FK violations: "
                f"{violations!r}"
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
            if_not_exists = "" if vendor == "mysql" else "IF NOT EXISTS "
            forward = (
                f'CREATE {unique}INDEX {if_not_exists}"{idx_name}" '
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
            vendor = getattr(connection, "vendor", "sqlite")
            if_not_exists = "" if vendor == "mysql" else "IF NOT EXISTS "
            forward = (
                f'CREATE {unique}INDEX {if_not_exists}"{idx_name}" '
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


class SeparateDatabaseAndState(Operation):
    """Apply a parallel pair of operations: one updates the
    :class:`ProjectState` (the migration graph's idea of the
    schema), the other runs the actual DDL. Useful when the
    autodetector's understanding of the schema diverges from the
    real database (post-manual edit, post-vendor-specific
    optimisation, post-data-migration that touched DDL outside
    the migration graph).

    Mirrors Django's ``django.db.migrations.operations.SeparateDatabaseAndState``.

    Example::

        SeparateDatabaseAndState(
            database_operations=[],  # already in DB; no DDL to run
            state_operations=[AddField(...)],  # but the graph needs updating
        )
    """

    def __init__(
        self,
        database_operations: list | None = None,
        state_operations: list | None = None,
    ) -> None:
        self.database_operations = list(database_operations or [])
        self.state_operations = list(state_operations or [])

    def state_forwards(self, app_label: str, state):
        for op in self.state_operations:
            op.state_forwards(app_label, state)

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        for op in self.database_operations:
            op.database_forwards(app_label, connection, from_state, to_state)

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        for op in self.database_operations:
            if hasattr(op, "database_backwards"):
                op.database_backwards(app_label, connection, from_state, to_state)

    def describe(self) -> str:
        n_db = len(self.database_operations)
        n_state = len(self.state_operations)
        return f"Custom state/database split ({n_db} DB, {n_state} state)"

    def __repr__(self):
        return (
            f"SeparateDatabaseAndState("
            f"database_operations={self.database_operations!r}, "
            f"state_operations={self.state_operations!r})"
        )


class AlterModelOptions(Operation):
    """Update :class:`Meta` options that don't require DDL —
    ``ordering``, ``verbose_name``, ``permissions``,
    ``default_manager_name``, ``base_manager_name``. The
    autodetector emits this when only the ``options`` dict
    differs between two states.

    No-op at the database level — only the in-memory project state
    moves.
    """

    def __init__(self, name: str, options: dict | None = None) -> None:
        self.name = name
        self.options = options or {}

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.name.lower()}"
        model = state.models.get(key)
        if model is not None:
            opts = dict(model.get("options") or {})
            opts.update(self.options)
            model["options"] = opts

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        # No DDL — Meta options live in Python only.
        pass

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        pass

    def describe(self) -> str:
        return f"Alter {self.name} options"


class AlterModelTable(Operation):
    """Rename the underlying ``db_table`` for a model. Maps to
    ``ALTER TABLE old RENAME TO new`` on every supported backend.
    """

    def __init__(self, name: str, table: str) -> None:
        self.name = name
        self.table = table

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.name.lower()}"
        model = state.models.get(key)
        if model is not None:
            opts = dict(model.get("options") or {})
            opts["db_table"] = self.table
            model["options"] = opts

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        from_model = from_state.models.get(f"{app_label}.{self.name.lower()}", {})
        old_table = from_model.get("options", {}).get("db_table") or (
            f"{app_label}_{self.name.lower()}"
        )
        if old_table == self.table:
            return
        connection.execute_script(
            f'ALTER TABLE "{old_table}" RENAME TO "{self.table}"'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        # Reverse: rename ``self.table`` back to whatever the
        # pre-forward state called it. ``to_state`` is the state
        # we're moving TO during backwards (i.e. before the
        # forward migration ran), so the old table name lives there.
        # Reading ``from_state`` would just return the current
        # post-forward name and produce a no-op.
        target_model = to_state.models.get(f"{app_label}.{self.name.lower()}", {})
        old_table = target_model.get("options", {}).get("db_table") or (
            f"{app_label}_{self.name.lower()}"
        )
        if old_table == self.table:
            return
        connection.execute_script(
            f'ALTER TABLE "{self.table}" RENAME TO "{old_table}"'
        )

    def describe(self) -> str:
        return f"Rename table for {self.name} to {self.table}"


class AlterModelManagers(Operation):
    """Track ``Meta.managers`` changes. Manager objects exist only
    in Python — no DDL — so this op is a state-only no-op that
    the autodetector emits to keep the migration graph honest."""

    def __init__(self, name: str, managers: list | None = None) -> None:
        self.name = name
        self.managers = list(managers or [])

    def state_forwards(self, app_label: str, state):
        key = f"{app_label}.{self.name.lower()}"
        model = state.models.get(key)
        if model is not None:
            opts = dict(model.get("options") or {})
            opts["managers"] = self.managers
            model["options"] = opts

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        pass

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        pass

    def describe(self) -> str:
        return f"Alter {self.name} managers"


# ── Materialised views ─────────────────────────────────────────────────────────


class CreateMaterializedView(Operation):
    """Create a PostgreSQL materialised view as a migration step.

    Materialised views cache a query's result set as a physical
    relation. ``REFRESH MATERIALIZED VIEW`` re-runs the underlying
    SELECT and replaces the cached rows. Unlike a regular view, the
    cached data survives connection drops and can be indexed — making
    them the canonical PostgreSQL answer to denormalised reporting
    aggregates.

    Example::

        CreateMaterializedView(
            "active_authors",
            'SELECT id, name FROM "authors" WHERE is_active = true',
        )

    Use :class:`RefreshMaterializedView` to re-run the query, and
    :class:`DropMaterializedView` to remove the view. The reverse
    operation drops the view automatically.

    PostgreSQL-only — other vendors raise NotImplementedError at
    apply time. SQLite has no materialised view support; MySQL has
    ``CREATE TABLE ... AS SELECT`` but no automatic refresh primitive.
    """

    reversible = True

    def __init__(
        self,
        name: str,
        sql: str,
        *,
        with_data: bool = True,
        if_not_exists: bool = False,
    ) -> None:
        self.name = name
        self.sql = sql
        self.with_data = with_data
        self.if_not_exists = if_not_exists

    def state_forwards(self, app_label: str, state):
        # Materialised views live outside the model graph — there's no
        # state to mutate. The state-forwards method exists so the
        # operation slots cleanly into ``executor.apply``.
        pass

    def _ensure_pg(self, connection) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"CreateMaterializedView: not supported on {vendor!r}. "
                "Materialised views are PostgreSQL-only."
            )

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        ifne = " IF NOT EXISTS" if self.if_not_exists else ""
        data = " WITH NO DATA" if not self.with_data else ""
        connection.execute_script(
            f'CREATE MATERIALIZED VIEW{ifne} "{self.name}" AS {self.sql}{data}'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(f'DROP MATERIALIZED VIEW IF EXISTS "{self.name}"')

    def describe(self) -> str:
        return f"Create materialized view {self.name}"

    def __repr__(self) -> str:
        return f"CreateMaterializedView(name={self.name!r}, sql={self.sql!r})"


class DropMaterializedView(Operation):
    """Drop a PostgreSQL materialised view. Reverse direction recreates
    it from *sql*; pass ``reverse_sql=""`` to make it irreversible."""

    def __init__(
        self,
        name: str,
        *,
        reverse_sql: str = "",
        if_exists: bool = True,
    ) -> None:
        self.name = name
        self.reverse_sql = reverse_sql
        self.if_exists = if_exists
        self.reversible = bool(reverse_sql)

    def state_forwards(self, app_label: str, state):
        pass

    def _ensure_pg(self, connection) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"DropMaterializedView: not supported on {vendor!r}."
            )

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        ifex = " IF EXISTS" if self.if_exists else ""
        connection.execute_script(f'DROP MATERIALIZED VIEW{ifex} "{self.name}"')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        if not self.reverse_sql:
            raise NotImplementedError(
                f"DropMaterializedView({self.name!r}) is irreversible: "
                "no reverse_sql was provided."
            )
        connection.execute_script(
            f'CREATE MATERIALIZED VIEW "{self.name}" AS {self.reverse_sql}'
        )

    def describe(self) -> str:
        return f"Drop materialized view {self.name}"


class RefreshMaterializedView(Operation):
    """Issue ``REFRESH MATERIALIZED VIEW`` against an existing view.

    Pass ``concurrently=True`` to use ``REFRESH MATERIALIZED VIEW
    CONCURRENTLY`` — non-blocking refresh that lets readers keep
    using the stale data while the new snapshot builds. Requires a
    unique index on the view (PostgreSQL constraint, not ours).

    The reverse direction is a no-op — refreshes don't have an
    inverse, and rolling back a refresh would be meaningless.
    """

    reversible = True

    def __init__(self, name: str, *, concurrently: bool = False) -> None:
        self.name = name
        self.concurrently = concurrently

    def state_forwards(self, app_label: str, state):
        pass

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"RefreshMaterializedView: not supported on {vendor!r}."
            )
        conc = " CONCURRENTLY" if self.concurrently else ""
        connection.execute_script(f'REFRESH MATERIALIZED VIEW{conc} "{self.name}"')

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        # Refresh has no inverse; skipping is the right behaviour on
        # rollback. The state isn't actually wrong because a later
        # forward will refresh again anyway.
        pass

    def describe(self) -> str:
        return f"Refresh materialized view {self.name}"


# ── PostgreSQL native partitioning ─────────────────────────────────────────────


class CreatePartitionedTable(Operation):
    """Create a PostgreSQL partitioned parent table.

    Partitioning splits a logical table into physical sub-tables
    keyed by a value (``RANGE``), discrete enum (``LIST``), or hash
    bucket (``HASH``). Queries hit the parent table; PG routes them
    to the right partition automatically.

    Example::

        CreatePartitionedTable(
            "events",
            columns_sql='id BIGSERIAL, occurred_at TIMESTAMP NOT NULL, payload JSONB',
            method="RANGE",
            key="occurred_at",
        )
        AttachPartition(
            parent="events",
            child="events_2025_q1",
            for_values_in="FROM ('2025-01-01') TO ('2025-04-01')",
        )

    For an end-to-end example see ``docs/partitioning.es.md``. PG-only.
    """

    reversible = True

    def __init__(
        self,
        name: str,
        *,
        columns_sql: str,
        method: str,
        key: str,
        if_not_exists: bool = False,
    ) -> None:
        method_u = method.upper()
        if method_u not in ("RANGE", "LIST", "HASH"):
            raise ValueError(
                "CreatePartitionedTable.method must be one of "
                f"'RANGE'/'LIST'/'HASH'; got {method!r}."
            )
        self.name = name
        self.columns_sql = columns_sql
        self.method = method_u
        self.key = key
        self.if_not_exists = if_not_exists

    def state_forwards(self, app_label: str, state):
        pass

    def _ensure_pg(self, connection) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"CreatePartitionedTable: not supported on {vendor!r}. "
                "Native declarative partitioning is PostgreSQL-only."
            )

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        ifne = "IF NOT EXISTS " if self.if_not_exists else ""
        connection.execute_script(
            f'CREATE TABLE {ifne}"{self.name}" ({self.columns_sql}) '
            f'PARTITION BY {self.method} ("{self.key}")'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(f'DROP TABLE IF EXISTS "{self.name}"')

    def describe(self) -> str:
        return f"Create partitioned table {self.name} BY {self.method}({self.key})"


class CreatePartition(Operation):
    """Create a partition table and attach it to a parent in one step.

    *for_values* is the partition bound expression that follows
    ``FOR VALUES`` in the DDL. Examples by method:

    - ``RANGE``: ``"FROM ('2025-01-01') TO ('2025-04-01')"``
    - ``LIST``:  ``"IN ('eu-west-1', 'eu-central-1')"``
    - ``HASH``:  ``"WITH (MODULUS 4, REMAINDER 0)"``

    The partition's column set is inherited from the parent — this op
    only emits the wrapping DDL. Indexes/constraints on the parent
    cascade automatically (PG ≥ 11).
    """

    reversible = True

    def __init__(
        self,
        parent: str,
        name: str,
        *,
        for_values: str,
        if_not_exists: bool = False,
    ) -> None:
        self.parent = parent
        self.name = name
        self.for_values = for_values
        self.if_not_exists = if_not_exists

    def state_forwards(self, app_label: str, state):
        pass

    def _ensure_pg(self, connection) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"CreatePartition: not supported on {vendor!r}."
            )

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        ifne = "IF NOT EXISTS " if self.if_not_exists else ""
        connection.execute_script(
            f'CREATE TABLE {ifne}"{self.name}" '
            f'PARTITION OF "{self.parent}" FOR VALUES {self.for_values}'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(f'DROP TABLE IF EXISTS "{self.name}"')

    def describe(self) -> str:
        return f"Create partition {self.name} of {self.parent}"


class AttachPartition(Operation):
    """Attach an existing standalone table as a partition of *parent*.

    Useful when migrating an unpartitioned table to a partitioned
    layout: build the new parent, copy data into a child table, then
    ``ATTACH PARTITION``. The reverse direction detaches.
    """

    reversible = True

    def __init__(self, parent: str, name: str, *, for_values: str) -> None:
        self.parent = parent
        self.name = name
        self.for_values = for_values

    def state_forwards(self, app_label: str, state):
        pass

    def _ensure_pg(self, connection) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"AttachPartition: not supported on {vendor!r}."
            )

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(
            f'ALTER TABLE "{self.parent}" ATTACH PARTITION "{self.name}" '
            f'FOR VALUES {self.for_values}'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(
            f'ALTER TABLE "{self.parent}" DETACH PARTITION "{self.name}"'
        )

    def describe(self) -> str:
        return f"Attach partition {self.name} to {self.parent}"


class DetachPartition(Operation):
    """Detach a partition from its parent without dropping the rows.
    Reverse direction re-attaches with the same *for_values* clause."""

    reversible = True

    def __init__(self, parent: str, name: str, *, for_values: str) -> None:
        self.parent = parent
        self.name = name
        self.for_values = for_values

    def state_forwards(self, app_label: str, state):
        pass

    def _ensure_pg(self, connection) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor != "postgresql":
            raise NotImplementedError(
                f"DetachPartition: not supported on {vendor!r}."
            )

    def database_forwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(
            f'ALTER TABLE "{self.parent}" DETACH PARTITION "{self.name}"'
        )

    def database_backwards(self, app_label: str, connection, from_state, to_state):
        self._ensure_pg(connection)
        connection.execute_script(
            f'ALTER TABLE "{self.parent}" ATTACH PARTITION "{self.name}" '
            f'FOR VALUES {self.for_values}'
        )

    def describe(self) -> str:
        return f"Detach partition {self.name} from {self.parent}"


# ── Zero-downtime / online operations ──────────────────────────────────────────


class AddFieldOnline(Operation):
    """Zero-downtime ``ADD COLUMN`` on PostgreSQL.

    The standard :class:`AddField` operation can rewrite the entire
    table when the new column is ``NOT NULL`` with a non-volatile
    default, which on a billion-row table means hours of downtime.
    This variant splits the work into three atomic steps that each
    finish in milliseconds and never hold a long lock:

    1. ``ALTER TABLE ... ADD COLUMN <name> <type> NULL``
    2. (optional) caller-driven backfill of the new column in
       chunks — typically driven by :class:`BackfillBatch` in a
       follow-up migration.
    3. ``ALTER TABLE ... ALTER COLUMN <name> SET NOT NULL`` once the
       backfill is complete and verified, plus optional
       ``ADD CHECK (col IS NOT NULL) NOT VALID`` followed by
       ``VALIDATE CONSTRAINT`` — both metadata-only locks on
       PG ≥ 12.

    Use ``set_not_null_now=False`` (default) to leave step 3 to a
    later migration; the field stays nullable after the operation
    even if the model declares it ``null=False``. The state graph
    is updated to match the model's eventual shape regardless, so
    the autodetector's idea of the schema stays correct.

    On non-PG backends the op falls back to the standard
    :class:`AddField` behaviour — there is no portable equivalent
    of "concurrent" DDL.
    """

    reversible = True

    def __init__(
        self,
        model_name: str,
        name: str,
        field: Any,
        *,
        set_not_null_now: bool = False,
        preserve_default: bool = True,
    ) -> None:
        self.model_name = model_name
        self.name = name
        self.field = field
        self.set_not_null_now = set_not_null_now
        self.preserve_default = preserve_default

    def state_forwards(self, app_label: str, state) -> None:
        key = f"{app_label}.{self.model_name.lower()}"
        if key in state.models:
            state.models[key]["fields"][self.name] = self.field

    def _resolve_table(self, app_label: str, state) -> str:
        model_state = state.models.get(
            f"{app_label}.{self.model_name.lower()}", {}
        )
        return (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        import copy as _copy

        vendor = getattr(connection, "vendor", "sqlite")
        table = self._resolve_table(app_label, to_state)

        # Step 1: add the column nullable, regardless of the model's
        # eventual ``null=`` value. Forces no rewrite on PG and is
        # the only form SQLite accepts at all (``ALTER TABLE ... ADD
        # COLUMN NOT NULL`` is rejected without a DEFAULT).
        #
        # Work on a shallow copy of the field so concurrent
        # migrations against different aliases (typical multi-tenant
        # rollout) don't race on the shared field instance's ``null``
        # attribute. Pre-fix the op temporarily flipped
        # ``self.field.null`` to ``True``; under parallel rollouts
        # the second call could observe the first's transient state.
        field_copy = _copy.copy(self.field)
        original_null = getattr(self.field, "null", False)
        field_copy.null = True
        col_sql = _field_to_column_sql(self.name, field_copy, connection)
        connection.execute_script(
            f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'
        )

        if vendor != "postgresql":
            # Step 3 is PG-only — other backends handle the
            # eventual NOT NULL via SetNotNullOnline (which falls
            # back to a plain ALTER) or stay nullable.
            return

        # Step 3 (optional immediate): only run when caller asks for
        # it AND the eventual model declared NOT NULL.
        if self.set_not_null_now and not original_null:
            column = getattr(self.field, "column", None) or self.name
            connection.execute_script(
                f'ALTER TABLE "{table}" '
                f'ALTER COLUMN "{column}" SET NOT NULL'
            )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        table = self._resolve_table(app_label, from_state)
        column = getattr(self.field, "column", None) or self.name
        connection.execute_script(
            f'ALTER TABLE "{table}" DROP COLUMN "{column}"'
        )

    def describe(self) -> str:
        return f"Add field {self.name} to {self.model_name} (online, nullable first)"

    def __repr__(self) -> str:
        return (
            f"AddFieldOnline(model_name={self.model_name!r}, "
            f"name={self.name!r}, set_not_null_now={self.set_not_null_now!r})"
        )


class BackfillBatch(Operation):
    """Run a column backfill in fixed-size batches inside short
    transactions, advancing by primary key. Intended as the second
    step of the zero-downtime ``AddFieldOnline`` recipe.

    Usage::

        BackfillBatch(
            table="orders",
            update_sql='UPDATE "orders" SET "status_v2" = "status" '
                       'WHERE "id" BETWEEN %s AND %s AND "status_v2" IS NULL',
            pk_column="id",
            batch_size=10_000,
        )

    Each iteration claims one batch via the PK range, runs the SQL,
    commits, and advances. Lock duration is bounded by the batch
    size, not the table size — safe to run against a primary
    serving live traffic.
    """

    reversible = True

    def __init__(
        self,
        table: str,
        *,
        update_sql: str,
        pk_column: str = "id",
        batch_size: int = 10_000,
        sleep_seconds: float = 0.0,
        max_batches: int | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if sleep_seconds < 0:
            raise ValueError("sleep_seconds must be >= 0")
        self.table = table
        self.update_sql = update_sql
        self.pk_column = pk_column
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.max_batches = max_batches

    def state_forwards(self, app_label: str, state) -> None:
        # Pure data migration — no state mutation.
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        import time as _time

        rows = connection.execute(
            f'SELECT MIN("{self.pk_column}") AS lo, MAX("{self.pk_column}") AS hi '
            f'FROM "{self.table}"'
        )
        if not rows:
            return
        row = rows[0]
        lo = row.get("lo") if isinstance(row, dict) else row[0]
        hi = row.get("hi") if isinstance(row, dict) else row[1]
        if lo is None or hi is None:
            return

        batches = 0
        start = int(lo)
        end_pk = int(hi)
        while start <= end_pk:
            stop = start + self.batch_size - 1
            connection.execute_write(self.update_sql, [start, stop])
            try:
                connection.commit()
            except Exception:
                pass
            start = stop + 1
            batches += 1
            if self.max_batches is not None and batches >= self.max_batches:
                break
            if self.sleep_seconds > 0:
                _time.sleep(self.sleep_seconds)

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        # Backfills don't have a generic inverse — the caller is
        # responsible for issuing a compensating ``RunSQL`` in the
        # reverse direction if one exists.
        pass

    def describe(self) -> str:
        return (
            f"Backfill {self.table} in batches of {self.batch_size} "
            f"by {self.pk_column}"
        )

    def __repr__(self) -> str:
        return (
            f"BackfillBatch(table={self.table!r}, batch_size={self.batch_size})"
        )


class SetNotNullOnline(Operation):
    """Promote a previously-nullable column to ``NOT NULL`` without a
    full table rewrite.

    On PostgreSQL ≥ 12 the recipe is::

        ALTER TABLE t ADD CONSTRAINT chk CHECK (col IS NOT NULL) NOT VALID;
        ALTER TABLE t VALIDATE CONSTRAINT chk;
        ALTER TABLE t ALTER COLUMN col SET NOT NULL;
        ALTER TABLE t DROP CONSTRAINT chk;

    The CHECK constraint without ``VALID`` adopts in O(1) — no scan;
    ``VALIDATE CONSTRAINT`` does the row-by-row check but holds only
    a ``SHARE UPDATE EXCLUSIVE`` lock (won't block readers/writers).
    The final ``SET NOT NULL`` is metadata-only because PG ≥ 12
    consults the validated CHECK and skips the rewrite.

    On non-PG backends, falls back to a plain
    ``ALTER COLUMN SET NOT NULL`` which IS a rewrite. Document the
    expected impact when targeting MySQL/SQLite.
    """

    reversible = True

    def __init__(
        self,
        model_name: str,
        column: str,
    ) -> None:
        self.model_name = model_name
        self.column = column

    def state_forwards(self, app_label: str, state) -> None:
        # The state already reflects ``null=False``; this op only
        # synchronises the database side.
        pass

    def _resolve_table(self, app_label: str, state) -> str:
        model_state = state.models.get(
            f"{app_label}.{self.model_name.lower()}", {}
        )
        return (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        table = self._resolve_table(app_label, to_state)
        constraint = f"chk_{table}_{self.column}_notnull"

        if vendor not in ("postgresql", "mysql"):
            # SQLite / DuckDB / libSQL do not support ALTER COLUMN; NOT NULL
            # can only be set by recreating the table, which is not safe
            # online.  Silently skip — the column was already backfilled
            # by BackfillBatch.
            return

        if vendor != "postgresql":
            connection.execute_script(
                f'ALTER TABLE "{table}" '
                f'ALTER COLUMN "{self.column}" SET NOT NULL'
            )
            return

        connection.execute_script(
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint}" '
            f'CHECK ("{self.column}" IS NOT NULL) NOT VALID'
        )
        connection.execute_script(
            f'ALTER TABLE "{table}" VALIDATE CONSTRAINT "{constraint}"'
        )
        connection.execute_script(
            f'ALTER TABLE "{table}" '
            f'ALTER COLUMN "{self.column}" SET NOT NULL'
        )
        connection.execute_script(
            f'ALTER TABLE "{table}" DROP CONSTRAINT "{constraint}"'
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor not in ("postgresql", "mysql"):
            return
        table = self._resolve_table(app_label, from_state)
        connection.execute_script(
            f'ALTER TABLE "{table}" '
            f'ALTER COLUMN "{self.column}" DROP NOT NULL'
        )

    def describe(self) -> str:
        return f"SET NOT NULL on {self.model_name}.{self.column} (online)"

    def __repr__(self) -> str:
        return f"SetNotNullOnline({self.model_name!r}, {self.column!r})"


class MakeTableAppendOnly(Operation):
    """Install a trigger that blocks ``UPDATE`` and ``DELETE`` on
    *table*, turning it into an append-only audit log.

    On PostgreSQL the recipe is::

        CREATE FUNCTION <table>_block_mod() RETURNS TRIGGER ...;
        CREATE TRIGGER <table>_block_mod_trg
            BEFORE UPDATE OR DELETE ON <table>
            FOR EACH ROW EXECUTE FUNCTION <table>_block_mod();

    The trigger raises an exception with SQLSTATE ``P0001`` when a
    ``UPDATE`` / ``DELETE`` reaches it, propagating as a dorm
    ``IntegrityError`` at the application layer. Use on
    ``@track_history`` sibling tables and any other immutable audit
    log.

    On SQLite a comparable trigger is supported (``RAISE(ABORT, ...)``).
    On MySQL / DuckDB the operation is a no-op with a warning logged
    at migration time — no portable append-only trigger.

    Args:
        table: physical table name to protect.
        allow_delete: when True, only block UPDATE (still allow DELETE
            — useful for log-retention policies).
    """

    reversible = True

    def __init__(self, table: str, *, allow_delete: bool = False) -> None:
        self.table = table
        self.allow_delete = allow_delete

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def _names(self) -> tuple[str, str]:
        return (
            f"{self.table}_block_mod",
            f"{self.table}_block_mod_trg",
        )

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        fn_name, trg_name = self._names()
        events = "UPDATE" if self.allow_delete else "UPDATE OR DELETE"
        if vendor == "postgresql":
            connection.execute_script(
                f"""CREATE OR REPLACE FUNCTION "{fn_name}"() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'table "{self.table}" is append-only — TG_OP=% blocked', TG_OP
        USING ERRCODE = 'P0001';
END;
$$ LANGUAGE plpgsql"""
            )
            connection.execute_script(
                f'DROP TRIGGER IF EXISTS "{trg_name}" ON "{self.table}"'
            )
            connection.execute_script(
                f'CREATE TRIGGER "{trg_name}" '
                f'BEFORE {events} ON "{self.table}" '
                f'FOR EACH ROW EXECUTE FUNCTION "{fn_name}"()'
            )
            return
        if vendor in ("sqlite", "libsql"):
            triggers = [("update", "UPDATE")]
            if not self.allow_delete:
                triggers.append(("delete", "DELETE"))
            for suffix, event in triggers:
                tname = f"{trg_name}_{suffix}"
                connection.execute_script(f'DROP TRIGGER IF EXISTS "{tname}"')
                connection.execute_script(
                    f'CREATE TRIGGER "{tname}" '
                    f'BEFORE {event} ON "{self.table}" '
                    f"BEGIN "
                    f"SELECT RAISE(ABORT, 'table {self.table} is append-only'); "
                    f"END"
                )
            return
        import logging
        logging.getLogger("dorm.migrations").warning(
            "MakeTableAppendOnly: vendor %r has no portable trigger — "
            "skipping. Use a database-level check or application-level "
            "enforcement.",
            vendor,
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        vendor = getattr(connection, "vendor", "sqlite")
        fn_name, trg_name = self._names()
        if vendor == "postgresql":
            connection.execute_script(
                f'DROP TRIGGER IF EXISTS "{trg_name}" ON "{self.table}"'
            )
            connection.execute_script(
                f'DROP FUNCTION IF EXISTS "{fn_name}"()'
            )
            return
        if vendor in ("sqlite", "libsql"):
            for suffix in ("update", "delete"):
                connection.execute_script(
                    f'DROP TRIGGER IF EXISTS "{trg_name}_{suffix}"'
                )
            return

    def describe(self) -> str:
        return f"Make {self.table} append-only"

    def __repr__(self) -> str:
        return f"MakeTableAppendOnly({self.table!r}, allow_delete={self.allow_delete!r})"


class AlterColumnTypeOnline(Operation):
    """``ALTER COLUMN ... TYPE`` with a bounded lock window.

    PostgreSQL's ``ALTER TABLE ... ALTER COLUMN ... TYPE`` acquires an
    ``ACCESS EXCLUSIVE`` lock and may rewrite the entire table when the
    type cast isn't binary-coercible. This op wraps the ALTER in a
    short transaction with ``SET LOCAL lock_timeout`` so the
    operation aborts quickly under contention rather than queueing
    behind every reader — the caller can retry during a quieter
    window.

    For type changes that PG can do without rewrite (e.g.
    ``VARCHAR(10) → VARCHAR(50)``, ``INTEGER → BIGINT``), the lock is
    released in milliseconds. For rewrite-requiring changes, the
    bounded lock means the operation fails fast rather than blocking
    indefinitely.

    For genuinely large rewrites that won't fit in the lock budget,
    use the manual "shadow column" recipe (add new column, backfill,
    swap names, drop old) — :class:`AddFieldOnline` +
    :class:`BackfillBatch` + a follow-up ``RunSQL`` cover the steps.

    Args:
        model_name: Model whose column is being altered.
        column: column name to alter.
        new_type: the target SQL type (e.g. ``"BIGINT"``,
            ``"VARCHAR(120)"``).
        using: optional ``USING <expr>`` clause for the cast. Defaults
            to ``"<column>::<new_type>"`` on PG.
        lock_timeout: ``SET LOCAL lock_timeout`` value. Default ``"5s"``.
        old_type: previous SQL type, required when the operation must
            be reversible (mirror the forward cast).
        old_using: optional reverse ``USING`` expression.

    PostgreSQL-only — every other backend raises
    :class:`NotImplementedError` because ``ALTER COLUMN TYPE`` is not
    portable.
    """

    def __init__(
        self,
        model_name: str,
        column: str,
        new_type: str,
        *,
        using: str | None = None,
        lock_timeout: str = "5s",
        old_type: str | None = None,
        old_using: str | None = None,
    ) -> None:
        if not new_type:
            raise ValueError("AlterColumnTypeOnline requires new_type")
        self.model_name = model_name
        self.column = column
        self.new_type = new_type
        self.using = using
        self.lock_timeout = lock_timeout
        self.old_type = old_type
        self.old_using = old_using
        self.reversible = old_type is not None

    def _resolve_table(self, app_label: str, state) -> str:
        model_state = state.models.get(
            f"{app_label}.{self.model_name.lower()}", {}
        )
        return (
            model_state.get("options", {}).get("db_table")
            or f"{app_label}_{self.model_name.lower()}"
        )

    def state_forwards(self, app_label: str, state) -> None:
        # Pure DB-side op — model state already reflects the new
        # field type (caller updates the field declaration).
        pass

    def _alter(
        self,
        connection,
        table: str,
        target_type: str,
        using: str | None,
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            raise NotImplementedError(
                "AlterColumnTypeOnline is PostgreSQL-only — "
                "other backends do not support bounded-lock "
                "ALTER COLUMN TYPE."
            )
        using_clause = (
            f' USING ({using})' if using
            else f' USING ("{self.column}"::{target_type})'
        )
        with connection.atomic():
            connection.execute_script(
                f"SET LOCAL lock_timeout = '{self.lock_timeout}'"
            )
            connection.execute_script(
                f'ALTER TABLE "{table}" '
                f'ALTER COLUMN "{self.column}" TYPE {target_type}'
                f"{using_clause}"
            )

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        self._alter(
            connection,
            self._resolve_table(app_label, to_state),
            self.new_type,
            self.using,
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if self.old_type is None:
            raise NotImplementedError(
                f"AlterColumnTypeOnline({self.model_name!r}, {self.column!r}) "
                "is irreversible — supply old_type for a reversible "
                "migration."
            )
        self._alter(
            connection,
            self._resolve_table(app_label, from_state),
            self.old_type,
            self.old_using,
        )

    def describe(self) -> str:
        return (
            f"Alter column type online on {self.model_name}.{self.column} "
            f"→ {self.new_type}"
        )

    def __repr__(self) -> str:
        return (
            f"AlterColumnTypeOnline({self.model_name!r}, {self.column!r}, "
            f"{self.new_type!r})"
        )


# ── PostgreSQL native ENUM types ───────────────────────────────────────────────


class CreatePGEnum(Operation):
    """Create a PostgreSQL ``CREATE TYPE ... AS ENUM`` type.

    Pair with :class:`~dorm.fields.EnumField` set to ``native=True``
    so the column references the type name instead of falling back
    to ``VARCHAR``. The migration order matters: emit
    :class:`CreatePGEnum` *before* the :class:`AddField` /
    :class:`CreateModel` that uses the enum.

    Example::

        operations = [
            CreatePGEnum("status_enum", ["active", "archived", "deleted"]),
            CreateModel(
                name="Article",
                fields=[
                    ("status", dorm.EnumField(Status, native=True)),
                    ...
                ],
            ),
        ]

    Backwards: drops the type. Fails if any column still references
    it — drop the column first.

    Non-PG backends are no-ops; the field's ``db_type`` falls back
    to VARCHAR so the column DDL still emits cleanly.
    """

    reversible = True

    def __init__(self, name: str, values: list[str]) -> None:
        if not values:
            raise ValueError("CreatePGEnum requires at least one value")
        for v in values:
            if not isinstance(v, str):
                raise ValueError(
                    f"CreatePGEnum values must be strings; got {type(v).__name__}"
                )
        self.name = name
        self.values = list(values)

    def state_forwards(self, app_label: str, state) -> None:
        # Pure schema-side op — no model state to mutate.
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        quoted = ", ".join(
            "'" + v.replace("'", "''") + "'" for v in self.values
        )
        connection.execute_script(
            f'CREATE TYPE "{self.name}" AS ENUM ({quoted})'
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(f'DROP TYPE IF EXISTS "{self.name}"')

    def describe(self) -> str:
        return f"Create PG enum type {self.name}"

    def __repr__(self) -> str:
        return f"CreatePGEnum({self.name!r}, {self.values!r})"


class DropPGEnum(Operation):
    """Drop a PostgreSQL ``ENUM`` type. Reverse direction recreates
    it from *reverse_values* (required for reversibility)."""

    def __init__(
        self,
        name: str,
        *,
        reverse_values: list[str] | None = None,
    ) -> None:
        self.name = name
        self.reverse_values = list(reverse_values or [])
        self.reversible = bool(self.reverse_values)

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(f'DROP TYPE IF EXISTS "{self.name}"')

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if not self.reverse_values:
            raise NotImplementedError(
                f"DropPGEnum({self.name!r}) is irreversible — no "
                "reverse_values supplied."
            )
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        quoted = ", ".join(
            "'" + v.replace("'", "''") + "'" for v in self.reverse_values
        )
        connection.execute_script(
            f'CREATE TYPE "{self.name}" AS ENUM ({quoted})'
        )

    def describe(self) -> str:
        return f"Drop PG enum type {self.name}"


class AddPGEnumValue(Operation):
    """``ALTER TYPE … ADD VALUE`` — append a new label to a PG enum.

    Notable PG quirk: ``ADD VALUE`` cannot run inside a transaction
    block on PG ≤ 11. The migration runner already commits between
    operations, so this only matters if you wrap the migration in
    your own atomic context manually."""

    reversible = False  # ALTER TYPE ... DROP VALUE doesn't exist on PG.

    def __init__(self, type_name: str, value: str, *, before: str | None = None) -> None:
        if not isinstance(value, str):
            raise ValueError("AddPGEnumValue value must be a string")
        self.type_name = type_name
        self.value = value
        self.before = before

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        v = "'" + self.value.replace("'", "''") + "'"
        sql = f'ALTER TYPE "{self.type_name}" ADD VALUE IF NOT EXISTS {v}'
        if self.before:
            b = "'" + self.before.replace("'", "''") + "'"
            sql += f" BEFORE {b}"
        connection.execute_script(sql)

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        # PG has no ``ALTER TYPE ... DROP VALUE``; the only way back
        # is to drop and recreate the entire type, which loses any
        # rows referencing the value being removed. Document the
        # irreversibility loud and clear.
        raise NotImplementedError(
            "AddPGEnumValue cannot be reversed — PostgreSQL has no "
            "DROP VALUE syntax. Recreate the type from scratch via "
            "DropPGEnum + CreatePGEnum if you really need to remove a "
            "value, and migrate every column referencing it first."
        )

    def describe(self) -> str:
        return f"Add value {self.value!r} to PG enum {self.type_name}"


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
            if vendor == "mysql":
                # MySQL / MariaDB use ``BIGINT AUTO_INCREMENT PRIMARY KEY``
                # for auto-numbered surrogate keys. The ``db_type`` for
                # ``AutoField`` / ``BigAutoField`` / ``SmallAutoField``
                # already returns ``INTEGER``; switch the integer width
                # here based on the field class so the column matches
                # what dorm generates on the other vendors.
                int_type = "BIGINT" if isinstance(field, BigAutoField) else (
                    "SMALLINT" if isinstance(field, SmallAutoField) else "INT"
                )
                parts = [f'"{col}" {int_type} NOT NULL AUTO_INCREMENT PRIMARY KEY']
                return parts[0]
        parts.append("PRIMARY KEY")

    if not field.null and not field.primary_key:
        parts.append("NOT NULL")

    if field.unique and not field.primary_key:
        parts.append("UNIQUE")

    # Emit DDL DEFAULT. Resolution: ``db_default`` (server-side, the
    # only one that fires for raw INSERTs that omit the column) wins
    # over ``default`` (Python-side fallback). Both can coexist —
    # they target different write paths.
    from ..fields import NOT_PROVIDED

    db_default = getattr(field, "db_default", NOT_PROVIDED)
    if db_default is not NOT_PROVIDED and db_default is not None:
        vendor = getattr(connection, "vendor", "sqlite")
        # ``db_default`` accepts either a Python literal (rendered
        # the same way ``default`` is) or a raw SQL string passed
        # through :class:`dorm.expressions.RawSQL`. RawSQL is the
        # escape hatch for vendor-specific server-side defaults
        # (``now()``, ``gen_random_uuid()``, sequence calls).
        from ..expressions import RawSQL

        if isinstance(db_default, RawSQL):
            parts.append(f"DEFAULT {db_default.sql}")
        else:
            literal = field.get_db_prep_value(db_default)
            if isinstance(db_default, bool):
                if vendor == "sqlite":
                    parts.append(f"DEFAULT {int(db_default)}")
                else:
                    parts.append("DEFAULT TRUE" if db_default else "DEFAULT FALSE")
            elif isinstance(literal, str):
                escaped = literal.replace("'", "''")
                parts.append(f"DEFAULT '{escaped}'")
            elif literal is not None:
                parts.append(f"DEFAULT {literal}")
    elif field.has_default() and field.default is not None:
        if field.default is not NOT_PROVIDED and not callable(field.default):
            vendor = getattr(connection, "vendor", "sqlite")
            default_val = field.get_db_prep_value(field.default)
            if isinstance(field.default, bool):
                if vendor == "sqlite":
                    parts.append(f"DEFAULT {int(field.default)}")
                else:
                    parts.append("DEFAULT TRUE" if field.default else "DEFAULT FALSE")
            elif isinstance(default_val, str):
                # Escape single quotes by SQL-standard doubling
                # (``'`` → ``''``). Without this a ``default="O'Brien"``
                # would emit ``DEFAULT 'O'Brien'`` — broken DDL and
                # an SQL-injection vector if the default ever comes
                # from anything user-influenced.
                escaped = default_val.replace("'", "''")
                parts.append(f"DEFAULT '{escaped}'")
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

    # ``db_comment`` (3.2+) — MySQL emits the comment inline on
    # the column DDL. PostgreSQL handles it via a separate
    # ``COMMENT ON COLUMN`` statement issued by ``CreateModel``;
    # SQLite has no comment syntax at all.
    db_comment = getattr(field, "db_comment", None)
    if db_comment:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "mysql":
            escaped = str(db_comment).replace("'", "''")
            parts.append(f"COMMENT '{escaped}'")

    return " ".join(parts)


# ── PostgreSQL Row-Level Security ─────────────────────────────────────────────


def _quote_ident(name: str) -> str:
    """Defensive identifier quoter for RLS DDL. RLS policy / role names
    flow from user-controlled migration files, so we double-quote and
    escape any embedded quotes — same shape as PG ``quote_ident()``.
    Names with control characters / nulls are rejected outright."""
    if not isinstance(name, str) or not name:
        raise ValueError(f"Identifier must be a non-empty string; got {name!r}")
    if "\x00" in name or any(c in name for c in "\r\n"):
        raise ValueError(
            f"Identifier {name!r} contains control characters."
        )
    return '"' + name.replace('"', '""') + '"'


class EnableRowLevelSecurity(Operation):
    """``ALTER TABLE ... ENABLE ROW LEVEL SECURITY``.

    RLS is opt-in per table on PostgreSQL — without this op a table's
    policies (see :class:`CreatePolicy`) are inert. Pair with
    :class:`ForceRowLevelSecurity` when superuser / table-owner roles
    must also be subject to policies (rare but useful for hardened
    multi-tenant setups).

    PostgreSQL-only — no-op on every other backend so caller code
    stays portable.
    """

    reversible = True

    def __init__(self, table: str) -> None:
        self.table = table

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"ALTER TABLE {_quote_ident(self.table)} ENABLE ROW LEVEL SECURITY"
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"ALTER TABLE {_quote_ident(self.table)} DISABLE ROW LEVEL SECURITY"
        )

    def describe(self) -> str:
        return f"Enable RLS on {self.table}"


class DisableRowLevelSecurity(Operation):
    """Inverse of :class:`EnableRowLevelSecurity`. PostgreSQL-only."""

    reversible = True

    def __init__(self, table: str) -> None:
        self.table = table

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"ALTER TABLE {_quote_ident(self.table)} DISABLE ROW LEVEL SECURITY"
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"ALTER TABLE {_quote_ident(self.table)} ENABLE ROW LEVEL SECURITY"
        )

    def describe(self) -> str:
        return f"Disable RLS on {self.table}"


class ForceRowLevelSecurity(Operation):
    """``ALTER TABLE ... FORCE ROW LEVEL SECURITY``.

    By default PG exempts the table owner from RLS policies; with FORCE
    enabled, the owner is subject to policies too. Recommended for
    multi-tenant deployments where the application connects as the
    table owner. Reverse direction is ``NO FORCE``.

    PostgreSQL-only.
    """

    reversible = True

    def __init__(self, table: str) -> None:
        self.table = table

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"ALTER TABLE {_quote_ident(self.table)} FORCE ROW LEVEL SECURITY"
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"ALTER TABLE {_quote_ident(self.table)} NO FORCE ROW LEVEL SECURITY"
        )

    def describe(self) -> str:
        return f"Force RLS on {self.table}"


class CreatePolicy(Operation):
    """``CREATE POLICY <name> ON <table> [...]``.

    Args:
        name: policy name; unique per table.
        table: table the policy applies to.
        command: one of ``"ALL"``, ``"SELECT"``, ``"INSERT"``,
            ``"UPDATE"``, ``"DELETE"``. Defaults to ``"ALL"``.
        roles: optional list of PostgreSQL role names the policy
            applies to. Defaults to ``PUBLIC`` (all roles).
        using: raw SQL expression for the ``USING`` clause (row-read
            predicate). Required for ``SELECT`` / ``UPDATE`` /
            ``DELETE`` / ``ALL`` policies.
        check: raw SQL expression for the ``WITH CHECK`` clause
            (row-write predicate). Required for ``INSERT`` /
            ``UPDATE`` / ``ALL`` policies when you want to constrain
            what new row data is allowed.
        permissive: True (default) for the standard OR-combined
            permissive policy; False emits a ``RESTRICTIVE`` policy
            that AND-combines with other policies (PG 10+).

    Both *using* and *check* expressions are spliced into DDL as-is —
    they typically reference ``current_setting('app.tenant_id')`` or
    ``current_user`` and so must come from trusted migration code, not
    runtime user input.

    PostgreSQL-only.
    """

    _COMMANDS = frozenset({"ALL", "SELECT", "INSERT", "UPDATE", "DELETE"})

    reversible = True

    def __init__(
        self,
        name: str,
        table: str,
        *,
        command: str = "ALL",
        roles: list[str] | None = None,
        using: str | None = None,
        check: str | None = None,
        permissive: bool = True,
    ) -> None:
        cmd = command.upper()
        if cmd not in self._COMMANDS:
            raise ValueError(
                f"CreatePolicy.command must be one of {sorted(self._COMMANDS)}; "
                f"got {command!r}"
            )
        if using is None and cmd in ("SELECT", "UPDATE", "DELETE", "ALL"):
            # The DB itself accepts a policy without USING (defaults to
            # ``true``), but a silent default on a security-sensitive op
            # is exactly the wrong default. Force the migration author
            # to write it out.
            raise ValueError(
                f"CreatePolicy(command={cmd!r}) requires a 'using' "
                "expression — RLS policies without an explicit predicate "
                "are too easy to misread. Pass using='true' if you really "
                "want an unconstrained policy."
            )
        if check is None and cmd in ("INSERT",):
            raise ValueError(
                "CreatePolicy(command='INSERT') requires a 'check' expression."
            )
        self.name = name
        self.table = table
        self.command = cmd
        self.roles = list(roles) if roles else None
        self.using = using
        self.check = check
        self.permissive = permissive

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        parts = [
            "CREATE POLICY",
            _quote_ident(self.name),
            "ON",
            _quote_ident(self.table),
        ]
        if not self.permissive:
            parts.append("AS RESTRICTIVE")
        parts.extend(["FOR", self.command])
        if self.roles:
            parts.append("TO " + ", ".join(_quote_ident(r) for r in self.roles))
        if self.using is not None:
            parts.append(f"USING ({self.using})")
        if self.check is not None:
            parts.append(f"WITH CHECK ({self.check})")
        connection.execute_script(" ".join(parts))

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"DROP POLICY IF EXISTS {_quote_ident(self.name)} "
            f"ON {_quote_ident(self.table)}"
        )

    def describe(self) -> str:
        return f"Create RLS policy {self.name} on {self.table}"


class DropPolicy(Operation):
    """``DROP POLICY <name> ON <table>``.

    Reverse direction recreates the policy from the supplied
    *reverse_* arguments (same shape as :class:`CreatePolicy`'s
    constructor). Pass them when reversibility matters.
    """

    def __init__(
        self,
        name: str,
        table: str,
        *,
        reverse_command: str | None = None,
        reverse_roles: list[str] | None = None,
        reverse_using: str | None = None,
        reverse_check: str | None = None,
        reverse_permissive: bool = True,
    ) -> None:
        self.name = name
        self.table = table
        self.reverse_command = reverse_command
        self.reverse_roles = reverse_roles
        self.reverse_using = reverse_using
        self.reverse_check = reverse_check
        self.reverse_permissive = reverse_permissive
        self.reversible = reverse_command is not None

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        connection.execute_script(
            f"DROP POLICY IF EXISTS {_quote_ident(self.name)} "
            f"ON {_quote_ident(self.table)}"
        )

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if not self.reversible:
            raise NotImplementedError(
                f"DropPolicy({self.name!r}) is irreversible — no "
                "reverse_command supplied."
            )
        # Reuse CreatePolicy's emitter for symmetry.
        recreate = CreatePolicy(
            self.name,
            self.table,
            command=self.reverse_command or "ALL",
            roles=self.reverse_roles,
            using=self.reverse_using,
            check=self.reverse_check,
            permissive=self.reverse_permissive,
        )
        recreate.database_forwards(app_label, connection, from_state, to_state)

    def describe(self) -> str:
        return f"Drop RLS policy {self.name} on {self.table}"


class AlterPolicy(Operation):
    """``ALTER POLICY <name> ON <table>`` — change *roles*, *using*, or
    *check*. Each kwarg is optional; only the supplied fragments are
    emitted. PostgreSQL doesn't allow changing the ``command`` of an
    existing policy — use :class:`DropPolicy` + :class:`CreatePolicy`
    for that.

    Reverse direction restores the *previous_* values when supplied.
    """

    def __init__(
        self,
        name: str,
        table: str,
        *,
        roles: list[str] | None = None,
        using: str | None = None,
        check: str | None = None,
        previous_roles: list[str] | None = None,
        previous_using: str | None = None,
        previous_check: str | None = None,
    ) -> None:
        if roles is None and using is None and check is None:
            raise ValueError(
                "AlterPolicy requires at least one of roles=, using=, check=."
            )
        self.name = name
        self.table = table
        self.roles = roles
        self.using = using
        self.check = check
        self.previous_roles = previous_roles
        self.previous_using = previous_using
        self.previous_check = previous_check
        self.reversible = (
            previous_roles is not None
            or previous_using is not None
            or previous_check is not None
        )

    def _emit(
        self,
        connection,
        roles: list[str] | None,
        using: str | None,
        check: str | None,
    ) -> None:
        parts = [
            "ALTER POLICY",
            _quote_ident(self.name),
            "ON",
            _quote_ident(self.table),
        ]
        if roles is not None:
            parts.append(
                "TO " + (", ".join(_quote_ident(r) for r in roles) if roles else "PUBLIC")
            )
        if using is not None:
            parts.append(f"USING ({using})")
        if check is not None:
            parts.append(f"WITH CHECK ({check})")
        connection.execute_script(" ".join(parts))

    def state_forwards(self, app_label: str, state) -> None:
        pass

    def database_forwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        self._emit(connection, self.roles, self.using, self.check)

    def database_backwards(
        self, app_label: str, connection, from_state, to_state
    ) -> None:
        if not self.reversible:
            raise NotImplementedError(
                f"AlterPolicy({self.name!r}) is irreversible — no "
                "previous_* fragments supplied."
            )
        if getattr(connection, "vendor", "sqlite") != "postgresql":
            return
        self._emit(
            connection,
            self.previous_roles if self.roles is not None else None,
            self.previous_using if self.using is not None else None,
            self.previous_check if self.check is not None else None,
        )

    def describe(self) -> str:
        return f"Alter RLS policy {self.name} on {self.table}"
