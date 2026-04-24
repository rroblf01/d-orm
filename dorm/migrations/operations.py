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
        connection.execute_script(f'ALTER TABLE "{table}" DROP COLUMN IF EXISTS "{field}"')

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
        connection.execute_script(f'ALTER TABLE "{table}" DROP COLUMN IF EXISTS "{self.name}"')

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
    from ..fields import AutoField, BigAutoField, SmallAutoField

    col = getattr(field, "column", fname) or fname
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
            default_val = field.get_db_prep_value(field.default)
            if isinstance(default_val, str):
                parts.append(f"DEFAULT '{default_val}'")
            elif default_val is not None:
                parts.append(f"DEFAULT {default_val}")

    # FK reference
    from ..fields import ForeignKey, OneToOneField
    if isinstance(field, (ForeignKey, OneToOneField)):
        rel = field._resolve_related_model()
        ref_table = rel._meta.db_table
        ref_col = rel._meta.pk.column
        on_delete = getattr(field, "on_delete", "CASCADE")
        parts.append(f'REFERENCES "{ref_table}"("{ref_col}") ON DELETE {on_delete}')

    return " ".join(parts)
