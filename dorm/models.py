from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, ClassVar

from .exceptions import DoesNotExist, MultipleObjectsReturned, ValidationError

if TYPE_CHECKING:
    from typing import Self
    from .manager import Manager

# Global model registry: "AppLabel.ModelName" → class
_model_registry: dict[str, type] = {}


class Options:
    """Stores model metadata (equivalent to Django's _meta)."""

    def __init__(self, meta, app_label: str):
        self.meta = meta
        self.app_label = app_label
        self.model_name: str = ""
        self.db_table: str = ""
        self.ordering: list[str] = []
        self.unique_together: list[tuple] = []
        self.indexes: list = []
        self.constraints: list = []
        self.abstract: bool = False
        self.managed: bool = True
        self.fields: list = []
        self.pk: Any = None
        self.managers: list = []
        self._field_cache: dict[str, Any] = {}

    def contribute_to_class(self, cls, name: str):
        cls._meta = self
        self.model = cls
        if self.meta:
            for k, v in self.meta.__dict__.items():
                if not k.startswith("_"):
                    setattr(self, k, v)
        if not self.db_table:
            # Use only the last segment so "example.sales" → "sales_customer"
            label_segment = self.app_label.rsplit(".", 1)[-1]
            self.db_table = f"{label_segment}_{self.model_name}"
        if not self.ordering:
            self.ordering = []

    def add_field(self, field):
        self.fields.append(field)
        self._field_cache[field.name] = field
        if hasattr(field, "attname") and field.attname != field.name:
            self._field_cache[field.attname] = field
        if field.primary_key:
            self.pk = field

    def get_field(self, name: str):
        if name in self._field_cache:
            return self._field_cache[name]
        # Try by column
        for f in self.fields:
            if f.column == name:
                return f
        from .exceptions import FieldDoesNotExist
        raise FieldDoesNotExist(f"Field '{name}' does not exist on {self.model.__name__}.")

    def get_fields(self) -> list:
        return list(self.fields)

    @property
    def concrete_fields(self) -> list:
        return [f for f in self.fields if getattr(f, "concrete", True) and f.column]

    @property
    def local_fields(self) -> list:
        return [f for f in self.fields if not getattr(f, "many_to_many", False)]


class ModelBase(type):
    """Metaclass for all models."""

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        super_new = super().__new__

        # Skip ModelBase itself
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return super_new(mcs, name, bases, attrs)

        # Determine app_label from module.
        # Strip ".models" suffix so that "example.sales.models" → "example.sales"
        # and fall back to the first component for other modules.
        module = attrs.get("__module__", "")
        parts = module.split(".")
        if len(parts) > 1 and parts[-1] == "models":
            app_label = ".".join(parts[:-1])  # e.g. "example.sales"
        else:
            app_label = parts[0] if parts else "default"

        # Extract Meta
        meta = attrs.pop("Meta", None)

        # Build new class
        new_class = super_new(mcs, name, bases, attrs)

        # Set up Options
        opts = Options(meta, app_label)
        opts.model_name = name.lower()
        opts.contribute_to_class(new_class, "_meta")

        # Set db_table default if not set via Meta
        if not opts.db_table:
            opts.db_table = f"{app_label}_{name.lower()}"

        # Collect fields from class attributes
        declared_fields = []
        for k, v in list(attrs.items()):
            from .fields import Field
            if isinstance(v, Field):
                declared_fields.append((k, v))
                # Remove from class so descriptor works
                if k in new_class.__dict__:
                    delattr(new_class, k)

        # Also inherit fields from abstract parents
        for parent in parents:
            if hasattr(parent, "_meta") and parent._meta.abstract:
                for field in parent._meta.fields:
                    field_copy = copy.deepcopy(field)
                    declared_fields.append((field_copy.name, field_copy))

        # Sort by creation counter to preserve declaration order
        declared_fields.sort(key=lambda x: x[1].creation_counter)

        # Check if there's an existing pk from parents
        has_pk = any(f.primary_key for _, f in declared_fields)

        # Add default pk if needed
        if not has_pk:
            from .fields import BigAutoField
            pk = BigAutoField(primary_key=True)
            pk.creation_counter = -1
            declared_fields.insert(0, ("id", pk))

        # Contribute fields to class
        for fname, field in declared_fields:
            field.contribute_to_class(new_class, fname)

        # Add default manager if none defined
        from .manager import Manager
        if not any(isinstance(v, Manager) for v in attrs.values()):
            manager = Manager()
            manager.contribute_to_class(new_class, "objects")

        # Also inherit managers from concrete parents
        for parent in parents:
            if hasattr(parent, "_meta"):
                for mgr in parent._meta.managers:
                    if mgr.name not in new_class.__dict__:
                        new_mgr = mgr.__class__()
                        new_mgr.contribute_to_class(new_class, mgr.name)

        # Set up model-level DoesNotExist / MultipleObjectsReturned
        new_class.DoesNotExist = type(  # type: ignore
            "DoesNotExist", (DoesNotExist,), {"__module__": module}
        )
        new_class.MultipleObjectsReturned = type(  # type: ignore
            "MultipleObjectsReturned", (MultipleObjectsReturned,), {"__module__": module}
        )

        # Register model
        _model_registry[name] = new_class
        _model_registry[f"{app_label}.{name}"] = new_class

        return new_class


class Model(metaclass=ModelBase):
    """Base class for all ORM models."""

    if TYPE_CHECKING:
        objects: ClassVar[Manager[Self]]
        _meta: ClassVar[Options]
        DoesNotExist: type[BaseException]
        MultipleObjectsReturned: type[BaseException]

    class Meta:
        abstract = True

    def __init__(self, **kwargs):
        meta = self._meta
        # Set defaults first
        for field in meta.fields:
            if field.attname not in kwargs:
                if field.has_default():
                    self.__dict__[field.attname] = field.get_default()
                else:
                    self.__dict__[field.attname] = None

        # Apply provided values
        for key, value in kwargs.items():
            try:
                field = meta.get_field(key)
                self.__dict__[field.attname] = field.to_python(value)
            except Exception:
                # FK descriptor: key is relation name, value is instance or pk
                setattr(self, key, value)

    @property
    def pk(self):
        if self._meta.pk:
            return self.__dict__.get(self._meta.pk.attname)
        return None

    @pk.setter
    def pk(self, value):
        if self._meta.pk:
            self.__dict__[self._meta.pk.attname] = value

    # ── Sync persistence ──────────────────────────────────────────────────────

    def save(self, using: str = "default", force_insert: bool = False, force_update: bool = False, update_fields=None):
        from .db.connection import get_connection
        conn = get_connection(using)
        meta = self._meta

        if force_insert or self.pk is None:
            self._do_insert(conn, meta)
        else:
            self._do_update(conn, meta, update_fields)

    def _do_insert(self, conn, meta):
        from .fields import AutoField
        from .query import SQLQuery
        fields = []
        values = []
        for field in meta.fields:
            if isinstance(field, AutoField) and field.attname not in self.__dict__:
                continue
            if isinstance(field, AutoField) and self.__dict__.get(field.attname) is None:
                continue
            col_val = field.get_db_prep_value(self.__dict__.get(field.attname))
            if col_val is None and not field.null and not isinstance(field, AutoField):
                if field.has_default():
                    col_val = field.get_db_prep_value(field.get_default())
                    self.__dict__[field.attname] = field.get_default()
            fields.append(field)
            values.append(col_val)

        query = SQLQuery(self.__class__)
        sql, params = query.as_insert(fields, values, conn)
        pk = conn.execute_insert(sql, params)
        if meta.pk and pk is not None:
            self.__dict__[meta.pk.attname] = pk

    def _do_update(self, conn, meta, update_fields=None):
        from .query import SQLQuery
        fields_to_update = []
        if update_fields:
            for fname in update_fields:
                try:
                    fields_to_update.append(meta.get_field(fname))
                except Exception:
                    pass
        else:
            from .fields import AutoField
            fields_to_update = [f for f in meta.fields if not isinstance(f, AutoField)]

        col_kwargs = {}
        for field in fields_to_update:
            col_kwargs[field.column] = field.get_db_prep_value(
                self.__dict__.get(field.attname)
            )

        query = SQLQuery(self.__class__)
        pk_field = meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_update(col_kwargs, conn)
        conn.execute_write(sql, params)

    def delete(self, using: str = "default"):
        from .db.connection import get_connection
        from .query import SQLQuery
        conn = get_connection(using)
        query = SQLQuery(self.__class__)
        pk_field = self._meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_delete(conn)
        count = conn.execute_write(sql, params)
        self.pk = None
        return count, {f"{self._meta.app_label}.{self.__class__.__name__}": count}

    # ── Async persistence ─────────────────────────────────────────────────────

    async def asave(self, using: str = "default", force_insert: bool = False, force_update: bool = False, update_fields=None):
        from .db.connection import get_async_connection
        conn = await get_async_connection(using)
        meta = self._meta

        if force_insert or self.pk is None:
            await self._ado_insert(conn, meta)
        else:
            await self._ado_update(conn, meta, update_fields)

    async def _ado_insert(self, conn, meta):
        from .fields import AutoField
        from .query import SQLQuery
        fields = []
        values = []
        for field in meta.fields:
            if isinstance(field, AutoField) and self.__dict__.get(field.attname) is None:
                continue
            col_val = field.get_db_prep_value(self.__dict__.get(field.attname))
            fields.append(field)
            values.append(col_val)

        query = SQLQuery(self.__class__)
        sql, params = query.as_insert(fields, values, conn)
        pk = await conn.execute_insert(sql, params)
        if meta.pk and pk is not None:
            self.__dict__[meta.pk.attname] = pk

    async def _ado_update(self, conn, meta, update_fields=None):
        from .fields import AutoField
        from .query import SQLQuery
        fields_to_update = []
        if update_fields:
            for fname in update_fields:
                try:
                    fields_to_update.append(meta.get_field(fname))
                except Exception:
                    pass
        else:
            fields_to_update = [f for f in meta.fields if not isinstance(f, AutoField)]

        col_kwargs = {}
        for field in fields_to_update:
            col_kwargs[field.column] = field.get_db_prep_value(
                self.__dict__.get(field.attname)
            )

        query = SQLQuery(self.__class__)
        pk_field = meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_update(col_kwargs, conn)
        await conn.execute_write(sql, params)

    async def adelete(self, using: str = "default"):
        from .db.connection import get_async_connection
        from .query import SQLQuery
        conn = await get_async_connection(using)
        query = SQLQuery(self.__class__)
        pk_field = self._meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_delete(conn)
        count = await conn.execute_write(sql, params)
        self.pk = None
        return count, {f"{self._meta.app_label}.{self.__class__.__name__}": count}

    # ── Helpers ───────────────────────────────────────────────────────────────

    @classmethod
    def _from_db_row(cls, row, connection=None) -> "Self":
        """Construct a model instance from a database row."""
        instance = cls.__new__(cls)
        instance.__dict__ = {}
        concrete = [f for f in cls._meta.fields if f.column]
        if hasattr(row, "keys"):
            data = dict(row)
            for field in concrete:
                raw = data.get(field.column)
                instance.__dict__[field.attname] = field.from_db_value(raw)
        else:
            for i, field in enumerate(concrete):
                if i < len(row):
                    instance.__dict__[field.attname] = field.from_db_value(row[i])
        return instance

    def full_clean(self):
        from .fields import AutoField
        errors = {}
        for field in self._meta.fields:
            # Skip AutoField when pk is not yet assigned (new unsaved instance)
            if isinstance(field, AutoField) and self.__dict__.get(field.attname) is None:
                continue
            value = self.__dict__.get(field.attname)
            try:
                field.validate(value, self)
            except ValidationError as e:
                errors[field.name] = str(e)
        if errors:
            raise ValidationError(errors)

    def refresh_from_db(self, using: str = "default", fields=None):
        from .queryset import QuerySet
        obj = QuerySet(self.__class__, using).get(pk=self.pk)
        if fields:
            for fname in fields:
                try:
                    field = self._meta.get_field(fname)
                    self.__dict__[field.attname] = obj.__dict__[field.attname]
                except Exception:
                    pass
        else:
            self.__dict__.update(obj.__dict__)

    async def arefresh_from_db(self, using: str = "default", fields=None):
        from .queryset import QuerySet
        qs = QuerySet(self.__class__, using)
        obj = await qs.aget(pk=self.pk)
        if fields:
            for fname in fields:
                try:
                    field = self._meta.get_field(fname)
                    self.__dict__[field.attname] = obj.__dict__[field.attname]
                except Exception:
                    pass
        else:
            self.__dict__.update(obj.__dict__)

    def __repr__(self):
        return f"<{self.__class__.__name__}: pk={self.pk}>"

    def __eq__(self, other):
        if not isinstance(other, Model):
            return False
        if self.__class__ is not other.__class__:
            return False
        pk = self.pk
        return pk is not None and pk == other.pk

    def __hash__(self):
        if self.pk is None:
            raise TypeError("Model instances without pk are unhashable")
        return hash((self.__class__, self.pk))
