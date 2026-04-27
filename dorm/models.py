from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, ClassVar

from .exceptions import DoesNotExist, MultipleObjectsReturned, ValidationError

if TYPE_CHECKING:
    from typing import Self
    from .manager import Manager

# Global model registry: "AppLabel.ModelName" → class
_model_registry: dict[str, Any] = {}


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
        # Validate that db_table is safe to splice into SQL.
        if not self.abstract:
            from .conf import _validate_identifier
            _validate_identifier(
                self.db_table, kind=f"{cls.__name__}.Meta.db_table"
            )

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

        # Inherit Meta options from abstract parents (unless explicitly set on this class)
        explicitly_set = set(meta.__dict__) if meta else set()
        for parent in parents:
            if hasattr(parent, "_meta") and parent._meta.abstract:
                if "ordering" not in explicitly_set and parent._meta.ordering:
                    opts.ordering = list(parent._meta.ordering)

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

        # Contribute any user-declared Manager instances (custom managers
        # like ``objects = MyCustomManager()``). Without this step the
        # manager would just be a class attribute pointing at a Manager
        # whose ``model`` was never set — ``MyModel.objects.all()`` would
        # still call methods, but ``self.model`` is None inside them and
        # most queryset construction breaks. We collect names first so
        # the descriptor swap doesn't perturb the iteration.
        from .manager import Manager
        declared_manager_names = [
            (k, v) for k, v in attrs.items() if isinstance(v, Manager)
        ]
        for name_, mgr in declared_manager_names:
            mgr.contribute_to_class(new_class, name_)
        already = {n for n, _ in declared_manager_names}

        # Inherit managers from parents (including abstract ones — that's
        # how :class:`SoftDeleteModel` ships ``objects`` /
        # ``all_objects`` / ``deleted_objects`` to its subclasses). We
        # do this BEFORE the default-manager fallback so a child that
        # only redeclares some of the parents' managers still gets the
        # rest, and the default Manager doesn't clobber an inherited
        # ``objects``. Re-instantiate via ``mgr.__class__()`` so
        # ``self.model`` points at the *child* class, not the parent.
        for parent in parents:
            if hasattr(parent, "_meta"):
                for mgr in parent._meta.managers:
                    if mgr.name in already:
                        continue
                    new_mgr = mgr.__class__()
                    new_mgr.contribute_to_class(new_class, mgr.name)
                    already.add(mgr.name)

        # Add the default Manager only for concrete models that ended
        # up with no manager at all (no declared, no inherited).
        if not opts.abstract and "objects" not in already:
            manager = Manager()
            manager.contribute_to_class(new_class, "objects")

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

        # Resolve any pending reverse FK relations that target this model
        from .fields import _pending_reverse_relations
        from .related_managers import ReverseFKDescriptor
        still_pending = []
        for src_model, fk_field, rel_name in _pending_reverse_relations:
            try:
                target = fk_field._resolve_related_model()
                setattr(target, rel_name, ReverseFKDescriptor(src_model, fk_field))
            except Exception:
                still_pending.append((src_model, fk_field, rel_name))
        _pending_reverse_relations[:] = still_pending

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
        # Set defaults first (skip M2M fields — they have no column and use descriptors)
        for field in meta.fields:
            if field.many_to_many:
                continue
            if field.attname not in kwargs:
                if field.has_default():
                    self.__dict__[field.attname] = field.get_default()
                else:
                    self.__dict__[field.attname] = None

        # Apply provided values
        from .exceptions import FieldDoesNotExist
        for key, value in kwargs.items():
            try:
                field = meta.get_field(key)
            except FieldDoesNotExist:
                # Not a known model field — fall back to setattr so
                # arbitrary attributes (e.g. from raw queryset hydration)
                # still land on the instance.
                setattr(self, key, value)
                continue
            from .fields import RelatedField
            if isinstance(field, RelatedField) and key == field.name:
                # Use FK descriptor so model instances get their PK extracted
                setattr(self, key, value)
            elif type(self).__dict__.get(field.attname) is field or hasattr(
                type(field), "_uses_class_descriptor"
            ):
                # The field installed itself as a class-level descriptor
                # (FileField, future custom fields). Route through
                # ``setattr`` so ``__set__`` fires — bypassing it would
                # write the raw value past the descriptor's logic
                # (pending-upload tracking, etc.).
                setattr(self, key, value)
            else:
                # NOTE: ``field.to_python`` may raise ValidationError
                # (e.g. EmailField rejecting an invalid address). We let
                # that propagate — better to fail at construction than
                # write a bogus row.
                self.__dict__[field.attname] = field.to_python(value)

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

    def save(
        self,
        using: str = "default",
        force_insert: bool = False,
        force_update: bool = False,
        update_fields: list[str] | None = None,
    ) -> None:
        from .db.connection import get_connection
        from .signals import post_save, pre_save

        conn = get_connection(using)
        meta = self._meta
        adding = force_insert or self.pk is None

        pre_save.send(
            self.__class__,
            instance=self,
            raw=False,
            using=using,
            update_fields=update_fields,
        )
        if adding:
            self._do_insert(conn, meta)
        else:
            self._do_update(conn, meta, update_fields)
        post_save.send(
            self.__class__,
            instance=self,
            created=adding,
            raw=False,
            using=using,
            update_fields=update_fields,
        )

    def _do_insert(self, conn, meta) -> None:
        from .fields import AutoField
        from .query import SQLQuery

        fields = []
        values = []
        for field in meta.fields:
            if not field.column:  # skip M2M and other non-column fields
                continue
            if isinstance(field, AutoField) and self.__dict__.get(field.attname) is None:
                continue
            col_val = field.get_db_prep_value(field.pre_save(self, add=True))
            if col_val is None and not field.null and not isinstance(field, AutoField):
                if field.has_default():
                    default = field.get_default()
                    col_val = field.get_db_prep_value(default)
                    self.__dict__[field.attname] = default
            fields.append(field)
            values.append(col_val)

        query = SQLQuery(self.__class__)
        sql, params = query.as_insert(fields, values, conn)
        pk_col = meta.pk.column if meta.pk else "id"
        pk = conn.execute_insert(sql, params, pk_col=pk_col)
        if meta.pk and pk is not None:
            self.__dict__[meta.pk.attname] = pk

    def _do_update(self, conn, meta, update_fields: list[str] | None = None) -> None:
        from .fields import AutoField
        from .query import SQLQuery

        if update_fields is not None:
            fields_to_update = []
            for fname in update_fields:
                try:
                    f = meta.get_field(fname)
                    if f.column:
                        fields_to_update.append(f)
                except Exception:
                    pass
        else:
            fields_to_update = [
                f for f in meta.fields if not isinstance(f, AutoField) and f.column
            ]

        col_kwargs = {}
        for field in fields_to_update:
            val = field.pre_save(self, add=False)
            col_kwargs[field.column] = field.get_db_prep_value(val)

        query = SQLQuery(self.__class__)
        pk_field = meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_update(col_kwargs, conn)
        conn.execute_write(sql, params)

    def _handle_on_delete(self, using: str = "default") -> None:
        """Apply Python-level on_delete behaviour for all reverse FK relations."""
        from .exceptions import ProtectedError
        from .fields import CASCADE, DO_NOTHING, PROTECT, SET_DEFAULT, SET_NULL
        from .related_managers import ReverseFKDescriptor

        for attr_val in type(self).__dict__.values():
            if not isinstance(attr_val, ReverseFKDescriptor):
                continue
            fk_field = attr_val.fk_field
            on_delete = getattr(fk_field, "on_delete", DO_NOTHING)
            if on_delete == DO_NOTHING:
                continue

            related_manager = attr_val.__get__(self, type(self))
            related_qs = related_manager.get_queryset()

            if on_delete == PROTECT:
                objs = list(related_qs)
                if objs:
                    raise ProtectedError(
                        f"Cannot delete {self!r} because related "
                        f"{attr_val.source_model.__name__} objects exist.",
                        objs,
                    )
            elif on_delete == CASCADE:
                for obj in list(related_qs):
                    obj.delete(using=using)
            elif on_delete == SET_NULL:
                related_qs.update(**{fk_field.name: None})
            elif on_delete == SET_DEFAULT:
                related_qs.update(**{fk_field.name: fk_field.get_default()})

    def delete(self, using: str = "default") -> tuple[int, dict[str, int]]:
        from .db.connection import get_connection
        from .query import SQLQuery
        from .signals import post_delete, pre_delete

        self._handle_on_delete(using=using)

        conn = get_connection(using)
        pre_delete.send(self.__class__, instance=self, using=using)

        query = SQLQuery(self.__class__)
        pk_field = self._meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_delete(conn)
        count = conn.execute_write(sql, params)

        post_delete.send(self.__class__, instance=self, using=using)
        self.pk = None
        return count, {f"{self._meta.app_label}.{self.__class__.__name__}": count}

    # ── Async persistence ─────────────────────────────────────────────────────

    async def asave(
        self,
        using: str = "default",
        force_insert: bool = False,
        force_update: bool = False,
        update_fields: list[str] | None = None,
    ) -> None:
        from .db.connection import get_async_connection
        from .signals import post_save, pre_save

        conn = get_async_connection(using)
        meta = self._meta
        adding = force_insert or self.pk is None

        await pre_save.asend(
            self.__class__,
            instance=self,
            raw=False,
            using=using,
            update_fields=update_fields,
        )
        if adding:
            await self._ado_insert(conn, meta)
        else:
            await self._ado_update(conn, meta, update_fields)
        await post_save.asend(
            self.__class__,
            instance=self,
            created=adding,
            raw=False,
            using=using,
            update_fields=update_fields,
        )

    async def _ado_insert(self, conn, meta) -> None:
        from .fields import AutoField
        from .query import SQLQuery

        fields = []
        values = []
        for field in meta.fields:
            # Skip M2M and other non-column fields (their `column` is None).
            if not field.column:
                continue
            if isinstance(field, AutoField) and self.__dict__.get(field.attname) is None:
                continue
            col_val = field.get_db_prep_value(field.pre_save(self, add=True))
            if col_val is None and not field.null and not isinstance(field, AutoField):
                if field.has_default():
                    default = field.get_default()
                    col_val = field.get_db_prep_value(default)
                    self.__dict__[field.attname] = default
            fields.append(field)
            values.append(col_val)

        query = SQLQuery(self.__class__)
        sql, params = query.as_insert(fields, values, conn)
        pk_col = meta.pk.column if meta.pk else "id"
        pk = await conn.execute_insert(sql, params, pk_col=pk_col)
        if meta.pk and pk is not None:
            self.__dict__[meta.pk.attname] = pk

    async def _ado_update(
        self, conn, meta, update_fields: list[str] | None = None
    ) -> None:
        from .fields import AutoField
        from .query import SQLQuery

        if update_fields is not None:
            fields_to_update = []
            for fname in update_fields:
                try:
                    f = meta.get_field(fname)
                    if f.column:
                        fields_to_update.append(f)
                except Exception:
                    pass
        else:
            fields_to_update = [
                f for f in meta.fields if not isinstance(f, AutoField) and f.column
            ]

        col_kwargs = {}
        for field in fields_to_update:
            val = field.pre_save(self, add=False)
            col_kwargs[field.column] = field.get_db_prep_value(val)

        query = SQLQuery(self.__class__)
        pk_field = meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_update(col_kwargs, conn)
        await conn.execute_write(sql, params)

    async def adelete(self, using: str = "default") -> tuple[int, dict[str, int]]:
        from .db.connection import get_async_connection
        from .query import SQLQuery
        from .signals import post_delete, pre_delete

        self._handle_on_delete(using=using)

        conn = get_async_connection(using)
        await pre_delete.asend(self.__class__, instance=self, using=using)

        query = SQLQuery(self.__class__)
        pk_field = self._meta.pk
        query.where_nodes.append(([pk_field.column], "exact", self.pk))
        sql, params = query.as_delete(conn)
        count = await conn.execute_write(sql, params)

        await post_delete.asend(self.__class__, instance=self, using=using)
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

    def clean_fields(self, exclude: list[str] | None = None) -> None:
        from .fields import AutoField

        errors: dict[str, str] = {}
        for field in self._meta.fields:
            if exclude and field.name in exclude:
                continue
            if isinstance(field, AutoField) and self.__dict__.get(field.attname) is None:
                continue
            value = self.__dict__.get(field.attname)
            try:
                field.validate(value, self)
            except ValidationError as e:
                errors[field.name] = str(e)
        if errors:
            raise ValidationError(errors)

    def clean(self) -> None:
        """Override to add model-level validation. Call super().clean() to chain."""

    def validate_unique(self, exclude: list[str] | None = None) -> None:
        """Check that the in-memory state of this instance does not
        clash with any unique / ``unique_together`` constraint.

        **Optimisation:** the previous implementation issued one
        ``EXISTS`` query per unique field AND per ``unique_together``
        combo, so a model with 3 unique fields and 2 combos paid 5
        round-trips on every ``full_clean()``. The new path runs a
        single combined ``OR`` query as a fast existence check; only
        when a violation is detected do we drill in with per-combo
        queries to produce the specific error messages. The happy path
        (no violations — by far the common case in API write handlers)
        now costs **one** query regardless of how many unique
        constraints the model carries.
        """
        from .exceptions import FieldDoesNotExist
        from .fields import AutoField
        from .expressions import Q
        from .queryset import QuerySet

        # ── Build a list of (label, Q) probes for each constraint ──
        #
        # ``label`` is the dict key under which the diagnostic message
        # lands in ``errors`` if this probe matches: the field name for
        # per-field uniques, ``__all__`` for unique_together combos.
        # ``message`` is the user-facing string we'll use if the slow-
        # path query confirms the violation.
        probes: list[tuple[str, Any, str]] = []

        for field in self._meta.fields:
            if not getattr(field, "unique", False) or field.primary_key:
                continue
            if isinstance(field, AutoField):
                continue
            if exclude and field.name in exclude:
                continue
            value = self.__dict__.get(field.attname)
            if value is None:
                continue
            probes.append(
                (
                    field.name,
                    Q(**{field.column: value}),
                    f"A {self.__class__.__name__} with this "
                    f"{field.verbose_name or field.name} already exists.",
                )
            )

        for combo in self._meta.unique_together:
            if exclude and any(f in exclude for f in combo):
                continue
            lookup: dict[str, Any] = {}
            skip = False
            for fname in combo:
                try:
                    f = self._meta.get_field(fname)
                    val = self.__dict__.get(f.attname)
                    if val is None:
                        skip = True
                        break
                    lookup[fname] = val
                except FieldDoesNotExist:
                    skip = True
                    break
            if skip:
                continue
            combo_str = ", ".join(combo)
            probes.append(
                (
                    "__all__",
                    Q(**lookup),
                    f"{self.__class__.__name__} with this {combo_str} "
                    "already exists.",
                )
            )

        if not probes:
            return

        # ── Fast path: single OR'd existence check ──────────────────
        combined = probes[0][1]
        for _, q, _ in probes[1:]:
            combined = combined | q
        fast_qs: Any = QuerySet(self.__class__).filter(combined)
        if self.pk is not None:
            fast_qs = fast_qs.exclude(pk=self.pk)
        if not fast_qs.exists():
            return

        # ── Slow path: re-issue per-probe to pin down which one(s) ──
        # Only reached on a confirmed violation (rare — typically a
        # bad request the caller is about to report). We accept the
        # extra round-trip here for diagnostic precision.
        errors: dict[str, str] = {}
        for label, q, message in probes:
            qs: Any = QuerySet(self.__class__).filter(q)
            if self.pk is not None:
                qs = qs.exclude(pk=self.pk)
            if qs.exists():
                errors[label] = message
        if errors:
            raise ValidationError(errors)

    def full_clean(self, exclude: list[str] | None = None) -> None:
        self.clean_fields(exclude=exclude)
        self.clean()
        self.validate_unique(exclude=exclude)

    def refresh_from_db(self, using: str = "default", fields=None):
        """Re-fetch this row from the database, optionally restricting
        to a subset of columns.

        When ``fields=`` is given, the ``SELECT`` is narrowed to those
        columns via :meth:`QuerySet.only` so the database transfers
        only what's actually being refreshed — important on tables
        with large TEXT/BLOB/JSON columns where the previous
        ``SELECT *`` paid a real bandwidth cost. Unknown field names
        are silently skipped (matches the long-standing behaviour
        callers depend on for partial refreshes after a
        ``__set_changed`` hook).
        """
        from .queryset import QuerySet
        from .exceptions import FieldDoesNotExist

        qs = QuerySet(self.__class__, using)
        if fields:
            cols: list[str] = []
            for fname in fields:
                try:
                    cols.append(self._meta.get_field(fname).column)
                except FieldDoesNotExist:
                    pass
            if cols:
                qs = qs.only(*cols)
        obj = qs.get(pk=self.pk)
        if fields:
            for fname in fields:
                try:
                    field = self._meta.get_field(fname)
                    if field.attname in obj.__dict__:
                        self.__dict__[field.attname] = obj.__dict__[field.attname]
                except FieldDoesNotExist:
                    pass
        else:
            self.__dict__.update(obj.__dict__)

    async def arefresh_from_db(self, using: str = "default", fields=None):
        """Async counterpart of :meth:`refresh_from_db`. Same
        ``fields=`` narrowing applies."""
        from .queryset import QuerySet
        from .exceptions import FieldDoesNotExist

        qs = QuerySet(self.__class__, using)
        if fields:
            cols: list[str] = []
            for fname in fields:
                try:
                    cols.append(self._meta.get_field(fname).column)
                except FieldDoesNotExist:
                    pass
            if cols:
                qs = qs.only(*cols)
        obj = await qs.aget(pk=self.pk)
        if fields:
            for fname in fields:
                try:
                    field = self._meta.get_field(fname)
                    if field.attname in obj.__dict__:
                        self.__dict__[field.attname] = obj.__dict__[field.attname]
                except FieldDoesNotExist:
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
