from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, ClassVar

from .exceptions import (
    DoesNotExist,
    ImproperlyConfigured,
    MultipleObjectsReturned,
    ValidationError,
)

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
        # Proxy models share the parent's DB table — they exist only
        # at the Python class layer to add methods / managers without
        # a separate migration. The autodetector skips them so
        # ``makemigrations`` doesn't emit a phantom CreateModel.
        self.proxy: bool = False
        # ``concrete_model`` is the closest non-proxy ancestor whose
        # table actually backs the rows. For non-proxy models it's
        # ``self.model``; for proxies it's the parent's concrete
        # model. Set in ``contribute_to_class`` once we have the
        # class object.
        self.concrete_model: Any = None
        # Custom permissions surface as ``[(codename, name), ...]``.
        # When ``dorm.contrib.auth`` is in ``INSTALLED_APPS``, the
        # post-migrate hook in ``dorm.contrib.auth.management``
        # ensures one ``Permission`` row per entry exists.
        self.permissions: list[tuple[str, str]] = []
        # Verbose names for admin / form labels — kept here so the
        # ORM core doesn't need an admin app to round-trip a
        # readable model name through migrations.
        self.verbose_name: str = ""
        self.verbose_name_plural: str = ""
        # ``db_table_comment`` (3.2+) — table-level comment string
        # surfaced in ``CreateModel`` for the PostgreSQL
        # ``COMMENT ON TABLE`` emit pass. Mirrors Django 4.1's
        # ``Meta.db_table_comment`` option.
        self.db_table_comment: str = ""
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
        # Resolve the concrete model: walk the MRO looking for the
        # first non-proxy ancestor that's also a Model. For non-proxy
        # classes that's ``self.model``; for proxies it's the parent
        # whose table actually exists.
        self.concrete_model = self.model
        if self.proxy:
            for parent in cls.__mro__[1:]:
                p_meta = getattr(parent, "_meta", None)
                if p_meta is None:
                    continue
                if not getattr(p_meta, "proxy", False) and not getattr(p_meta, "abstract", False):
                    self.concrete_model = parent
                    # Proxies share their parent's storage.
                    self.db_table = p_meta.db_table
                    break
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
        composite_pk = None
        for k, v in list(attrs.items()):
            from .fields import CompositePrimaryKey, Field
            if isinstance(v, Field):
                declared_fields.append((k, v))
                # Remove from class so descriptor works
                if k in new_class.__dict__:
                    delattr(new_class, k)
            elif isinstance(v, CompositePrimaryKey):
                # Pulled out separately — composite PKs aren't Fields,
                # they're a constraint over existing fields.
                composite_pk = (k, v)
                if k in new_class.__dict__:
                    delattr(new_class, k)

        # Also inherit fields from abstract parents
        for parent in parents:
            if hasattr(parent, "_meta") and parent._meta.abstract:
                for field in parent._meta.fields:
                    field_copy = copy.deepcopy(field)
                    declared_fields.append((field_copy.name, field_copy))

        # Proxy-model inheritance: ``Meta.proxy = True`` shares the
        # concrete parent's table — but we deep-copy each parent
        # field so the subsequent ``contribute_to_class`` call on
        # the proxy doesn't mutate the parent's ``field.model``
        # back-reference. Two model classes pointing at the same
        # field instance + the metaclass setting ``field.model``
        # last-writer-wins would silently break the parent's
        # queries (descriptors look up by ``self.model``).
        proxy_flag = bool(getattr(meta, "proxy", False)) if meta else False
        if proxy_flag:
            for parent in parents:
                p_meta = getattr(parent, "_meta", None)
                if p_meta is None:
                    continue
                if p_meta.abstract or getattr(p_meta, "proxy", False):
                    continue
                for field in p_meta.fields:
                    field_copy = copy.deepcopy(field)
                    declared_fields.append((field_copy.name, field_copy))
                break

        # Sort by creation counter to preserve declaration order
        declared_fields.sort(key=lambda x: x[1].creation_counter)

        # Check if there's an existing pk from parents OR a composite.
        has_pk = any(f.primary_key for _, f in declared_fields) or composite_pk is not None

        # Add default pk if needed
        if not has_pk:
            from .fields import BigAutoField
            pk = BigAutoField(primary_key=True)
            pk.creation_counter = -1
            declared_fields.insert(0, ("id", pk))

        # Contribute fields to class
        for fname, field in declared_fields:
            field.contribute_to_class(new_class, fname)

        # Composite PK wires last so the underlying fields are already
        # attached when ``_meta.pk`` resolves their columns. The
        # ``add_field`` call inside ``contribute_to_class`` sets
        # ``opts.pk`` because ``primary_key=True`` on the composite.
        if composite_pk is not None:
            cpk_name, cpk = composite_pk
            cpk.contribute_to_class(new_class, cpk_name)

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
                    # ``mgr.__class__()`` requires a zero-arg
                    # constructor — custom Manager subclasses with
                    # ``__init__(self, tenant_id, …)`` raised
                    # ``TypeError`` the moment a child model was
                    # defined. Use ``copy.copy`` to clone the parent
                    # instance instead: it preserves constructor
                    # args (and any post-init attributes) without
                    # re-running ``__init__``.
                    try:
                        new_mgr = copy.copy(mgr)
                    except Exception:
                        # Fall back to the legacy zero-arg path so
                        # managers that don't survive a shallow copy
                        # (rare — usually managers carrying open
                        # resources) keep their previous behaviour.
                        new_mgr = mgr.__class__()
                    new_mgr.contribute_to_class(new_class, mgr.name)
                    already.add(mgr.name)

        # Add the default Manager only for concrete models that ended
        # up with no manager at all (no declared, no inherited).
        if not opts.abstract and "objects" not in already:
            manager = Manager()
            manager.contribute_to_class(new_class, "objects")

        # Honour ``Meta.default_manager_name``: the named manager
        # becomes ``_default_manager`` (introspected by reverse-FK
        # descriptors and a handful of other internals). If the
        # name doesn't resolve, fall back to the first declared
        # manager — same precedence rule Django uses.
        default_name = getattr(opts, "default_manager_name", None)
        chosen = None
        if default_name:
            for mgr in opts.managers:
                if mgr.name == default_name:
                    chosen = mgr
                    break
            if chosen is None:
                raise ImproperlyConfigured(
                    f"{new_class.__name__}.Meta.default_manager_name = "
                    f"{default_name!r} but no manager with that name is "
                    f"declared on the model."
                )
        elif opts.managers:
            chosen = opts.managers[0]
        if chosen is not None:
            setattr(new_class, "_default_manager", chosen)

        # Set up model-level DoesNotExist / MultipleObjectsReturned
        new_class.DoesNotExist = type(  # type: ignore
            "DoesNotExist", (DoesNotExist,), {"__module__": module}
        )
        new_class.MultipleObjectsReturned = type(  # type: ignore
            "MultipleObjectsReturned", (MultipleObjectsReturned,), {"__module__": module}
        )

        # Register model. ``app_label`` here is the module-derived
        # label (``dorm.contrib.auth``); ``opts.app_label`` may differ
        # when ``Meta.app_label`` overrides it (``auth``). Register
        # under both so callers can address the model by either form
        # — qualified module path OR the canonical app label that
        # ``_meta.app_label`` reports.
        _model_registry[name] = new_class
        _model_registry[f"{app_label}.{name}"] = new_class
        if opts.app_label != app_label:
            _model_registry[f"{opts.app_label}.{name}"] = new_class

        # Resolve any pending reverse FK / O2O relations that
        # target this model. ``OneToOneField`` registers the
        # reverse side as a single-instance accessor; plain
        # ``ForeignKey`` registers the manager-style ``_set``
        # accessor. The descriptor class is chosen here so the
        # field's own ``contribute_to_class`` doesn't have to
        # repeat the import + isinstance branch.
        from .fields import OneToOneField, _pending_reverse_relations
        from .related_managers import ReverseFKDescriptor, ReverseOneToOneDescriptor
        still_pending = []
        for src_model, fk_field, rel_name in _pending_reverse_relations:
            try:
                target = fk_field._resolve_related_model()
                if isinstance(fk_field, OneToOneField):
                    setattr(
                        target,
                        rel_name,
                        ReverseOneToOneDescriptor(src_model, fk_field),
                    )
                else:
                    setattr(
                        target,
                        rel_name,
                        ReverseFKDescriptor(src_model, fk_field),
                    )
            except Exception:
                still_pending.append((src_model, fk_field, rel_name))
        _pending_reverse_relations[:] = still_pending

        return new_class


class _ModelState:
    """Per-instance state tracker — mirrors Django's ``Model._state``.

    ``adding`` is True for instances that have never been persisted.
    Used by :meth:`Model._is_unsaved` so an explicit ``Model(pk=0)``
    (or any other DB-controlled-but-falsy value) doesn't trick the
    save router into emitting ``UPDATE`` against a row that doesn't
    exist yet.
    """

    __slots__ = ("adding", "db")

    def __init__(self, *, adding: bool = True, db: str | None = None) -> None:
        self.adding = adding
        # Alias the row was hydrated from (or written to). Set by
        # :meth:`Model.from_db` and by the queryset's hydration
        # path; useful when third-party libs (history tracking,
        # multi-DB routers) need to know which alias an instance
        # belongs to without re-resolving via the router.
        self.db = db


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
        # ``_state.adding`` mirrors Django's flag: True when this
        # instance has never been written to the database. Set
        # before any field assignment so ``__set__`` hooks
        # (FileField, FK descriptors) can read it. ``_from_db_row``
        # flips it to False on hydrated rows; ``save()`` flips it
        # to False after a successful INSERT.
        self.__dict__["_state"] = _ModelState(adding=True)
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
            elif type(self).__dict__.get(field.attname) is field or getattr(
                type(field), "uses_class_descriptor", False
            ):
                # The field installed itself as a class-level descriptor
                # (FileField, future custom fields). Route through
                # ``setattr`` so ``__set__`` fires — bypassing it would
                # write the raw value past the descriptor's logic
                # (pending-upload tracking, etc.). The
                # ``uses_class_descriptor`` class attribute is the
                # documented opt-in for custom fields that need this
                # behaviour; see :class:`dorm.FileField` for the
                # canonical example.
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
            # Composite PK: read each component field and return a
            # tuple. The composite has ``column=None`` (no own column),
            # so we can't fall through to the dict read.
            from .fields import CompositePrimaryKey

            if isinstance(self._meta.pk, CompositePrimaryKey):
                return tuple(
                    self.__dict__.get(
                        self._meta.get_field(name).attname
                    )
                    for name in self._meta.pk.field_names
                )
            return self.__dict__.get(self._meta.pk.attname)
        return None

    @pk.setter
    def pk(self, value):
        if not self._meta.pk:
            return
        from .fields import CompositePrimaryKey

        if isinstance(self._meta.pk, CompositePrimaryKey):
            # Composite PK: distribute the tuple across the component
            # fields' ``attname`` slots. Accept ``None`` to clear them
            # all (used by ``delete()`` to invalidate the in-memory
            # instance after a successful row drop).
            if value is None:
                values = (None,) * len(self._meta.pk.field_names)
            else:
                if not isinstance(value, (tuple, list)) or len(value) != len(
                    self._meta.pk.field_names
                ):
                    raise ValueError(
                        f"CompositePrimaryKey expects a "
                        f"{len(self._meta.pk.field_names)}-tuple; got {value!r}."
                    )
                values = tuple(value)
            for fname, v in zip(self._meta.pk.field_names, values):
                self.__dict__[self._meta.get_field(fname).attname] = v
            return
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
        adding = force_insert or self._is_unsaved()

        pre_save.send(
            self.__class__,
            instance=self,
            raw=False,
            using=using,
            update_fields=update_fields,
        )
        if adding:
            self._do_insert(conn, meta)
            # Flip the state flag so a subsequent ``save()`` routes
            # through UPDATE rather than re-inserting.
            state = self.__dict__.get("_state")
            if state is not None:
                state.adding = False
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
        # Composite PK has no single column to RETURNING; the user
        # supplied all components by hand. Skip the auto-pk dance —
        # ``execute_write`` runs the INSERT without trying to read
        # back a generated id.
        from .fields import CompositePrimaryKey

        if isinstance(meta.pk, CompositePrimaryKey):
            conn.execute_write(sql, params)
            return
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
        self._add_pk_where_nodes(query)
        sql, params = query.as_update(col_kwargs, conn)
        conn.execute_write(sql, params)

    def _is_unsaved(self) -> bool:
        """True when the instance has not been persisted yet — i.e.
        ``save()`` should INSERT, not UPDATE.

        Authoritative source is ``self._state.adding``: it stays
        True for fresh instances built with explicit pk values
        (including ``pk=0`` and negative pks) and flips to False
        after a successful INSERT or DB hydration. Falling back to
        a pure ``pk is None`` check used to silently route
        ``Model(pk=0).save()`` through the UPDATE branch — affecting
        zero rows and producing no error or insert.

        Composite PK: same flag-driven path; the legacy "all
        components None" heuristic is kept as a fallback for older
        instances that predate ``_state``.
        """
        from .fields import CompositePrimaryKey

        state = self.__dict__.get("_state")
        if state is not None:
            return bool(getattr(state, "adding", True))
        if isinstance(self._meta.pk, CompositePrimaryKey):
            return all(v is None for v in self.pk or ())
        return self.pk is None

    def _add_pk_where_nodes(self, query: Any) -> None:
        """Append per-row WHERE clauses that match this instance's PK.

        Single-column PK: one ``WHERE pk_col = pk_value`` node.
        Composite PK: one ``WHERE col_i = component_i`` node per
        component field, AND-combined by the compiler. Used by
        update / delete on a single instance to address its row.
        """
        from .fields import CompositePrimaryKey

        meta = self._meta
        pk_field = meta.pk
        if isinstance(pk_field, CompositePrimaryKey):
            for fname in pk_field.field_names:
                comp = meta.get_field(fname)
                query.where_nodes.append(
                    ([comp.column], "exact", self.__dict__.get(comp.attname))
                )
            return
        query.where_nodes.append(([pk_field.column], "exact", self.pk))

    def _iter_reverse_fk_descriptors(self):
        """Yield ``(name, ReverseFKDescriptor)`` for every reverse FK
        descriptor reachable through the MRO.

        Walking ``type(self).__dict__`` alone misses descriptors
        installed on a parent class — a model that inherits from
        another concrete model would silently skip cascade
        handling for the parent's reverse relations. Use the MRO
        walk instead so multi-level model hierarchies behave the
        same as flat models.
        """
        from .related_managers import ReverseFKDescriptor

        seen: set[str] = set()
        for klass in type(self).__mro__:
            for attr_name, attr_val in klass.__dict__.items():
                if attr_name in seen:
                    continue
                if isinstance(attr_val, ReverseFKDescriptor):
                    seen.add(attr_name)
                    yield attr_name, attr_val

    def _handle_on_delete(self, using: str = "default") -> None:
        """Apply Python-level on_delete behaviour for all reverse FK relations."""
        from .exceptions import ProtectedError
        from .fields import CASCADE, DO_NOTHING, PROTECT, SET_DEFAULT, SET_NULL

        for _attr_name, attr_val in self._iter_reverse_fk_descriptors():
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

    async def _ahandle_on_delete(self, using: str = "default") -> None:
        """Async counterpart of :meth:`_handle_on_delete`.

        The sync version blocks the event loop with synchronous
        SQL when called from ``adelete``. This routes every
        cascade / SET_NULL / SET_DEFAULT step through the async
        queryset path so the event loop stays responsive.
        """
        from .exceptions import ProtectedError
        from .fields import CASCADE, DO_NOTHING, PROTECT, SET_DEFAULT, SET_NULL
        from .queryset import QuerySet

        for _attr_name, attr_val in self._iter_reverse_fk_descriptors():
            fk_field = attr_val.fk_field
            on_delete = getattr(fk_field, "on_delete", DO_NOTHING)
            if on_delete == DO_NOTHING:
                continue

            related_qs = QuerySet(attr_val.source_model, using).filter(
                **{fk_field.name: self.pk}
            )

            if on_delete == PROTECT:
                objs = [obj async for obj in related_qs]
                if objs:
                    raise ProtectedError(
                        f"Cannot delete {self!r} because related "
                        f"{attr_val.source_model.__name__} objects exist.",
                        objs,
                    )
            elif on_delete == CASCADE:
                async for obj in related_qs:
                    await obj.adelete(using=using)
            elif on_delete == SET_NULL:
                await related_qs.aupdate(**{fk_field.name: None})
            elif on_delete == SET_DEFAULT:
                await related_qs.aupdate(
                    **{fk_field.name: fk_field.get_default()}
                )

    def delete(self, using: str = "default") -> tuple[int, dict[str, int]]:
        from .db.connection import get_connection
        from .query import SQLQuery
        from .signals import post_delete, pre_delete

        self._handle_on_delete(using=using)

        conn = get_connection(using)
        pre_delete.send(self.__class__, instance=self, using=using)

        query = SQLQuery(self.__class__)
        self._add_pk_where_nodes(query)
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
        adding = force_insert or self._is_unsaved()

        await pre_save.asend(
            self.__class__,
            instance=self,
            raw=False,
            using=using,
            update_fields=update_fields,
        )
        if adding:
            await self._ado_insert(conn, meta)
            state = self.__dict__.get("_state")
            if state is not None:
                state.adding = False
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
        self._add_pk_where_nodes(query)
        sql, params = query.as_update(col_kwargs, conn)
        await conn.execute_write(sql, params)

    async def adelete(self, using: str = "default") -> tuple[int, dict[str, int]]:
        from .db.connection import get_async_connection
        from .query import SQLQuery
        from .signals import post_delete, pre_delete

        # Use the async cascade handler so reverse-FK CASCADE /
        # SET_NULL / SET_DEFAULT don't block the event loop with
        # sync SQL calls.
        await self._ahandle_on_delete(using=using)

        conn = get_async_connection(using)
        await pre_delete.asend(self.__class__, instance=self, using=using)

        query = SQLQuery(self.__class__)
        self._add_pk_where_nodes(query)
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
        instance.__dict__ = {"_state": _ModelState(adding=False)}
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

    @classmethod
    def from_db(cls, db: str | None, field_names: list[str], values: list) -> "Self":
        """Hook for custom hydration logic — Django parity.

        Default behaviour: zip ``field_names`` and ``values`` into
        a kwargs dict, build the instance via :meth:`_from_db_row`
        and stamp the resulting object's ``_state.db`` with *db*
        (the alias the row came from).

        Subclasses override this to add per-instance derived
        attributes that are cheap to compute on hydration but
        wasteful to compute on every access. The signature mirrors
        Django's ``Model.from_db`` so libraries that hook in
        through it (history-tracking, soft-delete extensions, …)
        keep working when migrated from Django.
        """
        instance = cls.__new__(cls)
        state = _ModelState(adding=False)
        state.db = db
        instance.__dict__ = {"_state": state}
        concrete = [f for f in cls._meta.fields if f.column]
        # Build a column → value map from the parallel lists. We
        # accept either column names or attnames in ``field_names``
        # so callers can hand us either Django-style.
        by_name = dict(zip(field_names, values))
        for field in concrete:
            raw = by_name.get(field.column, by_name.get(field.attname))
            instance.__dict__[field.attname] = field.from_db_value(raw)
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
