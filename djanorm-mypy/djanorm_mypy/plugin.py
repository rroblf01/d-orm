"""Core mypy plugin implementation for djanorm."""

from __future__ import annotations

from typing import Callable, Optional

from mypy.nodes import (
    AssignmentStmt,
    NameExpr,
    StrExpr,
    TypeInfo,
)
from mypy.plugin import (
    AttributeContext,
    ClassDefContext,
    MethodContext,
    Plugin,
)
from mypy.types import AnyType, Instance, Type as MypyType, TypeOfAny


# Set of dorm Manager / QuerySet methods that take field-name kwargs
# and should be checked. The plugin walks each kwarg against the
# resolved model's field set and emits an error for unknown names.
_KWARG_FILTERING_METHODS = frozenset(
    {
        "filter",
        "exclude",
        "get",
        "afilter",
        "aexclude",
        "aget",
        "get_or_create",
        "aget_or_create",
        "update_or_create",
        "aupdate_or_create",
    }
)

# Suffixes that are part of a field lookup. ``name__icontains`` parses
# as ``("name", "icontains")``; the suffix is checked against this
# whitelist while the prefix is checked against the model's fields.
_LOOKUP_SUFFIXES = frozenset(
    {
        "exact", "iexact", "contains", "icontains", "startswith",
        "istartswith", "endswith", "iendswith", "regex", "iregex",
        "gt", "gte", "lt", "lte", "in", "isnull", "range",
        "year", "month", "day", "hour", "minute", "second",
        "week", "weekday", "iso_week_day", "quarter",
        "date", "time",
        "len", "has_key", "has_keys", "has_any_keys",
        "contained_by", "overlap",
    }
)


def _model_from_manager(typ: MypyType) -> Optional[TypeInfo]:
    """Walk a Manager / QuerySet generic instance back to its model
    TypeInfo so we can look up the model's field set."""
    if not isinstance(typ, Instance):
        return None
    args = typ.args
    if not args:
        return None
    first = args[0]
    if isinstance(first, Instance):
        return first.type
    return None


def _model_field_names(info: TypeInfo) -> set[str]:
    """Collect every dorm Field name declared on *info* and its bases.

    The lookup is "field-shaped" — we accept anything whose declared
    type's fullname starts with ``dorm.fields.`` (covers both built-in
    fields and third-party subclasses that follow the ``Field[T]``
    convention).
    """
    out: set[str] = set()
    for base in info.mro:
        for name, sym in base.names.items():
            if name.startswith("_"):
                continue
            node = sym.node
            stmt = getattr(node, "type", None)
            if stmt is None:
                continue
            if isinstance(stmt, Instance) and stmt.type.fullname.startswith(
                "dorm.fields."
            ):
                out.add(name)
        # Also include explicit annotations like ``publisher_id: int |
        # None`` that the user added for FK descriptors.
        for stmt in getattr(base.defn, "defs", []) or []:
            if isinstance(stmt, AssignmentStmt):
                for lvalue in stmt.lvalues:
                    if isinstance(lvalue, NameExpr) and not lvalue.name.startswith("_"):
                        out.add(lvalue.name)
    out.update({"pk", "id"})  # always synthesised
    return out


def _check_kwarg_against_model(
    arg_name: str,
    field_names: set[str],
    ctx: MethodContext,
) -> None:
    """Validate ``filter(arg_name=...)`` against the model's fields.

    Splits ``name__icontains`` into the field part and the lookup
    suffix; both must resolve cleanly. Multi-hop lookups
    (``author__publisher__name``) are tolerated up to the first hop —
    deeper validation would need a transitive walk over related-model
    metadata, which is more invasive than this plugin aims to be.
    """
    head, _sep, _rest = arg_name.partition("__")
    if head in field_names:
        # Validate the immediate suffix when there's exactly one (the
        # next hop, if any, is a relation chain we don't follow).
        if _sep and _rest and "__" not in _rest:
            if _rest not in _LOOKUP_SUFFIXES:
                ctx.api.fail(
                    f'Unknown lookup suffix "{_rest}" on field '
                    f'"{head}". Valid suffixes: '
                    f'{sorted(_LOOKUP_SUFFIXES)[:6]} ...',
                    ctx.context,
                )
        return
    # Anything tagged with a known lookup suffix in head is also fine
    # if the head is a relation walk we can't resolve here.
    if "__" in arg_name:
        return
    ctx.api.fail(
        f'Unknown field "{arg_name}" on '
        f'{ctx.type.type.name if isinstance(ctx.type, Instance) else "model"}',
        ctx.context,
    )


def _filter_method_hook(ctx: MethodContext) -> MypyType:
    """Validate kwargs of ``Manager.filter / .exclude / .get / ...``.

    Always returns the original return type — we are only emitting
    errors for unknown field names, never rewriting the signature.
    """
    if not isinstance(ctx.type, Instance):
        return ctx.default_return_type
    model_info = _model_from_manager(ctx.type)
    if model_info is None:
        return ctx.default_return_type
    field_names = _model_field_names(model_info)

    for kw_names, kw_args in zip(ctx.arg_names, ctx.args):
        for name in kw_names:
            if name is None:
                continue
            _check_kwarg_against_model(name, field_names, ctx)
            del kw_args  # silence unused
            break
    return ctx.default_return_type


def _model_class_hook(ctx: ClassDefContext) -> None:
    """Class-creation hook: re-stamp the synthesised ``pk`` attribute
    so consumer code can assert ``model.pk`` types as ``Any`` rather
    than crashing the type checker. The runtime sets it dynamically;
    without this hook, mypy reports it as missing.
    """
    info = ctx.cls.info
    if "pk" in info.names:
        return
    # ``pk`` is the runtime alias of the primary-key column; type as
    # Any so callers don't fight the type system over its concrete
    # type (which can be int, UUID, str, composite, etc.).
    from mypy.nodes import SymbolTableNode, MDEF, Var

    var = Var("pk", AnyType(TypeOfAny.special_form))
    var.info = info
    var._fullname = f"{info.fullname}.pk"
    info.names["pk"] = SymbolTableNode(MDEF, var)


def _attribute_hook_id(ctx: AttributeContext) -> MypyType:
    """``model.pk`` and ``model.id`` resolve to ``Any`` so consumers
    don't need to special-case auto-PK / composite-PK / UUID-PK
    differences."""
    return AnyType(TypeOfAny.special_form)


class DjanormPlugin(Plugin):
    """Top-level mypy ``Plugin`` subclass.

    See module-level docstring for the surface this plugin shapes.
    """

    def get_method_hook(
        self, fullname: str
    ) -> Optional[Callable[[MethodContext], MypyType]]:
        # ``fullname`` is the dotted-import path of the method —
        # ``dorm.manager.Manager.filter``, ``dorm.queryset.QuerySet.filter``,
        # etc. We hook every lookup-bearing entry point so dual-class
        # access (manager-level + queryset-level) is uniformly checked.
        last = fullname.rsplit(".", 1)[-1]
        if last in _KWARG_FILTERING_METHODS and (
            "Manager" in fullname or "QuerySet" in fullname
        ):
            return _filter_method_hook
        return None

    def get_base_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        # Stamp ``pk`` on every concrete ``dorm.Model`` subclass.
        if fullname == "dorm.models.Model":
            return _model_class_hook
        return None

    def get_attribute_hook(
        self, fullname: str
    ) -> Optional[Callable[[AttributeContext], MypyType]]:
        if fullname.endswith(".pk") or fullname.endswith(".id"):
            return _attribute_hook_id
        return None


# Silence unused imports kept for the hook signature contract.
_ = StrExpr
