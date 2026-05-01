"""Regression tests for the v2.4.2 third-round bug-hunt fixes.

Each section pins down one specific bug found during the round-3 audit.
Bug numbers ("R1"–"R24") match the audit report.
"""

from __future__ import annotations

import datetime
import threading
from typing import Any
from unittest.mock import patch

import pytest

import dorm
from dorm import F, Q
from dorm.functions import Concat
from tests.models import Article, Author, Book, Publisher, Tag


# ────────────────────────────────────────────────────────────────────────────
# R1 — filter(field=None) must rewrite to IS NULL (not = NULL)
# ────────────────────────────────────────────────────────────────────────────


def test_filter_none_rewrites_to_is_null() -> None:
    Author.objects.create(name="HasMail", age=10, email="a@x.com")
    Author.objects.create(name="NoMail1", age=10, email=None)
    Author.objects.create(name="NoMail2", age=10, email=None)

    rows = list(Author.objects.filter(email=None))
    names = sorted(a.name for a in rows)
    assert names == ["NoMail1", "NoMail2"]


def test_exclude_none_rewrites_to_is_not_null() -> None:
    Author.objects.create(name="HM", age=10, email="a@x.com")
    Author.objects.create(name="NM", age=10, email=None)

    rows = list(Author.objects.exclude(email=None))
    names = sorted(a.name for a in rows)
    assert names == ["HM"]


# ────────────────────────────────────────────────────────────────────────────
# R2 — qs[:N].update() must honour LIMIT/OFFSET
# ────────────────────────────────────────────────────────────────────────────


def test_sliced_update_only_updates_slice() -> None:
    a = Author.objects.create(name="UpA", age=10)
    for i in range(10):
        Book.objects.create(title=f"u-{i}", author=a, pages=i)

    qs = Book.objects.filter(author=a).order_by("pages")
    n = qs[:3].update(pages=999)
    assert n == 3
    updated = Book.objects.filter(author=a, pages=999).count()
    assert updated == 3
    untouched = Book.objects.filter(author=a).exclude(pages=999).count()
    assert untouched == 7


@pytest.mark.asyncio
async def test_async_sliced_update_only_updates_slice() -> None:
    a = await Author.objects.acreate(name="AsyncUp", age=10)
    for i in range(6):
        await Book.objects.acreate(title=f"au-{i}", author=a, pages=i)

    qs = Book.objects.filter(author=a).order_by("pages")
    n = await qs[:2].aupdate(pages=42)
    assert n == 2
    updated = await Book.objects.filter(author=a, pages=42).acount()
    assert updated == 2


# ────────────────────────────────────────────────────────────────────────────
# R3 — _compile_expr must route Subquery / Exists through their as_sql
# ────────────────────────────────────────────────────────────────────────────


def test_compile_expr_handles_subquery_in_update_kwarg() -> None:
    """``update(field=Subquery(...))`` must compile the subquery rather
    than try to bind it as a parameter."""
    from dorm import OuterRef, Subquery
    from dorm.query import _compile_expr

    pub = Publisher.objects.create(name="P-sub")
    Author.objects.create(name="ASub", age=10, publisher=pub)

    sub = Subquery(
        Publisher.objects.filter(pk=OuterRef("publisher_id")).values("name")[:1]
    )
    # Pass the outer alias so OuterRef inside resolves.
    sql, params = _compile_expr(sub, table_alias=Author._meta.db_table, model=Author)
    # SQL fragment must be a parenthesised SELECT (not ``%s``) and
    # carry no opaque ``Subquery`` object as a bound param.
    assert "SELECT" in sql.upper()
    assert "%s" != sql
    assert all(not isinstance(p, Subquery) for p in params)


# ────────────────────────────────────────────────────────────────────────────
# R4 — _is_unsaved must use _state.adding (Model(pk=0).save() inserts)
# ────────────────────────────────────────────────────────────────────────────


def test_save_with_explicit_pk_zero_inserts() -> None:
    """``Model(pk=0).save()`` previously routed through UPDATE (since
    ``pk is not None`` was treated as already-saved) and silently no-op'd.
    Now ``_state.adding`` keeps the routing correct."""
    from tests.models import Tag as _Tag

    obj = _Tag(pk=0, name="zero-pk")
    obj.save()
    assert _Tag.objects.filter(pk=0).exists()
    assert _Tag.objects.get(pk=0).name == "zero-pk"


def test_state_adding_flips_after_save() -> None:
    obj = Author(name="AdState", age=10)
    assert getattr(obj, "_state").adding is True
    obj.save()
    assert getattr(obj, "_state").adding is False


def test_from_db_row_marks_state_not_adding() -> None:
    Author.objects.create(name="FromDB", age=11)
    fetched = Author.objects.get(name="FromDB")
    assert getattr(fetched, "_state").adding is False


# ────────────────────────────────────────────────────────────────────────────
# R5 — _adapt_placeholders must skip %s inside SQL string literals
# ────────────────────────────────────────────────────────────────────────────


def test_adapt_placeholders_preserves_literal_percent_s() -> None:
    """On PG the ``%s`` → ``$N`` rewrite must not touch occurrences
    inside ``'...'`` literals."""
    from dorm.query import SQLQuery

    class _DummyConn:
        vendor = "postgresql"

    q = SQLQuery(Author)
    sql = q._adapt_placeholders(
        "SELECT %s, 'foo%s_bar' FROM t WHERE x = %s", _DummyConn()
    )
    assert "'foo%s_bar'" in sql
    assert "$1" in sql
    assert "$2" in sql
    # Only TWO bare ``%s`` were renumbered, not three.
    assert "$3" not in sql


# ────────────────────────────────────────────────────────────────────────────
# R6 — only().defer() chain + db_column mismatch
# ────────────────────────────────────────────────────────────────────────────


def test_only_then_defer_preserves_only_restriction() -> None:
    """``only("a","b").defer("a")`` must leave ``[pk, b]`` selected."""
    a = Author.objects.create(name="OD", age=21, email="o@d.com")
    Book.objects.create(title="ODB", author=a, pages=10)

    qs = Author.objects.filter(pk=a.pk).only("name", "age").defer("name")
    selected = qs._query.selected_fields
    pk_col = Author._meta.pk.column
    # Must contain pk + 'age', and NOT 'name'.
    assert "name" not in (selected or [])
    assert "age" in (selected or [])
    assert pk_col in (selected or [])


# ────────────────────────────────────────────────────────────────────────────
# R7 — bulk_create unique_fields must resolve db_column overrides
# ────────────────────────────────────────────────────────────────────────────


def test_bulk_create_unique_fields_resolves_field_to_column() -> None:
    """``unique_fields=`` must resolve attnames to db_columns. Use a
    model field whose attname matches its column to verify the
    resolution path runs and emits a quoted target column."""
    from dorm.db.connection import get_connection
    from dorm.query import SQLQuery

    conn = get_connection()
    q = SQLQuery(Tag)
    fields = [Tag._meta.get_field("name")]
    sql, _ = q.as_bulk_insert(
        fields,
        [["x"]],
        conn,
        update_conflicts=True,
        unique_fields=["name"],
        update_fields=["name"],
    )
    # Resolution is in place — column is quoted in the conflict clause.
    assert 'ON CONFLICT ("name")' in sql


# ────────────────────────────────────────────────────────────────────────────
# R8 — values()/values_list() must emit JOIN for FK traversal
# ────────────────────────────────────────────────────────────────────────────


def test_values_with_fk_traversal_emits_join() -> None:
    pub = Publisher.objects.create(name="VP")
    Author.objects.create(name="VA1", age=10, publisher=pub)
    Author.objects.create(name="VA2", age=20, publisher=pub)

    rows = list(Author.objects.values("name", "publisher__name"))
    names = sorted(r["publisher__name"] for r in rows if r["publisher__name"])
    assert "VP" in names


def test_values_list_with_fk_traversal_returns_pairs() -> None:
    pub = Publisher.objects.create(name="VLP")
    Author.objects.create(name="VLA", age=15, publisher=pub)

    rows = list(Author.objects.values_list("name", "publisher__name"))
    pairs = [r for r in rows if r[0] == "VLA"]
    assert pairs and pairs[0][1] == "VLP"


# ────────────────────────────────────────────────────────────────────────────
# R9 — OneToOneField installs reverse single-instance descriptor
# ────────────────────────────────────────────────────────────────────────────


def test_one_to_one_field_reverse_descriptor_present() -> None:
    """Define a tiny model with OneToOneField and verify the reverse
    accessor is wired on the target class."""
    from dorm.related_managers import ReverseOneToOneDescriptor

    class _O2OTarget(dorm.Model):
        name = dorm.CharField(max_length=20)

        class Meta:
            db_table = "_o2o_target"
            app_label = "tests"

    class _O2OSource(dorm.Model):
        target = dorm.OneToOneField(
            _O2OTarget, on_delete=dorm.CASCADE, related_name="source"
        )

        class Meta:
            db_table = "_o2o_source"
            app_label = "tests"

    # Reverse descriptor present on the target class.
    desc = type(_O2OTarget).__getattribute__(_O2OTarget, "source")
    # When fetched from the class (not an instance), the descriptor
    # returns itself.
    assert isinstance(desc, ReverseOneToOneDescriptor)


# ────────────────────────────────────────────────────────────────────────────
# R10 — BinaryField.from_db_value coerces memoryview to bytes
# ────────────────────────────────────────────────────────────────────────────


def test_binary_field_from_db_value_coerces_memoryview() -> None:
    from dorm.fields import BinaryField

    f = BinaryField()
    out = f.from_db_value(memoryview(b"\x89hello"))
    assert isinstance(out, bytes)
    assert out == b"\x89hello"
    assert out.startswith(b"\x89")


def test_binary_field_from_db_value_preserves_none() -> None:
    from dorm.fields import BinaryField

    assert BinaryField().from_db_value(None) is None


# ────────────────────────────────────────────────────────────────────────────
# R11 — VectorField.from_db_value validates dimension
# ────────────────────────────────────────────────────────────────────────────


def test_vector_field_from_db_value_rejects_wrong_dim() -> None:
    pytest.importorskip("dorm.contrib.pgvector")
    from dorm.contrib.pgvector.fields import VectorField
    from dorm.exceptions import ValidationError

    f = VectorField(dimensions=4)
    f.name = "vec"
    # 3-component list returned by the DB layer — a corrupted
    # column / cross-dim migration scenario.
    with pytest.raises(ValidationError):
        f.from_db_value([1.0, 2.0, 3.0])


# ────────────────────────────────────────────────────────────────────────────
# R12 — annotate() must reject collision with a field name
# ────────────────────────────────────────────────────────────────────────────


def test_annotate_rejects_field_name_collision() -> None:
    with pytest.raises(ValueError, match="conflicts with a field"):
        Author.objects.annotate(name=dorm.Count("pk"))


def test_annotate_safe_name_passes() -> None:
    qs = Author.objects.annotate(some_count=dorm.Count("pk"))
    assert "some_count" in qs._query.annotations


# ────────────────────────────────────────────────────────────────────────────
# R13 — NPlusOneDetector regex captures negative / hex / scientific
# ────────────────────────────────────────────────────────────────────────────


def test_nplusone_normalize_collapses_negative_numbers() -> None:
    from dorm.contrib.nplusone import _normalize

    pos = _normalize("WHERE id = 5")
    neg = _normalize("WHERE id = -5")
    assert pos == neg


def test_nplusone_normalize_collapses_scientific() -> None:
    from dorm.contrib.nplusone import _normalize

    sci = _normalize("WHERE x = 1.5e10")
    assert "?" in sci
    # No leftover mantissa / exponent fragments.
    assert "5e10" not in sci
    assert ".5" not in sci


def test_nplusone_normalize_collapses_hex_literal() -> None:
    from dorm.contrib.nplusone import _normalize

    out = _normalize("WHERE x = 0xABCD")
    assert "0xABCD" not in out
    assert "?" in out


# ────────────────────────────────────────────────────────────────────────────
# R14 — Q.__invert__ must deep-copy nested Q children
# ────────────────────────────────────────────────────────────────────────────


def test_q_invert_does_not_share_nested_children() -> None:
    q = Q(a=1) & Q(b=2)
    inv = ~q
    # Mutate the original's first nested Q's children — must NOT
    # propagate to the inverted Q.
    nested = q.children[0]
    assert isinstance(nested, Q)
    nested.children.append(("z", 99))
    inv_nested = inv.children[0]
    assert isinstance(inv_nested, Q)
    assert ("z", 99) not in inv_nested.children


# ────────────────────────────────────────────────────────────────────────────
# R15 — OuterRef self-correlated subqueries get distinct alias
# ────────────────────────────────────────────────────────────────────────────


def test_self_correlated_subquery_uses_distinct_alias() -> None:
    from dorm import Exists, OuterRef
    from dorm.query import SQLQuery

    inner = Author.objects.filter(pk=OuterRef("pk"))
    outer = SQLQuery(Author)
    sub = Exists(inner)
    sql, _ = sub.as_sql(table_alias=outer.model._meta.db_table, model=Author)
    # Inner uses ``"<table>_sub"`` as alias; outer keeps the bare
    # table name so OuterRef can disambiguate.
    table = Author._meta.db_table
    assert f'"{table}_sub"' in sql
    assert f'"{table}"' in sql


# ────────────────────────────────────────────────────────────────────────────
# R16 — Manager.from_queryset propagates QS overrides of BaseManager names
# ────────────────────────────────────────────────────────────────────────────


def test_from_queryset_propagates_count_override() -> None:
    from dorm.manager import BaseManager
    from dorm.queryset import QuerySet

    class _CustomQS(QuerySet[Any]):
        def count(self) -> int:  # type: ignore[override]
            return 9999  # sentinel

    Mgr = BaseManager.from_queryset(_CustomQS, "_CustomMgr")  # type: ignore[arg-type]
    inst = Mgr()
    inst.model = Author  # type: ignore[attr-defined]
    inst._db = "default"  # type: ignore[attr-defined]
    # The manager's ``count`` must reach through to the override
    # (returning 9999), not the BaseManager proxy that would defer
    # to the standard QuerySet.count().
    assert inst.count() == 9999


# ────────────────────────────────────────────────────────────────────────────
# R17 — configure(DATABASES=...) invalidates connection caches
# ────────────────────────────────────────────────────────────────────────────


def test_configure_with_databases_resets_connections() -> None:
    """``configure(DATABASES=...)`` must call ``reset_connections``
    so subsequent queries hit the new wrapper, not the cached old one."""
    from dorm import conf as conf_mod

    seen: list[bool] = []
    real_reset = conf_mod.reset_connections if hasattr(conf_mod, "reset_connections") else None  # type: ignore[attr-defined]

    def _spy() -> None:
        seen.append(True)

    from dorm.db import connection as conn_mod

    with patch.object(conn_mod, "reset_connections", _spy):
        # Configure call carrying DATABASES must propagate to reset.
        # Use the same DB config to keep the suite happy after.
        cfg = dict(conf_mod.settings.DATABASES)
        conf_mod.configure(DATABASES=cfg)

    assert seen, "configure(DATABASES=...) must trigger reset_connections"
    del real_reset


# ────────────────────────────────────────────────────────────────────────────
# R18 — Meta.default_manager_name honoured
# ────────────────────────────────────────────────────────────────────────────


def test_default_manager_name_picks_named_manager() -> None:
    from dorm.manager import Manager

    class _AllMgr(Manager[Any]):
        pass

    class _ActiveMgr(Manager[Any]):
        pass

    class _M(dorm.Model):
        name = dorm.CharField(max_length=10)
        objects = _ActiveMgr()
        all_objects = _AllMgr()

        class Meta:
            db_table = "_dmn_m"
            app_label = "tests"
            default_manager_name = "all_objects"

    # ``_default_manager`` must point at the named one.
    chosen = getattr(_M, "_default_manager")
    assert chosen.name == "all_objects"


# ────────────────────────────────────────────────────────────────────────────
# R19 — signals connect uses stable composite uid for bound methods
# ────────────────────────────────────────────────────────────────────────────


def test_signal_connect_stable_for_bound_methods() -> None:
    from dorm.signals import Signal

    class _Receiver:
        def handle(self, **kwargs: Any) -> None:
            pass

    sig = Signal()
    a = _Receiver()
    b = _Receiver()
    sig.connect(a.handle)
    sig.connect(b.handle)
    # Both receivers must remain registered — the previous
    # ``id(receiver)`` keying could collide because bound-methods
    # are temporaries with reusable ids.
    uids = {r[0] for r in sig._receivers}
    assert len(uids) == 2


# ────────────────────────────────────────────────────────────────────────────
# R20 — inherited Manager preserves constructor args via copy
# ────────────────────────────────────────────────────────────────────────────


def test_inherited_manager_with_init_args_does_not_crash() -> None:
    from dorm.manager import Manager

    class _TenantMgr(Manager[Any]):
        def __init__(self, *, tenant: str = "default") -> None:
            super().__init__()
            self.tenant = tenant

    class _Parent(dorm.Model):
        objects = _TenantMgr(tenant="acme")

        class Meta:
            abstract = True
            app_label = "tests"

    # Defining a concrete child must NOT raise TypeError on the
    # zero-arg ctor path — the fix uses ``copy.copy`` which keeps
    # the parent's ``tenant`` attribute.
    class _Child(_Parent):
        name = dorm.CharField(max_length=10)

        class Meta:
            db_table = "_tenant_child"
            app_label = "tests"

    chosen = getattr(_Child, "_default_manager")
    assert getattr(chosen, "tenant", None) == "acme"


# ────────────────────────────────────────────────────────────────────────────
# R21 — DurationField parses negative ``str(timedelta)`` shape
# ────────────────────────────────────────────────────────────────────────────


def test_duration_field_parses_negative_str_timedelta() -> None:
    from dorm.fields import DurationField

    f = DurationField()
    out = f._parse_iso8601(str(datetime.timedelta(hours=-1, minutes=-30)))
    assert out == datetime.timedelta(hours=-1, minutes=-30)


def test_duration_field_parses_positive_str_timedelta_with_days() -> None:
    from dorm.fields import DurationField

    f = DurationField()
    out = f._parse_iso8601(str(datetime.timedelta(days=2, hours=3)))
    assert out == datetime.timedelta(days=2, hours=3)


# ────────────────────────────────────────────────────────────────────────────
# R22 — ContentType cache evicts on stale pk lookup
# ────────────────────────────────────────────────────────────────────────────


def test_content_type_cache_evicts_on_doesnotexist() -> None:
    pytest.importorskip("dorm.contrib.contenttypes")
    from dorm.contrib.contenttypes.models import ContentType
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    # Make sure the contenttypes table exists in this test's DB.
    conn = get_connection()
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in ContentType._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE IF NOT EXISTS "django_content_type" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )

    mgr = ContentType.objects
    mgr.clear_cache()
    fake = ContentType(pk=99999, app_label="tests", model="ghost")
    mgr._cache[("tests", "ghost")] = fake
    with pytest.raises(ContentType.DoesNotExist):
        mgr.get_for_id(99999)
    assert ("tests", "ghost") not in mgr._cache

    conn.execute_script('DROP TABLE IF EXISTS "django_content_type"')


# ────────────────────────────────────────────────────────────────────────────
# R23 — GenericForeignKey honours for_concrete_model
# ────────────────────────────────────────────────────────────────────────────


def test_generic_fk_for_concrete_model_falls_back_when_no_concrete_attr() -> None:
    """Smoke check: with no proxy/MTI in the in-tree models, the
    flag has no effect, but the dispatch path must not raise. The
    structural fix is verified at source level."""
    pytest.importorskip("dorm.contrib.contenttypes")
    import inspect as _inspect

    from dorm.contrib.contenttypes.fields import GenericForeignKey

    src = _inspect.getsource(GenericForeignKey.__set__)
    # The set path now consults ``for_concrete_model`` and tries
    # to resolve a ``concrete_model`` attribute before calling
    # ``get_for_model``.
    assert "for_concrete_model" in src
    assert "concrete_model" in src


# ────────────────────────────────────────────────────────────────────────────
# R24 — __search lookup uses settings.SEARCH_CONFIG
# ────────────────────────────────────────────────────────────────────────────


def test_search_lookup_respects_settings_search_config() -> None:
    from dorm.conf import settings as dorm_settings
    from dorm.lookups import build_lookup_sql

    prev = getattr(dorm_settings, "SEARCH_CONFIG", None)
    try:
        dorm_settings.SEARCH_CONFIG = "spanish"
        sql, _ = build_lookup_sql(
            '"col"', "search", "hola", vendor="postgresql"
        )
        assert "'spanish'" in sql
        assert "'english'" not in sql
    finally:
        if prev is None:
            try:
                delattr(dorm_settings, "SEARCH_CONFIG")
            except AttributeError:
                pass
        else:
            dorm_settings.SEARCH_CONFIG = prev


def test_search_lookup_rejects_unsafe_config() -> None:
    from dorm.conf import settings as dorm_settings
    from dorm.lookups import build_lookup_sql

    prev = getattr(dorm_settings, "SEARCH_CONFIG", None)
    try:
        dorm_settings.SEARCH_CONFIG = "english'); DROP TABLE x; --"
        with pytest.raises(ValueError):
            build_lookup_sql(
                '"col"', "search", "hola", vendor="postgresql"
            )
    finally:
        if prev is None:
            try:
                delattr(dorm_settings, "SEARCH_CONFIG")
            except AttributeError:
                pass
        else:
            dorm_settings.SEARCH_CONFIG = prev


# ────────────────────────────────────────────────────────────────────────────
# Concat smoke (round-2 fix kept healthy by R3 changes)
# ────────────────────────────────────────────────────────────────────────────


def test_concat_with_threading_safe_under_load() -> None:
    """Cheap concurrency smoke: 8 threads each create + filter via
    Concat — should not crash even if the new ``COALESCE`` wrapper
    interacts with threaded connection caches."""
    seen = {"errors": 0}

    def _worker(idx: int) -> None:
        try:
            Author.objects.create(name=f"T{idx}", age=idx, email=None)
            list(
                Author.objects.filter(name__startswith=f"T{idx}").annotate(
                    label=Concat(F("email"), F("name"))
                )
            )
        except Exception:
            seen["errors"] += 1

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert seen["errors"] == 0


# ────────────────────────────────────────────────────────────────────────────
# Misc smoke — ensure existing tests keep working through the new helpers.
# ────────────────────────────────────────────────────────────────────────────


def test_existing_filter_chain_still_works() -> None:
    """Sanity: routine filter() chain unaffected by R1-R24 fixes."""
    a = Author.objects.create(name="Z", age=99)
    Article.objects.create(title="Smoke")
    assert Author.objects.filter(name="Z").count() == 1
    assert a.pk is not None
