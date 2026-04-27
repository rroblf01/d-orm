"""Coverage-driven tests for the 2.1 surface.

Targets the branches that the feature-level tests in
``test_v2_1_features.py`` skipped — equality / hashing, error
construction, alternative compile paths, edge inputs. The aim is
"raise the floor" rather than re-test the happy path: every test
here either pins a documented invariant or trips a corner case that
would otherwise be a silent regression.
"""
from __future__ import annotations

from typing import Any, cast

import pytest

import dorm
from dorm.constraints import BaseConstraint, CheckConstraint, UniqueConstraint
from dorm.expressions import Q
from dorm.indexes import Index


def _any(value: object) -> Any:
    """Return *value* typed as ``Any`` for the static checker.

    A small helper used in tests that deliberately pass wrong-typed
    runtime inputs to verify the runtime guard fires. Going through
    a function return type makes ty / mypy treat the result as
    ``Any`` rather than narrowing back to the literal type at the
    call site.
    """
    return cast(Any, value)


# ── Connection sentinels (no DB roundtrip needed for DDL / SQL emit) ─────


class _PG:
    """Stand-in for a PostgreSQL connection wrapper. Only ``vendor`` is
    inspected by the code under test, plus an ``execute_script`` sink
    when we want to capture emitted SQL."""

    vendor = "postgresql"

    def __init__(self) -> None:
        self.scripts: list[str] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)


class _SQLite:
    vendor = "sqlite"

    def __init__(self) -> None:
        self.scripts: list[str] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)


# ── Stub state for migration ops that consult ProjectState ───────────────


class _StubState:
    """Minimal shape that ``database_forwards``/``backwards`` consult."""

    def __init__(self, db_table: str | None = None, key: str = "blog.post") -> None:
        opts: dict[str, Any] = {}
        if db_table:
            opts["db_table"] = db_table
        # Annotate the nested shape explicitly — without it, ty
        # narrows the inner dict to a TypedDict-like literal and
        # rejects ``["constraints"] = []`` later.
        self.models: dict[str, dict[str, Any]] = {
            key: {"name": "Post", "fields": {}, "options": opts}
        }


# =================================================================
# constraints.py
# =================================================================


class TestBaseConstraintProtocol:
    """Coverage for ``BaseConstraint``'s default ``constraint_sql``,
    ``__eq__``, ``__hash__`` and ``describe`` paths that the concrete
    subclasses can leave untouched."""

    def test_default_constraint_sql_is_abstract(self):
        # Constructing the bare base is allowed (some users may want
        # to subclass), but the default ``constraint_sql`` raises.
        c = BaseConstraint(name="dummy")
        with pytest.raises(NotImplementedError):
            c.constraint_sql("authors", _PG())

    def test_default_remove_sql_uses_alter_table(self):
        # ``remove_sql`` on the base is the PG fallback that emits a
        # plain ``DROP CONSTRAINT IF EXISTS``. A custom subclass that
        # forgets to override gets a sensible default.
        c = BaseConstraint(name="x")
        sql = c.remove_sql("authors", _PG())
        assert sql == 'ALTER TABLE "authors" DROP CONSTRAINT IF EXISTS "x"'

    def test_eq_returns_notimplemented_for_other_types(self):
        c = BaseConstraint(name="x")
        # Equality with a non-BaseConstraint must return NotImplemented
        # (so Python falls back to ``other.__eq__(self)``); checking via
        # ``c == "x"`` would compare to False but loses the signal.
        assert c.__eq__(object()) is NotImplemented

    def test_describe_default_includes_class_name(self):
        c = BaseConstraint(name="x")
        s = c.describe()
        assert "BaseConstraint" in s and "x" in s

    def test_repr_falls_back_to_describe(self):
        c = BaseConstraint(name="my_c")
        assert repr(c) == c.describe()

    def test_hash_distinguishes_subclass(self):
        # Two distinct subclasses must hash differently even with the
        # same name — otherwise dict-keyed registries collapse them.
        a = CheckConstraint(check=Q(age__gt=0), name="x")
        b = UniqueConstraint(fields=["age"], name="x")
        assert hash(a) != hash(b)


class TestCheckConstraintEdges:
    def test_construction_rejects_non_q(self):
        # Route the wrong-typed value through ``**kwargs`` so the
        # static checker sees ``Any`` (the runtime guard is what
        # we're really pinning here).
        bad: dict[str, Any] = {"check": "age > 0", "name": "bad"}
        with pytest.raises(dorm.ImproperlyConfigured):
            CheckConstraint(**bad)

    def test_describe_hides_predicate(self):
        c = CheckConstraint(check=Q(age__gt=0), name="age_pos")
        # describe() must NOT leak the Q tree (which can be huge);
        # show only the name + a placeholder.
        d = c.describe()
        assert "age_pos" in d
        assert "Q(" not in d

    def test_eq_compares_only_name(self):
        # The implementation deliberately uses name as identity (the
        # autodetector keys constraints by name). A check predicate
        # change without a name change should NOT register as equal —
        # but the current contract is "name-based"; pin it.
        a = CheckConstraint(check=Q(age__gt=0), name="x")
        b = CheckConstraint(check=Q(age__lt=99), name="x")
        c = CheckConstraint(check=Q(age__gt=0), name="other")
        assert a == b  # same name → equal (by current contract)
        assert a != c
        # Also covers __hash__: equal-by-name → equal hash.
        assert hash(a) == hash(b)
        # And cross-type comparison → NotImplemented.
        assert a.__eq__("not a constraint") is NotImplemented

    def test_constraint_sql_inlines_param_values(self):
        c = CheckConstraint(check=Q(age__gt=18), name="adult")
        sql = c.constraint_sql("authors", _SQLite())
        # The 18 must be spliced as a literal, not left as ``%s`` —
        # ``execute_script`` doesn't bind params on DDL paths.
        assert "%s" not in sql
        assert "18" in sql

    def test_constraint_sql_no_params_path(self):
        # A predicate that compiles to a fragment with zero ``%s``
        # placeholders (e.g. an ``isnull`` lookup) must skip the
        # ``_inline_literal`` branch and emit verbatim.
        c = CheckConstraint(check=Q(name__isnull=False), name="name_present")
        sql = c.constraint_sql("authors", _SQLite())
        assert "%s" not in sql
        assert "IS NOT NULL" in sql


class TestUniqueConstraintEdges:
    def test_empty_fields_rejected(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            UniqueConstraint(fields=[], name="x")

    def test_invalid_field_name_rejected(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            UniqueConstraint(fields=["bad name"], name="x")

    def test_non_q_condition_rejected(self):
        bad: dict[str, Any] = {
            "fields": ["email"],
            "name": "x",
            "condition": "active = true",
        }
        with pytest.raises(dorm.ImproperlyConfigured):
            UniqueConstraint(**bad)

    def test_remove_sql_partial_uses_drop_index(self):
        # Partial unique constraints land as a unique index, so their
        # removal path is ``DROP INDEX`` regardless of vendor — the
        # plain ``DROP CONSTRAINT`` would fail on PG (the constraint
        # never existed; the index did).
        c = UniqueConstraint(
            fields=["email"], name="ix_a", condition=Q(active=True)
        )
        assert c.remove_sql("authors", _PG()).startswith("DROP INDEX")
        assert c.remove_sql("authors", _SQLite()).startswith("DROP INDEX")

    def test_remove_sql_plain_branches(self):
        c = UniqueConstraint(fields=["email"], name="uniq_email")
        assert c.remove_sql("authors", _SQLite()).startswith("DROP INDEX")
        assert "DROP CONSTRAINT" in c.remove_sql("authors", _PG())

    def test_constraint_sql_pg_plain(self):
        c = UniqueConstraint(fields=["a", "b"], name="u_ab")
        sql = c.constraint_sql("t", _PG())
        assert "ALTER TABLE" in sql and "UNIQUE" in sql
        # Both columns must appear, in declaration order.
        assert sql.index('"a"') < sql.index('"b"')

    def test_eq_compares_name_and_fields_and_condition_identity(self):
        # By contract: condition is compared with ``is``, not by value
        # (Q has no structural equality). So two UniqueConstraints
        # with the *same shape* of Q but different instances are NOT
        # equal — this is the documented behaviour and protects the
        # autodetector from emitting spurious AddConstraint+RemoveConstraint
        # cycles only when both name and condition object are unchanged.
        q = Q(active=True)
        a = UniqueConstraint(fields=["x"], name="u", condition=q)
        b = UniqueConstraint(fields=["x"], name="u", condition=q)
        assert a == b
        c = UniqueConstraint(fields=["x"], name="u", condition=Q(active=True))
        assert a != c
        assert a.__eq__(object()) is NotImplemented

    def test_hash_changes_with_field_set(self):
        a = UniqueConstraint(fields=["x"], name="u")
        b = UniqueConstraint(fields=["x", "y"], name="u")
        assert hash(a) != hash(b)

    def test_describe_shows_condition_marker(self):
        a = UniqueConstraint(fields=["x"], name="u")
        b = UniqueConstraint(fields=["x"], name="u", condition=Q(active=True))
        assert "condition" not in a.describe()
        assert "condition" in b.describe()


# =================================================================
# indexes.py
# =================================================================


class TestIndexAuxiliary:
    def test_get_name_strips_parens_for_expression_index(self):
        # ``LOWER(email)`` → identifier-safe suffix.
        idx = Index(fields=["LOWER(email)"])
        name = idx.get_name("Author")
        assert "(" not in name and ")" not in name
        # Sanity: the model name is folded in lower case as a prefix.
        assert "author" in name

    def test_get_name_explicit_overrides_auto(self):
        idx = Index(fields=["x"], name="ix_explicit")
        assert idx.get_name("ignored") == "ix_explicit"

    def test_name_property_empty_when_unset(self):
        # The public ``name`` property returns "" rather than None so
        # callers can do ``str.startswith`` without a None-guard.
        idx = Index(fields=["x"])
        assert idx.name == ""
        idx2 = Index(fields=["x"], name="set")
        assert idx2.name == "set"

    def test_column_sql_renders_desc_and_opclasses(self):
        idx = Index(
            fields=["-created_at", "user_id"],
            name="ix_x",
            opclasses=["timestamptz_ops", "int4_ops"],
        )
        forward, _ = idx.create_sql("authors", vendor="postgresql")
        # Descending order on the first column.
        assert '"created_at" DESC' in forward
        # Operator class spliced after the column.
        assert "timestamptz_ops" in forward
        assert "int4_ops" in forward

    def test_opclasses_length_mismatch(self):
        with pytest.raises(ValueError):
            Index(fields=["a", "b"], name="x", opclasses=["only_one"])

    def test_opclasses_invalid_identifier(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            Index(fields=["a"], name="x", opclasses=["bad name"])

    def test_condition_must_be_q(self):
        bad: dict[str, Any] = {
            "fields": ["a"],
            "name": "x",
            "condition": "active = true",
        }
        with pytest.raises(ValueError):
            Index(**bad)

    def test_empty_fields_rejected(self):
        with pytest.raises(ValueError):
            Index(fields=[], name="x")

    def test_eq_distinguishes_method_and_condition(self):
        a = Index(fields=["x"], name="ix")
        b = Index(fields=["x"], name="ix", method="gin")
        assert a != b
        c = Index(fields=["x"], name="ix", condition=Q(active=True))
        assert a != c
        # Cross-type — NotImplemented sentinel.
        assert a.__eq__("ix") is NotImplemented

    def test_hash_uses_method_for_disambiguation(self):
        a = Index(fields=["x"], name="ix")
        b = Index(fields=["x"], name="ix", method="hash")
        assert hash(a) != hash(b)

    def test_repr_collapses_extras(self):
        # Repr must surface the non-default knobs so a developer can
        # diff two index declarations by eyeball.
        idx = Index(
            fields=["x"],
            name="ix",
            method="gin",
            condition=Q(a=1),
            opclasses=["jsonb_path_ops"],
        )
        r = repr(idx)
        assert "method='gin'" in r
        assert "condition=..." in r
        assert "opclasses=" in r


# =================================================================
# search.py
# =================================================================


class TestSearchEdges:
    def test_search_vector_requires_at_least_one_field(self):
        from dorm.search import SearchVector

        with pytest.raises(dorm.ImproperlyConfigured):
            SearchVector()

    def test_search_vector_invalid_weight(self):
        from dorm.search import SearchVector

        with pytest.raises(dorm.ImproperlyConfigured):
            SearchVector("title", weight="Z")

    def test_search_vector_with_weight_emits_setweight(self):
        from dorm.search import SearchVector

        sv = SearchVector("title", weight="A")
        sql, params = sv.as_sql(table_alias="articles")
        assert sql.startswith("setweight(")
        assert ", 'A')" in sql
        assert params == []

    def test_search_vector_repr_round_trip(self):
        from dorm.search import SearchVector

        sv = SearchVector("title", "body", config="spanish", weight="A")
        r = repr(sv)
        assert "spanish" in r
        assert "'A'" in r

    def test_search_query_invert_double_negation(self):
        from dorm.search import SearchQuery

        q = SearchQuery("foo")
        assert q.invert is False
        inv = ~q
        assert inv.invert is True
        assert inv is not q  # __invert__ returns a new instance
        assert (~inv).invert is False

    def test_search_query_invert_emits_negation_prefix(self):
        from dorm.search import SearchQuery

        q = ~SearchQuery("foo")
        sql, params = q.as_sql()
        assert sql.startswith("!!")
        assert params == ["foo"]

    def test_search_query_repr_marks_inverted(self):
        from dorm.search import SearchQuery

        q = ~SearchQuery("foo")
        assert repr(q).startswith("~SearchQuery(")

    def test_search_rank_validates_argument_types(self):
        from dorm.search import SearchRank, SearchVector, SearchQuery

        with pytest.raises(dorm.ImproperlyConfigured):
            SearchRank(_any("not a vector"), SearchQuery("q"))
        with pytest.raises(dorm.ImproperlyConfigured):
            SearchRank(SearchVector("title"), _any("not a query"))

    def test_search_rank_cover_density(self):
        from dorm.search import SearchRank, SearchVector, SearchQuery

        sr = SearchRank(
            SearchVector("title"), SearchQuery("q"), cover_density=True
        )
        sql, _ = sr.as_sql()
        assert sql.startswith("ts_rank_cd(")

    def test_search_rank_repr_includes_components(self):
        from dorm.search import SearchRank, SearchVector, SearchQuery

        sr = SearchRank(SearchVector("title"), SearchQuery("q"))
        r = repr(sr)
        assert "SearchRank" in r
        assert "title" in r and "q" in r


# =================================================================
# migrations/operations.py — 2.1 surface
# =================================================================


class TestAddRemoveConstraintOps:
    def test_add_constraint_state_forwards_appends(self):
        from dorm.migrations.operations import AddConstraint

        c = CheckConstraint(check=Q(age__gt=0), name="ck")
        op = AddConstraint("Post", c)
        state = _StubState()
        op.state_forwards("blog", state)
        assert state.models["blog.post"]["options"]["constraints"] == [c]

    def test_add_constraint_state_forwards_silent_on_missing_model(self):
        # If the autodetector hands an op for a model that the state
        # doesn't yet know about (replay races), the op must not raise.
        from dorm.migrations.operations import AddConstraint

        op = AddConstraint("Ghost", CheckConstraint(check=Q(x=1), name="ck"))
        op.state_forwards("blog", _StubState())  # no "blog.ghost" → noop

    def test_add_constraint_uses_db_table_override(self):
        # When ``options["db_table"]`` is set on the model state, the
        # emitted DDL must target that name, not the conventional
        # ``app_modelname`` derivation.
        from dorm.migrations.operations import AddConstraint

        c = UniqueConstraint(fields=["email"], name="uq_email")
        op = AddConstraint("Post", c)
        state_with_table = _StubState(db_table="custom_posts")
        conn = _PG()
        op.database_forwards(
            "blog", conn, _StubState(), state_with_table
        )
        assert any('"custom_posts"' in s for s in conn.scripts)

    def test_add_constraint_describe_and_repr(self):
        from dorm.migrations.operations import AddConstraint

        c = CheckConstraint(check=Q(age__gt=0), name="ck")
        op = AddConstraint("Post", c)
        assert "Add constraint" in op.describe()
        assert "Post" in op.describe()
        assert "AddConstraint" in repr(op)

    def test_add_constraint_backwards_emits_remove(self):
        from dorm.migrations.operations import AddConstraint

        c = UniqueConstraint(fields=["email"], name="uq_email")
        op = AddConstraint("Post", c)
        from_state = _StubState()
        conn = _PG()
        op.database_backwards("blog", conn, from_state, _StubState())
        # Backwards path must emit a DROP-style statement.
        assert any("DROP" in s for s in conn.scripts)

    def test_remove_constraint_state_forwards_drops_by_name(self):
        from dorm.migrations.operations import RemoveConstraint

        c = CheckConstraint(check=Q(age__gt=0), name="ck")
        # State already lists the constraint.
        state = _StubState()
        state.models["blog.post"]["options"]["constraints"] = [c]
        op = RemoveConstraint("Post", c)
        op.state_forwards("blog", state)
        assert state.models["blog.post"]["options"]["constraints"] == []

    def test_remove_constraint_describe_and_repr(self):
        from dorm.migrations.operations import RemoveConstraint

        c = CheckConstraint(check=Q(age__gt=0), name="ck")
        op = RemoveConstraint("Post", c)
        assert "Remove constraint" in op.describe()
        assert "RemoveConstraint" in repr(op)

    def test_remove_constraint_database_paths(self):
        from dorm.migrations.operations import RemoveConstraint

        c = UniqueConstraint(fields=["email"], name="uq_email")
        op = RemoveConstraint("Post", c)
        from_state = _StubState()
        to_state = _StubState()
        conn = _PG()
        op.database_forwards("blog", conn, from_state, to_state)
        assert any("DROP" in s for s in conn.scripts)
        op.database_backwards("blog", conn, from_state, to_state)
        assert any("ADD CONSTRAINT" in s for s in conn.scripts)


class TestSetLockTimeout:
    def test_describe_includes_value(self):
        from dorm.migrations.operations import SetLockTimeout

        assert "2000ms" in SetLockTimeout(ms=2000).describe()

    def test_repr_round_trip(self):
        from dorm.migrations.operations import SetLockTimeout

        assert repr(SetLockTimeout(ms=2000)) == "SetLockTimeout(ms=2000)"

    def test_state_forwards_is_noop(self):
        from dorm.migrations.operations import SetLockTimeout

        # Pure-runtime op: state must NOT change.
        state = _StubState()
        before = dict(state.models["blog.post"])
        SetLockTimeout(ms=1000).state_forwards("blog", state)
        assert dict(state.models["blog.post"]) == before

    def test_sqlite_forwards_and_backwards_no_op(self):
        from dorm.migrations.operations import SetLockTimeout

        conn = _SQLite()
        SetLockTimeout(ms=500).database_forwards("blog", conn, None, None)
        SetLockTimeout(ms=500).database_backwards("blog", conn, None, None)
        # SQLite has no per-statement lock_timeout — both paths emit
        # nothing rather than raising.
        assert conn.scripts == []

    def test_pg_backwards_resets(self):
        from dorm.migrations.operations import SetLockTimeout

        conn = _PG()
        SetLockTimeout(ms=500).database_backwards("blog", conn, None, None)
        assert any("RESET lock_timeout" in s for s in conn.scripts)

    def test_invalid_ms_rejected(self):
        from dorm.migrations.operations import SetLockTimeout

        with pytest.raises(ValueError):
            SetLockTimeout(ms=-5)
        with pytest.raises(ValueError):
            SetLockTimeout(ms=_any("500"))


class TestValidateConstraint:
    def test_describe_and_repr(self):
        from dorm.migrations.operations import ValidateConstraint

        op = ValidateConstraint(table="orders", name="fk_user")
        assert "fk_user" in op.describe() and "orders" in op.describe()
        assert "ValidateConstraint" in repr(op)

    def test_state_forwards_is_noop(self):
        from dorm.migrations.operations import ValidateConstraint

        # Same shape as SetLockTimeout — state must be untouched.
        state = _StubState()
        ValidateConstraint(table="t", name="c").state_forwards("blog", state)

    def test_pg_runs_alter_table(self):
        from dorm.migrations.operations import ValidateConstraint

        conn = _PG()
        ValidateConstraint(table="orders", name="fk_user").database_forwards(
            "blog", conn, None, None
        )
        assert any("VALIDATE CONSTRAINT" in s for s in conn.scripts)
        assert any('"orders"' in s for s in conn.scripts)

    def test_sqlite_raises_not_implemented(self):
        from dorm.migrations.operations import ValidateConstraint

        with pytest.raises(NotImplementedError):
            ValidateConstraint(table="t", name="c").database_forwards(
                "blog", _SQLite(), None, None
            )

    def test_backwards_is_noop(self):
        from dorm.migrations.operations import ValidateConstraint

        # Validation has no inverse; the backward path must succeed
        # without trying to "unvalidate".
        conn = _PG()
        ValidateConstraint(table="t", name="c").database_backwards(
            "blog", conn, None, None
        )
        assert conn.scripts == []

    def test_invalid_identifiers_rejected(self):
        from dorm.migrations.operations import ValidateConstraint

        with pytest.raises(dorm.ImproperlyConfigured):
            ValidateConstraint(table="bad name", name="c")
        with pytest.raises(dorm.ImproperlyConfigured):
            ValidateConstraint(table="t", name="bad name")


class TestAddIndexConcurrentlyEdges:
    def test_concurrently_drop_on_pg(self):
        from dorm.migrations.operations import AddIndex

        idx = Index(fields=["a"], name="ix")
        op = AddIndex("M", idx, concurrently=True)
        conn = _PG()
        op.database_backwards("blog", conn, _StubState(), _StubState())
        assert any("DROP INDEX CONCURRENTLY" in s for s in conn.scripts)

    def test_concurrently_describe_and_repr(self):
        from dorm.migrations.operations import AddIndex

        op = AddIndex("M", Index(fields=["a"], name="ix"), concurrently=True)
        assert "CONCURRENTLY" in op.describe()
        assert "concurrently=True" in repr(op)

    def test_remove_index_concurrently_forward_and_back(self):
        from dorm.migrations.operations import RemoveIndex

        idx = Index(fields=["a"], name="ix")
        op = RemoveIndex("M", idx, concurrently=True)
        conn = _PG()
        op.database_forwards("blog", conn, _StubState(), _StubState())
        assert any("DROP INDEX CONCURRENTLY" in s for s in conn.scripts)

        # ``backwards`` must recreate via the index's create_sql path.
        conn2 = _PG()
        op.database_backwards("blog", conn2, _StubState(), _StubState())
        assert any("CREATE" in s and "INDEX" in s for s in conn2.scripts)

    def test_remove_index_repr_marks_concurrently(self):
        from dorm.migrations.operations import RemoveIndex

        op = RemoveIndex("M", Index(fields=["a"], name="ix"), concurrently=True)
        assert "concurrently=True" in repr(op)


# =================================================================
# query.py — as_subquery_sql edge paths
# =================================================================


class TestSubqueryCompileEdges:
    """Hit the GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET branches of
    :meth:`SQLQuery.as_subquery_sql` that the happy-path features test
    didn't reach. We exercise ``Subquery`` against a real model so the
    compiler sees a populated query, then assert on the emitted SQL
    instead of executing it (these queryshapes aren't all
    semantically-meaningful — what we want is structural coverage)."""

    def test_subquery_with_group_by_having_order_limit(self):
        from tests.models import Author

        from dorm import Subquery
        from dorm.db.connection import get_connection

        inner = Author.objects.filter(age__gte=1)
        # Mutate the inner SQLQuery directly to exercise branches that
        # the public chain (filter / order_by) doesn't cover from a
        # plain QuerySet.
        inner._query.group_by_fields = ["id"]
        inner._query.having_nodes = list(inner._query.where_nodes)
        inner._query.order_by_fields = ["-id"]
        inner._query.limit_val = 5
        inner._query.offset_val = 1

        sub = Subquery(inner)
        sql, params = sub.as_sql(table_alias="authors", model=Author)
        assert "GROUP BY" in sql
        assert "HAVING" in sql
        assert "ORDER BY" in sql
        assert "LIMIT 5" in sql
        assert "OFFSET 1" in sql
        # Sanity — placeholder count matches param count.
        assert sql.count("%s") == len(params)
        # The connection rewrite step still runs once per outer query;
        # that's not exercised here, but the compiler must keep the
        # raw ``%s`` form so that step has something to rewrite.
        assert get_connection() is not None  # touch fixture path

    def test_subquery_with_alias_only_annotation_skipped(self):
        from tests.models import Author

        from dorm import Count, Subquery

        # ``alias()`` registers an annotation with the same shape as
        # ``annotate`` but flags it as ``alias_only`` — the inner
        # compiler must still walk it for side effects but skip it
        # from the projection. Hits the ``continue`` branch.
        inner = Author.objects.alias(c=Count("pk")).filter(c__gte=0)
        sub = Subquery(inner)
        sql, _ = sub.as_sql(table_alias="authors", model=Author)
        # The alias-only annotation must NOT appear in the projection.
        assert ' AS "c"' not in sql

    def test_outer_ref_outside_subquery_raises(self):
        from dorm import OuterRef
        from dorm.query import SQLQuery
        from tests.models import Book

        q = SQLQuery(Book)
        # Compiling an OuterRef without ``_outer_alias`` set must fail
        # loudly — otherwise we'd silently emit a malformed subquery.
        with pytest.raises(ValueError):
            q._resolve_outer_ref(OuterRef("pk"))

    def test_outer_ref_resolves_pk_via_outer_model(self):
        from dorm import OuterRef
        from dorm.query import SQLQuery
        from tests.models import Author, Book

        q = SQLQuery(Book)
        q._outer_alias = "authors"
        q._outer_model = Author
        # ``OuterRef("pk")`` must translate to the outer model's
        # primary-key column, not the literal "pk".
        sql = q._resolve_outer_ref(OuterRef("pk"))
        assert sql == '"authors"."id"'

    def test_outer_ref_resolves_field_via_outer_model(self):
        # When the outer model is known, named fields resolve to their
        # underlying column (so ``OuterRef("author")`` → ``author_id``
        # for an FK), not to the attribute name.
        from dorm import OuterRef
        from dorm.query import SQLQuery
        from tests.models import Book

        q = SQLQuery(Book)
        q._outer_alias = "books"
        q._outer_model = Book
        sql = q._resolve_outer_ref(OuterRef("author"))
        # The Book.author FK has ``column = "author_id"``.
        assert sql == '"books"."author_id"'


# =================================================================
# inspect.py — type mapping fallbacks
# =================================================================


class TestInspectTypeMapping:
    def test_unknown_type_falls_back_to_textfield_with_marker(self):
        from dorm.inspect import _map_type, render_models

        cls, kwargs = _map_type("geometry(Point, 4326)", "postgresql")
        assert cls == "TextField"
        # The marker is consumed by render_models to emit a NOTE
        # comment but should not appear as a constructor kwarg.
        assert "_inspect_unknown" in kwargs

        # Now exercise the comment path through render_models.
        rendered = render_models(
            [
                {
                    "name": "weird",
                    "vendor": "postgresql",
                    "fks": {},
                    "columns": [
                        {"name": "shape", "data_type": "geometry"},
                    ],
                }
            ]
        )
        assert "unrecognised" in rendered

    def test_lengthed_varchar_recovers_max_length(self):
        from dorm.inspect import _map_type

        cls, kwargs = _map_type("character varying(123)", "postgresql")
        assert cls == "CharField" and kwargs["max_length"] == 123

    def test_numeric_recovers_precision(self):
        from dorm.inspect import _map_type

        cls, kwargs = _map_type("numeric(10,2)", "postgresql")
        assert cls == "DecimalField"
        assert kwargs["max_digits"] == 10 and kwargs["decimal_places"] == 2

    def test_sqlite_prefix_match(self):
        from dorm.inspect import _map_type

        # SQLite types may carry a parenthesised modifier we don't
        # recover — the prefix match path handles them.
        cls, _ = _map_type("INTEGER PRIMARY KEY", "sqlite")
        assert cls == "IntegerField"

    def test_to_class_name_handles_separators(self):
        from dorm.inspect import _to_class_name

        assert _to_class_name("blog_post") == "BlogPost"
        # Non-identifier chars get split out.
        assert _to_class_name("blog-post") == "BlogPost"
        # Empty / weird input still produces a valid identifier.
        assert _to_class_name("") == "Table"

    def test_render_models_emits_fk_with_relation(self):
        from dorm.inspect import render_models

        rendered = render_models(
            [
                {
                    "name": "post",
                    "vendor": "sqlite",
                    "fks": {"author_id": "author"},
                    "columns": [
                        {"name": "id", "type": "INTEGER", "pk": 1, "notnull": 1},
                        {
                            "name": "author_id",
                            "type": "INTEGER",
                            "pk": 0,
                            "notnull": 1,
                        },
                    ],
                }
            ]
        )
        assert "ForeignKey('Author'" in rendered
        # The FK is generated with on_delete=CASCADE by default.
        assert "on_delete=dorm.CASCADE" in rendered

    def test_render_models_pk_note_when_missing(self):
        from dorm.inspect import render_models

        rendered = render_models(
            [
                {
                    "name": "headless",
                    "vendor": "sqlite",
                    "fks": {},
                    "columns": [
                        {"name": "label", "type": "TEXT", "pk": 0, "notnull": 0},
                    ],
                }
            ]
        )
        # When no column claims primary_key, the renderer leaves a
        # NOTE so the user knows dorm will inject a default PK.
        assert "did not find an explicit PK" in rendered

    def test_render_models_includes_meta_db_table(self):
        from dorm.inspect import render_models

        rendered = render_models(
            [
                {
                    "name": "weird_name_42",
                    "vendor": "sqlite",
                    "fks": {},
                    "columns": [
                        {"name": "id", "type": "INTEGER", "pk": 1, "notnull": 1},
                    ],
                }
            ]
        )
        assert "db_table = 'weird_name_42'" in rendered

    def test_render_models_empty_body_emits_pass(self):
        from dorm.inspect import render_models

        # A table with zero recoverable columns → ``pass`` body so the
        # generated source is at least syntactically valid.
        rendered = render_models(
            [
                {"name": "ghost", "vendor": "sqlite", "fks": {}, "columns": []},
            ]
        )
        assert "    pass" in rendered


# =================================================================
# conf.py — URL/DSN edge paths
# =================================================================


class TestDatabaseURLEdges:
    def test_configure_with_url_string_alias(self):
        # The configure() shortcut accepts a bare URL string per alias
        # — not just a dict. Pin that path.
        from dorm.conf import settings as s

        # Snapshot then restore so the rest of the suite isn't
        # destabilised.
        prev = dict(getattr(s, "DATABASES", {}))
        try:
            dorm.configure(
                DATABASES={"x": "sqlite://"}, INSTALLED_APPS=[]
            )
            cfg = s.DATABASES["x"]
            assert cfg["ENGINE"] == "sqlite"
            assert cfg["NAME"] == ":memory:"
        finally:
            dorm.configure(DATABASES=prev or {}, INSTALLED_APPS=[])

    def test_configure_with_url_dict_overrides_win(self):
        from dorm.conf import settings as s

        prev = dict(getattr(s, "DATABASES", {}))
        try:
            dorm.configure(
                DATABASES={
                    "x": {
                        "URL": "postgres://u:p@h/db?MAX_POOL_SIZE=20",
                        "MAX_POOL_SIZE": 30,
                    }
                },
                INSTALLED_APPS=[],
            )
            assert s.DATABASES["x"]["MAX_POOL_SIZE"] == 30
            # User key takes precedence over URL-derived value.
        finally:
            dorm.configure(DATABASES=prev or {}, INSTALLED_APPS=[])

    def test_url_with_pool_check_truthy_strings(self):
        # The boolean ``POOL_CHECK`` knob accepts a small allowlist of
        # truthy spellings — exercising each guards against a future
        # regex tweak silently turning "yes" off.
        cfg_y = dorm.parse_database_url(
            "postgres://u:p@h/db?POOL_CHECK=yes"
        )
        cfg_n = dorm.parse_database_url(
            "postgres://u:p@h/db?POOL_CHECK=no"
        )
        assert cfg_y["POOL_CHECK"] is True
        assert cfg_n["POOL_CHECK"] is False

    def test_url_default_port(self):
        # ``postgres://u:p@host/db`` (no explicit port) must default
        # to 5432, not whatever urlparse leaves as None.
        cfg = dorm.parse_database_url("postgres://u:p@host/db")
        assert cfg["PORT"] == 5432

    def test_sqlite_url_with_query_lands_in_options(self):
        cfg = dorm.parse_database_url("sqlite:///tmp/db.sqlite3?check_same_thread=0")
        assert cfg["OPTIONS"]["check_same_thread"] == "0"


# =================================================================
# CLI — doctor output paths
# =================================================================


class TestDoctorAuditPaths:
    """Drive ``dorm doctor`` through synthetic settings to hit branches
    the conftest-driven happy path doesn't naturally reach (small pool,
    huge timeout, missing sslmode, MIN > MAX)."""

    def _run_doctor(self, settings_overrides: dict) -> tuple[str, int]:
        """Run ``cmd_doctor`` against a temporary settings snapshot.

        Returns ``(stdout, exit_code)`` — exit code is 0 when no
        warnings fired, 1 otherwise (the documented contract).
        """
        import argparse
        import contextlib
        import io

        from dorm.cli import cmd_doctor
        from dorm.conf import settings as s

        prev_dbs = dict(getattr(s, "DATABASES", {}))
        prev_apps = list(getattr(s, "INSTALLED_APPS", []))
        # Apply override.
        s.configure(**settings_overrides)
        try:
            buf = io.StringIO()
            code = 0
            with contextlib.redirect_stdout(buf):
                try:
                    cmd_doctor(argparse.Namespace(settings=None))
                except SystemExit as exc:
                    code = int(exc.code or 0)
            return buf.getvalue(), code
        finally:
            s.configure(DATABASES=prev_dbs, INSTALLED_APPS=prev_apps)

    def test_doctor_warns_on_small_pool(self):
        out, code = self._run_doctor(
            {
                "DATABASES": {
                    "default": {
                        "ENGINE": "postgresql",
                        "NAME": "x",
                        "USER": "u",
                        "PASSWORD": "p",
                        "HOST": "remote-db.example.com",
                        "PORT": 5432,
                        "MAX_POOL_SIZE": 2,
                    }
                },
                "INSTALLED_APPS": [],
            }
        )
        assert "MAX_POOL_SIZE" in out
        # Remote host without sslmode must also fire.
        assert "sslmode" in out
        assert code == 1

    def test_doctor_warns_on_min_above_max(self):
        out, code = self._run_doctor(
            {
                "DATABASES": {
                    "default": {
                        "ENGINE": "postgresql",
                        "NAME": "x",
                        "MIN_POOL_SIZE": 50,
                        "MAX_POOL_SIZE": 5,
                        "HOST": "localhost",
                    }
                },
                "INSTALLED_APPS": [],
            }
        )
        assert "MIN_POOL_SIZE > MAX_POOL_SIZE" in out
        assert code == 1

    def test_doctor_warns_on_long_pool_timeout(self):
        out, code = self._run_doctor(
            {
                "DATABASES": {
                    "default": {
                        "ENGINE": "postgresql",
                        "NAME": "x",
                        "MAX_POOL_SIZE": 10,
                        "POOL_TIMEOUT": 600.0,
                        "HOST": "localhost",
                    }
                },
                "INSTALLED_APPS": [],
            }
        )
        assert "POOL_TIMEOUT" in out
        assert code == 1

    def test_doctor_pool_check_false_emits_note(self):
        # POOL_CHECK=False is documented as a *note*, not a warning —
        # i.e. it appears under the "notes:" header rather than the
        # "warnings:" one. Other warnings (e.g. unindexed FKs from
        # the test models loaded by the conftest) may still bump the
        # exit code, so we only assert the note's text is present.
        out, _code = self._run_doctor(
            {
                "DATABASES": {
                    "default": {
                        "ENGINE": "postgresql",
                        "NAME": "x",
                        "MAX_POOL_SIZE": 10,
                        "POOL_TIMEOUT": 30.0,
                        "POOL_CHECK": False,
                        "HOST": "localhost",
                    }
                },
                "INSTALLED_APPS": [],
            }
        )
        # The note text must appear under the ``notes:`` header, which
        # rules out a misclassification as a warning.
        assert "POOL_CHECK=False" in out
        assert "notes:" in out
        notes_idx = out.index("notes:")
        warns_idx = out.find("warnings:")
        # When warnings are present, the doctor prints them BEFORE
        # notes (deterministic by code order). Either way, our note
        # should land after the ``notes:`` header.
        assert out.index("POOL_CHECK=False") > notes_idx
        if warns_idx >= 0:
            assert out.index("POOL_CHECK=False") > warns_idx


# =================================================================
# RunPython.noop already covered in test_v2_1_features; describe extra
# =================================================================


# =================================================================
# fields._inline_literal — every value-shape branch
# =================================================================


class TestInlineLiteral:
    """The constraint-emit path inlines parameter values into DDL
    (``ALTER TABLE ... CHECK ($1)`` doesn't bind on every backend).
    Each value shape has its own escape contract; pin them so a
    refactor can't silently break a CheckConstraint with a string
    literal containing a quote."""

    def test_none_renders_as_null(self):
        from dorm.fields import _inline_literal

        assert _inline_literal("col = %s", [None]) == "col = NULL"

    def test_bool_true_and_false(self):
        from dorm.fields import _inline_literal

        assert _inline_literal("a = %s", [True]) == "a = TRUE"
        assert _inline_literal("a = %s", [False]) == "a = FALSE"

    def test_int_and_float(self):
        from dorm.fields import _inline_literal

        assert _inline_literal("a = %s", [42]) == "a = 42"
        assert _inline_literal("a = %s", [3.14]) == "a = 3.14"

    def test_string_quotes_doubled(self):
        from dorm.fields import _inline_literal

        # The SQL literal escape for a single quote is doubling it;
        # otherwise the resulting DDL would be syntactically invalid
        # AND a potential injection vector.
        out = _inline_literal("a = %s", ["O'Brien"])
        assert out == "a = 'O''Brien'"

    def test_string_with_nul_rejected(self):
        from dorm.fields import _inline_literal

        # NUL bytes cannot appear in a SQL string literal — must
        # raise loudly rather than emit broken DDL.
        with pytest.raises(ValueError):
            _inline_literal("a = %s", ["x\x00y"])

    def test_other_types_fall_back_to_str_quoted(self):
        from dorm.fields import _inline_literal
        import datetime as _dt

        # A datetime is neither bool/int/float/str/None; the path
        # quotes its ``str()`` form.
        out = _inline_literal("a = %s", [_dt.date(2026, 1, 1)])
        assert out == "a = '2026-01-01'"

    def test_more_placeholders_than_params(self):
        from dorm.fields import _inline_literal

        with pytest.raises(ValueError):
            _inline_literal("a = %s AND b = %s", [1])

    def test_unconsumed_params(self):
        from dorm.fields import _inline_literal

        with pytest.raises(ValueError):
            _inline_literal("a = %s", [1, 2])


# =================================================================
# fields._NotProvided sentinel
# =================================================================


class TestNotProvidedSentinel:
    def test_singleton_via_copy_and_deepcopy(self):
        import copy
        from dorm.fields import NOT_PROVIDED

        # The sentinel must survive copy / deepcopy as itself —
        # otherwise deepcopying a Field would silently turn its
        # "no default" marker into a different object that breaks
        # ``has_default()``.
        assert copy.copy(NOT_PROVIDED) is NOT_PROVIDED
        assert copy.deepcopy(NOT_PROVIDED) is NOT_PROVIDED

    def test_repr(self):
        from dorm.fields import NOT_PROVIDED

        assert repr(NOT_PROVIDED) == "NOT_PROVIDED"


# =================================================================
# fields.GeneratedField — read paths
# =================================================================


class TestGeneratedFieldReadPaths:
    def test_to_python_delegates_to_output_field(self):
        # to_python must round-trip through the output_field — that's
        # how INSERT/UPDATE-time hydration sees a typed value.
        f = dorm.GeneratedField(
            expression="age + 1", output_field=dorm.IntegerField()
        )
        assert f.to_python("42") == 42  # IntegerField coerces strings

    def test_from_db_value_delegates_when_supported(self):
        # JSONField has from_db_value (str → dict); GeneratedField must
        # delegate for the storage round-trip to work on read.
        f = dorm.GeneratedField(
            expression="data", output_field=dorm.JSONField()
        )
        assert f.from_db_value('{"a": 1}') == {"a": 1}

    def test_pre_save_returns_none(self):
        # The DB computes the value; the Python-side ``pre_save`` must
        # return None so the INSERT/UPDATE plan skips the column.
        f = dorm.GeneratedField(
            expression="x + 1", output_field=dorm.IntegerField()
        )
        f.attname = "x"

        class _Holder:
            __dict__: dict = {"x": 99}

        assert f.pre_save(_Holder(), add=True) is None

    def test_get_db_prep_value_returns_none(self):
        # Symmetric: never bind a value for a generated column.
        f = dorm.GeneratedField(
            expression="x + 1", output_field=dorm.IntegerField()
        )
        assert f.get_db_prep_value(123) is None


# =================================================================
# fields.GenericIPAddressField — error path
# =================================================================


class TestIPAddressValidation:
    def test_invalid_ip_raises_validation_error(self):
        f = dorm.GenericIPAddressField()
        with pytest.raises(dorm.ValidationError):
            f.to_python("not.an.ip.at.all")

    def test_none_passes_through(self):
        f = dorm.GenericIPAddressField()
        assert f.to_python(None) is None


# =================================================================
# queryset.acursor_paginate — async path mirror
# =================================================================


class TestAsyncCursorPagination:
    async def test_async_cursor_paginate_basic(self):
        from tests.models import Author

        # Seed a few rows.
        for i in range(7):
            await Author.objects.acreate(name=f"User{i:02d}", age=i)

        page = await Author.objects.acursor_paginate(
            order_by="age", page_size=3
        )
        ages = [a.age for a in page]
        assert ages == [0, 1, 2]
        assert page.has_next is True

        page2 = await Author.objects.acursor_paginate(
            order_by="age", page_size=3, after=page.next_cursor
        )
        assert [a.age for a in page2] == [3, 4, 5]

        page3 = await Author.objects.acursor_paginate(
            order_by="age", page_size=3, after=page2.next_cursor
        )
        # Final partial page → no more cursor.
        assert [a.age for a in page3] == [6]
        assert page3.has_next is False

    async def test_async_cursor_paginate_rejects_zero_page_size(self):
        from tests.models import Author

        with pytest.raises(ValueError):
            await Author.objects.acursor_paginate(page_size=0)


# =================================================================
# queryset.aaggregate — empty-result branch
# =================================================================


class TestAggregateEmptyTable:
    """Cover the empty-result branch of :meth:`QuerySet.aggregate` /
    :meth:`QuerySet.aaggregate`. With at least one aggregate kwarg the
    SQL is well-formed and runs against the live table; an empty
    result set materialises only when the WHERE clause filters
    everything out."""

    def test_aggregate_count_on_empty_filter(self):
        from dorm import Count
        from tests.models import Author

        Author.objects.create(name="A", age=1)
        result = Author.objects.filter(age=-999).aggregate(c=Count("pk"))
        # COUNT(*) over zero rows is 0 — exercises the row[0] branch
        # with a real result, not the ``return {}`` fallback.
        assert result == {"c": 0}

    async def test_aaggregate_count_on_empty_filter(self):
        from dorm import Count
        from tests.models import Author

        await Author.objects.acreate(name="A", age=1)
        result = await Author.objects.filter(age=-999).aaggregate(
            c=Count("pk")
        )
        assert result == {"c": 0}


# =================================================================
# queryset.aupdate_or_create — IntegrityError race-retry path
# =================================================================


class TestAUpdateOrCreateRace:
    """The race-retry branch fires when two concurrent callers both
    take the ``DoesNotExist`` path and one of them sneaks in an
    INSERT before the other commits. We simulate it by patching
    ``acreate`` to raise IntegrityError on the first call."""

    async def test_async_race_retry_path(self, monkeypatch):
        from tests.models import Author
        from dorm.exceptions import IntegrityError

        # Seed a row so the post-IntegrityError ``aget`` succeeds.
        existing = await Author.objects.acreate(name="Alice", age=30)

        original = Author.objects.get_queryset().acreate
        calls = {"n": 0}

        async def flaky_acreate(self_, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                # First attempt: simulate a concurrent INSERT.
                raise IntegrityError("duplicate")
            return await original(**kwargs)

        # Patch the QuerySet method (Manager.aupdate_or_create proxies
        # through to QuerySet.aupdate_or_create which calls
        # ``self.acreate`` after the DoesNotExist branch).
        from dorm.queryset import QuerySet

        monkeypatch.setattr(QuerySet, "acreate", flaky_acreate)

        obj, created = await Author.objects.aupdate_or_create(
            name="Alice", defaults={"age": 99}
        )
        # Race resolution: the existing row wins, defaults applied.
        assert created is False
        assert obj.pk == existing.pk
        assert obj.age == 99


# =================================================================
# CLI — argv parsing edges
# =================================================================


class TestCLISmoke:
    """Hit a few CLI dispatch paths that don't need a live DB.

    We bypass ``main()`` by calling the parsed ``argparse.Namespace``
    handlers directly — the dispatcher itself is well-covered by
    ``tests/test_cli.py``."""

    def test_cmd_help_prints_subcommands(self, capsys):
        import argparse
        from dorm.cli import cmd_help, main  # noqa: F401

        parser = argparse.ArgumentParser()
        # Minimal subparser surface so the help output isn't empty.
        sub = parser.add_subparsers(dest="command")
        sub.add_parser("doctor")
        ns = argparse.Namespace(parser=parser)
        cmd_help(ns)
        out = capsys.readouterr().out
        assert "doctor" in out

    def test_inspectdb_no_tables_warns(self, capsys, monkeypatch):
        import argparse
        from dorm.cli import cmd_inspectdb

        # Patch introspect_tables to return no user tables.
        from dorm import inspect as _inspect

        monkeypatch.setattr(_inspect, "introspect_tables", lambda conn: [])
        ns = argparse.Namespace(settings=None, database="default")
        cmd_inspectdb(ns)
        captured = capsys.readouterr()
        # The "no tables" message goes to stderr, not stdout.
        assert "no user tables" in captured.err


# =================================================================
# RunPython.noop already covered in test_v2_1_features; describe extra
# =================================================================


class TestRunPythonExtras:
    def test_runpython_describe_uses_function_name(self):
        from dorm.migrations.operations import RunPython

        def my_step(app_label, registry):
            return None

        op = RunPython(my_step, reverse_code=RunPython.noop)
        assert "my_step" in op.describe()

    def test_runpython_repr_round_trip(self):
        from dorm.migrations.operations import RunPython

        op = RunPython(RunPython.noop, reverse_code=RunPython.noop)
        # repr is for human inspection, must include the class name.
        assert "RunPython" in repr(op)

    def test_runpython_state_forwards_is_noop(self):
        from dorm.migrations.operations import RunPython

        # RunPython never changes schema state.
        state = _StubState()
        before = dict(state.models)
        RunPython(RunPython.noop).state_forwards("blog", state)
        assert dict(state.models) == before

    def test_runpython_no_reverse_skips_backwards(self):
        from dorm.migrations.operations import RunPython

        # When reverse_code is None, ``database_backwards`` returns
        # quietly — it does NOT fall back to running ``code``.
        called: list[str] = []

        def fwd(app_label, registry):
            called.append("fwd")

        op = RunPython(fwd)  # reverse_code defaults to None
        op.database_backwards("blog", object(), None, None)
        assert called == []
