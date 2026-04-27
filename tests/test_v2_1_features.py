"""Tests for djanorm 2.1.0 features:

- Subquery() / Exists() / OuterRef
- Window functions (Window, RowNumber, Rank, DenseRank, NTile, Lag, Lead)
- Non-recursive CTEs (with_cte)
- Extra SQL functions (Greatest, Least, Round, Trunc, Extract, Substr,
  Replace, StrIndex)
- CheckConstraint, UniqueConstraint(condition=)
- GeneratedField
- New Index types (GIN/GiST/BRIN/partial/expression — DDL emit only)
- Migration ops: AddIndex(concurrently=True), SetLockTimeout, ValidateConstraint
- URL/DSN parsing
- QuerySet.cursor_paginate
- inspectdb introspection
- dorm.contrib.search SearchVector/Query/Rank
"""
from __future__ import annotations

from typing import Any

import pytest

import dorm
from dorm.db.connection import get_connection
from tests.models import Author, Book, Publisher


# ── Helpers ─────────────────────────────────────────────────────────────────


def _seed_authors_books():
    p1 = Publisher.objects.create(name="P1")
    p2 = Publisher.objects.create(name="P2")
    alice = Author.objects.create(name="Alice", age=30, publisher=p1)
    bob = Author.objects.create(name="Bob", age=40, publisher=p2)
    carol = Author.objects.create(name="Carol", age=25, publisher=p1)

    Book.objects.create(title="Alpha", author=alice, pages=100, published=True)
    Book.objects.create(title="Beta", author=alice, pages=200, published=True)
    Book.objects.create(title="Gamma", author=bob, pages=50, published=False)
    Book.objects.create(title="Delta", author=bob, pages=300, published=True)
    return alice, bob, carol


# ── Subquery / Exists / OuterRef ────────────────────────────────────────────


class TestSubqueryAndExists:
    def test_exists_correlated(self):
        _seed_authors_books()
        # Authors who have at least one published book
        qs = Author.objects.filter(
            dorm.Exists(
                Book.objects.filter(author=dorm.OuterRef("pk"), published=True)
            )
        )
        names = sorted(a.name for a in qs)
        assert names == ["Alice", "Bob"]

    def test_not_exists(self):
        _seed_authors_books()
        # Authors with NO published book (= Carol)
        not_e = ~dorm.Exists(
            Book.objects.filter(author=dorm.OuterRef("pk"), published=True)
        )
        qs = Author.objects.filter(not_e)
        names = sorted(a.name for a in qs)
        assert names == ["Carol"]

    def test_exists_emits_correlated_sql(self):
        _seed_authors_books()
        qs = Author.objects.filter(
            dorm.Exists(Book.objects.filter(author=dorm.OuterRef("pk")))
        )
        conn = get_connection()
        sql, _params = qs._query.as_select(conn)
        # The subquery's WHERE should reference the outer table.
        assert 'EXISTS' in sql
        assert '"authors"."id"' in sql

    def test_outerref_field_other_than_pk(self):
        # filter(name=OuterRef("name")) — uses the outer column literally.
        _seed_authors_books()
        # Synthetic: same name on both sides → EXISTS resolves to all rows.
        qs = Author.objects.filter(
            dorm.Exists(Author.objects.filter(name=dorm.OuterRef("name")))
        )
        # All authors have themselves on both sides → all match.
        assert qs.count() == 3

    def test_subquery_in_filter(self):
        # A Subquery used as the RHS of an equality (=) filter.
        alice, _bob, _carol = _seed_authors_books()
        # Find authors whose pk equals a subquery returning alice's pk
        sub = Author.objects.filter(name="Alice").values("id")[:1]
        qs = Author.objects.filter(pk=dorm.Subquery(sub))
        assert [a.name for a in qs] == ["Alice"]


# ── Window functions ─────────────────────────────────────────────────────────


class TestWindowFunctions:
    def test_row_number_partition(self):
        _seed_authors_books()
        qs = Book.objects.annotate(
            rk=dorm.Window(
                dorm.RowNumber(),
                partition_by=["author_id"],
                order_by="-pages",
            )
        ).order_by("author_id", "rk")
        rows = list(qs)
        # Per author, sorted by pages desc → ranks 1..N
        ranks = [(b.title, b.__dict__["rk"]) for b in rows]
        # Alice's books: Beta (200) → 1, Alpha (100) → 2
        # Bob's books: Delta (300) → 1, Gamma (50) → 2
        names_with_rk = {(t, r) for (t, r) in ranks}
        assert ("Beta", 1) in names_with_rk
        assert ("Alpha", 2) in names_with_rk
        assert ("Delta", 1) in names_with_rk
        assert ("Gamma", 2) in names_with_rk

    def test_running_total(self):
        _seed_authors_books()
        qs = Book.objects.annotate(
            running=dorm.Window(
                dorm.Sum("pages"),
                partition_by=["author_id"],
                order_by="title",
            )
        )
        # Alice's books in title order: Alpha (100), Beta (200) → running 100, 300
        # Bob's books in title order: Delta (300), Gamma (50) → running 300, 350
        running = {b.title: b.__dict__["running"] for b in qs}
        assert running["Alpha"] == 100
        assert running["Beta"] == 300
        assert running["Delta"] == 300
        assert running["Gamma"] == 350

    def test_rank_requires_order_by(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            dorm.Window(dorm.RowNumber())  # no order_by → ill-defined

    def test_lag(self):
        _seed_authors_books()
        qs = Book.objects.filter(author__name="Alice").annotate(
            prev_pages=dorm.Window(
                dorm.Lag(dorm.F("pages")),
                partition_by=["author_id"],
                order_by="title",
            )
        )
        rows = {b.title: b.__dict__["prev_pages"] for b in qs}
        # Alpha is first → no prev → NULL
        assert rows["Alpha"] is None
        # Beta has Alpha as predecessor → 100
        assert rows["Beta"] == 100

    def test_ntile(self):
        _seed_authors_books()
        qs = Book.objects.annotate(
            quartile=dorm.Window(
                dorm.NTile(2),
                order_by="-pages",
            )
        )
        # 4 books split into 2 buckets → 2 in each.
        buckets = sorted(b.__dict__["quartile"] for b in qs)
        assert buckets == [1, 1, 2, 2]


# ── CTEs ─────────────────────────────────────────────────────────────────────


class TestCTEs:
    def test_simple_cte(self):
        _seed_authors_books()
        # CTE selects published books — outer query just runs over books
        # (the CTE is declared but unused in WHERE; we check the SQL emit).
        published = Book.objects.filter(published=True)
        qs = Book.objects.with_cte(pub=published)
        conn = get_connection()
        sql, _params = qs._query.as_select(conn)
        assert sql.startswith('WITH "pub" AS (')
        assert 'SELECT "books"."id"' in sql
        # Materialise — should still return all books (CTE unused in main).
        rows = list(qs)
        assert len(rows) == 4

    def test_invalid_cte_name(self):
        with pytest.raises(Exception):
            Book.objects.with_cte(**{"bad name": Book.objects.all()})

    def test_cte_rejects_non_queryset(self):
        # Route the deliberately-wrong runtime input through ``**kwargs``
        # so the static type checkers (mypy / ty) don't flag it — we
        # want to verify the runtime guard *fires*, not the static one.
        bad_kwargs: dict[str, Any] = {"x": "not a queryset"}
        with pytest.raises(TypeError):
            Book.objects.with_cte(**bad_kwargs)


# ── Extra SQL functions ──────────────────────────────────────────────────────


class TestExtraFunctions:
    def test_greatest_least(self):
        Author.objects.create(name="A", age=10)
        Author.objects.create(name="B", age=20)
        qs = Author.objects.annotate(
            big=dorm.Greatest(dorm.F("age"), dorm.Value(15)),
            small=dorm.Least(dorm.F("age"), dorm.Value(15)),
        )
        rows = sorted((a.name, a.__dict__["big"], a.__dict__["small"]) for a in qs)
        assert rows == [("A", 15, 10), ("B", 20, 15)]

    def test_round_decimals(self):
        Author.objects.create(name="A", age=42)
        qs = Author.objects.annotate(rnd=dorm.Round(dorm.F("age"), dorm.Value(0)))
        a = list(qs)[0]
        assert a.__dict__["rnd"] == 42

    def test_substr(self):
        Author.objects.create(name="HelloWorld", age=1)
        qs = Author.objects.annotate(
            head=dorm.Substr(dorm.F("name"), dorm.Value(1), dorm.Value(5))
        )
        a = list(qs)[0]
        assert a.__dict__["head"] == "Hello"

    def test_replace(self):
        Author.objects.create(name="foo bar baz", age=1)
        qs = Author.objects.annotate(
            r=dorm.Replace(dorm.F("name"), dorm.Value("bar"), dorm.Value("XX"))
        )
        a = list(qs)[0]
        assert a.__dict__["r"] == "foo XX baz"

    def test_strindex(self):
        Author.objects.create(name="hello", age=1)
        qs = Author.objects.annotate(p=dorm.StrIndex(dorm.F("name"), dorm.Value("ll")))
        a = list(qs)[0]
        # 1-based position of "ll" in "hello" = 3
        assert a.__dict__["p"] == 3

    def test_trunc_unit_validation(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            dorm.Trunc(dorm.F("age"), "millennium")

    def test_extract_unit_validation(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            dorm.Extract(dorm.F("age"), "fortnight")


# ── Cursor pagination ────────────────────────────────────────────────────────


class TestCursorPagination:
    def test_first_page_and_resume(self):
        for i in range(10):
            Author.objects.create(name=f"User{i:02d}", age=i)
        page = Author.objects.cursor_paginate(order_by="age", page_size=4)
        assert len(page) == 4
        assert page.has_next is True
        assert page.next_cursor is not None
        ages_page1 = [a.age for a in page]
        assert ages_page1 == [0, 1, 2, 3]

        page2 = Author.objects.cursor_paginate(
            order_by="age", page_size=4, after=page.next_cursor
        )
        assert [a.age for a in page2] == [4, 5, 6, 7]

        page3 = Author.objects.cursor_paginate(
            order_by="age", page_size=4, after=page2.next_cursor
        )
        # Last page has 2 rows → has_next False.
        assert [a.age for a in page3] == [8, 9]
        assert page3.has_next is False

    def test_descending(self):
        for i in range(5):
            Author.objects.create(name=f"U{i}", age=i)
        page = Author.objects.cursor_paginate(order_by="-age", page_size=3)
        assert [a.age for a in page] == [4, 3, 2]


# ── Constraints ──────────────────────────────────────────────────────────────


class TestConstraints:
    def test_check_constraint_sql_shape(self):
        c = dorm.CheckConstraint(check=dorm.Q(age__gt=0), name="age_pos")
        sql = c.constraint_sql("authors", get_connection())
        assert "ALTER TABLE" in sql
        assert "age_pos" in sql
        assert "CHECK" in sql

    def test_unique_constraint_basic(self):
        c = dorm.UniqueConstraint(fields=["name", "age"], name="uniq_name_age")
        conn = get_connection()
        sql = c.constraint_sql("authors", conn)
        assert "uniq_name_age" in sql
        if getattr(conn, "vendor", "sqlite") == "sqlite":
            # SQLite path uses CREATE UNIQUE INDEX.
            assert sql.startswith('CREATE UNIQUE INDEX')
        else:
            assert "ALTER TABLE" in sql
            assert "UNIQUE" in sql

    def test_partial_unique_constraint(self):
        c = dorm.UniqueConstraint(
            fields=["name"],
            name="uniq_active_name",
            condition=dorm.Q(is_active=True),
        )
        conn = get_connection()
        sql = c.constraint_sql("authors", conn)
        assert "CREATE UNIQUE INDEX" in sql
        assert "WHERE" in sql

    def test_constraint_invalid_name(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            dorm.CheckConstraint(check=dorm.Q(age__gt=0), name="bad name")  # space


# ── GeneratedField ───────────────────────────────────────────────────────────


class TestGeneratedField:
    def test_db_type_includes_generated_clause(self):
        f = dorm.GeneratedField(
            expression="age + 1", output_field=dorm.IntegerField()
        )
        # Need to call db_type with a fake connection.
        class _Conn:
            vendor = "postgresql"

        ddl = f.db_type(_Conn())
        assert "GENERATED ALWAYS AS" in ddl
        assert "age + 1" in ddl
        assert "STORED" in ddl

    def test_generated_field_rejects_unsafe_expression(self):
        with pytest.raises(dorm.ValidationError):
            dorm.GeneratedField(
                expression="age; DROP TABLE authors --",
                output_field=dorm.IntegerField(),
            )

    def test_generated_field_is_read_only(self):
        # Field instance directly: __set__ raises AttributeError on assignment.
        from dorm.fields import GeneratedField

        f = GeneratedField(expression="age + 1", output_field=dorm.IntegerField())
        f.attname = "computed"
        with pytest.raises(AttributeError):

            class _O:
                __dict__: dict = {}

            f.__set__(_O(), 99)


# ── Indexes ─────────────────────────────────────────────────────────────────


class TestIndexes:
    def test_partial_index_sql(self):
        idx = dorm.Index(
            fields=["email"],
            name="ix_active_email",
            condition=dorm.Q(is_active=True),
        )
        forward, reverse = idx.create_sql("authors", vendor="postgresql")
        assert "CREATE INDEX" in forward
        assert "WHERE" in forward
        assert "ix_active_email" in forward
        assert reverse == 'DROP INDEX IF EXISTS "ix_active_email"'

    def test_gin_method_emits_using(self):
        idx = dorm.Index(fields=["email"], name="ix_email_gin", method="gin")
        forward, _ = idx.create_sql("authors", vendor="postgresql")
        assert "USING gin" in forward

    def test_brin_method_sqlite_drops_using(self):
        idx = dorm.Index(fields=["email"], name="ix_email_brin", method="brin")
        forward, _ = idx.create_sql("authors", vendor="sqlite")
        # SQLite drops the USING clause silently.
        assert "USING" not in forward

    def test_expression_index_validates_grammar(self):
        # Allowed: simple FN(col) form
        idx = dorm.Index(fields=["LOWER(name)"], name="ix_lower_name")
        forward, _ = idx.create_sql("authors", vendor="postgresql")
        assert "LOWER(name)" in forward

    def test_expression_index_rejects_injection(self):
        with pytest.raises(ValueError):
            dorm.Index(
                fields=["LOWER(name); DROP TABLE x;--"], name="bad"
            )

    def test_invalid_method(self):
        with pytest.raises(ValueError):
            dorm.Index(fields=["name"], name="x", method="nonsense")


# ── DSN / URL ────────────────────────────────────────────────────────────────


class TestDatabaseURL:
    def test_postgres_url(self):
        cfg = dorm.parse_database_url(
            "postgres://alice:s3cret@db.example.com:6432/myapp?sslmode=require"
        )
        assert cfg["ENGINE"] == "postgresql"
        assert cfg["USER"] == "alice"
        assert cfg["PASSWORD"] == "s3cret"
        assert cfg["HOST"] == "db.example.com"
        assert cfg["PORT"] == 6432
        assert cfg["NAME"] == "myapp"
        assert cfg["OPTIONS"]["sslmode"] == "require"

    def test_postgres_url_lifts_pool_keys(self):
        cfg = dorm.parse_database_url(
            "postgresql://u:p@h/db?MAX_POOL_SIZE=20&POOL_TIMEOUT=5.5"
        )
        assert cfg["MAX_POOL_SIZE"] == 20
        assert cfg["POOL_TIMEOUT"] == 5.5

    def test_sqlite_url(self):
        cfg = dorm.parse_database_url("sqlite:///tmp/db.sqlite3")
        assert cfg["ENGINE"] == "sqlite"
        # Triple-slash absolute path → ``/tmp/db.sqlite3``.
        assert cfg["NAME"] in {"/tmp/db.sqlite3", "tmp/db.sqlite3"}

    def test_sqlite_memory(self):
        cfg = dorm.parse_database_url("sqlite://")
        assert cfg["NAME"] == ":memory:"

    def test_unrecognised_scheme(self):
        with pytest.raises(dorm.ImproperlyConfigured):
            dorm.parse_database_url("mysql://u:p@h/db")


# ── inspectdb ────────────────────────────────────────────────────────────────


class TestInspectDB:
    def test_introspects_existing_tables(self):
        _seed_authors_books()
        conn = get_connection()
        from dorm.inspect import introspect_tables, render_models

        tables = introspect_tables(conn)
        names = {t["name"] for t in tables}
        # Includes our seeded tables; excludes dorm_migrations.
        assert "authors" in names
        assert "books" in names
        assert "dorm_migrations" not in names

        rendered = render_models(tables)
        assert "class Authors" in rendered or "class Author" in rendered
        assert "import dorm" in rendered


# ── Search (PostgreSQL only) ────────────────────────────────────────────────


class TestSearch:
    def test_search_vector_compiles(self):
        from dorm.search import SearchVector

        sv = SearchVector("title", config="english")
        sql, params = sv.as_sql(table_alias="articles")
        assert "to_tsvector" in sql
        assert 'COALESCE("articles"."title"::text' in sql
        assert params == []

    def test_search_query_compiles(self):
        from dorm.search import SearchQuery

        sq = SearchQuery("hello world", search_type="plain")
        sql, params = sq.as_sql()
        assert sql.startswith("plainto_tsquery")
        assert params == ["hello world"]

    def test_search_rank_compiles(self):
        from dorm.search import SearchRank, SearchVector, SearchQuery

        sr = SearchRank(SearchVector("title"), SearchQuery("foo"))
        sql, params = sr.as_sql(table_alias="articles")
        assert "ts_rank(" in sql
        assert params == ["foo"]

    def test_invalid_config_rejected(self):
        from dorm.search import SearchVector

        with pytest.raises(dorm.ImproperlyConfigured):
            SearchVector("title", config="bad config; DROP")

    def test_invalid_search_type_rejected(self):
        from dorm.search import SearchQuery

        with pytest.raises(dorm.ImproperlyConfigured):
            SearchQuery("x", search_type="fuzzy")


# ── Migration helpers ────────────────────────────────────────────────────────


class TestMigrationHelpers:
    def test_set_lock_timeout_validates_input(self):
        from dorm.migrations.operations import SetLockTimeout

        with pytest.raises(ValueError):
            SetLockTimeout(ms=-1)

    def test_validate_constraint_validates_identifiers(self):
        from dorm.migrations.operations import ValidateConstraint

        with pytest.raises(dorm.ImproperlyConfigured):
            ValidateConstraint(table="bad name", name="x")

    def test_add_index_concurrently_pg_only_clause(self):
        from dorm.migrations.operations import AddIndex

        idx = dorm.Index(fields=["name"], name="ix_name")
        op = AddIndex("Author", idx, concurrently=True)

        class _PG:
            vendor = "postgresql"

            def execute_script(self, sql):
                self.last = sql

        c = _PG()
        sql_out = op._create_sql("authors", c)
        assert "CONCURRENTLY" in sql_out

        class _SL:
            vendor = "sqlite"

        sql_sl = op._create_sql("authors", _SL())
        # SQLite doesn't get CONCURRENTLY.
        assert "CONCURRENTLY" not in sql_sl

    def test_set_lock_timeout_emits_sql_on_pg(self):
        from dorm.migrations.operations import SetLockTimeout

        scripts: list[str] = []

        class _PG:
            vendor = "postgresql"

            def execute_script(self, sql):
                scripts.append(sql)

        SetLockTimeout(ms=2000).database_forwards("a", _PG(), None, None)
        assert any("lock_timeout" in s for s in scripts)

    def test_runpython_noop_is_callable(self):
        # The docs reference ``RunPython.noop`` as a reusable safe
        # ``reverse_code=``; it must (a) exist as an attribute and
        # (b) accept the (app_label, registry) contract without raising.
        from dorm.migrations.operations import RunPython

        assert callable(RunPython.noop)
        assert RunPython.noop("blog", {}) is None
        # Round-trip through a real RunPython instance — the apply path
        # must not break when noop is the reverse callable.
        op = RunPython(RunPython.noop, reverse_code=RunPython.noop)
        op.database_forwards("blog", object(), None, None)
        op.database_backwards("blog", object(), None, None)


# ── doctor ──────────────────────────────────────────────────────────────────


class TestDoctor:
    def test_doctor_runs_without_crash(self, capsys):
        import argparse
        from dorm.cli import cmd_doctor

        # Settings are already configured via the conftest fixtures.
        ns = argparse.Namespace(settings=None)
        try:
            cmd_doctor(ns)
        except SystemExit:
            # Doctor exits non-zero on warnings — that's normal.
            pass
        captured = capsys.readouterr()
        assert "dorm doctor" in captured.out
