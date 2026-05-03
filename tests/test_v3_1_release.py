"""End-to-end coverage for features that ship in v3.1.0.

3.1 closes the last Django-parity gaps that smoke testing surfaced
in 3.0:

- Reverse-FK filter / aggregate through ``<related_name>`` /
  ``<lower>_set``
- JSON path traversal in lookups
- Manager-level ``using(alias)``
- ``db_default=`` on every Field for server-side defaults
- Window function family extended (``NthValue``, ``PercentRank``,
  ``CumeDist``)
"""

from __future__ import annotations

import pytest

import dorm


# ──────────────────────────────────────────────────────────────────────────────
# Reverse-FK filter + aggregate now wired through join machinery
# ──────────────────────────────────────────────────────────────────────────────


def test_reverse_fk_filter_emits_left_join():
    from tests.models import Author, Book

    a = Author.objects.create(name="A1", age=10, email="a1@e.com")
    Author.objects.create(name="A2", age=20, email="a2@e.com")  # no books
    Book.objects.create(title="alpha", author=a, pages=1)
    Book.objects.create(title="beta", author=a, pages=2)

    rows = list(Author.objects.filter(book_set__title="alpha"))
    assert sorted(r.name for r in rows) == ["A1"]


def test_reverse_fk_count_aggregate():
    from dorm import Count
    from tests.models import Author, Book

    a1 = Author.objects.create(name="A1", age=10, email="a1@e.com")
    a2 = Author.objects.create(name="A2", age=20, email="a2@e.com")
    Book.objects.create(title="b1", author=a1, pages=1)
    Book.objects.create(title="b2", author=a1, pages=2)
    Book.objects.create(title="b3", author=a2, pages=3)
    Author.objects.create(name="A3", age=30, email="a3@e.com")  # 0 books

    annotated = list(
        Author.objects.annotate(book_count=Count("book_set")).order_by("name")
    )
    by_name = {r.name: r.book_count for r in annotated}  # ty:ignore[unresolved-attribute]
    assert by_name["A1"] == 2
    assert by_name["A2"] == 1
    assert by_name["A3"] == 0


def test_reverse_fk_filter_with_distinct_avoids_duplicates():
    """Reverse-FK joins multiply the outer row by the number of
    matching children. Combine with ``distinct()`` to dedupe when
    the user wanted the parents, not the join product."""
    from tests.models import Author, Book

    a = Author.objects.create(name="A", age=1, email="a@e.com")
    Book.objects.create(title="x", author=a, pages=1, published=True)
    Book.objects.create(title="y", author=a, pages=2, published=True)
    rows = list(
        Author.objects.filter(book_set__published=True).distinct()
    )
    assert len(rows) == 1


# ──────────────────────────────────────────────────────────────────────────────
# JSON path traversal in lookups
# ──────────────────────────────────────────────────────────────────────────────


def test_jsonfield_path_emits_extract_and_filters_correctly():
    from dorm.db.connection import get_connection

    class Doc31(dorm.Model):
        data = dorm.JSONField(default=dict)

        class Meta:
            db_table = "v3_1_jsondoc"
            app_label = "v3_1_json"

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "v3_1_jsondoc"{cascade}')
    json_type = "JSONB" if vendor == "postgresql" else "TEXT"
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "v3_1_jsondoc" ({pk_decl}, "data" {json_type} NOT NULL)'
    )
    Doc31.objects.create(data={"name": "alice", "address": {"city": "Madrid"}})
    Doc31.objects.create(data={"name": "bob", "address": {"city": "Lisbon"}})

    by_name = list(Doc31.objects.filter(data__name="alice"))
    assert [r.data["name"] for r in by_name] == ["alice"]
    by_city = list(Doc31.objects.filter(data__address__city="Lisbon"))
    assert [r.data["name"] for r in by_city] == ["bob"]


# ──────────────────────────────────────────────────────────────────────────────
# Manager-level using(alias) shortcut
# ──────────────────────────────────────────────────────────────────────────────


def test_manager_using_shortcut_returns_queryset_bound_to_alias():
    from tests.models import Author

    Author.objects.create(name="MU", age=1, email="mu@e.com")
    qs = Author.objects.using("default")
    rows = list(qs.filter(email="mu@e.com"))
    assert len(rows) == 1
    rows2 = list(Author.objects.all().using("default").filter(email="mu@e.com"))
    assert len(rows2) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Field.db_default
# ──────────────────────────────────────────────────────────────────────────────


def test_field_db_default_emits_default_clause_in_create_table():
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    f = dorm.IntegerField(db_default=42)
    f.column = "qty"
    f.name = "qty"
    sql = _field_to_column_sql("qty", f, get_connection())
    assert "DEFAULT 42" in sql


def test_field_db_default_overrides_python_default_on_ddl():
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    f = dorm.CharField(max_length=10, default="py", db_default="sql")
    f.column = "label"
    f.name = "label"
    sql = _field_to_column_sql("label", f, get_connection())
    assert "DEFAULT 'sql'" in sql
    assert "'py'" not in sql


def test_field_db_default_accepts_raw_sql_for_server_functions():
    from dorm.db.connection import get_connection
    from dorm.expressions import RawSQL
    from dorm.migrations.operations import _field_to_column_sql

    f = dorm.DateTimeField(db_default=RawSQL("CURRENT_TIMESTAMP"))
    f.column = "ts"
    f.name = "ts"
    sql = _field_to_column_sql("ts", f, get_connection())
    assert "DEFAULT CURRENT_TIMESTAMP" in sql


# ──────────────────────────────────────────────────────────────────────────────
# Window function family extras
# ──────────────────────────────────────────────────────────────────────────────


def test_window_extras_imports_and_runtime():
    """Pin the import surface and runtime behaviour of NthValue /
    PercentRank / CumeDist. Skip on SQLite if the build is too old
    to ship the function."""
    from dorm import CumeDist, NthValue, PercentRank, Window
    from dorm.db.connection import get_connection
    from tests.models import Author

    Author.objects.create(name="W1", age=10, email="w1@e.com")
    Author.objects.create(name="W2", age=20, email="w2@e.com")
    Author.objects.create(name="W3", age=30, email="w3@e.com")

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")

    from dorm import F

    if vendor == "sqlite":
        try:
            rows = list(
                Author.objects.annotate(
                    pct=Window(expression=PercentRank(), order_by=["age"]),
                ).order_by("age")
            )
        except Exception:
            pytest.skip("SQLite build doesn't ship PercentRank")
        assert rows[0].pct == 0  # ty:ignore[unresolved-attribute]
        return

    rows = list(
        Author.objects.annotate(
            pct=Window(expression=PercentRank(), order_by=["age"]),
            cd=Window(expression=CumeDist(), order_by=["age"]),
            nth=Window(
                expression=NthValue(F("age"), 2),
                order_by=["age"],
                frame="ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            ),
        ).order_by("age")
    )
    assert rows[0].pct == 0  # ty:ignore[unresolved-attribute]
    assert rows[-1].pct == 1  # ty:ignore[unresolved-attribute]
    cds = [r.cd for r in rows]  # ty:ignore[unresolved-attribute]
    assert cds == sorted(cds)
    assert cds[-1] == 1
    assert rows[0].nth == 20  # 2nd row's age via the explicit frame  # ty:ignore[unresolved-attribute]


def test_nth_value_rejects_non_positive():
    from dorm import NthValue

    with pytest.raises(ValueError):
        NthValue("age", 0)
    with pytest.raises(ValueError):
        NthValue("age", -3)


# ──────────────────────────────────────────────────────────────────────────────
# Reverse one-to-one filter (mirrors reverse-FK fix)
# ──────────────────────────────────────────────────────────────────────────────


def test_reverse_one_to_one_filter():
    from dorm.db.connection import get_connection

    class O2OProfile(dorm.Model):
        nick = dorm.CharField(max_length=20)

        class Meta:
            db_table = "v3_1_o2o_profile"
            app_label = "v3_1_o2o"

    class O2OAcct(dorm.Model):
        profile = dorm.OneToOneField(
            O2OProfile, on_delete=dorm.CASCADE, related_name="acct"
        )
        email = dorm.EmailField()

        class Meta:
            db_table = "v3_1_o2o_acct"
            app_label = "v3_1_o2o"

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    cascade = " CASCADE" if vendor == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "v3_1_o2o_acct"{cascade}')
    conn.execute_script(f'DROP TABLE IF EXISTS "v3_1_o2o_profile"{cascade}')
    pk_decl = (
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
        if vendor == "sqlite"
        else '"id" BIGSERIAL PRIMARY KEY'
    )
    conn.execute_script(
        f'CREATE TABLE "v3_1_o2o_profile" ({pk_decl}, "nick" VARCHAR(20) NOT NULL)'
    )
    conn.execute_script(
        f'CREATE TABLE "v3_1_o2o_acct" ({pk_decl}, '
        '"profile_id" BIGINT NOT NULL UNIQUE '
        'REFERENCES "v3_1_o2o_profile"("id"), '
        '"email" VARCHAR(254) NOT NULL)'
    )
    p = O2OProfile.objects.create(nick="ace")
    O2OAcct.objects.create(profile=p, email="ace@e.com")

    rows = list(O2OProfile.objects.filter(acct__email="ace@e.com"))
    assert [r.nick for r in rows] == ["ace"]
