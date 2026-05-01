"""Regression tests for the v2.4.2 second-round bug-hunt fixes.

Each section pins down one specific bug found during the round-2
audit. Bug numbers ("B1"–"B15") match the audit report.
"""

from __future__ import annotations

import os
import re
import tempfile
import threading
from typing import Any
from unittest.mock import patch

import pytest

from dorm import F, Q, Sum, Value
from dorm.functions import Concat
from tests.models import Author, Book


# ────────────────────────────────────────────────────────────────────────────
# B1 — Aggregate(filter=Q(...)) must emit FILTER / CASE WHEN
# ────────────────────────────────────────────────────────────────────────────


def test_aggregate_filter_q_emits_conditional_aggregate() -> None:
    """``Sum("amount", filter=Q(...))`` must skip non-matching rows."""
    a = Author.objects.create(name="AggA", age=10)
    Book.objects.create(title="t1", author=a, pages=10, published=True)
    Book.objects.create(title="t2", author=a, pages=20, published=False)
    Book.objects.create(title="t3", author=a, pages=40, published=True)

    res = Book.objects.filter(author=a).aggregate(
        published_pages=Sum("pages", filter=Q(published=True))
    )
    assert res["published_pages"] == 50, (
        "Sum(filter=Q(...)) must only sum matching rows; got "
        f"{res['published_pages']!r}"
    )


def test_aggregate_filter_q_sql_contains_conditional() -> None:
    from dorm.db.connection import get_connection

    conn = get_connection()
    agg = Sum("pages", filter=Q(published=True))
    sql, params = agg.as_sql("books", model=Book, connection=conn)
    vendor = getattr(conn, "vendor", "sqlite")
    if vendor == "postgresql":
        assert "FILTER (WHERE" in sql
    else:
        assert "CASE WHEN" in sql
    assert params  # filter Q value bound through


# ────────────────────────────────────────────────────────────────────────────
# B2 — _compile_subquery must preserve joins + .values() projection
# ────────────────────────────────────────────────────────────────────────────


def test_subquery_in_qs_with_values_projects_correct_column() -> None:
    """``parent.filter(child__in=Book.objects.values("author_id"))`` must
    project author_id, not the book PK."""
    a1 = Author.objects.create(name="SubA", age=10)
    a2 = Author.objects.create(name="SubB", age=20)
    Book.objects.create(title="b1", author=a1, pages=1)

    inner = Book.objects.filter(title="b1").values("author_id")
    parents = list(Author.objects.filter(pk__in=inner))
    pks = sorted(p.pk for p in parents)
    assert pks == [a1.pk]
    assert a2.pk not in pks


def test_subquery_in_qs_with_fk_traversal_emits_join() -> None:
    """Inner queryset that filters via FK traversal must emit the JOIN
    inside the subquery."""
    a1 = Author.objects.create(name="JoinA", age=10)
    a2 = Author.objects.create(name="JoinB", age=20)
    Book.objects.create(title="join-b1", author=a1, pages=1)
    Book.objects.create(title="join-b2", author=a2, pages=1)

    inner = Book.objects.filter(author__name="JoinA").values("author_id")
    matching = list(Author.objects.filter(pk__in=inner))
    assert len(matching) == 1
    assert matching[0].pk == a1.pk


# ────────────────────────────────────────────────────────────────────────────
# B3 — _compile_condition must walk FK paths + use vendor-aware lookups
# ────────────────────────────────────────────────────────────────────────────


def test_compile_condition_keeps_path_segments_qualified() -> None:
    from dorm.functions import _compile_condition

    sql, _ = _compile_condition(Q(author__name="x"), table_alias="books")
    # Bug previously produced ``"books"."name" = %s`` (drop of
    # ``author`` segment); fix qualifies via the join alias path.
    assert '"name"' in sql
    assert '"author"' in sql or "books_author" in sql


def test_compile_condition_vendor_pg_uses_extract() -> None:
    from dorm.functions import _compile_condition

    sql, _ = _compile_condition(
        Q(created__year=2026), table_alias=None, vendor="postgresql"
    )
    assert "EXTRACT" in sql
    assert "STRFTIME" not in sql


# ────────────────────────────────────────────────────────────────────────────
# B4 — Concat must skip NULL operands (COALESCE wrapping)
# ────────────────────────────────────────────────────────────────────────────


def test_concat_with_null_operand_returns_concatenation() -> None:
    """Concat(F('email'), Value('!')) on rows where email is NULL must
    return ``'!'``, not NULL (the previous ``||`` chain returned NULL)."""
    Author.objects.create(name="HasMail", age=10, email="a@x.com")
    Author.objects.create(name="NoMail", age=10, email=None)

    rows = list(
        Author.objects.filter(name__in=["HasMail", "NoMail"])
        .annotate(label=Concat(F("email"), Value("!")))
        .order_by("name")
    )
    labels = {r.name: getattr(r, "label") for r in rows}
    # The "NoMail" row used to be NULL; with the fix it's just '!'.
    assert labels["NoMail"] == "!"
    assert labels["HasMail"] == "a@x.com!"


# ────────────────────────────────────────────────────────────────────────────
# B5 — __in must accept generator / set / any iterable
# ────────────────────────────────────────────────────────────────────────────


def test_in_lookup_accepts_generator() -> None:
    a1 = Author.objects.create(name="GenA", age=10)
    a2 = Author.objects.create(name="GenB", age=20)
    Author.objects.create(name="GenC", age=30)

    pks = (x.pk for x in [a1, a2])  # actual generator
    rows = list(Author.objects.filter(pk__in=pks))
    pk_set = {r.pk for r in rows}
    assert pk_set == {a1.pk, a2.pk}


def test_in_lookup_accepts_set() -> None:
    a1 = Author.objects.create(name="SetA", age=11)
    a2 = Author.objects.create(name="SetB", age=21)
    pks: set[Any] = {a1.pk, a2.pk}
    rows = list(Author.objects.filter(pk__in=pks))
    assert {r.pk for r in rows} == {a1.pk, a2.pk}


# ────────────────────────────────────────────────────────────────────────────
# B6 — __regex / __iregex must use vendor-appropriate operators
# ────────────────────────────────────────────────────────────────────────────


def test_regex_lookup_pg_uses_tilde_operator() -> None:
    from dorm.lookups import build_lookup_sql

    sql, _ = build_lookup_sql('"col"', "regex", "^foo", vendor="postgresql")
    assert "~" in sql
    assert "REGEXP" not in sql


def test_iregex_lookup_pg_uses_tilde_star() -> None:
    from dorm.lookups import build_lookup_sql

    sql, _ = build_lookup_sql('"col"', "iregex", "^foo", vendor="postgresql")
    assert "~*" in sql
    assert "REGEXP" not in sql


# ────────────────────────────────────────────────────────────────────────────
# B7 — dry-run must not touch the migration recorder
# ────────────────────────────────────────────────────────────────────────────


def test_dry_run_does_not_record_squashed_migrations() -> None:
    """``_sync_squashed`` previously wrote through ``self.recorder``
    even when ``dry_run=True``; verify the record path is skipped."""
    from dorm.db.connection import get_connection
    from dorm.migrations.executor import MigrationExecutor

    conn = get_connection()
    executor = MigrationExecutor(conn, verbosity=0)

    called: list[bool] = []
    real_sync = executor._sync_squashed

    def _spy(app_label: str, all_migs: list) -> None:
        called.append(True)
        return real_sync(app_label, all_migs)

    with tempfile.TemporaryDirectory() as tmpdir:
        from pathlib import Path

        d = Path(tmpdir) / "migrations"
        d.mkdir()
        (d / "__init__.py").write_text("")

        with patch.object(executor, "_sync_squashed", _spy):
            executor.migrate("dryapp", d, dry_run=True)
        assert called == [], "dry-run must not invoke _sync_squashed"

        # Same call with dry_run=False does invoke it.
        called.clear()
        with patch.object(executor, "_sync_squashed", _spy):
            executor.migrate("dryapp", d, dry_run=False)
        assert called == [True]


# ────────────────────────────────────────────────────────────────────────────
# B8 — storage must reject symlink-based traversal (realpath)
# ────────────────────────────────────────────────────────────────────────────


def test_storage_resolve_path_blocks_symlink_escape() -> None:
    from dorm.exceptions import ImproperlyConfigured
    from dorm.storage import FileSystemStorage

    with tempfile.TemporaryDirectory() as outside, \
         tempfile.TemporaryDirectory() as media:
        # Make a symlink INSIDE the storage root pointing OUTSIDE it.
        link = os.path.join(media, "escape")
        os.symlink(outside, link)

        st = FileSystemStorage(location=media)
        # Naïve abspath used to allow this. Realpath rejects.
        with pytest.raises(ImproperlyConfigured):
            st._resolve_path("escape/secret.txt")


def test_storage_resolve_path_allows_safe_relative_name() -> None:
    from dorm.storage import FileSystemStorage

    with tempfile.TemporaryDirectory() as media:
        st = FileSystemStorage(location=media)
        target = st._resolve_path("ok/file.txt")
        assert target.startswith(os.path.realpath(media) + os.sep)


# ────────────────────────────────────────────────────────────────────────────
# B9 — CLI migrate must exit 1 on ValueError (missing target)
# ────────────────────────────────────────────────────────────────────────────


def test_cli_migrate_exit_code_on_missing_target_sourcecheck() -> None:
    """Ensure ``cmd_migrate``'s ValueError branch calls ``sys.exit(1)``.

    Mocking the full CLI plumbing (settings loader, app discovery,
    executor) is fragile. The bug under test is a single-line
    behavioural fix; assert the source carries the call and the
    surrounding ``except ValueError`` block still exists.
    """
    import inspect as _inspect

    from dorm.cli import cmd_migrate

    src = _inspect.getsource(cmd_migrate)
    # The fix is structural: a sys.exit(1) inside the except
    # ValueError handler that wraps migrate_to.
    except_block = src.split("except ValueError as exc:", 1)
    assert len(except_block) == 2, "cmd_migrate lost its ValueError handler"
    after_except = except_block[1].split("else:", 1)[0]
    assert "sys.exit(1)" in after_except, (
        "cmd_migrate must exit non-zero on ValueError from migrate_to"
    )


# ────────────────────────────────────────────────────────────────────────────
# B10 — serialize.load must defer FK validation for forward references
# ────────────────────────────────────────────────────────────────────────────


def test_loaddata_issues_fk_deferral_pragma() -> None:
    """Regression for the FK-deferral fix in :func:`dorm.serialize.load`.

    Verifies the deferral primitive runs at load time. Whether the
    fixture itself loads end-to-end depends on the user's schema
    (PG FKs must be DEFERRABLE for ``SET CONSTRAINTS`` to bite); the
    in-tree test schema isn't, so we assert on the *call* the fix
    introduces rather than the FK-violation outcome.
    """
    import json

    from dorm.db.connection import get_connection
    from dorm.serialize import load

    conn = get_connection()
    vendor = getattr(conn, "vendor", "sqlite")
    seen: list[str] = []
    real = conn.execute_script

    def _spy(sql: str) -> Any:
        seen.append(sql)
        return real(sql)

    fixture = json.dumps([
        {"model": "tests.Publisher", "pk": 901, "fields": {"name": "P901"}},
    ])
    with patch.object(conn, "execute_script", _spy):
        load(fixture)

    if vendor == "postgresql":
        assert any("SET CONSTRAINTS ALL DEFERRED" in s for s in seen), (
            "loaddata on PG must SET CONSTRAINTS DEFERRED so cyclic / "
            "self-referential fixtures load even when rows reference each "
            "other across the file."
        )
    else:
        assert any("defer_foreign_keys" in s for s in seen), (
            "loaddata on SQLite must PRAGMA defer_foreign_keys=ON so "
            "rows can reference targets that appear later in the file."
        )


# ────────────────────────────────────────────────────────────────────────────
# B11 — inspect.py must escape Python keywords / invalid identifiers
# ────────────────────────────────────────────────────────────────────────────


def test_inspect_safe_attr_name_escapes_keyword() -> None:
    from dorm.inspect import _safe_attr_name

    name, db_col = _safe_attr_name("from")
    assert name == "from_"
    assert db_col == "from"


def test_inspect_safe_attr_name_passes_normal_identifier() -> None:
    from dorm.inspect import _safe_attr_name

    name, db_col = _safe_attr_name("regular_col")
    assert name == "regular_col"
    assert db_col is None


def test_inspect_safe_attr_name_handles_invalid_identifier() -> None:
    from dorm.inspect import _safe_attr_name

    name, db_col = _safe_attr_name("2nd-column")
    # Must produce a valid identifier plus original db_column.
    assert name.isidentifier()
    assert not name[0].isdigit()
    assert db_col == "2nd-column"


# ────────────────────────────────────────────────────────────────────────────
# B12 — get_available_name must respect max_length even when stem is short
# ────────────────────────────────────────────────────────────────────────────


def test_get_available_name_respects_max_length() -> None:
    from dorm.storage import FileSystemStorage

    with tempfile.TemporaryDirectory() as media:
        st = FileSystemStorage(location=media)
        # Pre-create the original so the rename-loop fires.
        with open(os.path.join(media, "a.txt"), "w") as f:
            f.write("x")
        out = st.get_available_name("a.txt", max_length=10)
        assert len(out) <= 10


def test_get_available_name_extreme_max_length_no_negative_slice() -> None:
    """``cut > len(stem)`` must not yield a leading-underscore stem."""
    from dorm.storage import FileSystemStorage

    with tempfile.TemporaryDirectory() as media:
        st = FileSystemStorage(location=media)
        with open(os.path.join(media, "ab.txt"), "w") as f:
            f.write("x")
        out = st.get_available_name("ab.txt", max_length=8)
        assert len(out) <= 8
        # Must not start with the buggy "_<token>" form (the stem
        # collapsed to "" producing "_xxxx.txt"); the fix drops the
        # leading underscore entirely when the stem doesn't survive.
        assert not out.startswith("_")


# ────────────────────────────────────────────────────────────────────────────
# B13 — set_autocommit must propagate to all live thread-local connections
# ────────────────────────────────────────────────────────────────────────────


def test_sqlite_set_autocommit_updates_sibling_thread_conn() -> None:
    from dorm.db.connection import get_connection

    conn = get_connection()
    if getattr(conn, "vendor", "sqlite") != "sqlite":
        pytest.skip("set_autocommit thread propagation test targets SQLite.")

    # Materialise a sibling-thread connection on the wrapper.
    sibling: dict[str, Any] = {}

    def _open() -> None:
        sibling["c"] = conn.get_connection()

    t = threading.Thread(target=_open)
    t.start()
    t.join()

    # Switch autocommit on the main thread.
    prev_iso = sibling["c"].isolation_level
    conn.set_autocommit(True)
    try:
        assert sibling["c"].isolation_level is None, (
            "sibling-thread conn must reflect new autocommit setting"
        )
    finally:
        conn.set_autocommit(False)
        # restore for any subsequent tests
        del prev_iso


# ────────────────────────────────────────────────────────────────────────────
# B15 — async PG pool: dead-loop teardown must finish() libpq sockets
# ────────────────────────────────────────────────────────────────────────────


def test_pg_async_pool_dead_loop_uses_pgconn_finish_sourcecheck() -> None:
    """Source-level check that the dead-loop branch of ``_get_pool``
    drains libpq sockets via ``pgconn.finish()`` rather than letting
    the GC trip on them later.

    A live integration test would require simulating a closed event
    loop on a real PG connection — that turns the wrapper's lifecycle
    into a minefield because the test fixture's loop and the
    "dead" loop end up entangled. The behaviour the audit flagged
    is purely the cleanup branch, which we verify by reading the
    source.
    """
    pytest.importorskip("psycopg")
    import inspect as _inspect

    from dorm.db.backends import postgresql as _pg_mod

    wrapper_cls = next(
        v for k, v in vars(_pg_mod).items()
        if k.endswith("AsyncDatabaseWrapper")
    )

    src = _inspect.getsource(wrapper_cls._get_pool)
    # The branch we added — when the old loop is closed, walk the
    # pool's idle deque and finish() each pgconn.
    assert "pgconn.finish()" in src, (
        "_get_pool dead-loop branch must call pgconn.finish() on every "
        "leftover libpq connection"
    )
    assert "old_pool" in src and "is_closed" in src


# ────────────────────────────────────────────────────────────────────────────
# Misc helpers — exercise the path-traversal regex used in inspect
# (no bug: smoke check that the new helper does not regress).
# ────────────────────────────────────────────────────────────────────────────


def test_inspect_safe_attr_name_strips_dashes_to_underscores() -> None:
    from dorm.inspect import _safe_attr_name

    name, _ = _safe_attr_name("my-col-name")
    assert re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name)
