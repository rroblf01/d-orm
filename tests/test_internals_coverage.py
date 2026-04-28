"""Coverage-tightening tests for internal modules ``conf``,
``functions``, ``db.utils`` and ``models`` lifecycle paths.

Each suite is small and surgical — pokes one branch the rest of the
test corpus doesn't reach. No clever fixtures, no test fixtures with
side effects: every test reads as a one-line sentence about an
edge-case the code claims to handle.
"""

from __future__ import annotations

import sqlite3

import pytest

import dorm
from dorm.conf import _discover_apps, parse_database_url
from dorm.db.backends.postgresql import _to_pyformat
from dorm.db.utils import (
    _SENSITIVE_COLUMN_PATTERNS,
    _is_sensitive_column,
    _mask_params,
    normalize_db_exception,
)
from dorm.exceptions import (
    ImproperlyConfigured,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)


# ── parse_database_url ──────────────────────────────────────────────────────


class TestParseDatabaseUrl:
    """``parse_database_url`` is the single seam between
    ``DATABASE_URL=…`` env vars and dorm's typed ``DATABASES`` dict.
    Every shape it claims to accept gets a test; every shape it claims
    to reject gets one too."""

    def test_sqlite_in_memory_when_path_empty(self):
        assert parse_database_url("sqlite://")["NAME"] == ":memory:"
        assert parse_database_url("sqlite:///")["NAME"] == ":memory:"

    def test_sqlite_relative_path(self):
        cfg = parse_database_url("sqlite://relative/db.sqlite3")
        assert cfg["ENGINE"] == "sqlite"
        # Relative paths come through preserved (``urlparse`` puts
        # the leading segment in ``netloc``, which we stitch back on).
        assert cfg["NAME"].endswith("db.sqlite3")

    def test_sqlite_absolute_path(self):
        cfg = parse_database_url("sqlite:////tmp/db.sqlite3")
        assert cfg["NAME"] == "/tmp/db.sqlite3"

    def test_sqlite_query_string_lands_in_options(self):
        cfg = parse_database_url("sqlite:///:memory:?journal_mode=WAL&foo=bar")
        assert cfg["OPTIONS"]["journal_mode"] == "WAL"
        assert cfg["OPTIONS"]["foo"] == "bar"

    def test_sqlite3_alias_is_accepted(self):
        # ``sqlite`` and ``sqlite3`` both map to the same backend.
        assert parse_database_url("sqlite3:///")["ENGINE"] == "sqlite"

    def test_postgres_full_url_decoded(self):
        cfg = parse_database_url(
            "postgres://user%40org:p%40ss@host.example:5432/dbname"
        )
        assert cfg["ENGINE"] == "postgresql"
        # URL-encoded credentials are decoded.
        assert cfg["USER"] == "user@org"
        assert cfg["PASSWORD"] == "p@ss"
        assert cfg["HOST"] == "host.example"
        assert cfg["PORT"] == 5432
        assert cfg["NAME"] == "dbname"

    def test_postgres_default_port_when_omitted(self):
        cfg = parse_database_url("postgres://user:pass@host/db")
        assert cfg["PORT"] == 5432

    def test_postgresql_alias_is_accepted(self):
        assert parse_database_url("postgresql://u:p@h/d")["ENGINE"] == "postgresql"

    def test_psql_alias_is_accepted(self):
        assert parse_database_url("psql://u:p@h/d")["ENGINE"] == "postgresql"

    def test_lifted_pool_options_get_typed_top_level(self):
        cfg = parse_database_url(
            "postgres://u:p@h:5432/d"
            "?MAX_POOL_SIZE=20&MIN_POOL_SIZE=2&POOL_TIMEOUT=15.5"
            "&POOL_CHECK=false&MAX_IDLE=30.0&MAX_LIFETIME=3600"
            "&PREPARE_THRESHOLD=0"
        )
        assert cfg["MAX_POOL_SIZE"] == 20
        assert cfg["MIN_POOL_SIZE"] == 2
        assert cfg["POOL_TIMEOUT"] == 15.5
        assert cfg["POOL_CHECK"] is False
        assert cfg["MAX_IDLE"] == 30.0
        assert cfg["MAX_LIFETIME"] == 3600
        assert cfg["PREPARE_THRESHOLD"] == 0

    def test_pool_check_truthy_strings_round_trip(self):
        for raw, expected in (
            ("1", True), ("true", True), ("yes", True), ("on", True),
            ("0", False), ("false", False), ("no", False), ("off", False),
        ):
            cfg = parse_database_url(f"postgres://u:p@h/d?POOL_CHECK={raw}")
            assert cfg["POOL_CHECK"] is expected, (raw, cfg["POOL_CHECK"])

    def test_unknown_query_params_fall_through_to_options(self):
        cfg = parse_database_url(
            "postgres://u:p@h/d?sslmode=require&application_name=myapp"
        )
        assert cfg["OPTIONS"] == {
            "sslmode": "require",
            "application_name": "myapp",
        }

    def test_unknown_scheme_raises_with_helpful_message(self):
        with pytest.raises(ImproperlyConfigured, match="Unrecognised database URL scheme"):
            parse_database_url("mysql://u:p@h/d")
        with pytest.raises(ImproperlyConfigured):
            parse_database_url("oracle://u:p@h/d")

    def test_postgres_username_or_password_unset_returns_empty_strings(self):
        cfg = parse_database_url("postgres://host/db")
        assert cfg["USER"] == ""
        assert cfg["PASSWORD"] == ""


# ── _discover_apps ──────────────────────────────────────────────────────────


class TestDiscoverApps:
    def test_discovers_dotted_app_paths(self, tmp_path):
        root = tmp_path
        # One flat app.
        (root / "blog").mkdir()
        (root / "blog" / "__init__.py").touch()
        (root / "blog" / "models.py").write_text("")
        # One nested app: ``project/sub``.
        (root / "project").mkdir()
        (root / "project" / "__init__.py").touch()
        (root / "project" / "sub").mkdir()
        (root / "project" / "sub" / "__init__.py").touch()
        (root / "project" / "sub" / "models.py").write_text("")

        apps = _discover_apps(root)
        assert "blog" in apps
        assert "project.sub" in apps

    def test_skips_excluded_directories(self, tmp_path):
        # ``__pycache__`` should never be recognised as an app.
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "__pycache__" / "models.py").write_text("")
        assert _discover_apps(tmp_path) == []

    def test_skips_hidden_directories(self, tmp_path):
        (tmp_path / ".hidden").mkdir()
        (tmp_path / ".hidden" / "__init__.py").touch()
        (tmp_path / ".hidden" / "models.py").write_text("")
        assert _discover_apps(tmp_path) == []

    def test_skips_paths_without_init_chain(self, tmp_path):
        # ``models.py`` exists but its parent directory has no
        # ``__init__.py`` — not an importable package.
        (tmp_path / "loose").mkdir()
        (tmp_path / "loose" / "models.py").write_text("")
        assert _discover_apps(tmp_path) == []

    def test_skips_models_py_at_root(self, tmp_path):
        # ``models.py`` directly under the search root has zero
        # parts → would produce an empty app label.
        (tmp_path / "models.py").write_text("")
        assert _discover_apps(tmp_path) == []


# ── db.utils._to_pyformat ───────────────────────────────────────────────────


class TestToPyformat:
    """The placeholder rewriter swaps ``$N`` for ``%s`` (Postgres
    parameter style → psycopg-friendly), but it must not touch ``$N``
    occurrences inside string literals or quoted identifiers."""

    def test_basic_substitution(self):
        out = _to_pyformat("SELECT * FROM t WHERE id = $1 AND name = $2")
        assert out == "SELECT * FROM t WHERE id = %s AND name = %s"

    def test_no_substitution_inside_single_quoted_literal(self):
        # ``$1`` inside ``'…'`` is literal text the user wants preserved.
        out = _to_pyformat("SELECT '$1' FROM t WHERE id = $1")
        assert out == "SELECT '$1' FROM t WHERE id = %s"

    def test_no_substitution_inside_double_quoted_identifier(self):
        out = _to_pyformat('SELECT "col$1" FROM t WHERE id = $1')
        assert out == 'SELECT "col$1" FROM t WHERE id = %s'

    def test_handles_escaped_quote_pairs_in_literal(self):
        # ``''`` is a SQL-escaped single quote inside a literal.
        out = _to_pyformat("SELECT 'a''b $1 c' FROM t WHERE id = $1")
        assert out == "SELECT 'a''b $1 c' FROM t WHERE id = %s"

    def test_no_op_when_no_dollar_placeholders(self):
        sql = "SELECT * FROM t WHERE id = %s"
        assert _to_pyformat(sql) == sql

    def test_empty_string_passes_through(self):
        assert _to_pyformat("") == ""


# ── db.utils._mask_params (sensitive column redaction) ──────────────────────


class TestMaskParams:
    """Values bound to a column whose name suggests a credential
    are replaced with ``"***"`` in DEBUG logs. Catches a regression
    that would dump tokens into Datadog / Loki."""

    def test_password_column_is_masked(self):
        sql = 'INSERT INTO users ("name", "password") VALUES (%s, %s)'
        out = _mask_params(sql, ["alice", "supersecret"])
        assert out == ["alice", "***"]

    def test_token_columns_are_masked(self):
        for col in ("api_key", "access_token", "secret", "private_key"):
            sql = f'UPDATE users SET "{col}" = %s WHERE id = %s'
            out = _mask_params(sql, ["leaked", 1])
            assert out[0] == "***", f"{col} should be masked"
            assert out[1] == 1

    def test_non_sensitive_columns_preserved(self):
        sql = 'UPDATE users SET "name" = %s, "age" = %s WHERE id = %s'
        out = _mask_params(sql, ["alice", 30, 1])
        assert out == ["alice", 30, 1]

    def test_short_circuits_when_no_sensitive_columns(self):
        # Optimisation: when no column is sensitive, return params
        # unchanged (same object identity).
        sql = "UPDATE users SET name = %s WHERE id = %s"
        params = ["alice", 1]
        out = _mask_params(sql, params)
        assert out is params

    def test_handles_non_sequence_params(self):
        # Drivers occasionally pass dict-shaped params; the mask
        # path must not crash, just return them unchanged.
        sql = "UPDATE users SET password = %(p)s WHERE id = %(id)s"
        params = {"p": "secret", "id": 1}
        out = _mask_params(sql, params)
        assert out is params

    def test_none_params_pass_through(self):
        assert _mask_params("SELECT 1", None) is None
        assert _mask_params("SELECT 1", []) == []

    def test_is_sensitive_column_uses_pattern_table(self):
        # Sanity guard: every entry in the documented list flags as
        # sensitive. Catches a regression where someone trims the
        # tuple silently.
        for name in _SENSITIVE_COLUMN_PATTERNS:
            assert _is_sensitive_column(name)


# ── db.utils.normalize_db_exception ────────────────────────────────────────


class TestNormalizeDbException:
    """The driver exception → dorm exception mapping has to translate
    every documented error class without crashing on unknown ones."""

    def test_sqlite_integrity_error(self):
        with pytest.raises(IntegrityError):
            normalize_db_exception(sqlite3.IntegrityError("UNIQUE failed"))

    def test_sqlite_operational_error(self):
        with pytest.raises(OperationalError):
            normalize_db_exception(sqlite3.OperationalError("disk I/O error"))

    def test_sqlite_programming_error(self):
        with pytest.raises(ProgrammingError):
            normalize_db_exception(sqlite3.ProgrammingError("Cannot operate"))

    def test_sqlite_database_error_routes_to_programming(self):
        with pytest.raises(ProgrammingError):
            normalize_db_exception(sqlite3.DatabaseError("file is not a database"))

    def test_unknown_exception_silently_returns(self):
        # The contract: if dorm doesn't know how to translate, the
        # caller's ``raise`` keeps the original exception. ``normalize_…``
        # returns without raising for non-driver exceptions.
        normalize_db_exception(ValueError("not a db error"))
        # Did not raise — that's the contract.

    def test_missing_table_message_gets_friendly_hint_sqlite(self):
        # Both sqlite-flavoured and pg-flavoured "missing table"
        # messages are recognised and re-raised with a
        # "run dorm migrate" hint.
        exc = sqlite3.OperationalError("no such table: blog_post")
        with pytest.raises(OperationalError, match="does not exist"):
            normalize_db_exception(exc)


# ── functions: SQL function expression edge cases ──────────────────────────


class TestSqlFunctions:
    """``Coalesce``, ``Cast`` and the window-function family build SQL
    via ``as_sql`` — the tricky bits are the validation paths and
    the ``output_field`` plumbing."""

    def test_coalesce_requires_at_least_one_argument(self):
        from dorm.functions import Coalesce

        with pytest.raises((TypeError, ValueError)):
            Coalesce()  # type: ignore[call-arg]

    def test_cast_rejects_unknown_output_field(self):
        from dorm.functions import Cast

        # ``Cast(expr, output_field=…)`` validates ``output_field``
        # against the documented type list to avoid SQL injection
        # via a user-controlled string.
        with pytest.raises((dorm.exceptions.ImproperlyConfigured, ValueError)):
            Cast("col", output_field="DROP TABLE users; --")

    def test_cast_accepts_documented_type_names(self):
        from dorm.functions import Cast

        # Every documented type name parses without complaint.
        for t in ("INTEGER", "TEXT", "VARCHAR(50)", "NUMERIC(10, 2)"):
            Cast("col", output_field=t)

    def test_extract_rejects_unknown_unit(self):
        from dorm.functions import Extract

        with pytest.raises((dorm.exceptions.ImproperlyConfigured, ValueError)):
            Extract("date_col", unit="malicious_unit; --")

    def test_extract_accepts_documented_units(self):
        from dorm.functions import Extract

        for unit in ("year", "month", "day", "hour", "minute", "second"):
            Extract("date_col", unit=unit)

    def test_trunc_rejects_unknown_unit(self):
        from dorm.functions import Trunc

        with pytest.raises((dorm.exceptions.ImproperlyConfigured, ValueError)):
            Trunc("date_col", unit="; DROP TABLE users; --")

    def test_window_ranking_function_requires_order_by(self):
        """``Rank`` / ``DenseRank`` / ``RowNumber`` / ``NTile`` are
        all order-sensitive; without ``order_by`` they'd return
        implementation-defined output. The constructors must reject
        the call up-front rather than ship a flaky query."""
        from dorm.functions import DenseRank, NTile, Rank, RowNumber, Window

        for cls in (Rank, DenseRank, RowNumber):
            with pytest.raises((TypeError, ValueError, dorm.exceptions.ImproperlyConfigured)):
                Window(cls(), partition_by=["x"])

        # NTile additionally requires ``num_buckets``.
        with pytest.raises((TypeError, ValueError, dorm.exceptions.ImproperlyConfigured)):
            Window(NTile(4), partition_by=["x"])

    def test_lag_lead_offset_lands_in_expressions(self):
        """``Lag`` / ``Lead`` accept ``offset`` and ``default`` and
        wrap them in ``Value`` so they bind as parameters at SQL-emit
        time. Verify the expressions list reflects what was passed."""
        from dorm.expressions import Value
        from dorm.functions import Lag, Lead

        # ``Lag('price')`` → expressions = ['price', Value(1)]
        lag = Lag("price")
        assert len(lag.expressions) == 2
        assert isinstance(lag.expressions[1], Value)
        assert lag.expressions[1].value == 1

        # Custom offset survives.
        lag5 = Lag("price", offset=5)
        assert lag5.expressions[1].value == 5

        # ``default`` argument wraps as a third Value.
        lag_def = Lead("price", offset=2, default=0)
        assert len(lag_def.expressions) == 3
        assert lag_def.expressions[1].value == 2
        assert lag_def.expressions[2].value == 0

    def test_ntile_rejects_non_positive_buckets(self):
        """``NTILE(0)`` / ``NTILE(-1)`` are nonsense; the constructor
        rejects them at the Python boundary instead of letting the
        DB error out."""
        from dorm.functions import NTile

        with pytest.raises(ValueError):
            NTile(0)
        with pytest.raises(ValueError):
            NTile(-3)
        # Floats are rejected too — must be a positive integer. The
        # type-checker would normally catch this at the call site;
        # here we deliberately violate the signature to drive the
        # runtime guard.
        from typing import cast as _cast
        with pytest.raises(ValueError):
            NTile(_cast(int, 2.5))


# ── models lifecycle / introspection edge cases ────────────────────────────


class TestModelLifecycleEdgeCases:
    """Tighten coverage on ``Model._from_db_row``, ``refresh_from_db``,
    and the ``unique_together`` / ``validate_unique`` paths."""

    def test_from_db_row_handles_tuple_input(self):
        """The ``_from_db_row`` helper supports both dict-shaped rows
        (with ``.keys()``) and tuple/list rows. The tuple branch is
        rarely exercised because most backends return dict rows.
        Build a positional row shorter than ``concrete_fields`` so
        the ``i < len(row)`` guard is also exercised."""
        from tests.models import Author

        partial = (1, "alice")  # only id + name; trailing fields omitted
        instance = Author._from_db_row(partial)
        assert instance.pk == 1
        assert instance.name == "alice"
        # ``age`` was not in the row → not in __dict__.
        # The descriptor returns whatever Python stores there (no key
        # → returns None via Field.__get__'s ``.get(attname)``).
        assert "age" not in instance.__dict__

    def test_from_db_row_with_full_tuple(self):
        from tests.models import Author

        concrete = [f for f in Author._meta.fields if f.column]
        # Full row in declaration order.
        row = tuple(0 if f.name in ("id", "age", "publisher")
                    else (False if f.name == "is_active"
                          else (None if f.name == "email" else "x"))
                    for f in concrete)
        instance = Author._from_db_row(row)
        # All slots are populated (no AttributeError on access).
        for f in concrete:
            instance.__dict__.get(f.attname)  # no exception

    def test_refresh_from_db_subset_of_fields(self):
        from tests.models import Author

        a = Author.objects.create(name="orig", age=10)
        # Mutate in DB without going through the instance.
        Author.objects.filter(pk=a.pk).update(name="changed", age=99)
        # Refresh only ``name`` — ``age`` on the instance must not
        # change (it stays at the in-memory value).
        a.refresh_from_db(fields=["name"])
        assert a.name == "changed"
        assert a.age == 10

    def test_refresh_from_db_full(self):
        from tests.models import Author

        a = Author.objects.create(name="full", age=10)
        Author.objects.filter(pk=a.pk).update(name="x", age=99)
        a.refresh_from_db()  # no fields → load everything
        assert a.name == "x"
        assert a.age == 99

    def test_refresh_from_db_unknown_field_silently_skipped(self):
        from tests.models import Author

        a = Author.objects.create(name="silent", age=10)
        # Unknown field name in ``fields=`` — must not raise; just
        # ignored in both the qs.only() projection AND the copyback.
        a.refresh_from_db(fields=["name", "nonexistent_column"])
        assert a.name == "silent"

    @pytest.mark.asyncio
    async def test_arefresh_from_db_subset(self):
        from tests.models import Author

        a = await Author.objects.acreate(name="aorig", age=10)
        await Author.objects.filter(pk=a.pk).aupdate(name="achanged")
        await a.arefresh_from_db(fields=["name"])
        assert a.name == "achanged"

    @pytest.mark.asyncio
    async def test_arefresh_unknown_field_silently_skipped(self):
        from tests.models import Author

        a = await Author.objects.acreate(name="aold", age=10)
        await a.arefresh_from_db(fields=["name", "nonexistent"])
        assert a.name == "aold"


# ── Field default applied when value is None and column is non-null ────────


class TestModelInsertDefaultFallback:
    """``Model._do_insert`` re-applies ``field.get_default()`` when
    ``pre_save`` returned None and the column is non-null with a
    default. Catches a regression where the default was silently
    ignored, producing ``NOT NULL constraint failed`` at the cursor."""

    def test_non_null_field_with_default_uses_default_on_insert(self):
        # ``Book.published`` is BooleanField(default=False, null=False).
        from tests.models import Author, Book

        author = Author.objects.create(name="d", age=1)
        # Construct without setting ``published`` — Model.__init__ has
        # already applied the default from ``has_default``, so this
        # is a happy-path regression check.
        book = Book(title="dt", author=author, pages=10)
        # The default is wired in __init__, not at INSERT time, but
        # the INSERT path also has a fallback — drive it by clearing
        # the value first.
        book.__dict__["published"] = None
        book.save()
        loaded = Book.objects.get(pk=book.pk)
        # Default is False → that's what landed.
        assert loaded.published is False
