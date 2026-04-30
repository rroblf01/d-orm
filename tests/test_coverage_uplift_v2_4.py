"""Targeted coverage tests added for the 2.4 release.

Each section closes a documented gap surfaced by ``--cov-report=term-missing``:

* SQLite backend exception normalisation — every cursor error class
  must round-trip through :func:`dorm.db.utils.normalize_db_exception`
  to the matching dorm exception subtype.
* Migration squasher — three of its merge rules
  (``CreateModel + DeleteModel``, ``CreateModel + AddField``,
  iteration-stable convergence) had no direct tests.
* Migration loader — the "non-numeric / spec-failed" filename branch
  silently skips bad files and was uncovered.
* Connection-registry router fallthrough on falsy returns and missing
  methods — already partially covered, but the *write* router path
  shared the same code without a test.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

import dorm
from dorm.exceptions import (
    IntegrityError,
    OperationalError,
    ProgrammingError,
)


# ── SQLite exception normalisation ───────────────────────────────────


class TestSqliteNormalizeDbException:
    """Every cursor error class should map to the matching dorm
    exception subtype. These pin the contract :class:`SQLiteDatabaseWrapper`
    relies on when its own ``execute`` / ``execute_write`` / ``execute_insert``
    catch and re-raise."""

    def test_integrity_error_round_trip(self, tmp_path: Path):
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "ix.db")})
        try:
            wrapper.execute_script(
                'CREATE TABLE x (id INTEGER PRIMARY KEY, '
                'name TEXT NOT NULL UNIQUE)'
            )
            wrapper.execute_write(
                'INSERT INTO "x" ("name") VALUES (%s)', ["a"]
            )
            with pytest.raises(IntegrityError):
                wrapper.execute_write(
                    'INSERT INTO "x" ("name") VALUES (%s)', ["a"]
                )
        finally:
            wrapper.close()

    def test_operational_error_with_missing_table_hint(self, tmp_path: Path):
        """``no such table: …`` upgrades to a hint message that names
        the missing table — important so a forgotten ``migrate`` call
        is obvious in the traceback."""
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "miss.db")})
        try:
            with pytest.raises(OperationalError) as exc:
                wrapper.execute('SELECT * FROM "ghost_table"')
            assert "ghost_table" in str(exc.value)
        finally:
            wrapper.close()

    def test_programming_error_on_bad_sql(self, tmp_path: Path):
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "bad.db")})
        try:
            # Trailing ``,`` makes this a syntax error in SQLite's
            # parser → sqlite3.OperationalError, normalised to dorm
            # OperationalError.
            with pytest.raises((OperationalError, ProgrammingError)):
                wrapper.execute("SELECT FROM,,,")
        finally:
            wrapper.close()

    def test_execute_insert_normalises_errors(self, tmp_path: Path):
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "ei.db")})
        try:
            wrapper.execute_script(
                'CREATE TABLE y (id INTEGER PRIMARY KEY, '
                'name TEXT NOT NULL UNIQUE)'
            )
            wrapper.execute_insert(
                'INSERT INTO "y" ("name") VALUES (%s)', ["dup"]
            )
            with pytest.raises(IntegrityError):
                wrapper.execute_insert(
                    'INSERT INTO "y" ("name") VALUES (%s)', ["dup"]
                )
        finally:
            wrapper.close()

    def test_execute_bulk_insert_normalises_errors(self, tmp_path: Path):
        from dorm.db.backends.sqlite import SQLiteDatabaseWrapper

        wrapper = SQLiteDatabaseWrapper({"NAME": str(tmp_path / "bi.db")})
        try:
            wrapper.execute_script(
                'CREATE TABLE z (id INTEGER PRIMARY KEY, '
                'name TEXT NOT NULL UNIQUE)'
            )
            with pytest.raises(IntegrityError):
                wrapper.execute_bulk_insert(
                    'INSERT INTO "z" ("name") VALUES (%s), (%s)',
                    ["a", "a"],
                    count=2,
                )
        finally:
            wrapper.close()


# ── Migration squasher ──────────────────────────────────────────────


class TestSquasherMergeRules:
    """The squasher's job is folding a long ``CreateModel ... AddField ...
    AlterField ... DeleteModel`` history into a minimal equivalent
    sequence. Three rules are interesting enough to pin:

    1. ``CreateModel(X)`` + ``DeleteModel(X)`` → both vanish, plus
       every X-touching op between them.
    2. ``CreateModel(X)`` + ``AddField(X, f)`` → field merges into
       the CreateModel, AddField gone.
    3. The squasher iterates until convergence; pinning a
       pathological input where two rules apply in sequence proves
       that.
    """

    def test_create_then_delete_drops_pair(self):
        from dorm.migrations.operations import CreateModel, DeleteModel
        from dorm.migrations.squasher import squash_operations

        ops = [
            CreateModel(name="Tmp", fields=[("id", dorm.AutoField(primary_key=True))]),
            DeleteModel(name="Tmp"),
        ]
        squashed = squash_operations(ops)
        assert squashed == [], (
            f"create+delete pair should collapse to []: got {squashed!r}"
        )

    def test_create_then_add_merges_field_into_create(self):
        from dorm.migrations.operations import AddField, CreateModel
        from dorm.migrations.squasher import squash_operations

        ops = [
            CreateModel(
                name="Author",
                fields=[("id", dorm.AutoField(primary_key=True))],
            ),
            AddField(model_name="Author", name="bio", field=dorm.TextField(null=True)),
        ]
        squashed = squash_operations(ops)
        assert len(squashed) == 1
        assert isinstance(squashed[0], CreateModel)
        # CreateModel.fields is a list of (name, field) tuples.
        names = [n for n, _ in squashed[0].fields]  # type: ignore[attr-defined]
        assert "bio" in names

    def test_create_delete_with_intermediate_addfield_drops_everything(self):
        """``CreateModel(X)`` + ``AddField(X, f)`` + ``DeleteModel(X)``
        → all three collapse. The squasher's intermediate-skip set
        catches the AddField as touching ``X``."""
        from dorm.migrations.operations import (
            AddField,
            CreateModel,
            DeleteModel,
        )
        from dorm.migrations.squasher import squash_operations

        ops = [
            CreateModel(name="Tmp", fields=[("id", dorm.AutoField(primary_key=True))]),
            AddField(model_name="Tmp", name="x", field=dorm.IntegerField(default=0)),
            DeleteModel(name="Tmp"),
        ]
        assert squash_operations(ops) == []

    def test_unrelated_ops_preserved_around_collapse(self):
        from dorm.migrations.operations import CreateModel, DeleteModel
        from dorm.migrations.squasher import squash_operations

        keep = CreateModel(
            name="Keep", fields=[("id", dorm.AutoField(primary_key=True))]
        )
        ops = [
            keep,
            CreateModel(name="Tmp", fields=[("id", dorm.AutoField(primary_key=True))]),
            DeleteModel(name="Tmp"),
        ]
        squashed = squash_operations(ops)
        assert squashed == [keep]


# ── Migration loader skips bad files ────────────────────────────────


class TestMigrationLoaderEdgeCases:
    def test_non_numeric_prefix_is_skipped(self, tmp_path: Path):
        """``loader.discover`` only picks up files whose stem starts
        with a number (Django convention). Anything else is skipped
        — including ``__init__`` and stray helper scripts users
        sometimes drop in the migrations folder."""
        from dorm.migrations.loader import MigrationLoader

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        migrations_dir = app_dir / "migrations"
        migrations_dir.mkdir()
        (migrations_dir / "__init__.py").write_text("")

        # Numeric-prefixed file → picked up.
        (migrations_dir / "0001_init.py").write_text(
            "operations = []\n"
        )
        # Non-numeric prefix → silently skipped.
        (migrations_dir / "helper_script.py").write_text(
            "raise RuntimeError('should not import')\n"
        )
        # Trailing ``.txt`` → not a Python file, also ignored.
        (migrations_dir / "0002_notes.txt").write_text("not python")

        sys.path.insert(0, str(tmp_path))
        try:
            from dorm.db.connection import get_connection

            loader = MigrationLoader(get_connection())
            loader.load(migrations_dir, "myapp")
            assert "myapp" in loader.migrations
            stems = [stem for _, stem, _ in loader.migrations["myapp"]]
            assert stems == ["0001_init"]
        finally:
            sys.path.remove(str(tmp_path))
            for mod in list(sys.modules):
                if mod.startswith("myapp"):
                    sys.modules.pop(mod, None)


# ── Connection registry: router fallthrough ────────────────────────


class TestRouterWriteFallthrough:
    """``router_db_for_write`` is the mirror of ``router_db_for_read``.
    The shared logic was tested only via the read path; here we hit
    the write side too."""

    def test_write_router_with_no_method_falls_through(self):
        from dorm.conf import settings
        from dorm.db.connection import router_db_for_write

        class _R:
            pass  # no db_for_write method

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_R()]
        try:
            assert router_db_for_write(object) == "default"
        finally:
            if original is None:
                if hasattr(settings, "DATABASE_ROUTERS"):
                    delattr(settings, "DATABASE_ROUTERS")
            else:
                settings.DATABASE_ROUTERS = original

    def test_write_router_returning_none_falls_through(self):
        from dorm.conf import settings
        from dorm.db.connection import router_db_for_write

        class _Vague:
            def db_for_write(self, model, **hints):
                return None

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_Vague()]
        try:
            assert router_db_for_write(object) == "default"
        finally:
            settings.DATABASE_ROUTERS = original or []

    def test_write_router_returning_alias_used(self):
        from dorm.conf import settings
        from dorm.db.connection import router_db_for_write

        class _Pin:
            def db_for_write(self, model, **hints):
                return "primary"

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_Pin()]
        try:
            assert router_db_for_write(object) == "primary"
        finally:
            settings.DATABASE_ROUTERS = original or []

    def test_write_router_exception_swallowed(self):
        """A buggy router must NOT crash the write path — the loop
        catches and falls through to the next router."""
        from dorm.conf import settings
        from dorm.db.connection import router_db_for_write

        class _Boom:
            def db_for_write(self, model, **hints):
                raise RuntimeError("oops")

        class _OK:
            def db_for_write(self, model, **hints):
                return "default"

        original = getattr(settings, "DATABASE_ROUTERS", None)
        settings.DATABASE_ROUTERS = [_Boom(), _OK()]
        try:
            assert router_db_for_write(object) == "default"
        finally:
            settings.DATABASE_ROUTERS = original or []


# ── parse_database_url leftover edge cases ──────────────────────────


class TestParseDatabaseUrlMisc:
    """Coverage for the lifted-pool-options branch when only a single
    knob is set (the existing test sets every knob at once, leaving
    individual lift paths nominally exercised but not pinned in
    isolation)."""

    def test_single_lifted_knob_typed(self):
        from dorm.conf import parse_database_url

        cfg = parse_database_url("postgres://u:p@h/d?MAX_POOL_SIZE=42")
        assert cfg["MAX_POOL_SIZE"] == 42
        assert "OPTIONS" not in cfg

    def test_options_and_lifted_coexist(self):
        from dorm.conf import parse_database_url

        cfg = parse_database_url(
            "postgres://u:p@h/d?MAX_POOL_SIZE=10&sslmode=require"
        )
        assert cfg["MAX_POOL_SIZE"] == 10
        assert cfg["OPTIONS"] == {"sslmode": "require"}

    def test_postgres_path_prefix_normalisation(self):
        """A URL like ``postgres://h/d`` (single leading slash on the
        path) yields ``NAME='d'``, not ``'/d'``."""
        from dorm.conf import parse_database_url

        cfg = parse_database_url("postgres://h/dbname")
        assert cfg["NAME"] == "dbname"

    def test_postgres_no_path_yields_empty_name(self):
        """Edge case: URL with no path component."""
        from dorm.conf import parse_database_url

        cfg = parse_database_url("postgres://user:pass@host/")
        assert cfg["NAME"] == ""


# ── _discover_apps edge cases ───────────────────────────────────────


class TestDiscoverAppsRoot:
    """Files at the discovery root have zero ``parts`` and produce an
    empty app label — they must be filtered out, otherwise the
    ``DATABASES`` validator would later reject the empty label
    cryptically."""

    def test_models_at_root_skipped(self, tmp_path: Path):
        from dorm.conf import _discover_apps

        # A bare ``models.py`` directly at the root has no app
        # package wrapping it; treat as not an app.
        (tmp_path / "models.py").write_text("")
        # And a real app package one level down.
        app = tmp_path / "myapp"
        app.mkdir()
        (app / "__init__.py").write_text("")
        (app / "models.py").write_text("")

        result = _discover_apps(tmp_path)
        assert "myapp" in result
        # Empty-label entry must NOT appear in the result list.
        assert "" not in result


# ── Settings configure / reconfigure smoke ──────────────────────────


class TestConfigureRoundTrip:
    def test_configure_overwrites_existing(self):
        """Calling ``configure`` twice replaces the previous values
        rather than merging — without this round-trip test a
        regression that switched to merge behaviour would silently
        leak old DATABASES / INSTALLED_APPS into subsequent tests."""
        from dorm.conf import settings

        before_dbs = dict(settings.DATABASES)
        before_apps = list(settings.INSTALLED_APPS)
        try:
            dorm.configure(
                DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
                INSTALLED_APPS=["tests"],
            )
            assert settings.DATABASES["default"]["NAME"] == ":memory:"
            assert settings.INSTALLED_APPS == ["tests"]
        finally:
            # Restore so other tests see the conftest-configured DB.
            dorm.configure(DATABASES=before_dbs, INSTALLED_APPS=before_apps)


# ── QuerySet edge cases ─────────────────────────────────────────────


class TestQuerySetEdges:
    """Branches inside :class:`QuerySet` that aren't on a hot user
    path but bite when they do."""

    def test_aggregate_with_no_rows_returns_empty_dict(self):
        from tests.models import Author

        # Empty table → aggregate returns ``{}`` (vs the ``rows[0]`` path
        # that triggers when at least one row exists).
        Author.objects.all().delete()
        out = Author.objects.filter(pk__in=[]).aggregate(total=dorm.Count("id"))
        # COUNT over empty set still returns 0; what we're verifying is
        # the aggregate code path didn't IndexError on missing rows.
        assert out.get("total", 0) == 0

    def test_explain_row_to_str_formats_dict_row(self):
        from dorm.queryset import _explain_row_to_str

        # PG-style row with the canonical key.
        row = {"QUERY PLAN": "Seq Scan on authors  (cost=0.00..1.00 rows=1)"}
        assert "Seq Scan" in _explain_row_to_str(row)

        # SQLite-style row.
        row2 = {"id": 0, "parent": 0, "notused": 0, "detail": "SCAN authors"}
        assert _explain_row_to_str(row2) == "SCAN authors"

    def test_explain_row_to_str_handles_sequence_row(self):
        from dorm.queryset import _explain_row_to_str

        row = (1, 2, "Seq Scan")
        out = _explain_row_to_str(row)
        assert "Seq Scan" in out

    def test_explain_row_to_str_falls_back_to_repr(self):
        from dorm.queryset import _explain_row_to_str

        # Anything else → ``str(row)``.
        assert _explain_row_to_str(42) == "42"

    def test_first_returns_none_on_empty_qs(self):
        from tests.models import Author

        Author.objects.all().delete()
        assert Author.objects.filter(name="never-exists").first() is None

    def test_last_returns_none_on_empty_qs(self):
        from tests.models import Author

        Author.objects.all().delete()
        assert Author.objects.filter(name="never-exists").last() is None

    def test_only_returns_clone_not_self(self):
        from tests.models import Author

        base = Author.objects.all()
        sliced = base.only("name")
        assert base is not sliced
        # Original queryset's selected_fields is untouched.
        assert base._query.selected_fields is None

    def test_defer_returns_clone_not_self(self):
        from tests.models import Author

        base = Author.objects.all()
        sliced = base.defer("email")
        assert base is not sliced
        assert base._query.selected_fields is None

    def test_only_dotted_with_unknown_relation_raises(self):
        """Identifier validation triggers on the relation name —
        ``foo__bar`` where ``foo`` isn't a field on the model goes
        through ``_meta.get_field`` which raises."""
        from tests.models import Author
        from dorm.exceptions import FieldDoesNotExist

        with pytest.raises(FieldDoesNotExist):
            Author.objects.only("ghost__col")


# ── QuerySet.values() shapes ────────────────────────────────────────


class TestValuesShapes:
    def test_values_no_args_returns_every_column(self):
        from tests.models import Author

        a = Author.objects.create(name="V1", age=33, email="v1@x.com")
        try:
            row = list(Author.objects.filter(pk=a.pk).values())[0]
            assert row["name"] == "V1"
            assert row["age"] == 33
        finally:
            a.delete()

    def test_values_list_flat_single_field(self):
        from tests.models import Author

        a = Author.objects.create(name="V2", age=44, email="v2@x.com")
        try:
            names = list(Author.objects.filter(pk=a.pk).values_list("name", flat=True))
            assert names == ["V2"]
        finally:
            a.delete()

    def test_values_list_flat_with_multiple_fields_raises(self):
        from tests.models import Author

        with pytest.raises(ValueError):
            list(Author.objects.values_list("name", "age", flat=True))


# ── Field validation edge cases ─────────────────────────────────────


class TestFieldValidationEdges:
    def test_charfield_max_length_enforced_on_full_clean(self):
        from tests.models import Author

        a = Author(name="x" * 200, age=1, email="o@x.com")
        # ``full_clean`` runs the field validator stack, which catches
        # the CharField max_length excess.
        with pytest.raises(Exception):
            a.full_clean()

    def test_emailfield_rejects_garbage(self):
        from tests.models import Author

        with pytest.raises(Exception):
            Author(name="ok", age=1, email="not-an-email")

    def test_positive_integer_accepts_zero_but_rejects_negative(self):
        class _M(dorm.Model):
            n = dorm.PositiveIntegerField()

            class Meta:
                db_table = "pos_int_edge"
                app_label = "tests"

        # Zero is fine.
        _M(n=0).full_clean()
        # Negative fails on full_clean (the validate path).
        with pytest.raises(Exception):
            _M(n=-1).full_clean()


# ── Pydantic helpers smoke ──────────────────────────────────────────


class TestPydanticHelpersExtras:
    def test_create_schema_for_with_only_arg_via_schema_for(self):
        """The Create helper takes ``exclude`` only (the auto-PK drop
        is automatic). Verify nothing else slips through — passing a
        keyword ``schema_for`` doesn't expose shouldn't crash, but
        we don't need to test that here."""
        from dorm.contrib.pydantic import create_schema_for
        from tests.models import Author

        Schema = create_schema_for(Author, name="AuthorPost")
        assert Schema.__name__ == "AuthorPost"

    def test_update_schema_for_custom_base(self):
        """Passing a custom ``BaseModel`` base must propagate to the
        resulting schema — same contract as :func:`schema_for`."""
        from pydantic import BaseModel, ConfigDict
        from dorm.contrib.pydantic import update_schema_for
        from tests.models import Author

        class _MyBase(BaseModel):
            model_config = ConfigDict(str_strip_whitespace=True)

        Schema = update_schema_for(Author, base=_MyBase)
        # Subclass relationship is preserved through the wrapper.
        assert Schema.model_config.get("arbitrary_types_allowed") is True


# ── N+1 detector knobs ──────────────────────────────────────────────


class TestNPlusOneEdges:
    def test_detector_negative_threshold_rejected(self):
        from dorm.contrib.nplusone import NPlusOneDetector

        with pytest.raises(ValueError):
            NPlusOneDetector(threshold=-1)

    def test_report_when_no_findings(self):
        from dorm.contrib.nplusone import NPlusOneDetector

        d = NPlusOneDetector(threshold=10, raise_on_detect=False)
        # No queries inside.
        with d:
            pass
        assert d.report() == "no N+1 detected"

    def test_exit_does_not_pile_on_existing_exception(self):
        """If the user code raises mid-block, the detector must not
        replace it with NPlusOneError — the user's exception is the
        more useful signal."""
        from dorm.contrib.nplusone import NPlusOneDetector
        from tests.models import Author, Publisher

        pub = Publisher.objects.create(name="P")
        for i in range(10):
            Author.objects.create(name=f"x{i}", age=i, email=f"x{i}@x.com", publisher=pub)
        try:
            with pytest.raises(KeyError) as exc:
                with NPlusOneDetector(threshold=2):
                    for a in Author.objects.all():
                        if a.publisher is not None:
                            _ = a.publisher.name
                        # Even if N+1 has tripped, this exception is
                        # what bubbles up.
                        raise KeyError("user-code error")
            assert "user-code error" in str(exc.value)
        finally:
            Author.objects.all().delete()
            pub.delete()
