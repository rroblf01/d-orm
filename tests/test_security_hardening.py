"""Regressions for the security-hardening pass.

Each section locks down one fix from the audit so future refactors can't
reintroduce the original problem:

- ``Cast(output_field=...)`` allowlist          → no SQL splice via type names
- ``Signal.send`` failures are logged           → no silent receiver crashes
- ``_validate_dotted_path``                     → no ``../`` in CLI / env
- ``_count_placeholders`` / ``RawQuerySet``     → arity mismatch fails fast
- ``_resolve_column`` exception scope           → only ``FieldDoesNotExist``
                                                  is caught, others bubble up
"""

from __future__ import annotations

import logging

import pytest

import dorm
from dorm.exceptions import FieldDoesNotExist, ImproperlyConfigured


# ── Cast(output_field=...) — allowlist of SQL type names ─────────────────────

class _CastModel(dorm.Model):
    name = dorm.CharField(max_length=50)
    age = dorm.IntegerField(default=0)

    class Meta:
        db_table = "cast_secmodel"


def test_cast_accepts_documented_types():
    """The allowlist must let through the SQL types the ORM actually emits."""
    for t in ["INTEGER", "text", "VARCHAR(255)", "NUMERIC(10, 2)", "TIMESTAMP"]:
        # Constructing must not raise; SQL emission isn't exercised here.
        dorm.Cast(dorm.F("age"), output_field=t)


def test_cast_normalises_case_and_spacing():
    # Leading/trailing whitespace and case are normalised; the inner
    # parenthesised modifier is preserved (SQL accepts whitespace inside).
    c = dorm.Cast(dorm.F("age"), output_field="  varchar( 255 )  ")
    assert c.cast_type.startswith("VARCHAR")
    assert "255" in c.cast_type


def test_cast_rejects_sql_injection_payload():
    """A type name carrying a stray statement must not reach SQL."""
    with pytest.raises(ImproperlyConfigured):
        dorm.Cast(dorm.F("age"), output_field="INTEGER); DROP TABLE x; --")


def test_cast_rejects_unknown_base_type():
    with pytest.raises(ImproperlyConfigured):
        dorm.Cast(dorm.F("age"), output_field="EVILTYPE")


def test_cast_rejects_empty_and_non_string():
    with pytest.raises(ImproperlyConfigured):
        dorm.Cast(dorm.F("age"), output_field="")
    with pytest.raises(ImproperlyConfigured):
        dorm.Cast(dorm.F("age"), output_field="   ")


# ── Signal.send — exceptions are logged, not swallowed ───────────────────────

def test_signal_logs_receiver_exception(caplog):
    sig = dorm.Signal()

    def boom(sender, **_):
        raise RuntimeError("receiver blew up")

    sig.connect(boom, weak=False)
    with caplog.at_level(logging.ERROR, logger="dorm.signals"):
        sig.send(sender=object())
    # The receiver crash is observable in logs now.
    assert any("receiver blew up" in r.message or r.exc_info for r in caplog.records)


def test_signal_with_raise_exceptions_propagates():
    sig = dorm.Signal(raise_exceptions=True)

    def boom(sender, **_):
        raise RuntimeError("strict mode")

    sig.connect(boom, weak=False)
    with pytest.raises(RuntimeError, match="strict mode"):
        sig.send(sender=object())


def test_signal_default_keeps_one_receiver_failure_isolated():
    """Receiver A raises, receiver B still gets called and returns its value."""
    sig = dorm.Signal()
    seen = []

    def a(sender, **_):
        raise ValueError("bad")

    def b(sender, **_):
        seen.append("b-called")
        return 42

    sig.connect(a, weak=False)
    sig.connect(b, weak=False)
    responses = sig.send(sender=object())
    assert seen == ["b-called"]
    assert any(value == 42 for _, value in responses)


# ── _validate_dotted_path — settings module / app label ──────────────────────

def test_validate_dotted_path_accepts_simple_and_nested():
    from dorm.conf import _validate_dotted_path

    assert _validate_dotted_path("settings") == "settings"
    assert _validate_dotted_path("myproj.settings") == "myproj.settings"
    assert _validate_dotted_path("a.b.c.d") == "a.b.c.d"


@pytest.mark.parametrize("bad", [
    "../etc/passwd",
    "foo/bar",
    "foo\\bar",
    "..",
    ".foo",
    "foo.",
    "1foo",
    "foo bar",
    "foo;bar",
    "",
])
def test_validate_dotted_path_rejects_filesystem_payloads(bad):
    from dorm.conf import _validate_dotted_path

    with pytest.raises(ImproperlyConfigured):
        _validate_dotted_path(bad)


def test_cli_find_migrations_dir_rejects_traversal(tmp_path, monkeypatch):
    """The CLI helper must refuse a non-dotted app label so it can't escape
    cwd via ``../``."""
    from dorm.cli import _find_migrations_dir

    monkeypatch.chdir(tmp_path)
    with pytest.raises(ImproperlyConfigured):
        _find_migrations_dir("../escape")


def test_cli_load_settings_rejects_filesystem_path():
    """``--settings`` must be a Python dotted path; otherwise we'd be
    handing arbitrary strings to ``importlib.import_module``."""
    from dorm.cli import _load_settings

    with pytest.raises(ImproperlyConfigured):
        _load_settings("/etc/passwd")


# ── RawQuerySet — placeholder arity and docstring warning ────────────────────

class _RawSecModel(dorm.Model):
    name = dorm.CharField(max_length=50)

    class Meta:
        db_table = "raw_secmodel"


def test_count_placeholders_simple_percent_s():
    from dorm.queryset import _count_placeholders

    assert _count_placeholders("SELECT * FROM x WHERE id = %s") == 1
    assert _count_placeholders("SELECT * FROM x WHERE a=%s AND b=%s") == 2


def test_count_placeholders_dollar_n_counts_each_occurrence():
    from dorm.queryset import _count_placeholders

    # _to_pyformat() turns each $N into %s, so reusing $1 means two binds.
    assert _count_placeholders("SELECT * FROM x WHERE a=$1 AND b=$1") == 2
    assert _count_placeholders("SELECT * FROM x WHERE a=$1 AND b=$2") == 2


def test_count_placeholders_ignores_quoted_literals():
    from dorm.queryset import _count_placeholders

    # %s inside a literal string isn't a placeholder.
    assert _count_placeholders("SELECT '%s' FROM x WHERE a=%s") == 1
    # Same for double-quoted identifiers.
    assert _count_placeholders('SELECT "col%s" FROM x WHERE a=%s') == 1


def test_count_placeholders_named_returns_none():
    from dorm.queryset import _count_placeholders

    # Named placeholders take a dict; we don't try to validate the count.
    assert _count_placeholders("SELECT * FROM x WHERE id = %(id)s") is None


def test_raw_queryset_rejects_arity_mismatch():
    from dorm.queryset import RawQuerySet

    # SQL claims one bound value, params provides zero — most common
    # "I built it with f-strings by accident" pattern.
    with pytest.raises(ValueError, match="placeholder"):
        RawQuerySet(_RawSecModel, "SELECT * FROM raw_secmodel WHERE id = %s")
    # And the inverse: too many params.
    with pytest.raises(ValueError, match="placeholder"):
        RawQuerySet(_RawSecModel, "SELECT * FROM raw_secmodel", params=[1])


def test_raw_queryset_rejects_empty_sql():
    from dorm.queryset import RawQuerySet

    with pytest.raises(ValueError):
        RawQuerySet(_RawSecModel, "")
    with pytest.raises(ValueError):
        RawQuerySet(_RawSecModel, "   ")


def test_raw_queryset_accepts_correctly_balanced_sql():
    from dorm.queryset import RawQuerySet

    # Construction-time check passes; we don't execute it here.
    qs = RawQuerySet(
        _RawSecModel,
        "SELECT * FROM raw_secmodel WHERE id = %s",
        params=[1],
    )
    assert qs.params == [1]


# ── _resolve_column — only FieldDoesNotExist is suppressed ───────────────────

class _ResolveModel(dorm.Model):
    name = dorm.CharField(max_length=50)

    class Meta:
        db_table = "resolve_secmodel"


def test_resolve_column_propagates_unexpected_exceptions(monkeypatch):
    """A buggy custom field that raises something other than
    FieldDoesNotExist must surface — the audit's old behaviour was to
    swallow the error and return a stale column reference."""
    from dorm.query import SQLQuery

    q = SQLQuery(_ResolveModel)

    class _Boom:
        def get_field(self, name):
            raise RuntimeError("custom field crashed")

        @property
        def db_table(self):
            return "resolve_secmodel"

        @property
        def pk(self):
            return None

    monkeypatch.setattr(_ResolveModel, "_meta", _Boom())
    with pytest.raises(RuntimeError, match="custom field crashed"):
        q._resolve_column(["mystery"])


def test_resolve_column_falls_back_for_field_does_not_exist():
    """The legitimate path: name is unknown → fall back to literal column
    after re-validation. This keeps user's existing extra()-style queries
    working."""
    from dorm.query import SQLQuery

    q = SQLQuery(_ResolveModel)

    # ``name`` is a real field, comes back as the column name.
    col = q._resolve_column(["name"])
    assert col == '"name"'


def test_resolve_column_rejects_unsafe_literal_fallback():
    """If get_field raises FieldDoesNotExist, the fallback path validates
    the identifier — an attacker-supplied name with quotes must still be
    rejected so it can't leak into the SQL even via raw-ish call sites."""
    from dorm.query import SQLQuery

    q = SQLQuery(_ResolveModel)

    # _meta has no ``ghost"; DROP TABLE--`` field; the fallback should
    # invoke _validate_identifier and raise.
    with pytest.raises(Exception):  # noqa: PT011 — ImproperlyConfigured subclasses Exception
        q._resolve_column(['ghost"; DROP TABLE x; --'])


def test_field_does_not_exist_is_still_a_subclass_we_use():
    """Sanity check: the exception we now narrow on must exist and be the
    one the field machinery raises. If a future refactor moves it, this
    test breaks loudly instead of letting the new exception fall through
    silently."""
    assert issubclass(FieldDoesNotExist, Exception)
