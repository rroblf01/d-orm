"""Regression tests for the v2.6 self-audit fixes.

Each test pins the behaviour described in the v2.6 review:

- DORM-M002 now fires on EVERY ``AlterField`` (the old version
  required an ``old_field`` attribute that ``AlterField`` doesn't
  carry — so it never fired in practice).
- ``assertNumQueriesFactory`` detects ``async def`` test functions
  and returns an async wrapper instead of a sync one (the sync
  wrapper exited the ``assertNumQueries`` block before the
  coroutine awaited any query, so every async test failed with
  count 0).
- ``lint_migration_file`` cleans its ``sys.modules`` entry on
  exit so a repeated run doesn't accumulate ``_dorm_lint_*``
  module objects.
- ``_slow_query_ms`` / ``_retry_attempts`` / ``_retry_backoff``
  use real ``isinstance`` checks (not ``assert``) so ``python -O``
  can't disarm the type guard around the memoised value.
"""

from __future__ import annotations

import sys

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Fix 1 — DORM-M002 fires on every AlterField
# ──────────────────────────────────────────────────────────────────────────────


def test_dorm_m002_fires_on_every_alter_field():
    from dorm.fields import IntegerField
    from dorm.migrations import operations as ops
    from dorm.migrations.lint import lint_operations

    op = ops.AlterField("MyModel", "score", IntegerField(null=True))
    result = lint_operations([op], file="0001_test.py")
    codes = [f.code for f in result.findings]
    assert "DORM-M002" in codes, (
        "AlterField now triggers DORM-M002 unconditionally so reviewers "
        "explicitly approve type / nullability changes (the previous "
        "version checked an op.old_field attribute that AlterField never "
        "carries — so the rule was dead code)."
    )


def test_dorm_m002_suppressible_with_noqa(tmp_path):
    """A ``# noqa: DORM-M002`` comment in the migration file silences
    the finding for that whole file."""
    from dorm.migrations.lint import lint_migration_file

    # Build a real migration file with the suppression.
    p = tmp_path / "0001_alter.py"
    p.write_text(
        "from dorm.migrations import operations as ops\n"
        "from dorm.fields import IntegerField  # noqa: DORM-M002\n"
        "\n"
        "class Migration:\n"
        "    operations = [ops.AlterField('MyModel', 'score', IntegerField())]\n"
    )
    result = lint_migration_file(p)
    codes = {f.code for f in result.findings}
    assert "DORM-M002" not in codes


# ──────────────────────────────────────────────────────────────────────────────
# Fix 2 — assertNumQueriesFactory works on async functions
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_assert_num_queries_factory_async():
    """The decorator must detect ``async def`` and return an async
    wrapper — otherwise the inner queries fire AFTER the count
    context manager has already exited."""
    from dorm.test import assertNumQueriesFactory
    from tests.models import Author

    @assertNumQueriesFactory(1)
    async def acount():
        return await Author.objects.filter(name="x").acount()

    # If the decorator returned a sync wrapper, this awaitable would
    # be a coroutine that never ran inside the count window — the
    # assertion would fail with "expected 1, got 0".
    n = await acount()
    assert n == 0


def test_assert_num_queries_factory_sync_unchanged():
    """The sync path keeps working — the async detection didn't break
    the original behaviour."""
    from dorm.test import assertNumQueriesFactory
    from tests.models import Author

    @assertNumQueriesFactory(1)
    def count():
        return Author.objects.filter(name="x").count()

    assert count() == 0


# ──────────────────────────────────────────────────────────────────────────────
# Fix 3 — lint_migration_file pops sys.modules on exit
# ──────────────────────────────────────────────────────────────────────────────


def test_lint_migration_file_cleans_sys_modules(tmp_path):
    from dorm.migrations.lint import lint_migration_file

    p = tmp_path / "0042_test_clean.py"
    p.write_text(
        "from dorm.migrations import operations as ops\n"
        "from dorm.fields import IntegerField\n"
        "\n"
        "class Migration:\n"
        "    operations = [ops.AddField('MyModel', 'score', IntegerField(null=True))]\n"
    )
    spec_name = "_dorm_lint_0042_test_clean"
    assert spec_name not in sys.modules
    lint_migration_file(p)
    assert spec_name not in sys.modules, (
        "lint_migration_file must drop its temporary sys.modules entry "
        "so a long linter run doesn't accumulate _dorm_lint_* keys."
    )


def test_lint_migration_file_cleans_sys_modules_on_import_error(tmp_path):
    """Even when the migration fails to import, the temporary entry
    must be dropped — otherwise a corrupt migration leaves a
    half-loaded ghost module behind."""
    from dorm.migrations.lint import lint_migration_file

    p = tmp_path / "0099_broken.py"
    p.write_text("raise RuntimeError('intentionally broken migration')\n")

    spec_name = "_dorm_lint_0099_broken"
    assert spec_name not in sys.modules
    result = lint_migration_file(p)
    assert spec_name not in sys.modules
    # And the import error itself surfaces as a DORM-M000 finding so
    # the CI gate still fails loudly.
    codes = {f.code for f in result.findings}
    assert "DORM-M000" in codes


# ──────────────────────────────────────────────────────────────────────────────
# Fix 4 — explicit isinstance (no `assert`) survives `python -O`
# ──────────────────────────────────────────────────────────────────────────────


def test_slow_query_cache_recovers_from_corrupt_value():
    """If something pollutes the slow-query cache with a non-numeric
    value, the next read must resolve a fresh value instead of
    returning the garbage. Pre-fix the guard was ``assert
    isinstance(...)`` which is stripped by ``python -O`` — under -O
    the corrupt value would have leaked straight to the comparison."""
    import dorm.db.utils as u

    u._invalidate_slow_query_cache()
    # Simulate corruption by writing an unexpected type into the
    # memoised cache directly.
    u._SLOW_QUERY_MS_SETTING._cache = object()
    val = u._slow_query_ms()
    # Either a float threshold or None — never the corrupt object.
    assert val is None or isinstance(val, float)


def test_retry_cache_recovers_from_corrupt_value():
    import dorm.db.utils as u

    u._invalidate_retry_cache()
    u._RETRY_ATTEMPTS_SETTING._cache = "not-an-int"
    u._RETRY_BACKOFF_SETTING._cache = "not-a-float"

    a = u._retry_attempts()
    b = u._retry_backoff()
    assert isinstance(a, int)
    assert isinstance(b, float)


# ──────────────────────────────────────────────────────────────────────────────
# Fix 5 — cmd_lint_migrations loads apps (smoke test via API)
# ──────────────────────────────────────────────────────────────────────────────


def test_lint_directory_handles_app_imports(tmp_path):
    """Migration files that import from a sibling ``app.models``
    module work as long as the app is importable. The CLI handles
    the ``_load_apps`` dance; this test exercises the underlying
    ``lint_directory`` API."""
    from dorm.migrations.lint import lint_directory

    # Create an app with a models.py and a migration that imports
    # from it. The migration is otherwise clean — no findings.
    (tmp_path / "myapp").mkdir()
    (tmp_path / "myapp" / "__init__.py").write_text("")
    (tmp_path / "myapp" / "models.py").write_text(
        "import dorm\n"
        "class Widget(dorm.Model):\n"
        "    name = dorm.CharField(max_length=20)\n"
        "    class Meta:\n"
        "        app_label = 'myapp'\n"
    )
    (tmp_path / "myapp" / "migrations").mkdir()
    (tmp_path / "myapp" / "migrations" / "__init__.py").write_text("")
    (tmp_path / "myapp" / "migrations" / "0001_initial.py").write_text(
        "from dorm.migrations import operations as ops\n"
        "from dorm.fields import IntegerField\n"
        "from myapp.models import Widget  # noqa: F401\n"
        "\n"
        "class Migration:\n"
        "    operations = [\n"
        "        ops.AddField('Widget', 'count', IntegerField(null=True))\n"
        "    ]\n"
    )

    sys.path.insert(0, str(tmp_path))
    try:
        # The import inside the migration ``from myapp.models import
        # Widget`` succeeds because we put tmp_path on sys.path —
        # mirrors what ``cmd_lint_migrations`` does via
        # ``_load_apps``.
        result = lint_directory(tmp_path / "myapp" / "migrations")
    finally:
        sys.path.remove(str(tmp_path))
        # Drop test-only modules so they don't pollute later tests.
        for name in list(sys.modules):
            if name == "myapp" or name.startswith("myapp."):
                sys.modules.pop(name, None)

    # Clean migration → no findings.
    codes = [f.code for f in result.findings]
    assert "DORM-M000" not in codes, (
        f"Import error swallowed silently — got findings: {codes!r}"
    )
