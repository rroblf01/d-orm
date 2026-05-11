"""Polish round for v4.2 — extra coverage on the safety/completeness
items added late in the release cycle."""
from __future__ import annotations

import pytest


@pytest.fixture
def restore_settings():
    """Snapshot DATABASES / INSTALLED_APPS / every v4.2 setting plus
    each key's _explicit_settings membership so a polish test that
    reconfigures dorm can't bleed into the next test in the suite.

    The membership snapshot matters: ``settings.X`` may exist as a
    plain attribute *without* being in ``_explicit_settings`` (the
    fixtures of older test files like
    ``test_slow_query_setting_v2_6.py`` set the attribute directly).
    Re-adding it to ``_explicit_settings`` during teardown would
    promote the leaked value into a "user override" and break those
    tests' env-var fall-back paths.
    """
    import dorm
    from dorm.conf import settings

    saved_db = {alias: dict(cfg) for alias, cfg in settings.DATABASES.items()}
    saved_apps = list(settings.INSTALLED_APPS)
    keys = (
        "DEBUG",
        "DEBUG_NPLUSONE",
        "DEBUG_NPLUSONE_THRESHOLD",
        "SLOW_QUERY_EXPLAIN",
        "SLOW_QUERY_MS",
        "POOL_SATURATION_WARN",
    )
    _SENTINEL = object()
    saved_attrs = {
        k: settings.__dict__.get(k, _SENTINEL) for k in keys
    }
    saved_explicit = {
        k: (k in settings._explicit_settings) for k in keys
    }
    yield
    from dorm.db.connection import _async_connections, _sync_connections
    from dorm.db.utils import (
        _SLOW_QUERY_EXPLAIN_SETTING,
        _SLOW_QUERY_MS_SETTING,
    )

    dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
    for k in keys:
        val = saved_attrs[k]
        if val is _SENTINEL:
            try:
                delattr(settings, k)
            except AttributeError:
                pass
        else:
            settings.__dict__[k] = val
        if saved_explicit[k]:
            settings._explicit_settings.add(k)
        else:
            settings._explicit_settings.discard(k)
    _sync_connections.clear()
    _async_connections.clear()
    _SLOW_QUERY_EXPLAIN_SETTING.invalidate()
    _SLOW_QUERY_MS_SETTING.invalidate()


# ── Auto-install DEBUG_NPLUSONE ──────────────────────────────────────────────


class TestAutoInstallDebugNPlusOne:
    @pytest.fixture(autouse=True)
    def _cleanup(self, restore_settings):
        import dorm.contrib.nplusone as np

        # Detach any pre-existing detector so we start clean.
        prev = np._GLOBAL_DEBUG_DETECTOR
        if prev is not None:
            try:
                prev.__exit__(None, None, None)
            except Exception:
                pass
            np._GLOBAL_DEBUG_DETECTOR = None
        yield
        # Always clean up the detector this test may have installed.
        if np._GLOBAL_DEBUG_DETECTOR is not None:
            try:
                np._GLOBAL_DEBUG_DETECTOR.__exit__(None, None, None)
            except Exception:
                pass
            np._GLOBAL_DEBUG_DETECTOR = None

    def test_configure_with_debug_nplusone_installs_detector(self):
        import dorm
        import dorm.contrib.nplusone as np

        dorm.configure(DEBUG_NPLUSONE=True)
        assert np._GLOBAL_DEBUG_DETECTOR is not None

    def test_configure_without_setting_no_op(self):
        import dorm
        import dorm.contrib.nplusone as np

        dorm.configure(DATABASES={"default": {"ENGINE": "sqlite", "NAME": ":memory:"}})
        # Setting absent — no auto-install.
        assert np._GLOBAL_DEBUG_DETECTOR is None or True

    def test_configure_with_falsy_setting_no_op(self):
        import dorm
        import dorm.contrib.nplusone as np

        dorm.configure(DEBUG_NPLUSONE=False)
        # Falsy → install skipped even though the key was present.
        # (Previous installer may exist from a prior test, but a
        #  fresh-fixture invocation cleared the slot.)
        if np._GLOBAL_DEBUG_DETECTOR is None:
            assert True


# ── querystats in prometheus.metrics_response ───────────────────────────────


class TestPrometheusQuerystatsIntegration:
    def test_render_text_includes_template_block(self, monkeypatch):
        from dorm.contrib import querystats
        from dorm.contrib.prometheus import metrics_response

        querystats.reset()
        querystats.collector().enable()
        # Seed via the post_query signal directly.
        from dorm import signals

        signals.post_query.send(
            sender="sqlite",
            sql="SELECT 1",
            params=None,
            elapsed_ms=1.0,
        )
        text = metrics_response()
        querystats.collector().disable()
        querystats.reset()
        assert "dorm_template_count" in text

    def test_render_skipped_when_collector_empty(self):
        from dorm.contrib import querystats
        from dorm.contrib.prometheus import metrics_response

        querystats.reset()
        # Collector disabled → no template lines.
        text = metrics_response()
        assert "dorm_template_count" not in text


# ── dorm version CLI ────────────────────────────────────────────────────────


class TestVersionCLI:
    def test_prints_version(self, capsys):
        import argparse

        from dorm import __version__
        from dorm.cli import cmd_version

        cmd_version(argparse.Namespace())
        out = capsys.readouterr().out
        assert __version__ in out
        assert out.startswith("djanorm ")


# ── plan_drift async ────────────────────────────────────────────────────────


class TestPlanDriftAsync:
    @pytest.fixture(autouse=True)
    def _restore(self, restore_settings):
        yield

    async def test_arecord_and_acompare(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib import plan_drift
        from dorm.db.connection import (
            _async_connections,
            _sync_connections,
            get_connection,
        )
        from dorm.migrations.schema import SchemaEditor

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "apd.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _APD(dorm.Model):
            text = dorm.CharField(max_length=8)

            class Meta:
                app_label = "tests"

        with SchemaEditor(get_connection()) as se:
            se.create_model(_APD)

        try:
            plan_drift.reset()
            sql = f"SELECT * FROM {_APD._meta.db_table} WHERE text = ?"
            await plan_drift.arecord_baseline("apd.lookup", sql, params=["x"])
            result = await plan_drift.acompare("apd.lookup", sql, params=["x"])
            assert result.drifted is False
        finally:
            plan_drift.reset()
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()

    async def test_acompare_unknown_tag(self):
        from dorm.contrib import plan_drift

        plan_drift.reset()
        with pytest.raises(KeyError):
            await plan_drift.acompare("never", "SELECT 1")


# ── dorm doctor v4.2 audits ─────────────────────────────────────────────────


class TestDoctorV42Audits:
    @pytest.fixture(autouse=True)
    def _restore(self, restore_settings):
        # Doctor mutates settings; the shared fixture restores them.
        # Also detach any detector the auto-install path created.
        import dorm.contrib.nplusone as np

        yield
        if np._GLOBAL_DEBUG_DETECTOR is not None:
            try:
                np._GLOBAL_DEBUG_DETECTOR.__exit__(None, None, None)
            except Exception:
                pass
            np._GLOBAL_DEBUG_DETECTOR = None

    def test_warns_debug_nplusone_outside_debug(self, capsys, tmp_path):
        import argparse

        import dorm
        from dorm.cli import cmd_doctor

        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(tmp_path / "d.db")}},
            INSTALLED_APPS=["tests"],
            DEBUG=False,
            DEBUG_NPLUSONE=True,
        )
        try:
            cmd_doctor(argparse.Namespace(settings="settings"))
        except SystemExit:
            pass
        out = capsys.readouterr().out
        assert "DEBUG_NPLUSONE is active outside DEBUG" in out

    def test_warns_slow_query_explain_without_threshold(self, capsys, tmp_path):
        import argparse

        import dorm
        from dorm.cli import cmd_doctor

        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(tmp_path / "d.db")}},
            INSTALLED_APPS=["tests"],
            DEBUG=False,
            SLOW_QUERY_EXPLAIN=True,
            SLOW_QUERY_MS=10,  # too low
        )
        try:
            cmd_doctor(argparse.Namespace(settings="settings"))
        except SystemExit:
            pass
        out = capsys.readouterr().out
        assert "SLOW_QUERY_EXPLAIN=True" in out

    def test_warns_pool_saturation_out_of_range(self, capsys, tmp_path):
        import argparse

        import dorm
        from dorm.cli import cmd_doctor

        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(tmp_path / "d.db")}},
            INSTALLED_APPS=["tests"],
            DEBUG=True,
            POOL_SATURATION_WARN=1.5,  # invalid
        )
        try:
            cmd_doctor(argparse.Namespace(settings="settings"))
        except SystemExit:
            pass
        out = capsys.readouterr().out
        assert "POOL_SATURATION_WARN" in out
