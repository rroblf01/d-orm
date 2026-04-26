"""Extra unit tests covering internals that were under-covered:
conf.py (settings, _discover_apps, _autodiscover_settings),
signals.py (connect/disconnect/dispatch, weak refs, dispatch_uid),
expressions.py (Q, F, CombinedExpression, Value, RawSQL),
db.connection (router resolution, error paths, health_check)."""

from __future__ import annotations

from pathlib import Path

import pytest

import dorm
from dorm.exceptions import ImproperlyConfigured


# ── conf.py ───────────────────────────────────────────────────────────────────


def test_validate_identifier_rejects_empty():
    from dorm.conf import _validate_identifier

    with pytest.raises(ImproperlyConfigured):
        _validate_identifier("")
    with pytest.raises(ImproperlyConfigured):
        _validate_identifier(None)  # ty: ignore[invalid-argument-type]


def test_validate_identifier_rejects_too_long():
    from dorm.conf import _validate_identifier

    with pytest.raises(ImproperlyConfigured):
        _validate_identifier("a" * 100)


def test_validate_identifier_rejects_special_chars():
    from dorm.conf import _validate_identifier

    with pytest.raises(ImproperlyConfigured):
        _validate_identifier('"; DROP TABLE x; --')
    with pytest.raises(ImproperlyConfigured):
        _validate_identifier("has-dash")
    with pytest.raises(ImproperlyConfigured):
        _validate_identifier("123starts_with_digit")


def test_validate_identifier_accepts_valid():
    from dorm.conf import _validate_identifier

    assert _validate_identifier("foo") == "foo"
    assert _validate_identifier("_bar") == "_bar"
    assert _validate_identifier("Field42") == "Field42"


def test_discover_apps_finds_packages_with_models(tmp_path: Path):
    from dorm.conf import _discover_apps

    (tmp_path / "blog").mkdir()
    (tmp_path / "blog" / "__init__.py").touch()
    (tmp_path / "blog" / "models.py").write_text("# blog\n")

    nested = tmp_path / "store" / "products"
    nested.mkdir(parents=True)
    (tmp_path / "store" / "__init__.py").touch()
    (tmp_path / "store" / "products" / "__init__.py").touch()
    (tmp_path / "store" / "products" / "models.py").write_text("# nested\n")

    apps = _discover_apps(tmp_path)
    assert "blog" in apps
    assert "store.products" in apps


def test_discover_apps_skips_excluded_dirs(tmp_path: Path):
    from dorm.conf import _discover_apps

    venv = tmp_path / ".venv" / "site-packages" / "fakelib"
    venv.mkdir(parents=True)
    (venv / "__init__.py").touch()
    (venv / "models.py").write_text("# vendored\n")

    apps = _discover_apps(tmp_path)
    assert apps == []


def test_discover_apps_skips_dirs_without_init(tmp_path: Path):
    """A package needs __init__.py — a bare folder with a models.py is not an app."""
    from dorm.conf import _discover_apps

    bare = tmp_path / "loose"
    bare.mkdir()
    (bare / "models.py").write_text("# no __init__\n")

    apps = _discover_apps(tmp_path)
    assert apps == []


def test_unconfigured_setting_raises_improperly_configured():
    from dorm.conf import Settings

    s = Settings.__new__(Settings)
    with pytest.raises(ImproperlyConfigured):
        _ = s.NONEXISTENT_SETTING


def test_configure_sets_arbitrary_keys():
    """`dorm.configure()` accepts arbitrary kwargs and exposes them on settings."""
    from dorm.conf import settings

    saved = {
        "DATABASES": dict(settings.DATABASES),
        "INSTALLED_APPS": list(settings.INSTALLED_APPS),
    }
    try:
        dorm.configure(MY_CUSTOM_SETTING="hello")
        assert settings.MY_CUSTOM_SETTING == "hello"
    finally:
        dorm.configure(**saved)


# ── signals.py ────────────────────────────────────────────────────────────────


def test_signal_connect_and_send():
    from dorm.signals import Signal

    sig = Signal()
    received = []

    def handler(sender, value, **kw):
        received.append((sender, value))

    sig.connect(handler, weak=False)
    sig.send(sender="src", value=42)
    assert received == [("src", 42)]


def test_signal_disconnect_by_receiver():
    from dorm.signals import Signal

    sig = Signal()
    calls = []

    def handler(sender, **kw):
        calls.append(sender)

    sig.connect(handler, weak=False)
    assert sig.disconnect(handler) is True
    sig.send(sender="x")
    assert calls == []


def test_signal_disconnect_by_dispatch_uid():
    from dorm.signals import Signal

    sig = Signal()
    calls = []

    def handler(sender, **kw):
        calls.append(sender)

    sig.connect(handler, weak=False, dispatch_uid="my-uid")
    sig.disconnect(dispatch_uid="my-uid")
    sig.send(sender="x")
    assert calls == []


def test_signal_disconnect_by_sender():
    from dorm.signals import Signal

    class A:
        pass

    class B:
        pass

    sig = Signal()

    def h_a(sender, **kw): pass
    def h_b(sender, **kw): pass

    sig.connect(h_a, sender=A, weak=False)
    sig.connect(h_b, sender=B, weak=False)
    sig.disconnect(sender=A)
    # Only h_b should remain
    assert len(sig._receivers) == 1
    assert sig._receivers[0][2] is B


def test_signal_filter_by_sender():
    from dorm.signals import Signal

    class A:
        pass

    class B:
        pass

    sig = Signal()
    seen = []

    def handler(sender, **kw):
        seen.append(sender)

    sig.connect(handler, sender=A, weak=False)
    sig.send(sender=A)
    sig.send(sender=B)   # filtered out
    assert seen == [A]


def test_signal_swallows_handler_exceptions():
    """A signal handler that raises must not break the dispatch loop —
    other receivers still run."""
    from dorm.signals import Signal

    sig = Signal()
    seen = []

    def bad(sender, **kw):
        raise RuntimeError("boom")

    def good(sender, **kw):
        seen.append(sender)

    sig.connect(bad, weak=False)
    sig.connect(good, weak=False)
    sig.send(sender="x")
    assert seen == ["x"]


def test_signal_weak_ref_is_dropped_after_gc():
    """Weakly-connected receivers should be auto-cleaned when their target dies."""
    import gc
    from dorm.signals import Signal

    sig = Signal()
    seen = []

    class Recv:
        def __call__(self, sender, **kw):
            seen.append(sender)

    r = Recv()
    sig.connect(r)  # weak by default
    sig.send(sender="alive")
    assert seen == ["alive"]

    del r
    gc.collect()
    sig.send(sender="dead")
    # No new entry — the weakref returned None so the handler was skipped
    assert seen == ["alive"]


def test_signal_repr():
    from dorm.signals import Signal
    assert "Signal" in repr(Signal())


def test_signal_connect_replaces_same_uid():
    """Connecting a second receiver with the same dispatch_uid replaces the first."""
    from dorm.signals import Signal

    sig = Signal()
    log = []

    def first(sender, **kw):
        log.append(("first", sender))

    def second(sender, **kw):
        log.append(("second", sender))

    sig.connect(first, dispatch_uid="x", weak=False)
    sig.connect(second, dispatch_uid="x", weak=False)
    sig.send(sender="hi")
    assert log == [("second", "hi")]


# ── expressions.py ────────────────────────────────────────────────────────────


def test_q_object_combine_and_repr():
    from dorm.expressions import Q

    q1 = Q(name="A")
    q2 = Q(age__gte=18)
    combined = q1 & q2
    assert combined.connector == Q.AND
    assert q1 in combined.children and q2 in combined.children

    or_combined = q1 | q2
    assert or_combined.connector == Q.OR

    inverted = ~q1
    assert inverted.negated is True
    assert "~Q" in repr(inverted)
    assert "Q" in repr(q1)


def test_q_combine_with_non_q_raises():
    from dorm.expressions import Q

    with pytest.raises(TypeError):
        Q(a=1) & "not-a-q"  # ty: ignore[unsupported-operator]


def test_f_arithmetic_returns_combined_expression():
    from dorm.expressions import CombinedExpression, F

    expr = F("views") + 1
    assert isinstance(expr, CombinedExpression)
    assert expr.operator == "+"
    assert expr.rhs == 1

    assert (F("a") - 1).operator == "-"
    assert (F("a") * 2).operator == "*"
    assert (F("a") / 4).operator == "/"


def test_f_negation_returns_combined_expression():
    from dorm.expressions import CombinedExpression, F, Value

    neg = -F("amount")
    assert isinstance(neg, CombinedExpression)
    assert neg.operator == "-"
    assert isinstance(neg.lhs, Value) and neg.lhs.value == 0


def test_f_repr():
    from dorm.expressions import F
    assert repr(F("score")) == "F('score')"


def test_value_repr():
    from dorm.expressions import Value
    assert "Value(42)" in repr(Value(42))


def test_raw_sql_as_sql_returns_sql_and_params():
    from dorm.expressions import RawSQL

    rs = RawSQL("SELECT * FROM t WHERE x = %s", (42,))
    sql, params = rs.as_sql()
    assert sql == "SELECT * FROM t WHERE x = %s"
    assert params == [42]


# ── db.connection: routers and error paths ───────────────────────────────────


def test_router_db_for_read_picks_first_truthy():
    from dorm.conf import settings
    from dorm.db.connection import router_db_for_read

    class R1:
        def db_for_read(self, model, **h):
            return None

    class R2:
        def db_for_read(self, model, **h):
            return "replica"

    saved = list(getattr(settings, "DATABASE_ROUTERS", []))
    try:
        settings.DATABASE_ROUTERS = [R1(), R2()]
        assert router_db_for_read(model=object()) == "replica"
    finally:
        settings.DATABASE_ROUTERS = saved


def test_router_db_for_read_swallows_router_exceptions():
    from dorm.conf import settings
    from dorm.db.connection import router_db_for_read

    class Bad:
        def db_for_read(self, model, **h):
            raise RuntimeError("router crashed")

    class Good:
        def db_for_read(self, model, **h):
            return "secondary"

    saved = list(getattr(settings, "DATABASE_ROUTERS", []))
    try:
        settings.DATABASE_ROUTERS = [Bad(), Good()]
        assert router_db_for_read(model=object()) == "secondary"
    finally:
        settings.DATABASE_ROUTERS = saved


def test_router_db_for_write_falls_back_to_default():
    from dorm.conf import settings
    from dorm.db.connection import router_db_for_write

    class None_returning:
        def db_for_write(self, model, **h):
            return None

    saved = list(getattr(settings, "DATABASE_ROUTERS", []))
    try:
        settings.DATABASE_ROUTERS = [None_returning()]
        assert router_db_for_write(model=object()) == "default"
    finally:
        settings.DATABASE_ROUTERS = saved


def test_router_skips_when_method_missing():
    """A router that doesn't implement db_for_read should be skipped, not crash."""
    from dorm.conf import settings
    from dorm.db.connection import router_db_for_read

    class WriteOnly:
        def db_for_write(self, model, **h): return "primary"

    class ReadOnly:
        def db_for_read(self, model, **h): return "replica"

    saved = list(getattr(settings, "DATABASE_ROUTERS", []))
    try:
        settings.DATABASE_ROUTERS = [WriteOnly(), ReadOnly()]
        assert router_db_for_read(model=object()) == "replica"
    finally:
        settings.DATABASE_ROUTERS = saved


def test_get_settings_unknown_alias_raises():
    from dorm.db.connection import _get_settings

    with pytest.raises(ImproperlyConfigured) as exc:
        _get_settings("does-not-exist")
    assert "does-not-exist" in str(exc.value)


def test_create_connection_unsupported_engine():
    from dorm.db.connection import _create_sync_connection, _create_async_connection

    with pytest.raises(ImproperlyConfigured):
        _create_sync_connection("default", {"ENGINE": "mysql", "NAME": "x"})
    with pytest.raises(ImproperlyConfigured):
        _create_async_connection("default", {"ENGINE": "mysql", "NAME": "x"})


def test_health_check_returns_ok_dict():
    """health_check on the active alias must always answer with status."""
    from dorm.db.connection import health_check

    result = health_check("default")
    assert result["status"] == "ok"
    assert result["alias"] == "default"
    assert isinstance(result["elapsed_ms"], float)


def test_health_check_unknown_alias_returns_error_dict():
    """health_check never raises — it converts the failure into a dict."""
    from dorm.db.connection import health_check

    result = health_check("alias-that-does-not-exist")
    assert result["status"] == "error"
    assert "error" in result
    assert "elapsed_ms" in result


@pytest.mark.asyncio
async def test_ahealth_check_returns_ok_dict():
    from dorm.db.connection import ahealth_check

    result = await ahealth_check("default")
    assert result["status"] == "ok"
    assert result["alias"] == "default"


@pytest.mark.asyncio
async def test_ahealth_check_unknown_alias_returns_error():
    from dorm.db.connection import ahealth_check

    result = await ahealth_check("alias-that-does-not-exist")
    assert result["status"] == "error"
    assert "error" in result
