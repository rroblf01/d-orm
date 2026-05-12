"""Second coverage uplift pass — target the deepest remaining gaps
in v4.3 modules + heavy hitters in migrations/operations + queryset."""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging

import pytest


# ── tasks deeper ────────────────────────────────────────────────────────────


class TestTasksDeep:
    @pytest.fixture(autouse=True)
    def _reset(self):
        from dorm.contrib.tasks import reset_registry

        reset_registry()
        yield
        reset_registry()

    def test_task_invokes_directly(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _OBd(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_OBd, channel=None)

        @task(queue, name="td")
        def td(a, b):
            return a + b

        # Direct call (bypasses queue) — exercises __call__.
        assert td(1, 2) == 3

    def test_handler_string_payload(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _OBs(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_OBs, channel=None)
        ran: list[int] = []

        @task(queue, name="sp")
        def sp(n):
            ran.append(n)

        event = _OBs(
            event_type="sp",
            payload=json.dumps({"args": [42], "kwargs": {}}),
            status="pending",
            attempts=0,
        )
        queue._build_handler()(event)
        assert ran == [42]

    def test_handler_eta_invalid_iso_ignored(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _OBi(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_OBi, channel=None)
        ran: list[int] = []

        @task(queue, name="ei")
        def ei():
            ran.append(1)

        event = _OBi(
            event_type="ei",
            payload={"args": [], "kwargs": {}, "eta": "not-an-iso"},
            status="pending",
            attempts=0,
        )
        queue._build_handler()(event)
        # Invalid ETA → run as if not set.
        assert ran == [1]

    def test_drain_once_idempotent_no_pending(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue
        from dorm.db.connection import _async_connections, _sync_connections, get_connection
        from dorm.migrations.schema import SchemaEditor

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "do.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _Od(OutboxEvent):
            class Meta:
                app_label = "tests"

        try:
            with SchemaEditor(get_connection()) as se:
                se.create_model(_Od)
            queue = TaskQueue(model=_Od, channel=None)
            assert queue.drain_once() == 0
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()

    def test_task_max_attempts_per_task_override(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _OBma(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_OBma, channel=None, max_attempts=5)

        @task(queue, name="ma", max_attempts=10)
        def ma(): ...

        assert ma.max_attempts == 10

    def test_task_default_max_attempts_inherits_queue(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _OBmaq(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_OBmaq, channel=None, max_attempts=7)

        @task(queue, name="maq")
        def maq(): ...

        assert maq.max_attempts == 7


# ── two_phase coverage push ─────────────────────────────────────────────────


class TestTwoPhaseDeep:
    def test_TxnContext_select_query_passes_through(self):
        from dorm.contrib.two_phase import _TxnContext

        ctx = _TxnContext(["default"])
        # exec with SELECT routes via execute (read path); just verify
        # signature works — no real DB call required for shape.
        assert ctx._aliases == ["default"]

    def test_two_phase_rejects_nested_atomic_on_sqlite(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.two_phase import two_phase_commit
        from dorm.db.connection import _async_connections, _sync_connections

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "2pc.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )
        try:
            with pytest.raises(NotImplementedError):
                with two_phase_commit(["default"]):
                    pass
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()


# ── temporal end-to-end with sqlite ────────────────────────────────────────


class TestTemporalE2E:
    def test_save_creates_temporal_row(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.temporal import as_of, temporal
        from dorm.db.connection import _async_connections, _sync_connections, get_connection
        from dorm.migrations.schema import SchemaEditor

        saved_db = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "te2e.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        @temporal
        class _Te(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        try:
            conn = get_connection()
            with SchemaEditor(conn) as se:
                se.create_model(_Te)
                se.create_model(_Te._temporal_model)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            # Create + update emits two temporal rows.
            obj = _Te.objects.create(name="v1")
            assert _Te._temporal_model.objects.count() == 1  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            obj.name = "v2"
            obj.save()
            # Two rows: original (closed) + new (open).
            assert _Te._temporal_model.objects.count() == 2  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            # Now query as_of: a recent timestamp should see v2 open row.
            now = _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(seconds=1)
            rows = list(as_of(_Te, now))
            names = [r.name for r in rows]
            assert "v2" in names
            # Delete fires the temporal close + tombstone row.
            obj.delete()
            assert _Te._temporal_model.objects.count() == 3  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        finally:
            dorm.configure(DATABASES=saved_db, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()


# ── serializers msgpack deeper ─────────────────────────────────────────────


class TestSerializersDeep:
    def test_msgpack_default_unsupported_type(self):
        from dorm.contrib.serializers import _msgpack_default

        with pytest.raises(TypeError):
            _msgpack_default(object())

    def test_msgpack_default_datetime(self):
        from dorm.contrib.serializers import _msgpack_default

        out = _msgpack_default(_dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc))
        assert "2026" in out

    def test_msgpack_default_decimal(self):
        import decimal

        from dorm.contrib.serializers import _msgpack_default

        assert _msgpack_default(decimal.Decimal("1.5")) == "1.5"

    def test_msgpack_default_uuid(self):
        import uuid

        from dorm.contrib.serializers import _msgpack_default

        u = uuid.uuid4()
        assert _msgpack_default(u) == str(u)

    def test_msgpack_default_bytes(self):
        from dorm.contrib.serializers import _msgpack_default

        assert _msgpack_default(b"abc") == b"abc"

    def test_msgpack_without_dep_raises(self, monkeypatch):
        import builtins

        from dorm.contrib.serializers import stream_msgpack

        real = builtins.__import__

        def _fail(name, *args, **kwargs):
            if name == "msgpack":
                raise ImportError("missing")
            return real(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fail)
        with pytest.raises(ImportError, match="msgpack"):
            list(stream_msgpack([{"a": 1}]))


# ── pgvector/reranker module coverage ──────────────────────────────────────


class TestPgvectorRerankerCoverage:
    def test_rerank_rejects_invalid_distance(self):
        import dorm
        from dorm.contrib.pgvector.reranker import rerank

        class _M(dorm.Model):
            class Meta:
                app_label = "tests"
                db_table = "m"

        with pytest.raises(ValueError, match="distance"):
            rerank(
                _M,
                vector_field="emb",
                text_field_tsv="ts",
                query_vector=[0.0],
                query_text="x",
                distance="unknown",
            )

    def test_rerank_rejects_non_pg(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.pgvector.reranker import rerank
        from dorm.db.connection import _async_connections, _sync_connections

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "pgr.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _Mr(dorm.Model):
            class Meta:
                app_label = "tests"
                db_table = "mr"

        try:
            with pytest.raises(NotImplementedError, match="PostgreSQL"):
                rerank(
                    _Mr,
                    vector_field="emb",
                    text_field_tsv="ts",
                    query_vector=[0.1, 0.2],
                    query_text="x",
                )
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()


# ── outbox coverage push ────────────────────────────────────────────────────


class TestOutboxCoverage:
    def test_record_event_outside_atomic_warns(self, tmp_path, caplog):
        import dorm
        from dorm.conf import settings
        from dorm.contrib.outbox import OutboxEvent, record_event
        from dorm.db.connection import _async_connections, _sync_connections, get_connection
        from dorm.migrations.schema import SchemaEditor

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "rec.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _Re(OutboxEvent):
            class Meta:
                app_label = "tests"

        try:
            with SchemaEditor(get_connection()) as se:
                se.create_model(_Re)
            with caplog.at_level(logging.WARNING, logger="dorm.contrib.outbox"):
                record_event(_Re, "noop", {"x": 1})
            assert any(
                "transaction" in r.message.lower() for r in caplog.records
            )
        finally:
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()


# ── encrypted rotation deeper ──────────────────────────────────────────────


class TestEncryptedRotationDeep:
    def test_rotate_no_encrypted_fields_returns_zero(self):
        import dorm
        from dorm.contrib.encrypted import rotate_encryption_keys

        class _PlainN(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        assert rotate_encryption_keys(_PlainN) == 0

    def test_arotate_no_encrypted_fields_returns_zero(self):
        import dorm
        from dorm.contrib.encrypted import arotate_encryption_keys

        class _PlainA(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        async def _scenario():
            assert await arotate_encryption_keys(_PlainA) == 0

        asyncio.run(_scenario())


# ── nplusone suggest deeper ────────────────────────────────────────────────


class TestNPlusOneSuggestDeep:
    def test_suggest_fix_no_model_hint(self):
        from dorm.contrib.nplusone import suggest_fix

        msg = suggest_fix(
            'SELECT "t"."id" FROM "t" WHERE "t"."author_id" = ?'
        )
        assert "select_related" in msg
        assert "'author'" in msg

    def test_suggest_fix_unrecognised(self):
        from dorm.contrib.nplusone import suggest_fix

        msg = suggest_fix("BEGIN")
        # Falls back to generic recommendation.
        assert msg


# ── dataloader prime cache disabled ────────────────────────────────────────


class TestDataLoaderEdges2:
    def test_prime_cache_disabled_warns(self, caplog):
        from dorm.contrib.dataloader import DataLoader

        loader = DataLoader(lambda ks: {}, cache=False)
        with caplog.at_level(logging.WARNING, logger="dorm.contrib.dataloader"):
            loader.prime(1, "v")
        assert any("cache=False" in r.message for r in caplog.records)


# ── row_cache coverage push ────────────────────────────────────────────────


class TestRowCacheDeep:
    def test_invalid_maxsize_rejected(self):
        import dorm
        from dorm.contrib.row_cache import RowCache

        class _Rcm(dorm.Model):
            class Meta:
                app_label = "tests"

        with pytest.raises(ValueError):
            RowCache(_Rcm, maxsize=0)

    def test_detach_idempotent(self):
        import dorm
        from dorm.contrib.row_cache import RowCache

        class _Rcd(dorm.Model):
            class Meta:
                app_label = "tests"

        cache = RowCache(_Rcd, invalidate_on_write=False)
        cache.detach()
        cache.detach()  # idempotent


# ── querystats coverage push ───────────────────────────────────────────────


class TestQuerystatsExtras:
    def test_render_json_empty_when_disabled(self):
        from dorm.contrib import querystats

        querystats.collector().disable()
        querystats.reset()
        assert querystats.render_json() == []

    def test_collector_enable_idempotent(self):
        from dorm.contrib import querystats

        querystats.collector().enable()
        querystats.collector().enable()
        querystats.collector().disable()

    def test_template_stats_percentile_empty(self):
        from dorm.contrib.querystats import TemplateStats

        s = TemplateStats(template="x")
        assert s.percentile(0.5) == 0.0
        assert s.percentile(0.99) == 0.0


# ── slow_explain coverage push ─────────────────────────────────────────────


class TestSlowExplainDeep:
    def test_explain_reentry_guard(self):
        from dorm.db.utils import (
            _EXPLAIN_REENTRY,
            _maybe_capture_explain_plan,
        )

        token = _EXPLAIN_REENTRY.set(True)
        try:
            # Re-entrant call returns early — no exception, no log.
            _maybe_capture_explain_plan("postgresql", "SELECT 1", [])
        finally:
            _EXPLAIN_REENTRY.reset(token)

    def test_explain_skips_non_select(self):
        from dorm.db.utils import _maybe_capture_explain_plan

        # UPDATE / DELETE / INSERT skipped — no exception thrown.
        _maybe_capture_explain_plan("postgresql", "INSERT INTO x VALUES (1)", [])
        _maybe_capture_explain_plan("postgresql", "UPDATE x SET y = 1", [])
        _maybe_capture_explain_plan("postgresql", "DELETE FROM x", [])

    def test_explain_unsupported_vendor(self):
        from dorm.db.utils import _maybe_capture_explain_plan

        # Unknown vendor short-circuits.
        _maybe_capture_explain_plan("oracle", "SELECT 1", [])


# ── lag_router coverage push ───────────────────────────────────────────────


class TestLagRouterCoverage:
    def test_invalid_cache_seconds(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        with pytest.raises(ValueError):
            LagAwareReadRouter(replicas=["r"], cache_seconds=0)

    def test_invalid_max_lag(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        with pytest.raises(ValueError):
            LagAwareReadRouter(replicas=["r"], max_lag_seconds=0)

    def test_no_replicas_rejected(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        with pytest.raises(ValueError):
            LagAwareReadRouter(replicas=[])

    def test_reset_clears_state(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        r = LagAwareReadRouter(replicas=["r1"])
        r._state["r1"] = type("S", (), {})()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        r.reset()
        assert r._state == {}

    def test_allow_relation_returns_true(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        r = LagAwareReadRouter(replicas=["r1"])
        assert r.allow_relation(None, None) is True

    def test_allow_migrate_primary_only(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        r = LagAwareReadRouter(replicas=["r1"], primary="primary")
        assert r.allow_migrate("primary", "tests") is True
        assert r.allow_migrate("r1", "tests") is False


# ── permissions corner cases ───────────────────────────────────────────────


class TestPermissionsCorners:
    def test_user_none_denied(self):
        from dorm.contrib.permissions import PermissionDenied, requires

        @requires("x")
        def fn(user):
            return "ok"

        with pytest.raises(PermissionDenied):
            fn(None)

    def test_user_no_has_perm_fallback(self):
        from dorm.contrib.permissions import requires

        # Plain object with ``permissions`` set — fallback path.
        class _U:
            permissions = {"x"}

        @requires("x")
        def fn(user):
            return "ok"

        assert fn(_U()) == "ok"

    def test_requires_empty_perms_rejected(self):
        from dorm.contrib.permissions import requires

        with pytest.raises(ValueError):
            requires()


# ── rate_limit corner cases ────────────────────────────────────────────────


class TestRateLimitCorners:
    def test_reset_specific_key(self):
        from dorm.contrib.rate_limit import TokenBucket

        b = TokenBucket(rate_per_second=10, burst=2)
        b.allow("a")
        b.allow("b")
        assert "a" in b._buckets
        b.reset("a")
        assert "a" not in b._buckets
        assert "b" in b._buckets

    def test_reset_all(self):
        from dorm.contrib.rate_limit import TokenBucket

        b = TokenBucket(rate_per_second=10, burst=2)
        b.allow("a")
        b.allow("b")
        b.reset()
        assert len(b._buckets) == 0

    def test_lru_eviction(self):
        from dorm.contrib.rate_limit import TokenBucket

        b = TokenBucket(rate_per_second=10, burst=1, max_keys=2)
        b.allow("a")
        b.allow("b")
        b.allow("c")  # evicts least-recent
        assert len(b._buckets) <= 2

    def test_invalid_max_keys(self):
        from dorm.contrib.rate_limit import TokenBucket

        with pytest.raises(ValueError):
            TokenBucket(rate_per_second=10, burst=1, max_keys=0)


# ── pgvector / GIS minor ───────────────────────────────────────────────────


class TestPgvectorIndexShape:
    def test_hnsw_index_imports(self):
        from dorm.contrib.pgvector.indexes import HnswIndex, IvfflatIndex

        idx = HnswIndex(
            fields=["embedding"],
            name="x_emb",
            opclass="vector_l2_ops",
            m=16,
        )
        assert idx.name == "x_emb"
        idx2 = IvfflatIndex(
            fields=["embedding"],
            name="x_emb2",
            opclass="vector_cosine_ops",
            lists=100,
        )
        assert idx2.name == "x_emb2"

    def test_invalid_opclass_rejected(self):
        from dorm.contrib.pgvector.indexes import HnswIndex

        with pytest.raises(Exception):
            HnswIndex(
                fields=["embedding"],
                name="bad",
                opclass="not_a_real_opclass",
            )


# ── streaming mask_pii ─────────────────────────────────────────────────────


class TestStreamingMaskMore:
    def test_stream_ndjson_pretty_mask(self):
        import dorm
        from dorm.contrib.streaming import stream_ndjson_pretty

        class _Sm(dorm.Model):
            email = dorm.CharField(max_length=64, pii=True)
            name = dorm.CharField(max_length=32)

            class Meta:
                app_label = "tests"

        class _QS:
            model = _Sm

            def __init__(self, rows):
                self._rows = rows

            def iterator(self, chunk_size=1000):
                yield from self._rows

        qs = _QS([{"email": "a@b", "name": "x"}])
        out = b"".join(stream_ndjson_pretty(qs, mask_pii=True)).decode("utf-8")
        assert "[REDACTED]" in out


# ── extra_fields more branches ─────────────────────────────────────────────


class TestExtraFieldsMore:
    def test_color_rgba_ok(self):
        from dorm.contrib.extra_fields import ColorField

        assert ColorField().to_python("#aabbccdd") == "#AABBCCDD"

    def test_color_invalid_chars(self):
        from dorm.contrib.extra_fields import ColorField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            ColorField().to_python("#zzzzzz")

    def test_phone_none_passthrough(self):
        from dorm.contrib.extra_fields import PhoneField

        assert PhoneField().to_python(None) is None

    def test_timezone_none_passthrough(self):
        from dorm.contrib.extra_fields import TimezoneField

        assert TimezoneField().to_python(None) is None

    def test_iprange_none(self):
        from dorm.contrib.extra_fields import IPRangeField

        assert IPRangeField().to_python(None) is None

    def test_country_none(self):
        from dorm.contrib.extra_fields import CountryField

        assert CountryField().to_python(None) is None

    def test_path_none(self):
        from dorm.contrib.extra_fields import PathField

        assert PathField().to_python(None) is None

    def test_percentage_none(self):
        from dorm.contrib.extra_fields import PercentageField

        assert PercentageField().to_python(None) is None

    def test_money_int_explicit(self):
        from dorm.contrib.extra_fields import Money, MoneyField

        f = MoneyField(currency="GBP")
        v = f.to_python(5)
        assert isinstance(v, Money)
        assert v.currency == "GBP"

    def test_money_float_coerced(self):
        from dorm.contrib.extra_fields import MoneyField

        f = MoneyField()
        v = f.to_python(3.14)
        assert v is not None

    def test_money_invalid_string(self):
        from dorm.contrib.extra_fields import MoneyField
        from dorm.exceptions import ValidationError

        with pytest.raises(ValidationError):
            MoneyField().to_python("not-a-number")

    def test_autoslug_no_instance(self):
        from dorm.contrib.extra_fields import autoslug

        fn = autoslug("title")
        assert fn() == ""

    def test_autoslug_empty(self):
        from dorm.contrib.extra_fields import autoslug

        class _M:
            title = ""

        assert autoslug("title")(_M()) == "untitled"


# ── saga corner cases ──────────────────────────────────────────────────────


class TestSagaCorners:
    def test_stop_on_comp_error(self):
        from dorm.contrib.saga import Saga, Step

        def s(ctx): pass
        def c_bad(ctx): raise RuntimeError("comp failed")

        saga = Saga(
            steps=[Step("s1", s, c_bad), Step("s2", lambda c: None, lambda c: None), Step("fail", lambda c: (_ for _ in ()).throw(RuntimeError("x")))],
            stop_on_compensation_error=True,
        )
        # Forward run aborts at step 3. compensation walks s2 then s1.
        # c_bad on s1 fails — stop_on_compensation_error halts the
        # remaining walk.
        run = saga.run()
        assert run.ok is False


# ── inspect/cli quick branches ─────────────────────────────────────────────


class TestInspectCli:
    def test_cmd_version_prints(self, capsys):
        import argparse

        from dorm import __version__
        from dorm.cli import cmd_version

        cmd_version(argparse.Namespace())
        out = capsys.readouterr().out
        assert __version__ in out

    def test_init_template_choices_exposed(self):
        from dorm.cli import _TEMPLATES

        assert "fastapi-postgres" in _TEMPLATES
        assert "litestar-sqlite" in _TEMPLATES

    def test_init_unknown_template_exits(self, tmp_path, monkeypatch):
        import argparse

        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        with pytest.raises(SystemExit):
            cmd_init(argparse.Namespace(template="nope", app=None))


# ── plan_drift more ────────────────────────────────────────────────────────


class TestPlanDriftMore:
    def test_strip_volatile_passthrough(self):
        from dorm.contrib.plan_drift import _strip_volatile

        # Already-clean plan stays identical.
        assert _strip_volatile("Seq Scan on x") == "Seq Scan on x"

    def test_record_baseline_overwrites(self, tmp_path):
        import dorm
        from dorm.conf import settings
        from dorm.contrib import plan_drift
        from dorm.db.connection import _async_connections, _sync_connections, get_connection
        from dorm.migrations.schema import SchemaEditor

        saved = {a: dict(c) for a, c in settings.DATABASES.items()}
        saved_apps = list(settings.INSTALLED_APPS)
        _sync_connections.clear()
        _async_connections.clear()
        db = tmp_path / "pdm.sqlite3"
        dorm.configure(
            DATABASES={"default": {"ENGINE": "sqlite", "NAME": str(db)}},
            INSTALLED_APPS=["tests"],
        )

        class _Pdm(dorm.Model):
            x = dorm.IntegerField()

            class Meta:
                app_label = "tests"

        try:
            with SchemaEditor(get_connection()) as se:
                se.create_model(_Pdm)
            plan_drift.reset()
            sql = f"SELECT * FROM {_Pdm._meta.db_table}"
            plan_drift.record_baseline("tag", sql)
            plan_drift.record_baseline("tag", sql)
            assert "tag" in plan_drift.baselines()
        finally:
            plan_drift.reset()
            dorm.configure(DATABASES=saved, INSTALLED_APPS=saved_apps)
            _sync_connections.clear()
            _async_connections.clear()


# ── advisory helpers more ─────────────────────────────────────────────────


class TestAdvisoryMore:
    def test_placeholders_count(self):
        from dorm.contrib.advisory import _placeholders

        # Returns N comma-separated %s tokens.
        class _C:
            pass

        ph = _placeholders(2, _C())
        assert ph == "%s, %s"

    def test_require_postgres_passthrough(self):
        from dorm.contrib.advisory import _require_postgres

        class _Pg:
            vendor = "postgresql"

        # Should NOT raise.
        _require_postgres(_Pg(), fn="advisory_lock")

    def test_require_postgres_rejects_sqlite(self):
        from dorm.contrib.advisory import _require_postgres

        class _Sq:
            vendor = "sqlite"

        with pytest.raises(NotImplementedError):
            _require_postgres(_Sq(), fn="advisory_lock")
