"""Fifth coverage uplift — target low-coverage contrib modules to
push coverage and surface latent bugs.

Modules touched:

- ``dorm.contrib.gis.lookups`` (62%)
- ``dorm.contrib.active_passive`` (66%)
- ``dorm.cache.invalidation`` (68%)
- ``dorm.contrib.concurrency`` (69%)
- ``dorm.contrib.asgi`` (71%)
- ``dorm.contrib.listen_notify`` (72%)
- ``dorm.contrib.encrypted`` (74%)
- ``dorm.contrib.outbox`` (62%)
- ``dorm.contrib.two_phase`` (31%)

Pure-Python paths only — no live PG required.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any

import pytest


# ── gis.lookups ─────────────────────────────────────────────────────────────


class TestGisLookups:
    def _geom(self):
        from dorm.contrib.gis.fields import Geom

        return Geom.point(0, 0)

    def test_intersects_with_geom(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        sql, params = lookup_intersects("zone", self._geom())
        assert sql == "ST_Intersects(zone, ST_GeomFromText(%s, %s))"
        assert params == ["POINT(0 0)", 4326]

    def test_within_with_geom(self):
        from dorm.contrib.gis.lookups import lookup_within

        sql, _ = lookup_within("zone", self._geom())
        assert sql.startswith("ST_Within(")

    def test_contains_with_geom(self):
        from dorm.contrib.gis.lookups import lookup_contains

        sql, _ = lookup_contains("zone", self._geom())
        assert sql.startswith("ST_Contains(")

    def test_distance_lte_appends_distance_param(self):
        from dorm.contrib.gis.lookups import lookup_distance_lte

        sql, params = lookup_distance_lte("zone", self._geom(), 100)
        assert sql.endswith("<= %s")
        assert params[-1] == 100.0

    def test_distance_gte_appends_distance_param(self):
        from dorm.contrib.gis.lookups import lookup_distance_gte

        sql, params = lookup_distance_gte("zone", self._geom(), 50)
        assert sql.endswith(">= %s")
        assert params[-1] == 50.0

    def test_wrap_accepts_geojson_dict(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        geojson = {"type": "Point", "coordinates": [1.0, 2.0]}
        sql, params = lookup_intersects("zone", geojson)
        assert sql.startswith("ST_Intersects(")
        assert params[-1] == 4326  # default SRID

    def test_wrap_accepts_plain_wkt_string(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        sql, params = lookup_intersects("zone", "POINT(1 1)")
        assert params == ["POINT(1 1)", 4326]
        assert sql.startswith("ST_Intersects(")

    def test_wrap_accepts_srid_prefixed_wkt(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        sql, params = lookup_intersects("zone", "SRID=3857;POINT(1 1)")
        assert params == ["POINT(1 1)", 3857]
        assert sql.startswith("ST_Intersects(")

    def test_wrap_srid_with_bad_int_falls_back(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        sql, params = lookup_intersects("zone", "SRID=oops;POINT(1 1)")
        # malformed SRID number — _wrap silently defaults to 4326.
        assert params == ["POINT(1 1)", 4326]
        assert sql.startswith("ST_Intersects(")

    def test_wrap_rejects_unsupported_type(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        with pytest.raises(TypeError, match="must be Geom"):
            lookup_intersects("zone", 12345)  # type: ignore[arg-type]

    def test_safe_column_rejects_injection(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        with pytest.raises(ValueError, match="invalid column"):
            lookup_intersects("zone; DROP TABLE x", self._geom())

    def test_safe_column_accepts_qualified(self):
        from dorm.contrib.gis.lookups import lookup_intersects

        sql, _ = lookup_intersects('"public"."zone"', self._geom())
        assert "public" in sql

    def test_register_gis_lookups_idempotent(self):
        from dorm.contrib.gis.lookups import register_gis_lookups

        # Idempotent — second call is a no-op.
        register_gis_lookups()
        register_gis_lookups()
        from dorm import lookups

        for name in ("intersects", "within", "contains", "distance_lte", "distance_gte"):
            assert hasattr(lookups, f"_gis_{name}")


# ── active_passive ─────────────────────────────────────────────────────────


class _FakeRow(dict):
    def values(self):
        return list(super().values())


class _FakeConn:
    def __init__(self, vendor: str = "postgresql", *, in_recovery: bool = False, raise_exc: bool = False) -> None:
        self.vendor = vendor
        self._in_recovery = in_recovery
        self._raise = raise_exc

    def execute(self, sql: str, *_a, **_kw):
        if self._raise:
            raise RuntimeError("probe-fail")
        return [_FakeRow(rec=self._in_recovery)]


class TestActivePassiveRouter:
    def test_rejects_single_alias(self):
        from dorm.contrib.active_passive import ActivePassiveRouter

        with pytest.raises(ValueError, match="at least 2"):
            ActivePassiveRouter(aliases=["a"])

    def test_rejects_empty_aliases(self):
        from dorm.contrib.active_passive import ActivePassiveRouter

        with pytest.raises(ValueError, match="at least 2"):
            ActivePassiveRouter(aliases=[])

    def test_rejects_zero_probe_seconds(self):
        from dorm.contrib.active_passive import ActivePassiveRouter

        with pytest.raises(ValueError, match="probe_seconds"):
            ActivePassiveRouter(aliases=["a", "b"], probe_seconds=0)

    def test_db_for_write_returns_none_when_disabled(self, monkeypatch):
        from dorm.contrib.active_passive import ActivePassiveRouter

        r = ActivePassiveRouter(
            aliases=["a", "b"], probe_seconds=10, prefer_primary_for_writes=False
        )
        assert r.db_for_write(None) is None

    def test_allow_relation_always_true(self):
        from dorm.contrib.active_passive import ActivePassiveRouter

        r = ActivePassiveRouter(aliases=["a", "b"])
        assert r.allow_relation(None, None) is True

    def test_non_pg_fallback(self, monkeypatch):
        """Non-PG aliases fall through the recovery probe and are
        treated as a primary."""
        from dorm.contrib.active_passive import ActivePassiveRouter
        from dorm.contrib import active_passive

        fakes = {"a": _FakeConn(vendor="sqlite"), "b": _FakeConn(vendor="sqlite")}
        monkeypatch.setattr(
            active_passive,
            "get_connection",
            lambda alias: fakes[alias],
            raising=False,
        )
        # Pull get_connection from inside _refresh via the import.
        from dorm.db import connection as _conn_mod

        monkeypatch.setattr(_conn_mod, "get_connection", lambda alias: fakes[alias])
        r = ActivePassiveRouter(aliases=["a", "b"], probe_seconds=0.001)
        # First call triggers probe.
        write = r.db_for_write(None)
        assert write == "a"
        # allow_migrate returns True only for the primary.
        assert r.allow_migrate("a", "appx") is True
        assert r.allow_migrate("b", "appx") is False

    def test_probe_detects_primary_and_replicas(self, monkeypatch):
        from dorm.contrib.active_passive import ActivePassiveRouter
        from dorm.db import connection as _conn_mod

        fakes = {
            "primary": _FakeConn(in_recovery=False),
            "replica": _FakeConn(in_recovery=True),
        }
        monkeypatch.setattr(_conn_mod, "get_connection", lambda alias: fakes[alias])
        r = ActivePassiveRouter(aliases=["primary", "replica"], probe_seconds=10)
        assert r.db_for_write(None) == "primary"
        # Reads route to replicas when present.
        assert r.db_for_read(None) == "replica"

    def test_split_brain_logs_warning(self, monkeypatch, caplog):
        from dorm.contrib.active_passive import ActivePassiveRouter
        from dorm.db import connection as _conn_mod

        # Two primaries — both report in_recovery=False.
        fakes = {
            "a": _FakeConn(in_recovery=False),
            "b": _FakeConn(in_recovery=False),
        }
        monkeypatch.setattr(_conn_mod, "get_connection", lambda alias: fakes[alias])
        r = ActivePassiveRouter(aliases=["a", "b"], probe_seconds=10)
        with caplog.at_level("WARNING", logger="dorm.contrib.active_passive"):
            assert r.db_for_write(None) == "a"
        assert any("more than one node" in rec.message for rec in caplog.records)

    def test_probe_failure_logged(self, monkeypatch, caplog):
        from dorm.contrib.active_passive import ActivePassiveRouter
        from dorm.db import connection as _conn_mod

        fakes = {
            "a": _FakeConn(raise_exc=True),
            "b": _FakeConn(in_recovery=False),
        }
        monkeypatch.setattr(_conn_mod, "get_connection", lambda alias: fakes[alias])
        r = ActivePassiveRouter(aliases=["a", "b"], probe_seconds=10)
        with caplog.at_level("WARNING", logger="dorm.contrib.active_passive"):
            r.db_for_write(None)
        assert any("probe of alias" in rec.message for rec in caplog.records)

    def test_cache_hit_skips_probe(self, monkeypatch):
        from dorm.contrib.active_passive import ActivePassiveRouter
        from dorm.db import connection as _conn_mod

        calls = {"n": 0}
        fakes = {
            "a": _FakeConn(in_recovery=False),
            "b": _FakeConn(in_recovery=True),
        }

        def _gc(alias):
            calls["n"] += 1
            return fakes[alias]

        monkeypatch.setattr(_conn_mod, "get_connection", _gc)
        r = ActivePassiveRouter(aliases=["a", "b"], probe_seconds=60)
        r.db_for_write(None)
        first = calls["n"]
        # Subsequent calls within the TTL window must not re-probe.
        r.db_for_write(None)
        r.db_for_read(None)
        assert calls["n"] == first


# ── concurrency primitives ─────────────────────────────────────────────────


class TestConcurrencyPrimitives:
    def test_serializable_validates_max_attempts(self):
        from dorm.contrib.concurrency import SerializableSnapshot

        with pytest.raises(ValueError, match="max_attempts"):
            SerializableSnapshot(max_attempts=0)

    def test_named_lock_inproc_fallback(self):
        """``named_lock`` on a SQLite alias uses the in-process
        threading.Lock fallback — re-entering it from the same thread
        is allowed via separate ``with`` blocks (we release on exit)."""
        from dorm.contrib.concurrency import named_lock

        with named_lock("test-lock"):
            pass
        # Second acquisition must succeed (lock released on exit).
        with named_lock("test-lock"):
            pass

    def test_with_optimistic_lock_rejects_unsaved(self):
        from dorm.contrib.concurrency import (
            OptimisticLockError,
            with_optimistic_lock,
        )

        class _Inst:
            class _Meta:
                class _Pk:
                    attname = "id"

                pk = _Pk()
                fields: list[Any] = []

            _meta = _Meta()
            __dict__: dict[str, Any] = {}  # type: ignore[assignment]

        inst = _Inst()
        # No pk → OptimisticLockError before any DB write.
        with pytest.raises(OptimisticLockError, match="no PK"):
            with_optimistic_lock(inst)


# ── two_phase pure-Python guards ───────────────────────────────────────────


class TestTwoPhasePureGuards:
    def test_empty_aliases_raises(self):
        from dorm.contrib.two_phase import two_phase_commit

        with pytest.raises(ValueError, match="at least one alias"):
            with two_phase_commit([]):
                pass

    def test_non_pg_alias_rejected(self, monkeypatch):
        from dorm.contrib import two_phase
        from dorm.db import connection as _conn_mod

        fake = _FakeConn(vendor="sqlite")
        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: fake)
        with pytest.raises(NotImplementedError, match="not PostgreSQL"):
            with two_phase.two_phase_commit(["a"]):
                pass

    def test_txnctx_rejects_unknown_alias(self):
        from dorm.contrib.two_phase import _TxnContext

        ctx = _TxnContext(["a", "b"])
        with pytest.raises(KeyError, match="not in participants"):
            ctx.execute("c", "SELECT 1")

    def test_txnctx_routes_select_to_execute(self, monkeypatch):
        from dorm.db import connection as _conn_mod
        from dorm.contrib.two_phase import _TxnContext

        seen = {"reads": 0, "writes": 0}

        class _F:
            vendor = "postgresql"

            def execute(self, sql, params=None):
                seen["reads"] += 1
                return [{"n": 1}]

            def execute_write(self, sql, params=None):
                seen["writes"] += 1
                return None

        monkeypatch.setattr(_conn_mod, "get_connection", lambda _a: _F())
        ctx = _TxnContext(["a"])
        ctx.execute("a", "SELECT 1")
        ctx.execute("a", "  WITH cte AS (SELECT 1) SELECT * FROM cte")
        ctx.execute("a", "INSERT INTO t VALUES (1)")
        assert seen == {"reads": 2, "writes": 1}


# ── outbox serializer + relay branches ─────────────────────────────────────


class TestOutboxBranches:
    def test_serialize_payload_is_stable(self):
        from dorm.contrib.outbox import serialize_payload

        a = serialize_payload({"b": 1, "a": 2, "c": [3, 2, 1]})
        b = serialize_payload({"c": [3, 2, 1], "a": 2, "b": 1})
        assert a == b
        assert a == '{"a":2,"b":1,"c":[3,2,1]}'

    def test_serialize_payload_default_str(self):
        from dorm.contrib.outbox import serialize_payload

        out = serialize_payload({"u": uuid.UUID("00000000-0000-0000-0000-000000000001")})
        assert "00000000-0000-0000-0000-000000000001" in out

    def test_relay_validates_batch_size(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        with pytest.raises(ValueError, match="batch_size"):
            OutboxRelay(OutboxEvent, batch_size=0)

    def test_relay_validates_poll_interval(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        with pytest.raises(ValueError, match="poll_interval_s"):
            OutboxRelay(OutboxEvent, poll_interval_s=0)

    def test_relay_stop_sets_flag(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        relay = OutboxRelay(OutboxEvent)
        assert relay._stop is False
        relay.stop()
        assert relay._stop is True

    def test_process_one_handler_raises_dead_letters(self):
        """After ``max_attempts`` handler exceptions, row.status becomes
        ``"dead"`` so it stops blocking the queue."""
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        class _Row:
            attempts = 4
            status = "pending"
            last_error: Any = None
            id = uuid.uuid4()

            def save(self, **_kw):
                pass

        relay = OutboxRelay(OutboxEvent, max_attempts=5)
        row = _Row()
        ok = relay._process_one(
            row,  # ty: ignore[invalid-argument-type]
            lambda r: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert ok is False
        assert row.status == "dead"
        assert row.attempts == 5
        assert row.last_error and "RuntimeError" in row.last_error

    def test_process_one_handler_returns_falsy(self):
        """Handler returning False bumps ``attempts`` without dead-
        lettering until the limit is hit."""
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        class _Row:
            attempts = 0
            status = "pending"
            last_error = None
            id = uuid.uuid4()

            def save(self, **_kw):
                pass

        relay = OutboxRelay(OutboxEvent, max_attempts=3)
        row = _Row()
        relay._process_one(row, lambda _r: False)  # ty: ignore[invalid-argument-type]
        assert row.status == "pending"
        assert row.attempts == 1

    def test_process_one_success_marks_published(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        class _Row:
            attempts = 0
            status = "pending"
            last_error = "prev"
            published_at = None
            id = uuid.uuid4()

            def save(self, **_kw):
                pass

        relay = OutboxRelay(OutboxEvent)
        row = _Row()
        ok = relay._process_one(row, lambda _r: True)  # ty: ignore[invalid-argument-type]
        assert ok is True
        assert row.status == "published"
        assert row.last_error is None
        assert isinstance(row.published_at, datetime)

    @pytest.mark.asyncio
    async def test_aprocess_one_handler_raises(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        class _Row:
            attempts = 0
            status = "pending"
            last_error = None
            id = uuid.uuid4()

            async def asave(self, **_kw):
                pass

        relay = OutboxRelay(OutboxEvent, max_attempts=2)
        row = _Row()
        ok = await relay._aprocess_one(
            row,
            lambda r: (_ for _ in ()).throw(RuntimeError("boom-async")),
        )
        assert ok is False
        # 1st failure — not yet dead.
        assert row.attempts == 1
        # 2nd failure trips the dead-letter limit.
        ok = await relay._aprocess_one(
            row,
            lambda r: (_ for _ in ()).throw(RuntimeError("boom-async")),
        )
        assert ok is False
        assert row.status == "dead"

    @pytest.mark.asyncio
    async def test_aprocess_one_async_handler_success(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        class _Row:
            attempts = 0
            status = "pending"
            last_error = None
            published_at: Any = None
            id = uuid.uuid4()

            async def asave(self, **_kw):
                pass

        async def _ok(_row):
            return True

        relay = OutboxRelay(OutboxEvent)
        row = _Row()
        ok = await relay._aprocess_one(row, _ok)
        assert ok is True
        assert row.status == "published"

    @pytest.mark.asyncio
    async def test_aprocess_one_async_handler_falsy(self):
        from dorm.contrib.outbox import OutboxEvent, OutboxRelay

        class _Row:
            attempts = 0
            status = "pending"
            last_error = None
            id = uuid.uuid4()

            async def asave(self, **_kw):
                pass

        async def _no(_row):
            return False

        relay = OutboxRelay(OutboxEvent, max_attempts=1)
        row = _Row()
        ok = await relay._aprocess_one(row, _no)
        assert ok is False
        assert row.status == "dead"  # max_attempts=1 → falsy trips dead


# ── listen_notify guards ──────────────────────────────────────────────────


class TestListenNotifyGuards:
    def test_quote_ident_doubles_internal_quotes(self):
        from dorm.contrib.listen_notify import _quote_ident

        assert _quote_ident('orders') == '"orders"'
        assert _quote_ident('odd"name') == '"odd""name"'

    def test_ensure_postgres_rejects_sqlite(self):
        from dorm.contrib.listen_notify import _ensure_postgres

        class _C:
            vendor = "sqlite"

        with pytest.raises(NotImplementedError, match="PostgreSQL-only"):
            _ensure_postgres(_C())

    def test_listen_requires_channel(self):
        from dorm.contrib.listen_notify import listen

        # ``listen()`` is an async context manager — entering with no
        # channels must raise ValueError synchronously (the helper
        # validates before any await).
        async def _go():
            async with listen():  # type: ignore[call-overload]
                pass

        with pytest.raises(ValueError, match="at least one channel"):
            asyncio.run(_go())


# ── encrypted rotate validation ───────────────────────────────────────────


class TestEncryptedRotateValidation:
    def test_rotate_rejects_non_encrypted_field(self):
        """Explicit ``fields`` containing a plain (non-encrypted) field
        must raise ``TypeError`` — the helper would otherwise silently
        no-op on plaintext columns."""
        from dorm.contrib.encrypted import rotate_encryption_keys
        from tests.models import Author

        with pytest.raises(TypeError, match="not an EncryptedField"):
            rotate_encryption_keys(Author, fields=["name"])

    def test_rotate_empty_fields_returns_zero(self):
        """A model with no ``EncryptedField`` columns short-circuits
        and returns 0 — no work, no spurious DB queries."""
        from dorm.contrib.encrypted import rotate_encryption_keys
        from tests.models import Author

        assert rotate_encryption_keys(Author) == 0

    @pytest.mark.asyncio
    async def test_arotate_rejects_non_encrypted_field(self):
        from dorm.contrib.encrypted import arotate_encryption_keys
        from tests.models import Author

        with pytest.raises(TypeError, match="not an EncryptedField"):
            await arotate_encryption_keys(Author, fields=["name"])

    @pytest.mark.asyncio
    async def test_arotate_empty_fields_returns_zero(self):
        from dorm.contrib.encrypted import arotate_encryption_keys
        from tests.models import Author

        assert await arotate_encryption_keys(Author) == 0


# ── asgi OTel middleware non-http + missing tracer ────────────────────────


class TestOTelDormMiddleware:
    @pytest.mark.asyncio
    async def test_non_http_passes_through(self):
        from dorm.contrib.asgi import OTelDormMiddleware

        seen = {"called": False}

        async def app(scope, receive, send):
            seen["called"] = True

        mw = OTelDormMiddleware(app)
        await mw({"type": "lifespan"}, None, None)  # ty: ignore[invalid-argument-type]
        assert seen["called"] is True

    @pytest.mark.asyncio
    async def test_no_tracer_passes_through(self, monkeypatch):
        from dorm.contrib.asgi import OTelDormMiddleware

        seen = {"called": False}

        async def app(scope, receive, send):
            seen["called"] = True

        mw = OTelDormMiddleware(app)
        # Force _get_tracer to None.
        monkeypatch.setattr(mw, "_get_tracer", lambda: None)
        await mw({"type": "http", "method": "GET", "path": "/"}, None, lambda m: None)  # ty: ignore[invalid-argument-type]
        assert seen["called"] is True


# ── invalidation public API ───────────────────────────────────────────────


class TestInvalidationModule:
    def test_ensure_signals_connected_idempotent(self):
        from dorm.cache.invalidation import ensure_signals_connected

        # First call connects; subsequent calls must be no-ops (the
        # internal ``_signals_connected`` guard catches the repeat).
        ensure_signals_connected()
        ensure_signals_connected()
        ensure_signals_connected()

    def test_invalidate_model_handles_no_caches(self):
        """``CACHES`` empty / unset → invalidate_model must not raise."""
        from dorm.cache.invalidation import invalidate_model
        from tests.models import Author

        # Should not raise even with no caches configured.
        invalidate_model(Author)

    @pytest.mark.asyncio
    async def test_ainvalidate_model_handles_no_caches(self):
        from dorm.cache.invalidation import ainvalidate_model
        from tests.models import Author

        await ainvalidate_model(Author)

    def test_do_drop_swallows_internal_errors(self):
        """``_do_drop`` must swallow exceptions raised by missing
        cache backends — a Redis outage should never break a save."""
        from dorm.cache.invalidation import _do_drop

        class _Bad:
            pass

        # No registered backend for arbitrary class — _do_drop must
        # exit cleanly without propagating.
        _do_drop(_Bad)
