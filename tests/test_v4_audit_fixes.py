"""Regression tests for the v4.0 pre-release audit fixes.

Each test pins down a bug found in the pre-tag review so a future
refactor can't silently undo the fix. One test per finding from the
audit punch-list.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

import dorm


# ── #1 — GIS SQL injection ────────────────────────────────────────────


def test_gis_lookups_parametrise_user_payload():
    """Crafted Geom coordinates must NOT inline into SQL — values
    travel as bound parameters."""
    from dorm.contrib.gis import Geom
    from dorm.contrib.gis.lookups import SPATIAL_LOOKUPS

    malicious = Geom(kind="Point", coordinates=["1)' OR 1=1 --", "0"], srid=4326)
    sql, params = SPATIAL_LOOKUPS["intersects"]("zone", malicious)
    # The injection vector lands in params, never inline in SQL.
    assert "OR 1=1" not in sql
    assert "OR 1=1" in str(params[0])
    # Placeholder syntax preserved.
    assert "ST_GeomFromText(%s, %s)" in sql


def test_gis_lookups_reject_column_injection():
    """The column slot must reject anything that isn't a plain
    field path."""
    from dorm.contrib.gis import Geom
    from dorm.contrib.gis.lookups import SPATIAL_LOOKUPS

    g = Geom.point(1, 2)
    with pytest.raises(ValueError, match="invalid column"):
        SPATIAL_LOOKUPS["intersects"]('zone"; DROP TABLE x;--', g)


def test_gis_distance_parametrises_threshold():
    from dorm.contrib.gis import Geom
    from dorm.contrib.gis.lookups import SPATIAL_LOOKUPS

    sql, params = SPATIAL_LOOKUPS["distance_lte"]("zone", Geom.point(0, 0), 1000)
    assert sql.endswith("<= %s")
    assert params[-1] == 1000.0


# ── #2 — AsyncOnlyManager whitelist order ─────────────────────────────


def test_asyncmodel_blocks_all_method():
    """Sync ``all()`` starts with 'a' but must still be rejected by
    ``AsyncOnlyManager``. Pre-fix the prefix check ran first and let
    it through."""
    from dorm.contrib.asyncmodel import AsyncModel, AsyncOnlyError

    class _A(AsyncModel):
        name = dorm.CharField(max_length=10)

        class Meta:
            db_table = "_audit_async_a"
            app_label = "tests"

    with pytest.raises(AsyncOnlyError, match="all"):
        _A.objects.all()


def test_asyncmodel_blocks_aggregate_method():
    from dorm.contrib.asyncmodel import AsyncModel, AsyncOnlyError

    class _B(AsyncModel):
        name = dorm.CharField(max_length=10)

        class Meta:
            db_table = "_audit_async_b"
            app_label = "tests"

    with pytest.raises(AsyncOnlyError, match="aggregate"):
        _B.objects.aggregate()


def test_asyncmodel_async_methods_pass_through():
    """Real async methods (``acreate``, ``aget``, ``aget_or_none`` …)
    keep working — the whitelist still allows them after the order
    swap."""
    from dorm.contrib.asyncmodel import AsyncModel

    class _C(AsyncModel):
        name = dorm.CharField(max_length=10)

        class Meta:
            db_table = "_audit_async_c"
            app_label = "tests"

    # ``acreate`` is callable; we don't actually invoke it here
    # (no DB table) — only verify attribute access doesn't raise.
    assert callable(_C.objects.acreate)
    assert callable(_C.objects.aget)
    assert callable(_C.objects.aget_or_none)


# ── #3 — AsyncOnly + Tenant composite manager ────────────────────────


def test_make_async_tenant_manager_blocks_sync_calls():
    from dorm.contrib.asyncmodel import AsyncModel, AsyncOnlyError
    from dorm.contrib.tenants_row import (
        TenantModel,
        current_tenant,
        make_async_tenant_manager,
    )

    AsyncTenantManager = make_async_tenant_manager()

    class _Order(TenantModel, AsyncModel):
        title = dorm.CharField(max_length=100)
        objects = AsyncTenantManager()

        class Meta:
            db_table = "_audit_async_tenant_orders"
            app_label = "tests"

    # Sync access raises even with a tenant active.
    with current_tenant("acme"):
        with pytest.raises(AsyncOnlyError, match="filter"):
            _Order.objects.filter(title="x")


# ── #4 — arecord_event tx detection ───────────────────────────────────


@pytest.mark.asyncio
async def test_arecord_event_warns_outside_atomic(caplog):
    """The async record helper must log a warning when called
    outside an aatomic() block (matches the sync path)."""
    import logging as _logging

    from dorm.contrib.outbox import OutboxEvent, arecord_event
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    class _Outbox(OutboxEvent):
        class Meta:
            db_table = "_audit_outbox_arecord"
            app_label = "tests"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "_audit_outbox_arecord"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Outbox._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "_audit_outbox_arecord" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    try:
        with caplog.at_level(_logging.WARNING, logger="dorm.contrib.outbox"):
            await arecord_event(_Outbox, "evt", {"a": 1})
        assert any(
            "outside an aatomic" in r.message for r in caplog.records
        )
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "_audit_outbox_arecord"{cascade}')


# ── #5 — OutboxRelay.arun / adrain_once exist ────────────────────────


def test_outbox_relay_has_async_methods():
    from dorm.contrib.outbox import OutboxEvent, OutboxRelay

    class _Tbl(OutboxEvent):
        class Meta:
            db_table = "_audit_relay_async_smoke"
            app_label = "tests"

    relay = OutboxRelay(_Tbl)
    assert callable(getattr(relay, "arun", None))
    assert callable(getattr(relay, "adrain_once", None))
    assert callable(getattr(relay, "_aprocess_one", None))


# ── #6 — aidempotency_key exists and works ────────────────────────────


@pytest.mark.asyncio
async def test_aidempotency_key_caches_response():
    from dorm.contrib.idempotency import IdempotencyRecord, aidempotency_key
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    class _Idp(IdempotencyRecord):
        class Meta:
            db_table = "_audit_aidp"
            app_label = "tests"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "_audit_aidp"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Idp._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "_audit_aidp" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    try:
        async with aidempotency_key("k1", model=_Idp) as ctx:
            assert ctx.replay is False
            await ctx.astore({"x": 1}, status_code=201)

        async with aidempotency_key("k1", model=_Idp) as ctx:
            assert ctx.replay is True
            assert ctx.cached_response == {"x": 1}
            assert ctx.cached_status_code == 201
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "_audit_aidp"{cascade}')


# ── #7 — budget.py dead code removal ─────────────────────────────────


def test_budget_dead_code_gone():
    import dorm.budget as _budget

    assert not hasattr(_budget, "_apply_timeout")
    assert not hasattr(_budget, "_clear_timeout")


# ── #8 — TenantManager reads Meta.tenant_field ───────────────────────


def test_tenant_manager_resolves_field_from_meta():
    """``TenantManager`` reads the column name from ``Meta.tenant_field``
    so subclasses don't have to override the manager too. Field-level
    inheritance is a separate concern — this test pins the manager
    behaviour only."""
    from dorm.contrib.tenants_row import TenantManager, TenantModel

    class _Override(TenantModel):
        title = dorm.CharField(max_length=100)

        class Meta:
            db_table = "_audit_tenant_meta_override"
            app_label = "tests"
            tenant_field = "org_id"

    mgr: TenantManager = _Override.objects  # type: ignore[assignment]
    # Manager picks up the override via ``Meta`` rather than the
    # class-level fallback.
    assert mgr._resolved_field() == "org_id"


def test_tenant_autofill_uses_meta_field():
    """``_autofill_tenant`` should write to the column named in
    ``Meta.tenant_field``."""
    from dorm.contrib.tenants_row import TenantModel, current_tenant

    class _Override(TenantModel):
        title = dorm.CharField(max_length=100)

        class Meta:
            db_table = "_audit_tenant_autofill"
            app_label = "tests"
            tenant_field = "tenant_id"  # default, but explicit

    with current_tenant("acme"):
        obj = _Override(title="t")
        obj._autofill_tenant()
        assert obj.tenant_id == "acme"


# ── #9 — CircuitBreaker snapshot consistency ─────────────────────────


def test_circuit_breaker_open_message_self_consistent():
    """Even under concurrent record_failure() calls, the OPEN error
    message reads a coherent (failures, opened_at) snapshot."""
    from dorm.contrib.circuit_breaker import CircuitBreaker, CircuitOpenError

    cb = CircuitBreaker("audit-9", failure_threshold=1, open_window_s=60.0)
    cb.record_failure()

    # Under contention from other threads, the message must reflect
    # a single locked read.
    def _hammer():
        for _ in range(20):
            cb.record_failure()

    t = threading.Thread(target=_hammer)
    t.start()
    with pytest.raises(CircuitOpenError) as exc:
        with cb:
            pass
    t.join()
    # Message includes a finite cooldown estimate (no /0, no exception).
    assert "cooldown ends in" in str(exc.value)


# ── #10 — aprotect ignores CancelledError ─────────────────────────────


@pytest.mark.asyncio
async def test_aprotect_ignores_cancelled_error():
    from dorm.contrib.circuit_breaker import CircuitBreaker

    cb = CircuitBreaker("audit-10", failure_threshold=2)
    try:
        async with cb.aprotect():
            raise asyncio.CancelledError()
    except asyncio.CancelledError:
        pass
    # Cancellation must NOT count as a breaker failure.
    assert cb.failures == 0


@pytest.mark.asyncio
async def test_aprotect_still_counts_real_exceptions():
    from dorm.contrib.circuit_breaker import CircuitBreaker

    cb = CircuitBreaker("audit-10b", failure_threshold=5)
    with pytest.raises(RuntimeError):
        async with cb.aprotect():
            raise RuntimeError("boom")
    assert cb.failures == 1


# ── #11 — Lag router cache lock ──────────────────────────────────────


def test_lag_router_concurrent_probe_does_not_double_count(monkeypatch):
    """Two threads probing simultaneously should not both
    issue separate ``_measure_lag`` calls if the cache fills in
    between them. We can't easily detect duplicates; instead we
    confirm the read after a probe is served from cache (no
    second ``_measure_lag`` call)."""
    from dorm.contrib.lag_router import LagAwareReadRouter

    router = LagAwareReadRouter(
        primary="primary",
        replicas=["r1"],
        max_lag_seconds=2.0,
        cache_seconds=60.0,
    )
    calls = [0]

    def _probe(alias):
        calls[0] += 1
        return 0.5

    monkeypatch.setattr(router, "_measure_lag", _probe)

    class _Stub:
        pass

    router.db_for_read(_Stub())
    router.db_for_read(_Stub())
    router.db_for_read(_Stub())
    # 1 probe total — cache hits the rest.
    assert calls[0] == 1


# ── #12 + #20 — DuckDB pk_col validation + double RETURNING ──────────


def test_duckdb_execute_insert_rejects_invalid_pk_col():
    duckdb = pytest.importorskip("duckdb")  # noqa: F841
    from dorm.db.backends.duckdb import DuckDBDatabaseWrapper

    w = DuckDBDatabaseWrapper({"NAME": ":memory:"})
    try:
        with pytest.raises(ValueError, match="invalid pk_col"):
            w.execute_insert(
                "INSERT INTO t VALUES (1)", pk_col='id"; DROP TABLE t;--'
            )
    finally:
        w.close()


def test_duckdb_execute_insert_skips_double_returning():
    duckdb = pytest.importorskip("duckdb")  # noqa: F841
    from dorm.db.backends.duckdb import DuckDBDatabaseWrapper

    w = DuckDBDatabaseWrapper({"NAME": ":memory:"})
    try:
        w.execute_script("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        # Caller-supplied SQL already carries RETURNING.
        result = w.execute_insert(
            "INSERT INTO t VALUES (1, 'x') RETURNING id", pk_col="id"
        )
        assert result == 1
    finally:
        w.close()


# ── #13 — streaming iterator signature inspection ────────────────────


def test_streaming_iterator_propagates_real_typeerror():
    """Pre-fix, a TypeError raised inside the iterator body would be
    caught and silently retried with no chunk_size. After the fix,
    the iterator's own errors propagate cleanly."""
    from dorm.contrib.streaming import stream_jsonl

    class _BadSource:
        def iterator(self, chunk_size: int = 100):
            raise TypeError("real bug — must not be swallowed")

    with pytest.raises(TypeError, match="real bug"):
        list(stream_jsonl(_BadSource()))


def test_streaming_iterator_handles_no_kwarg_signature():
    """Iterators without a ``chunk_size`` parameter still work."""
    from dorm.contrib.streaming import stream_jsonl

    class _Source:
        def iterator(self):
            for i in range(2):
                yield {"i": i}

    out = b"".join(stream_jsonl(_Source()))
    assert out.count(b"\n") == 2


# ── #14 — AddFieldOnline copies the field ────────────────────────────


def test_add_field_online_does_not_mutate_shared_field():
    from dorm.migrations.operations import AddFieldOnline

    field = dorm.IntegerField(null=False, default=0)

    class _State:
        def __init__(self):
            self.models = {
                "tests.x": {
                    "fields": {},
                    "options": {"db_table": "audit_share_field"},
                }
            }

    from dorm.db.connection import get_connection

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "audit_share_field"{cascade}')
    if getattr(conn, "vendor", "sqlite") == "sqlite":
        conn.execute_script(
            'CREATE TABLE "audit_share_field" (id INTEGER PRIMARY KEY AUTOINCREMENT)'
        )
    else:
        conn.execute_script(
            'CREATE TABLE "audit_share_field" (id BIGSERIAL PRIMARY KEY)'
        )

    state = _State()
    op = AddFieldOnline("X", "score", field)
    try:
        original_null_before = field.null
        op.database_forwards("tests", conn, state, state)
        # Critical: the shared ``field`` instance was NOT mutated.
        assert field.null == original_null_before
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "audit_share_field"{cascade}')


# ── #15 — purge_expired uses delete() return ─────────────────────────


def test_purge_expired_returns_authoritative_count():
    """``delete()`` returns the row count it actually removed; we
    surface that instead of a follow-up ``count()`` (which races)."""
    from datetime import datetime, timedelta, timezone

    from dorm.contrib.idempotency import IdempotencyRecord, purge_expired
    from dorm.db.connection import get_connection
    from dorm.migrations.operations import _field_to_column_sql

    class _Idp(IdempotencyRecord):
        class Meta:
            db_table = "_audit_idp_purge"
            app_label = "tests"

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "_audit_idp_purge"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _Idp._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "_audit_idp_purge" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    try:
        for i in range(3):
            _Idp.objects.create(key=f"k{i}", response={"i": i})
        # Move them all into the past so the cutoff catches them.
        long_ago = datetime.now(timezone.utc) - timedelta(days=10)
        _Idp.objects.update(created_at=long_ago)
        n = purge_expired(_Idp, older_than_seconds=86400)
        assert n == 3
    finally:
        conn.execute_script(f'DROP TABLE IF EXISTS "_audit_idp_purge"{cascade}')


# ── #16 — streaming docstring fixed (smoke check) ────────────────────


def test_streaming_module_doc_uses_async_helper_in_async_example():
    """The module docstring's async example references ``astream_jsonl``
    not ``stream_jsonl`` — pin it so a future rewrite doesn't
    regress to the misleading sync name in an async block."""
    import dorm.contrib.streaming as _s

    doc = _s.__doc__ or ""
    # Find the async example body. The fix replaces ``stream_jsonl``
    # with ``astream_jsonl`` inside the ``async def`` block.
    if "async def" in doc:
        # The block doesn't reference the sync name.
        async_block = doc.split("async def", 1)[1]
        assert "astream_jsonl" in async_block
        # And the sync name is NOT inside the async block.
        sync_in_async = "stream_jsonl" in async_block.replace(
            "astream_jsonl", ""
        )
        assert not sync_in_async


# ── #17 — idempotency doc warns about replay branching ───────────────


def test_idempotency_doc_emphasises_replay_branch():
    from dorm.contrib.idempotency import idempotency_key

    doc = idempotency_key.__doc__ or ""
    # The fix adds a prominent ``ctx.replay`` branch warning + example.
    assert "ctx.replay" in doc
    assert "short-circuit" in doc.lower() or "if ctx.replay" in doc


# ── #18 — listen_notify _quote_ident ─────────────────────────────────


def test_quote_ident_doubles_internal_double_quotes():
    """Channel names containing ``"`` are escaped via doubling."""
    from dorm.contrib.listen_notify import _quote_ident

    out = _quote_ident('foo"bar')
    assert out == '"foo""bar"'


def test_quote_ident_handles_plain_name():
    from dorm.contrib.listen_notify import _quote_ident

    assert _quote_ident("orders") == '"orders"'
